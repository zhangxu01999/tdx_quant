"""盘中观察池轮询和模拟交易命令行入口。

正常启动后进程会在交易时段持续运行，午间停止请求，15:05 读取最新日 K
完成收盘对账后退出。``--once`` 只采样一轮，适合检查 pytdx 连通性。
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from scripts.data_pipeline.intraday.paper import PaperBroker, PaperBrokerConfig
from scripts.data_pipeline.intraday.provider import PytdxWatchlistProvider
from scripts.data_pipeline.intraday.service import (
    IntradayPaperService,
    IntradayServiceConfig,
    load_daily_baselines,
)
from scripts.data_pipeline.intraday.signals import IntradaySignalConfig, IntradaySignalEngine
from scripts.data_pipeline.intraday.store import IntradayDuckDBStore
from scripts.data_pipeline.intraday.watchlist import load_watchlist


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a JSON object")
    return dict(value)


def _path(value: str | Path) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (Path.cwd() / path).resolve()


def load_config(path: str | Path) -> dict[str, Any]:
    config_path = _path(path)
    value = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("intraday paper config must contain a JSON object")
    value["_config_path"] = config_path
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Poll a research watchlist through pytdx and run local paper trading"
    )
    parser.add_argument("--config", default="configs/intraday-paper.json")
    parser.add_argument(
        "--once",
        action="store_true",
        help="fetch and process one quote batch, then exit",
    )
    parser.add_argument(
        "--ignore-session",
        action="store_true",
        help="allow --once outside continuous trading time for connectivity testing",
    )
    parser.add_argument(
        "--reconcile-only",
        action="store_true",
        help="skip polling and reconcile one day with the latest pytdx daily bars",
    )
    parser.add_argument("--trade-date", help="reconciliation date, YYYY-MM-DD")
    parser.add_argument(
        "--status",
        action="store_true",
        help="show DuckDB table counts and paper account without network requests",
    )
    return parser


def _build_runtime(payload: Mapping[str, Any]):
    data_root = _path(str(payload.get("data_root") or "data"))
    database = _path(str(payload.get("database") or "data/intraday-paper.duckdb"))
    watch_payload = _mapping(payload.get("watchlist"), "watchlist")
    manifest = watch_payload.get("manifest")
    if "maximum_age_days" in watch_payload:
        maximum_age_days = (
            int(watch_payload["maximum_age_days"])
            if watch_payload["maximum_age_days"] is not None
            else None
        )
    else:
        maximum_age_days = 5
    loaded = load_watchlist(
        manifest=_path(str(manifest)) if manifest else None,
        manual_symbols=watch_payload.get("manual_symbols") or [],
        sections=watch_payload.get("sections") or ["observation", "target", "positions"],
        maximum_symbols=int(watch_payload.get("maximum_symbols", 200)),
        now=datetime.now(SHANGHAI_TZ).replace(tzinfo=None),
        maximum_age_days=maximum_age_days,
    )
    signal_config = IntradaySignalConfig.from_mapping(
        _mapping(payload.get("signals"), "signals")
    )
    broker_config = PaperBrokerConfig.from_mapping(
        _mapping(payload.get("paper_broker"), "paper_broker")
    )
    service_values = _mapping(payload.get("service"), "service")
    service_config = IntradayServiceConfig(
        poll_seconds=float(service_values.get("poll_seconds", 5.0)),
        reconcile_time=str(service_values.get("reconcile_time", "15:05")),
        account_snapshot_every_polls=int(
            service_values.get("account_snapshot_every_polls", 12)
        ),
        maximum_consecutive_failures=int(
            service_values.get("maximum_consecutive_failures", 5)
        ),
    )
    quote_values = _mapping(payload.get("quote_provider"), "quote_provider")
    provider = PytdxWatchlistProvider(
        batch_size=int(quote_values.get("batch_size", 80)),
        retries=int(quote_values.get("retries", 3)),
        retry_backoff_seconds=float(quote_values.get("retry_backoff_seconds", 1.0)),
    )
    baselines = load_daily_baselines(
        data_root,
        [item.symbol for item in loaded.items],
        average_days=signal_config.average_volume_days,
        as_of=loaded.source_as_of,
    )
    return database, loaded, signal_config, broker_config, service_config, provider, baselines


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = load_config(args.config)
    database = _path(str(payload.get("database") or "data/intraday-paper.duckdb"))
    if args.status:
        with IntradayDuckDBStore(database) as store:
            result: dict[str, Any] = {"database": str(database), "tables": store.table_counts()}
            account_id = str(
                _mapping(payload.get("paper_broker"), "paper_broker").get(
                    "account_id", "intraday-paper"
                )
            )
            try:
                result["account"] = store.account(account_id)
                result["positions"] = list(store.positions(account_id).values())
            except KeyError:
                result["account"] = None
                result["positions"] = []
            result["recent_orders"] = store.recent_orders(account_id)
            result["recent_signals"] = store.recent_signals()
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return 0

    (
        database,
        loaded,
        signal_config,
        broker_config,
        service_config,
        provider,
        baselines,
    ) = _build_runtime(payload)
    print(
        f"watchlist loaded: symbols={len(loaded.items)} "
        f"as_of={loaded.source_as_of} source={loaded.source_path}",
        flush=True,
    )
    with IntradayDuckDBStore(database) as store, provider:
        now = datetime.now(SHANGHAI_TZ).replace(tzinfo=None)
        store.record_watchlist(
            loaded.items,
            loaded_at=now,
            source_as_of=loaded.source_as_of,
            source_path=str(loaded.source_path) if loaded.source_path else None,
        )
        broker = PaperBroker(store, broker_config)
        broker.initialize(now)
        service = IntradayPaperService(
            store=store,
            provider=provider,
            watchlist=loaded.items,
            baselines=baselines,
            signal_engine=IntradaySignalEngine(signal_config),
            broker=broker,
            config=service_config,
        )
        if args.reconcile_only:
            trade_date = date.fromisoformat(args.trade_date) if args.trade_date else now.date()
            result = service.reconcile(trade_date)
        else:
            result = service.run(once=args.once, ignore_session=args.ignore_session)
        result["database"] = str(database)
        result["table_counts"] = store.table_counts()
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
