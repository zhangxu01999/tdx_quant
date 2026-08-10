"""9:25集合竞价全市场采集、特征计算和日频候选影子复核入口。

正常用法是在交易日上午提前启动。程序默认等待到9:25:05，然后复用一条
通达信长连接串行分批读取全市场，不会为每只股票单独建连，也不会并发轰炸
行情节点。默认只落库和生成影子建议；显式启用 ``java_broker`` 后，只向
Pig PAPER 账户发送模拟信号，任何模式下都不会向真实券商报单。
"""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime, time as time_value
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import pandas as pd

from scripts.data_pipeline.intraday.auction import (
    AuctionScoringConfig,
    build_auction_features,
    build_auction_report,
)
from scripts.data_pipeline.intraday.java_broker import JavaBrokerConfig, JavaPaperBrokerClient
from scripts.data_pipeline.intraday.provider import PytdxWatchlistProvider
from scripts.data_pipeline.intraday.service import load_daily_baselines
from scripts.data_pipeline.intraday.store import IntradayDuckDBStore
from scripts.data_pipeline.intraday.watchlist import load_watchlist


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
_SYMBOL = re.compile(r"^\d{6}\.(?:SH|SZ)$")
_SERVER_TIME = re.compile(r"^(\d{1,2}):(\d{2})(?::(\d{2}(?:\.\d+)?))?$")


def _path(value: str | Path) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (Path.cwd() / path).resolve()


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a JSON object")
    return dict(value)


def _clock_time(value: Any, name: str) -> time_value:
    try:
        return time_value.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(f"{name} must use HH:MM or HH:MM:SS") from exc


def _load_config(path: str | Path) -> dict[str, Any]:
    config_path = _path(path)
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("auction config must contain a JSON object")
    payload["_config_path"] = config_path
    return payload


def _latest_master(data_root: Path) -> tuple[Path, pd.DataFrame]:
    root = data_root / "security_master_daily"
    paths = sorted(root.glob("trade_date=*/data.parquet"))
    if not paths:
        raise FileNotFoundError(
            f"security-master is missing under {root}; run historical security-master sync first"
        )
    path = paths[-1]
    frame = pd.read_parquet(path)
    required = {"symbol", "name", "board", "industry"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(
            f"security-master is missing columns {sorted(missing)}: {path}"
        )
    return path, frame


def _load_universe(
    data_root: Path,
    payload: Mapping[str, Any],
) -> tuple[list[str], dict[str, dict[str, Any]], Path]:
    """从本地最新证券主表读取全市场代码和行业，不临时联网补名单。"""

    universe = _mapping(payload.get("universe"), "universe")
    mode = str(universe.get("mode") or "all-a-shares")
    if mode not in {"all-a-shares", "configured"}:
        raise ValueError("universe.mode must be 'all-a-shares' or 'configured'")
    path, frame = _latest_master(data_root)
    frame = frame[frame["symbol"].astype(str).str.fullmatch(_SYMBOL)]
    if mode == "all-a-shares":
        # 全市场横截面应包含当日仍上市但可能停牌/ST的证券；真正没有实时报价
        # 的股票会自然缺失，而已有持仓不会仅因昨日不可选就从复核池消失。
        if "is_listed" in frame.columns:
            frame = frame[frame["is_listed"].fillna(False).astype(bool)]
    configured = {
        str(value).strip().upper()
        for value in universe.get("symbols") or []
        if _SYMBOL.fullmatch(str(value).strip().upper())
    }
    if mode == "configured":
        if not configured:
            raise ValueError("universe.symbols cannot be empty in configured mode")
        frame = frame[frame["symbol"].isin(configured)]
    allowed_boards = {str(value) for value in universe.get("allowed_boards") or []}
    if allowed_boards:
        frame = frame[frame["board"].isin(allowed_boards)]
    frame = frame.drop_duplicates("symbol", keep="last").sort_values("symbol")
    maximum = universe.get("maximum_symbols")
    if maximum is not None:
        maximum = int(maximum)
        if maximum < 1:
            raise ValueError("universe.maximum_symbols must be positive or null")
        frame = frame.head(maximum)
    metadata = {
        str(row.symbol): {
            "name": row.name,
            "board": row.board,
            "industry": row.industry,
        }
        for row in frame.itertuples(index=False)
    }
    if not metadata:
        raise ValueError("auction universe is empty")
    return list(metadata), metadata, path


def _load_candidates(payload: Mapping[str, Any], now: datetime):
    watchlist = _mapping(payload.get("watchlist"), "watchlist")
    manifest = watchlist.get("manifest")
    if not manifest:
        raise ValueError("watchlist.manifest is required")
    maximum_age = watchlist.get("maximum_age_days", 5)
    return load_watchlist(
        manifest=_path(str(manifest)),
        manual_symbols=watchlist.get("manual_symbols") or [],
        sections=watchlist.get("sections") or ["positions", "target", "observation"],
        maximum_symbols=int(watchlist.get("maximum_symbols", 200)),
        now=now,
        maximum_age_days=int(maximum_age) if maximum_age is not None else None,
    )


def _wait_for_capture_window(payload: Mapping[str, Any], *, ignore_session: bool) -> datetime:
    schedule = _mapping(payload.get("schedule"), "schedule")
    now = datetime.now(SHANGHAI_TZ).replace(tzinfo=None)
    if ignore_session:
        return now
    if now.weekday() >= 5:
        raise RuntimeError("auction capture is only allowed on a weekday unless --ignore-session is used")
    target = datetime.combine(
        now.date(),
        _clock_time(schedule.get("capture_time", "09:25:05"), "schedule.capture_time"),
    )
    latest = datetime.combine(
        now.date(),
        _clock_time(schedule.get("latest_start_time", "09:29:20"), "schedule.latest_start_time"),
    )
    wait = bool(schedule.get("wait_until_capture_time", True))
    if now < target:
        if not wait:
            raise RuntimeError(f"auction capture is early: now={now.time()} target={target.time()}")
        seconds = (target - now).total_seconds()
        print(f"waiting for final auction: target={target.isoformat()} seconds={seconds:.1f}", flush=True)
        while seconds > 0:
            time.sleep(min(seconds, 30.0))
            now = datetime.now(SHANGHAI_TZ).replace(tzinfo=None)
            seconds = (target - now).total_seconds()
    now = datetime.now(SHANGHAI_TZ).replace(tzinfo=None)
    if now > latest:
        raise RuntimeError(
            f"auction capture window has closed: now={now.time()} latest={latest.time()}; "
            "do not label a later continuous-auction quote as the 09:25 snapshot"
        )
    return now


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    temporary.replace(path)


def _server_time(snapshot) -> time_value | None:
    """读取pytdx服务器时间，用于拒绝休市日遗留的上一交易日快照。"""

    try:
        raw = json.loads(snapshot.raw_json)
    except (TypeError, json.JSONDecodeError):
        return None
    match = _SERVER_TIME.fullmatch(str(raw.get("servertime") or "").strip())
    if match is None:
        return None
    seconds = float(match.group(3) or 0.0)
    whole_seconds = int(seconds)
    microseconds = int(round((seconds - whole_seconds) * 1_000_000))
    try:
        return time_value(
            int(match.group(1)),
            int(match.group(2)),
            whole_seconds,
            microseconds,
        )
    except ValueError:
        return None


def run_capture(
    payload: Mapping[str, Any],
    *,
    ignore_session: bool = False,
    provider_factory=PytdxWatchlistProvider,
) -> dict[str, Any]:
    """运行一次最终竞价采集；依赖注入入口供离线测试使用。"""

    # 全市场近5日基线在当前数据规模下需要十几秒，应在9:25之前准备完，
    # 不能等最终竞价形成后才开始读取本地Parquet。
    preparation_started_at = datetime.now(SHANGHAI_TZ).replace(tzinfo=None)
    data_root = _path(str(payload.get("data_root") or "data"))
    database = _path(str(payload.get("database") or "data/intraday-paper.duckdb"))
    output = _path(str(payload.get("output") or "data/auction-review-latest.json"))
    symbols, metadata, master_path = _load_universe(data_root, payload)
    candidates = _load_candidates(payload, preparation_started_at)
    missing_candidates = sorted({item.symbol for item in candidates.items}.difference(metadata))
    if missing_candidates:
        raise RuntimeError(
            "daily watchlist contains symbols outside the configured auction universe: "
            + ", ".join(missing_candidates[:20])
        )

    baseline_days = int(payload.get("baseline_days", 5))
    if baseline_days < 1:
        raise ValueError("baseline_days must be positive")
    baselines = load_daily_baselines(
        data_root,
        symbols,
        average_days=baseline_days,
        as_of=candidates.source_as_of,
    )
    started_at = _wait_for_capture_window(payload, ignore_session=ignore_session)
    provider_payload = _mapping(payload.get("quote_provider"), "quote_provider")
    provider = provider_factory(
        batch_size=int(provider_payload.get("batch_size", 80)),
        retries=int(provider_payload.get("retries", 3)),
        retry_backoff_seconds=float(provider_payload.get("retry_backoff_seconds", 1.0)),
    )
    progress_every = int(provider_payload.get("progress_every_batches", 10))
    minimum_coverage = float(provider_payload.get("minimum_coverage", 0.85))
    minimum_server_time_coverage = float(
        provider_payload.get("minimum_server_time_coverage", 0.80)
    )
    if not 0 < minimum_coverage <= 1:
        raise ValueError("quote_provider.minimum_coverage must be in (0, 1]")
    if not 0 <= minimum_server_time_coverage <= 1:
        raise ValueError("quote_provider.minimum_server_time_coverage must be in [0, 1]")
    if progress_every < 0:
        raise ValueError("quote_provider.progress_every_batches cannot be negative")
    schedule = _mapping(payload.get("schedule"), "schedule")
    capture_deadline = datetime.combine(
        started_at.date(),
        _clock_time(
            schedule.get("latest_capture_time", "09:29:50"),
            "schedule.latest_capture_time",
        ),
    )
    snapshots = []
    capture_first: datetime | None = None
    capture_last: datetime | None = None
    with IntradayDuckDBStore(database) as store, provider:
        for batch_number, batch in enumerate(provider.iter_quote_batches(symbols), start=1):
            if batch:
                batch_received_at = max(value.received_at for value in batch)
                if not ignore_session and batch_received_at > capture_deadline:
                    raise RuntimeError(
                        "full-market auction capture crossed into the continuous-auction window: "
                        f"received_at={batch_received_at.isoformat()} deadline={capture_deadline.isoformat()}"
                    )
                store.record_auction_snapshots(batch, stage="final")
                snapshots.extend(batch)
                capture_first = capture_first or min(value.received_at for value in batch)
                capture_last = max(value.received_at for value in batch)
            if progress_every > 0 and batch_number % progress_every == 0:
                print(
                    f"auction progress batches={batch_number} snapshots={len(snapshots)}/{len(symbols)}",
                    flush=True,
                )
        coverage = len({snapshot.symbol for snapshot in snapshots}) / len(symbols)
        if coverage < minimum_coverage:
            raise RuntimeError(
                f"auction snapshot coverage is too low: {coverage:.2%} < {minimum_coverage:.2%}"
            )
        server_time_earliest = _clock_time(
            schedule.get("server_time_earliest", "09:24:30"),
            "schedule.server_time_earliest",
        )
        server_time_latest = _clock_time(
            schedule.get("server_time_latest", "09:29:59"),
            "schedule.server_time_latest",
        )
        valid_server_times = [
            value
            for value in (_server_time(snapshot) for snapshot in snapshots)
            if value is not None and server_time_earliest <= value <= server_time_latest
        ]
        server_time_coverage = len(valid_server_times) / len(snapshots) if snapshots else 0.0
        if not ignore_session and server_time_coverage < minimum_server_time_coverage:
            raise RuntimeError(
                "TDX server-time coverage does not look like the 09:25 auction window: "
                f"{server_time_coverage:.2%} < {minimum_server_time_coverage:.2%}; "
                "the market may be closed or the quote node may be stale"
            )
        calculated_at = datetime.now(SHANGHAI_TZ).replace(tzinfo=None)
        scoring = AuctionScoringConfig.from_mapping(
            _mapping(payload.get("scoring"), "scoring")
        )
        features = build_auction_features(
            snapshots,
            baselines=baselines,
            security_metadata=metadata,
            candidates=candidates.items,
            calculated_at=calculated_at,
            config=scoring,
        )
        store.record_auction_features(features)
        table_counts = store.table_counts()

    report = build_auction_report(features)
    java_broker = JavaPaperBrokerClient(
        JavaBrokerConfig.from_mapping(_mapping(payload.get("java_broker"), "java_broker"))
    )
    remote_acceptances = java_broker.publish_auction_features(
        features,
        calculated_at=calculated_at,
    )
    report.update(
        {
            "status": "success",
            "source": "tdx_realtime_quote",
            "security_master": str(master_path),
            "watchlist_source": str(candidates.source_path),
            "watchlist_as_of": candidates.source_as_of.isoformat()
            if candidates.source_as_of
            else None,
            "capture": {
                "preparation_started_at": preparation_started_at.isoformat(),
                "started_at": started_at.isoformat(),
                "first_received_at": capture_first.isoformat() if capture_first else None,
                "last_received_at": capture_last.isoformat() if capture_last else None,
                "requested_symbols": len(symbols),
                "received_symbols": len({snapshot.symbol for snapshot in snapshots}),
                "coverage": coverage,
                "valid_server_time_symbols": len(valid_server_times),
                "server_time_coverage": server_time_coverage,
                "single_connection": True,
            },
            "database": str(database),
            "table_counts": table_counts,
            "java_broker": {
                "enabled": java_broker.enabled,
                "published": len(remote_acceptances),
                "acceptances": remote_acceptances,
            },
            "output": str(output),
        }
    )
    _write_json(output, report)
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str), flush=True)
    return report


def _status(payload: Mapping[str, Any]) -> dict[str, Any]:
    database = _path(str(payload.get("database") or "data/intraday-paper.duckdb"))
    with IntradayDuckDBStore(database) as store:
        recent = store.connection.execute(
            """
            SELECT trade_date, count(*) AS symbols,
                   min(captured_at) AS first_received_at,
                   max(captured_at) AS last_received_at
            FROM auction_snapshots
            GROUP BY trade_date
            ORDER BY trade_date DESC
            LIMIT 10
            """
        ).fetchall()
        return {
            "database": str(database),
            "table_counts": store.table_counts(),
            "recent_auction_days": [
                {
                    "trade_date": row[0],
                    "symbols": row[1],
                    "first_received_at": row[2],
                    "last_received_at": row[3],
                }
                for row in recent
            ],
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Capture the 09:25 TDX call-auction snapshot and build shadow reviews"
    )
    parser.add_argument("--config", default="configs/auction-open.json")
    parser.add_argument(
        "--ignore-session",
        action="store_true",
        help="run immediately for connectivity testing; output is still labeled with actual capture time",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="inspect local auction tables without a network request",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = _load_config(args.config)
    if args.status:
        print(json.dumps(_status(payload), ensure_ascii=False, indent=2, default=str))
        return 0
    run_capture(payload, ignore_session=args.ignore_session)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
