"""Serial, resumable full-market backfill of final 09:25 auction records."""

from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from scripts.data_pipeline.connectors.pytdx_client import (
    connect_first_available,
    create_hq_api,
    fetch_history_transaction_payload,
)
from scripts.data_pipeline.fetch_realtime_watchlist import infer_hq_market
from scripts.data_pipeline.jobs.historical_auction_job import fetch_final_auction


_SYMBOL = re.compile(r"^\d{6}\.(?:SH|SZ)$")
_MASTER_PARTITION = re.compile(r"^trade_date=(\d{8})$")
_TERMINAL_STATUSES = {"success", "no_auction_trade", "no_auction", "no_data"}
_COLUMNS = [
    "trade_date",
    "ts_code",
    "name",
    "exchange",
    "board",
    "industry",
    "is_suspended",
    "status",
    "auction_time",
    "auction_price",
    "auction_volume_lots",
    "auction_volume_shares",
    "auction_amount",
    "buyorsell",
    "buyorsell_label",
    "source",
    "pages_requested",
    "records_scanned",
    "error",
]


def _normalize_date(value: str | None) -> str | None:
    if value is None or not str(value).strip():
        return None
    return datetime.strptime(str(value).strip().replace("-", ""), "%Y%m%d").strftime("%Y%m%d")


def _path(value: str | Path) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (Path.cwd() / path).resolve()


@dataclass(frozen=True)
class HistoricalAuctionSyncConfig:
    data_root: Path
    start: str | None = None
    end: str | None = None
    date_order: str = "descending"
    page_size: int = 1800
    initial_offset: int = 1800
    retries: int = 3
    retry_backoff_seconds: float = 1.0
    request_delay_seconds: float = 0.05
    checkpoint_every: int = 50
    progress_every: int = 20
    maximum_consecutive_failures: int = 10
    maximum_days: int | None = None
    maximum_symbols: int | None = None
    retry_no_data: bool = False
    output_domain: str = "auction_history_daily"
    report: Path | None = None

    @classmethod
    def from_json(cls, path: Path) -> "HistoricalAuctionSyncConfig":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("historical auction config must be a JSON object")
        config = cls(
            data_root=_path(str(payload.get("data_root") or "data")),
            start=_normalize_date(payload.get("start")),
            end=_normalize_date(payload.get("end")),
            date_order=str(payload.get("date_order") or "descending"),
            page_size=int(payload.get("page_size", 1800)),
            initial_offset=int(payload.get("initial_offset", 1800)),
            retries=int(payload.get("retries", 3)),
            retry_backoff_seconds=float(payload.get("retry_backoff_seconds", 1.0)),
            request_delay_seconds=float(payload.get("request_delay_seconds", 0.05)),
            checkpoint_every=int(payload.get("checkpoint_every", 50)),
            progress_every=int(payload.get("progress_every", 20)),
            maximum_consecutive_failures=int(payload.get("maximum_consecutive_failures", 10)),
            maximum_days=(int(payload["maximum_days"]) if payload.get("maximum_days") else None),
            maximum_symbols=(
                int(payload["maximum_symbols"]) if payload.get("maximum_symbols") else None
            ),
            retry_no_data=bool(payload.get("retry_no_data", False)),
            output_domain=str(payload.get("output_domain") or "auction_history_daily"),
            report=_path(str(payload["report"])) if payload.get("report") else None,
        )
        config.validate()
        return config

    def validate(self) -> None:
        if self.date_order not in {"ascending", "descending"}:
            raise ValueError("date_order must be ascending or descending")
        if self.start and self.end and self.start > self.end:
            raise ValueError("start cannot be later than end")
        if self.page_size < 1 or self.initial_offset < 0:
            raise ValueError("page_size must be positive and initial_offset cannot be negative")
        if not 1 <= self.retries <= 10:
            raise ValueError("retries must be between 1 and 10")
        if self.retry_backoff_seconds < 0 or self.request_delay_seconds < 0:
            raise ValueError("retry and request delays cannot be negative")
        for name, value in (
            ("checkpoint_every", self.checkpoint_every),
            ("progress_every", self.progress_every),
            ("maximum_consecutive_failures", self.maximum_consecutive_failures),
        ):
            if value < 1:
                raise ValueError(f"{name} must be positive")
        if self.maximum_days is not None and self.maximum_days < 1:
            raise ValueError("maximum_days must be positive or null")
        if self.maximum_symbols is not None and self.maximum_symbols < 1:
            raise ValueError("maximum_symbols must be positive or null")
        if not re.fullmatch(r"[a-zA-Z0-9_-]+", self.output_domain):
            raise ValueError("output_domain contains unsupported characters")


class PytdxHistoricalAuctionProvider:
    """One serial TDX connection with reconnect-on-error and request pacing."""

    def __init__(
        self,
        config: HistoricalAuctionSyncConfig,
        *,
        api_factory: Callable[[], Any] | None = None,
    ) -> None:
        self.config = config
        self.api_factory = api_factory or (lambda: create_hq_api(raise_exception=True))
        self.api: Any | None = None
        self.last_request_at = 0.0

    def connect(self) -> None:
        self.close()
        api = self.api_factory()
        connect_first_available(api)
        self.api = api

    def close(self) -> None:
        if self.api is not None:
            try:
                self.api.disconnect()
            finally:
                self.api = None

    def __enter__(self) -> "PytdxHistoricalAuctionProvider":
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()

    def fetch_page(
        self,
        *,
        market: int,
        code: str,
        trade_date: str,
        start: int,
        count: int,
    ) -> list[dict]:
        last_error: Exception | None = None
        for attempt in range(1, self.config.retries + 1):
            try:
                if self.api is None:
                    self.connect()
                elapsed = time.monotonic() - self.last_request_at
                remaining = self.config.request_delay_seconds - elapsed
                if remaining > 0:
                    time.sleep(remaining)
                rows = fetch_history_transaction_payload(
                    self.api,
                    market=market,
                    code=code,
                    start=start,
                    count=count,
                    date=int(trade_date),
                )
                self.last_request_at = time.monotonic()
                return rows
            except Exception as exc:  # noqa: BLE001 - retry the network boundary
                last_error = exc
                self.close()
                if attempt < self.config.retries and self.config.retry_backoff_seconds:
                    time.sleep(self.config.retry_backoff_seconds * attempt)
        raise ConnectionError(
            f"TDX historical auction request failed for {code} on {trade_date}"
        ) from last_error


def _master_paths(config: HistoricalAuctionSyncConfig) -> list[tuple[str, Path]]:
    root = config.data_root / "security_master_daily"
    available: list[tuple[str, Path]] = []
    for path in root.glob("trade_date=*/data.parquet"):
        match = _MASTER_PARTITION.fullmatch(path.parent.name)
        if match:
            available.append((match.group(1), path))
    available.sort()
    if not available:
        raise FileNotFoundError(
            f"security-master is missing under {root}; sync historical security master first"
        )

    first, last = available[0][0], available[-1][0]
    if config.start and config.start < first:
        raise RuntimeError(
            f"requested start {config.start} predates security-master coverage {first}; "
            "extend security_master_daily first to avoid survivorship bias"
        )
    if config.end and config.end > last:
        raise RuntimeError(
            f"requested end {config.end} exceeds security-master coverage {last}; "
            "sync security_master_daily to the latest trading day first"
        )
    start, end = config.start or first, config.end or last
    selected = [(day, path) for day, path in available if start <= day <= end]
    if not selected:
        raise RuntimeError(f"no security-master partitions in requested range {start}..{end}")
    if config.date_order == "descending":
        selected.reverse()
    if config.maximum_days:
        selected = selected[: config.maximum_days]
    return selected


def _load_universe(path: Path, maximum_symbols: int | None) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    required = {"symbol", "name", "exchange", "board", "industry", "is_listed"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"security-master is missing columns {sorted(missing)}: {path}")
    frame = frame[frame["symbol"].astype(str).str.fullmatch(_SYMBOL)]
    frame = frame[frame["is_listed"].fillna(False).astype(bool)]
    frame = frame.drop_duplicates("symbol", keep="last").sort_values("symbol")
    if maximum_symbols:
        frame = frame.head(maximum_symbols)
    if frame.empty:
        raise RuntimeError(f"historical auction universe is empty: {path}")
    if "is_suspended" not in frame.columns:
        frame["is_suspended"] = False
    return frame


def _output_path(config: HistoricalAuctionSyncConfig, trade_date: str) -> Path:
    return config.data_root / config.output_domain / f"trade_date={trade_date}" / "data.parquet"


def _checkpoint_path(config: HistoricalAuctionSyncConfig, trade_date: str) -> Path:
    return config.data_root / ".auction-history-checkpoints" / f"trade_date={trade_date}.parquet"


def _atomic_write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    frame.reindex(columns=_COLUMNS).to_parquet(temporary, index=False)
    temporary.replace(path)


def _buyorsell_label(value: Any) -> str | None:
    try:
        return {0: "buy", 1: "sell", 2: "neutral"}.get(int(value), "other")
    except (TypeError, ValueError):
        return None


def _result_row(
    master: Any,
    trade_date: str,
    result: Any | None,
    *,
    error: str | None = None,
) -> dict[str, Any]:
    record = result.record if result is not None and result.record is not None else {}
    price = pd.to_numeric(record.get("price"), errors="coerce")
    volume_lots = pd.to_numeric(record.get("vol"), errors="coerce")
    price_value = float(price) if pd.notna(price) else None
    lots_value = float(volume_lots) if pd.notna(volume_lots) else None
    shares = lots_value * 100.0 if lots_value is not None else None
    return {
        "trade_date": trade_date,
        "ts_code": str(master.symbol),
        "name": str(master.name),
        "exchange": str(master.exchange),
        "board": str(master.board),
        "industry": str(master.industry),
        "is_suspended": bool(master.is_suspended),
        "status": result.status if result is not None else "failed",
        "auction_time": str(record.get("time")) if record.get("time") is not None else None,
        "auction_price": price_value,
        "auction_volume_lots": lots_value,
        "auction_volume_shares": shares,
        "auction_amount": price_value * shares if price_value is not None and shares is not None else None,
        "buyorsell": int(record["buyorsell"]) if record.get("buyorsell") is not None else None,
        "buyorsell_label": _buyorsell_label(record.get("buyorsell")),
        "source": "tdx_history_transaction",
        "pages_requested": int(result.pages_requested) if result is not None else 0,
        "records_scanned": int(result.records_scanned) if result is not None else 0,
        "error": error,
    }


def _read_rows(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    frame = pd.read_parquet(path)
    missing = set(_COLUMNS).difference(frame.columns)
    if missing:
        raise ValueError(f"checkpoint is missing columns {sorted(missing)}: {path}")
    return {
        str(row["ts_code"]): dict(row)
        for row in frame.drop_duplicates("ts_code", keep="last").to_dict("records")
    }


def _frame(rows: dict[str, dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame([rows[symbol] for symbol in sorted(rows)]).reindex(columns=_COLUMNS)


def _existing_complete(path: Path, expected: set[str]) -> bool:
    if not path.exists():
        return False
    frame = pd.read_parquet(path, columns=["ts_code", "status"])
    symbols = set(frame["ts_code"].astype(str))
    statuses = set(frame["status"].astype(str))
    if len(frame) != len(expected) or symbols != expected or not statuses <= _TERMINAL_STATUSES:
        raise RuntimeError(f"existing auction partition is incomplete or inconsistent: {path}")
    return True


def sync_day(
    config: HistoricalAuctionSyncConfig,
    trade_date: str,
    master_path: Path,
    provider: PytdxHistoricalAuctionProvider,
) -> dict[str, Any]:
    universe = _load_universe(master_path, config.maximum_symbols)
    expected = set(universe["symbol"].astype(str))
    final_path = _output_path(config, trade_date)
    if _existing_complete(final_path, expected):
        return {
            "trade_date": trade_date,
            "status": "skipped",
            "symbols": len(expected),
            "path": str(final_path),
        }

    checkpoint = _checkpoint_path(config, trade_date)
    rows = _read_rows(checkpoint)
    rows = {symbol: row for symbol, row in rows.items() if symbol in expected}
    if config.retry_no_data:
        rows = {symbol: row for symbol, row in rows.items() if row.get("status") != "no_data"}
    completed = {
        symbol for symbol, row in rows.items() if str(row.get("status")) in _TERMINAL_STATUSES
    }
    pending = [row for row in universe.itertuples(index=False) if str(row.symbol) not in completed]
    started = time.monotonic()
    dirty = False
    consecutive_failures = 0

    print(
        f"auction-history date={trade_date} universe={len(expected)} resume={len(completed)} pending={len(pending)}",
        flush=True,
    )
    try:
        for index, master in enumerate(pending, start=1):
            symbol = str(master.symbol)
            code = symbol[:6]
            try:
                result = fetch_final_auction(
                    lambda start, count, code=code: provider.fetch_page(
                        market=infer_hq_market(code),
                        code=code,
                        trade_date=trade_date,
                        start=start,
                        count=count,
                    ),
                    page_size=config.page_size,
                    initial_offset=config.initial_offset,
                )
                rows[symbol] = _result_row(master, trade_date, result)
                consecutive_failures = 0
            except Exception as exc:  # noqa: BLE001 - persist and resume per symbol
                rows[symbol] = _result_row(
                    master,
                    trade_date,
                    None,
                    error=f"{type(exc).__name__}: {exc}",
                )
                consecutive_failures += 1
            dirty = True

            done = len(completed) + index
            if index % config.checkpoint_every == 0:
                _atomic_write_parquet(checkpoint, _frame(rows))
                dirty = False
            if index % config.progress_every == 0 or index == len(pending):
                elapsed = max(time.monotonic() - started, 0.001)
                rate = index / elapsed
                remaining = (len(pending) - index) / rate if rate else 0.0
                print(
                    f"[{done}/{len(expected)}] {trade_date} {symbol} "
                    f"status={rows[symbol]['status']} rate={rate:.2f}/s eta={remaining / 60:.1f}m",
                    flush=True,
                )
            if consecutive_failures >= config.maximum_consecutive_failures:
                raise RuntimeError(
                    f"aborting after {consecutive_failures} consecutive symbol failures; "
                    f"checkpoint preserved at {checkpoint}"
                )
    finally:
        if dirty:
            _atomic_write_parquet(checkpoint, _frame(rows))

    result_frame = _frame(rows)
    failed = result_frame[result_frame["status"] == "failed"]
    actual = set(result_frame["ts_code"].astype(str))
    if actual != expected or not failed.empty:
        return {
            "trade_date": trade_date,
            "status": "incomplete",
            "symbols": len(expected),
            "completed": len(actual),
            "failed": len(failed),
            "checkpoint": str(checkpoint),
        }

    _atomic_write_parquet(final_path, result_frame)
    checkpoint.unlink(missing_ok=True)
    counts = {str(key): int(value) for key, value in result_frame["status"].value_counts().items()}
    return {
        "trade_date": trade_date,
        "status": "completed",
        "symbols": len(expected),
        "status_counts": counts,
        "path": str(final_path),
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def status(config: HistoricalAuctionSyncConfig) -> dict[str, Any]:
    partitions = sorted((config.data_root / config.output_domain).glob("trade_date=*/data.parquet"))
    checkpoints = sorted((config.data_root / ".auction-history-checkpoints").glob("trade_date=*.parquet"))
    rows = 0
    status_counts: dict[str, int] = {}
    for path in partitions:
        frame = pd.read_parquet(path, columns=["status"])
        rows += len(frame)
        for key, value in frame["status"].value_counts().items():
            status_counts[str(key)] = status_counts.get(str(key), 0) + int(value)
    return {
        "output_domain": config.output_domain,
        "completed_trade_dates": len(partitions),
        "first_trade_date": partitions[0].parent.name.removeprefix("trade_date=") if partitions else None,
        "last_trade_date": partitions[-1].parent.name.removeprefix("trade_date=") if partitions else None,
        "rows": rows,
        "status_counts": status_counts,
        "incomplete_checkpoints": [path.stem.removeprefix("trade_date=") for path in checkpoints],
    }


def run(config: HistoricalAuctionSyncConfig) -> dict[str, Any]:
    master_paths = _master_paths(config)
    report: dict[str, Any] = {
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "source": "tdx_history_transaction",
        "connection_mode": "single_serial",
        "requested_start": config.start,
        "requested_end": config.end,
        "selected_trade_dates": len(master_paths),
        "days": [],
    }
    with PytdxHistoricalAuctionProvider(config) as provider:
        for index, (trade_date, master_path) in enumerate(master_paths, start=1):
            print(f"day [{index}/{len(master_paths)}] {trade_date}", flush=True)
            report["days"].append(sync_day(config, trade_date, master_path, provider))
    report["finished_at"] = datetime.now().isoformat(timespec="seconds")
    report["summary"] = status(config)
    if config.report:
        _write_json(config.report, report)
    incomplete = [item for item in report["days"] if item["status"] == "incomplete"]
    if incomplete:
        raise RuntimeError(
            f"historical auction sync left {len(incomplete)} incomplete trading days; rerun to resume"
        )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="同步全市场历史9:25最终集合竞价数据")
    parser.add_argument("--config", default="configs/auction-history-sync.json")
    parser.add_argument("--start", help="覆盖配置起始日期，YYYY-MM-DD")
    parser.add_argument("--end", help="覆盖配置结束日期，YYYY-MM-DD")
    parser.add_argument("--max-days", type=int, help="仅处理指定数量交易日，用于分批运行")
    parser.add_argument("--max-symbols", type=int, help="仅处理指定数量股票，用于连通性测试")
    parser.add_argument("--status", action="store_true", help="只查看本地历史竞价库状态")
    args = parser.parse_args()

    config = HistoricalAuctionSyncConfig.from_json(_path(args.config))
    overrides: dict[str, Any] = {}
    if args.start:
        overrides["start"] = _normalize_date(args.start)
    if args.end:
        overrides["end"] = _normalize_date(args.end)
    if args.max_days:
        overrides["maximum_days"] = args.max_days
    if args.max_symbols:
        overrides["maximum_symbols"] = args.max_symbols
        overrides["output_domain"] = f"{config.output_domain}_smoke"
        overrides["report"] = config.data_root / "auction-history-smoke-report.json"
    if overrides:
        config = replace(config, **overrides)
        config.validate()

    result = status(config) if args.status else run(config)
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
