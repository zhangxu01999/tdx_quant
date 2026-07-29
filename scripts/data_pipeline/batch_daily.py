"""按配置批量下载并增量更新 A 股日线 Parquet。"""

from __future__ import annotations

import argparse
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from typing import Callable

import pandas as pd

from scripts.data_pipeline.code_mapping import market_code_to_ts_code
from scripts.data_pipeline.fetch_realtime_watchlist import infer_hq_market
from scripts.data_pipeline.tdx_client import TdxDownloader


_BARE_CODE = re.compile(r"^\d{6}$")
_A_SHARE_CODE = re.compile(r"^(000|001|002|003|300|301|600|601|603|605|688|689)\d{3}$")
_CHECKPOINT_SCHEMA_VERSION = 1
_CHECKPOINT_FLUSH_EVERY = 50


@dataclass(frozen=True)
class DailySyncConfig:
    """批量同步参数；默认适合短线策略所需的近三年左右日线。"""

    data_root: Path
    symbols: tuple[str, ...]
    universe: str = "configured"
    history_bars: int = 800
    refresh_bars: int = 30
    workers: int = 4
    retries: int = 2
    progress_every: int = 20
    max_symbols: int | None = None
    indices: tuple[tuple[str, int], ...] = ()
    report: Path | None = None

    @classmethod
    def from_json(cls, path: Path) -> "DailySyncConfig":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("daily sync config must be a JSON object")
        indices = tuple(
            (str(item["code"]), int(item["market"]))
            for item in payload.get("indices", [])
        )
        config = cls(
            data_root=Path(str(payload.get("data_root", "data"))),
            symbols=tuple(str(value) for value in payload.get("symbols", [])),
            universe=str(payload.get("universe", "configured")),
            history_bars=int(payload.get("history_bars", 800)),
            refresh_bars=int(payload.get("refresh_bars", 30)),
            workers=int(payload.get("workers", 4)),
            retries=int(payload.get("retries", 2)),
            progress_every=int(payload.get("progress_every", 20)),
            max_symbols=(int(payload["max_symbols"]) if payload.get("max_symbols") else None),
            indices=indices,
            report=Path(str(payload["report"])) if payload.get("report") else None,
        )
        config.validate()
        return config

    def validate(self) -> None:
        if self.universe not in {"configured", "all-a-shares"}:
            raise ValueError("universe must be 'configured' or 'all-a-shares'")
        if self.history_bars <= 0 or self.refresh_bars <= 0:
            raise ValueError("history_bars and refresh_bars must be positive")
        if not 1 <= self.workers <= 16:
            raise ValueError("workers must be between 1 and 16")
        if not 1 <= self.retries <= 10:
            raise ValueError("retries must be between 1 and 10")
        if not 1 <= self.progress_every <= 1000:
            raise ValueError("progress_every must be between 1 and 1000")
        if self.max_symbols is not None and self.max_symbols <= 0:
            raise ValueError("max_symbols must be positive")
        for code, market in self.indices:
            if not _BARE_CODE.fullmatch(code) or market not in {0, 1}:
                raise ValueError(f"invalid index configuration: code={code!r}, market={market!r}")


def _bare_code(value: str) -> str:
    code = value.strip().upper().split(".", 1)[0]
    if not _BARE_CODE.fullmatch(code):
        raise ValueError(f"invalid stock code: {value!r}")
    return code


def _all_a_share_codes(data_root: Path) -> list[str]:
    """从最新证券列表快照中提取沪深 A 股代码。"""

    source = data_root / "security_list"
    if not source.exists() or next(source.rglob("*.parquet"), None) is None:
        raise FileNotFoundError(
            f"security list is missing under {source}; download SH/SZ security lists first"
        )
    frame = pd.read_parquet(source)
    if "code" not in frame.columns:
        raise ValueError(f"security list does not contain a code column: {source}")
    return sorted({str(code) for code in frame["code"] if _A_SHARE_CODE.fullmatch(str(code))})


def resolve_symbols(config: DailySyncConfig) -> list[str]:
    """合并配置股票池和可选全市场列表，去重后稳定排序。"""

    codes = {_bare_code(value) for value in config.symbols}
    if config.universe == "all-a-shares":
        codes.update(_all_a_share_codes(config.data_root))
    result = sorted(codes)
    if not result:
        raise ValueError("no symbols configured for daily sync")
    return result[: config.max_symbols] if config.max_symbols else result


def _daily_path(data_root: Path, code: str) -> Path:
    ts_code = market_code_to_ts_code(infer_hq_market(code), code)
    return data_root / "daily" / f"ts_code={ts_code}" / "data.parquet"


def _normalized_trade_date(value: object) -> str:
    """把 Parquet/行情返回的日期统一成可直接比较的 YYYYMMDD。"""

    timestamp = pd.to_datetime(value, errors="raise")
    return timestamp.strftime("%Y%m%d")


def _latest_trade_date(frame: pd.DataFrame) -> str:
    if frame.empty or "trade_date" not in frame.columns:
        raise ValueError("daily bars do not contain a trade_date")
    return max(_normalized_trade_date(value) for value in frame["trade_date"])


def _existing_snapshot(data_root: Path, code: str) -> dict[str, object] | None:
    path = _daily_path(data_root, code)
    if not path.exists():
        return None
    frame = pd.read_parquet(path, columns=["trade_date"])
    return {
        "rows": len(frame),
        "latest_trade_date": _latest_trade_date(frame),
    }


def _checkpoint_path(data_root: Path) -> Path:
    return data_root / ".daily-sync-checkpoint.json"


def _load_checked_symbols(path: Path, target_trade_date: str | None) -> set[str]:
    """只复用与本次目标交易日一致的成功检查记录。"""

    if target_trade_date is None or not path.exists():
        return set()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    if (
        payload.get("schema_version") != _CHECKPOINT_SCHEMA_VERSION
        or payload.get("target_trade_date") != target_trade_date
    ):
        return set()
    values = payload.get("checked_symbols", [])
    return {str(value) for value in values if _BARE_CODE.fullmatch(str(value))}


def _write_checkpoint(
    path: Path,
    *,
    target_trade_date: str,
    checked_symbols: set[str],
) -> None:
    """原子保存断点，进程中断也不会留下半个 JSON。"""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(
        json.dumps(
            {
                "schema_version": _CHECKPOINT_SCHEMA_VERSION,
                "target_trade_date": target_trade_date,
                "checked_symbols": sorted(checked_symbols),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    temporary.replace(path)


def _sync_one(
    config: DailySyncConfig,
    code: str,
    downloader: TdxDownloader,
    *,
    target_trade_date: str | None,
    previously_checked: set[str],
) -> dict[str, object]:
    path = _daily_path(config.data_root, code)
    if target_trade_date is not None and code in previously_checked and path.exists():
        return {
            "code": code,
            "status": "skipped",
            "rows_before": None,
            "rows_after": None,
            "rows_added": 0,
            "latest_trade_date": None,
            "target_trade_date": target_trade_date,
            "skip_reason": "checked_for_target_trade_date",
            "attempts": 0,
        }

    try:
        existing = _existing_snapshot(config.data_root, code)
    except Exception as exc:  # noqa: BLE001 - do not silently overwrite corrupt local data
        return {
            "code": code,
            "status": "failed",
            "error": f"{type(exc).__name__}: {exc}",
            "attempts": 0,
        }

    before = int(existing["rows"]) if existing is not None else 0
    if (
        target_trade_date is not None
        and existing is not None
        and str(existing["latest_trade_date"]) >= target_trade_date
    ):
        return {
            "code": code,
            "status": "skipped",
            "rows_before": before,
            "rows_after": before,
            "rows_added": 0,
            "latest_trade_date": str(existing["latest_trade_date"]),
            "target_trade_date": target_trade_date,
            "skip_reason": "local_data_is_current",
            "attempts": 0,
        }

    last_error: Exception | None = None
    for attempt in range(1, config.retries + 1):
        try:
            frame = downloader.update_daily(
                code,
                history_bars=config.history_bars,
                refresh_bars=config.refresh_bars,
            )
            return {
                "code": code,
                "ts_code": str(frame["ts_code"].iloc[-1]),
                "status": "downloaded" if before == 0 else "updated",
                "rows_before": before,
                "rows_after": len(frame),
                "rows_added": len(frame) - before,
                "latest_trade_date": _latest_trade_date(frame),
                "target_trade_date": target_trade_date,
                "attempts": attempt,
            }
        except Exception as exc:  # noqa: BLE001 - each stock is retried and reported
            last_error = exc
            if attempt < config.retries:
                reconnect = getattr(downloader, "reconnect", None)
                if callable(reconnect):
                    try:
                        reconnect()
                    except Exception:
                        # 下一次 update_daily 会给出最终、可审计的失败原因。
                        pass
                time.sleep(attempt)
    return {
        "code": code,
        "status": "failed",
        "error": f"{type(last_error).__name__}: {last_error}",
        "attempts": config.retries,
    }


def _sync_partition(
    config: DailySyncConfig,
    codes: list[str],
    downloader_factory: Callable[[Path], TdxDownloader],
    *,
    target_trade_date: str | None,
    previously_checked: set[str],
    output: Queue,
) -> None:
    """一个工作线程复用一个下载器和一条 TDX 连接处理整批股票。"""

    completed = 0
    try:
        downloader = downloader_factory(config.data_root)
        context = (
            downloader
            if hasattr(downloader, "__enter__") and hasattr(downloader, "__exit__")
            else nullcontext(downloader)
        )
        with context as active_downloader:
            for code in codes:
                output.put(
                    _sync_one(
                        config,
                        code,
                        active_downloader,
                        target_trade_date=target_trade_date,
                        previously_checked=previously_checked,
                    )
                )
                completed += 1
    except Exception as exc:  # noqa: BLE001 - every unprocessed symbol must be reported
        for code in codes[completed:]:
            output.put(
                {
                    "code": code,
                    "status": "failed",
                    "error": f"{type(exc).__name__}: {exc}",
                    "attempts": 0,
                }
            )


def _sync_indices(
    config: DailySyncConfig,
    downloader_factory: Callable[[Path], TdxDownloader],
) -> tuple[list[dict[str, object]], str | None, list[str]]:
    """先刷新基准指数，用真实返回值确定当前可用的最新交易日。"""

    results: list[dict[str, object]] = []
    latest_dates: list[str] = []
    warnings: list[str] = []
    for code, market in config.indices:
        try:
            frame = downloader_factory(config.data_root).download_index(
                code,
                market=market,
                max_bars=config.history_bars,
            )
            latest = _latest_trade_date(frame)
            latest_dates.append(latest)
            results.append(
                {
                    "code": code,
                    "market": market,
                    "status": "updated",
                    "rows": len(frame),
                    "latest_trade_date": latest,
                }
            )
        except Exception as exc:  # noqa: BLE001 - keep the complete index audit
            results.append(
                {
                    "code": code,
                    "market": market,
                    "status": "failed",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
    target_trade_date = min(latest_dates) if latest_dates else None
    if len(set(latest_dates)) > 1:
        warnings.append(
            "configured indices returned different latest dates; "
            f"using the conservative common target {target_trade_date}"
        )
    if config.indices and target_trade_date is None:
        warnings.append(
            "all configured index downloads failed; current-file auto-skip is disabled"
        )
    if not config.indices:
        warnings.append(
            "no benchmark indices are configured; current-file auto-skip is disabled"
        )
    return results, target_trade_date, warnings


def run_sync(
    config: DailySyncConfig,
    *,
    downloader_factory: Callable[[Path], TdxDownloader] = TdxDownloader,
) -> dict[str, object]:
    """按数据源最新交易日自动续传股票日线，并返回可持久化报告。"""

    config.data_root.mkdir(parents=True, exist_ok=True)
    symbols = resolve_symbols(config)
    index_results, target_trade_date, warnings = _sync_indices(
        config,
        downloader_factory,
    )
    checkpoint_path = _checkpoint_path(config.data_root)
    checked_symbols = _load_checked_symbols(checkpoint_path, target_trade_date)
    previously_checked = set(checked_symbols)
    results: list[dict[str, object]] = []
    # 股票按轮询方式均分给固定工作线程。每个线程只连接一次通达信，
    # 避免 5200 多只股票逐只 TCP 握手，同时仍把并发限制在配置值内。
    partitions = [
        symbols[index::config.workers]
        for index in range(config.workers)
        if symbols[index::config.workers]
    ]
    output: Queue = Queue()
    with ThreadPoolExecutor(max_workers=len(partitions)) as executor:
        futures = [
            executor.submit(
                _sync_partition,
                config,
                partition,
                downloader_factory,
                target_trade_date=target_trade_date,
                previously_checked=previously_checked,
                output=output,
            )
            for partition in partitions
        ]
        completed = 0
        completed_since_checkpoint = 0
        while completed < len(symbols):
            result = output.get()
            results.append(result)
            completed += 1
            if target_trade_date is not None and result["status"] != "failed":
                checked_symbols.add(str(result["code"]))
                completed_since_checkpoint += 1
                if completed_since_checkpoint >= _CHECKPOINT_FLUSH_EVERY:
                    _write_checkpoint(
                        checkpoint_path,
                        target_trade_date=target_trade_date,
                        checked_symbols=checked_symbols,
                    )
                    completed_since_checkpoint = 0
            message = result.get("error")
            if message is None and result["status"] == "skipped":
                message = (
                    f"{result.get('skip_reason')}, "
                    f"latest={result.get('latest_trade_date') or 'checkpoint'}, "
                    f"target={target_trade_date}"
                )
            if message is None:
                message = (
                    f"{result['rows_before']} -> {result['rows_after']} rows, "
                    f"latest={result['latest_trade_date']}"
                )
            if (
                result["status"] == "failed"
                or completed == len(symbols)
                or completed % config.progress_every == 0
            ):
                print(
                    f"[{completed}/{len(symbols)}] "
                    f"{result['code']} {result['status']}: {message}"
                )
        for future in as_completed(futures):
            future.result()

    if target_trade_date is not None:
        _write_checkpoint(
            checkpoint_path,
            target_trade_date=target_trade_date,
            checked_symbols=checked_symbols,
        )

    results.sort(key=lambda item: str(item["code"]))
    report = {
        "requested": len(symbols),
        "succeeded": sum(item["status"] != "failed" for item in results),
        "failed": sum(item["status"] == "failed" for item in results),
        "skipped": sum(item["status"] == "skipped" for item in results),
        "target_trade_date": target_trade_date,
        "stocks": results,
        "indices": index_results,
        "warnings": warnings,
    }
    if config.report:
        config.report.parent.mkdir(parents=True, exist_ok=True)
        config.report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Batch/incremental pytdx daily-bar sync")
    parser.add_argument("--config", type=Path, default=Path("configs/daily-sync.json"))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = DailySyncConfig.from_json(args.config)
    report = run_sync(config)
    print(
        f"daily sync complete: requested={report['requested']} "
        f"succeeded={report['succeeded']} skipped={report['skipped']} "
        f"failed={report['failed']} target={report['target_trade_date']}"
    )
    return 1 if report["failed"] or any(i["status"] == "failed" for i in report["indices"]) else 0


if __name__ == "__main__":
    raise SystemExit(main())
