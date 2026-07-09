"""按配置批量下载并增量更新 A 股日线 Parquet。"""

from __future__ import annotations

import argparse
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd

from scripts.data_pipeline.code_mapping import market_code_to_ts_code
from scripts.data_pipeline.fetch_realtime_watchlist import infer_hq_market
from scripts.data_pipeline.tdx_client import TdxDownloader


_BARE_CODE = re.compile(r"^\d{6}$")
_A_SHARE_CODE = re.compile(r"^(000|001|002|003|300|301|600|601|603|605|688|689)\d{3}$")


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


def _existing_rows(data_root: Path, code: str) -> int:
    ts_code = market_code_to_ts_code(infer_hq_market(code), code)
    path = data_root / "daily" / f"ts_code={ts_code}" / "data.parquet"
    if not path.exists():
        return 0
    return len(pd.read_parquet(path, columns=["trade_date"]))


def _sync_one(
    config: DailySyncConfig,
    code: str,
    downloader_factory: Callable[[Path], TdxDownloader],
) -> dict[str, object]:
    before = _existing_rows(config.data_root, code)
    last_error: Exception | None = None
    for attempt in range(1, config.retries + 1):
        try:
            frame = downloader_factory(config.data_root).update_daily(
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
                "latest_trade_date": str(frame["trade_date"].iloc[-1]),
                "attempts": attempt,
            }
        except Exception as exc:  # noqa: BLE001 - each stock is retried and reported
            last_error = exc
            if attempt < config.retries:
                time.sleep(attempt)
    return {
        "code": code,
        "status": "failed",
        "error": f"{type(last_error).__name__}: {last_error}",
        "attempts": config.retries,
    }


def run_sync(
    config: DailySyncConfig,
    *,
    downloader_factory: Callable[[Path], TdxDownloader] = TdxDownloader,
) -> dict[str, object]:
    """并发同步股票，并刷新少量基准指数，返回可持久化报告。"""

    config.data_root.mkdir(parents=True, exist_ok=True)
    symbols = resolve_symbols(config)
    results: list[dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=config.workers) as executor:
        futures = {
            executor.submit(_sync_one, config, code, downloader_factory): code
            for code in symbols
        }
        completed = 0
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            completed += 1
            message = result.get("error") or (
                f"{result['rows_before']} -> {result['rows_after']} rows, "
                f"latest={result['latest_trade_date']}"
            )
            print(f"[{completed}/{len(symbols)}] {result['code']} {result['status']}: {message}")

    index_results: list[dict[str, object]] = []
    for code, market in config.indices:
        try:
            frame = downloader_factory(config.data_root).download_index(
                code, market=market, max_bars=config.history_bars
            )
            index_results.append(
                {
                    "code": code,
                    "market": market,
                    "status": "updated",
                    "rows": len(frame),
                    "latest_trade_date": str(frame["trade_date"].iloc[-1]),
                }
            )
        except Exception as exc:  # noqa: BLE001 - report all completed stock work first
            index_results.append(
                {
                    "code": code,
                    "market": market,
                    "status": "failed",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )

    results.sort(key=lambda item: str(item["code"]))
    report = {
        "requested": len(symbols),
        "succeeded": sum(item["status"] != "failed" for item in results),
        "failed": sum(item["status"] == "failed" for item in results),
        "stocks": results,
        "indices": index_results,
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
        f"succeeded={report['succeeded']} failed={report['failed']}"
    )
    return 1 if report["failed"] or any(i["status"] == "failed" for i in report["indices"]) else 0


if __name__ == "__main__":
    raise SystemExit(main())
