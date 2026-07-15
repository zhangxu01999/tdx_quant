"""按配置批量同步沪深 A 股股本结构快照。"""

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

from scripts.data_pipeline.batch_daily import _all_a_share_codes
from scripts.data_pipeline.code_mapping import market_code_to_ts_code
from scripts.data_pipeline.fetch_realtime_watchlist import infer_hq_market
from scripts.data_pipeline.tdx_client import TdxDownloader


_BARE_CODE = re.compile(r"^\d{6}$")


@dataclass(frozen=True)
class FinanceCapitalSyncConfig:
    """股本结构同步参数；默认跟日线同步一样从证券列表取全市场 A 股。"""

    data_root: Path
    symbols: tuple[str, ...]
    universe: str = "configured"
    workers: int = 4
    retries: int = 2
    max_symbols: int | None = None
    skip_existing: bool = False
    report: Path | None = None

    @classmethod
    def from_json(cls, path: Path) -> "FinanceCapitalSyncConfig":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("finance capital sync config must be a JSON object")
        config = cls(
            data_root=Path(str(payload.get("data_root", "data"))),
            symbols=tuple(str(value) for value in payload.get("symbols", [])),
            universe=str(payload.get("universe", "configured")),
            workers=int(payload.get("workers", 4)),
            retries=int(payload.get("retries", 2)),
            max_symbols=(int(payload["max_symbols"]) if payload.get("max_symbols") else None),
            skip_existing=bool(payload.get("skip_existing", False)),
            report=Path(str(payload["report"])) if payload.get("report") else None,
        )
        config.validate()
        return config

    def validate(self) -> None:
        if self.universe not in {"configured", "all-a-shares"}:
            raise ValueError("universe must be 'configured' or 'all-a-shares'")
        if not 1 <= self.workers <= 16:
            raise ValueError("workers must be between 1 and 16")
        if not 1 <= self.retries <= 10:
            raise ValueError("retries must be between 1 and 10")
        if self.max_symbols is not None and self.max_symbols <= 0:
            raise ValueError("max_symbols must be positive")


def _bare_code(value: str) -> str:
    """兼容 000001、000001.SZ 两种写法，统一返回 6 位代码。"""

    code = value.strip().upper().split(".", 1)[0]
    if not _BARE_CODE.fullmatch(code):
        raise ValueError(f"invalid stock code: {value!r}")
    return code


def resolve_symbols(config: FinanceCapitalSyncConfig) -> list[str]:
    """合并手写股票池和全市场证券列表，去重后按代码排序。"""

    codes = {_bare_code(value) for value in config.symbols}
    if config.universe == "all-a-shares":
        codes.update(_all_a_share_codes(config.data_root))
    result = sorted(codes)
    if not result:
        raise ValueError("no symbols configured for finance capital sync")
    return result[: config.max_symbols] if config.max_symbols else result


def _capital_path(data_root: Path, code: str) -> Path:
    market = infer_hq_market(code)
    ts_code = market_code_to_ts_code(market, code)
    return data_root / "finance_capital" / f"ts_code={ts_code}" / "data.parquet"


def _existing_snapshot(data_root: Path, code: str) -> dict[str, object] | None:
    path = _capital_path(data_root, code)
    if not path.exists():
        return None
    try:
        frame = pd.read_parquet(path)
    except Exception:  # noqa: BLE001 - 破损文件交给同步任务重新覆盖
        return None
    return {
        "records": len(frame),
        "path": str(path),
    }


def _sync_one(
    config: FinanceCapitalSyncConfig,
    code: str,
    downloader_factory: Callable[[Path], TdxDownloader],
) -> dict[str, object]:
    existing = _existing_snapshot(config.data_root, code)
    market = infer_hq_market(code)
    ts_code = market_code_to_ts_code(market, code)
    if config.skip_existing and existing is not None:
        return {
            "code": code,
            "ts_code": ts_code,
            "status": "skipped",
            "records": existing["records"],
            "attempts": 0,
        }

    last_error: Exception | None = None
    for attempt in range(1, config.retries + 1):
        try:
            frame = downloader_factory(config.data_root).download_finance_capital(code)
            return {
                "code": code,
                "ts_code": ts_code,
                "status": "updated" if existing is not None else "downloaded",
                "records": len(frame),
                "attempts": attempt,
            }
        except Exception as exc:  # noqa: BLE001 - 单股失败不影响全市场任务继续
            last_error = exc
            if attempt < config.retries:
                time.sleep(attempt)
    return {
        "code": code,
        "ts_code": ts_code,
        "status": "failed",
        "error": f"{type(last_error).__name__}: {last_error}",
        "attempts": config.retries,
    }


def run_sync(
    config: FinanceCapitalSyncConfig,
    *,
    downloader_factory: Callable[[Path], TdxDownloader] = TdxDownloader,
) -> dict[str, object]:
    """并发同步股本结构，返回可写入 JSON 的执行报告。"""

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
            message = result.get("error") or f"records={result.get('records', 0)}"
            print(f"[{completed}/{len(symbols)}] {result['code']} {result['status']}: {message}")

    results.sort(key=lambda item: str(item["code"]))
    report = {
        "requested": len(symbols),
        "succeeded": sum(item["status"] in {"downloaded", "updated", "skipped"} for item in results),
        "failed": sum(item["status"] == "failed" for item in results),
        "skipped": sum(item["status"] == "skipped" for item in results),
        "stocks": results,
    }
    if config.report:
        config.report.parent.mkdir(parents=True, exist_ok=True)
        config.report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Batch pytdx finance-capital snapshot sync")
    parser.add_argument("--config", type=Path, default=Path("configs/finance-capital-sync.json"))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = FinanceCapitalSyncConfig.from_json(args.config)
    report = run_sync(config)
    print(
        f"finance capital sync complete: requested={report['requested']} "
        f"succeeded={report['succeeded']} skipped={report['skipped']} failed={report['failed']}"
    )
    return 1 if report["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
