"""股本结构全市场批量同步编排测试。"""

from pathlib import Path

import pandas as pd

from scripts.data_pipeline.batch_finance_capital import (
    FinanceCapitalSyncConfig,
    resolve_symbols,
    run_sync,
)


class _FakeDownloader:
    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root

    def download_finance_capital(self, code: str) -> pd.DataFrame:
        suffix = "SH" if code.startswith("6") else "SZ"
        return pd.DataFrame(
            [
                {
                    "ts_code": f"{code}.{suffix}",
                    "liutongguben": 100_000_000,
                    "zongguben": 120_000_000,
                }
            ]
        )


def test_resolve_configured_symbols_normalizes_suffix_and_deduplicates(tmp_path: Path) -> None:
    config = FinanceCapitalSyncConfig(
        data_root=tmp_path,
        symbols=("000001.SZ", "600000", "000001"),
        workers=1,
    )
    assert resolve_symbols(config) == ["000001", "600000"]


def test_all_a_share_universe_comes_from_security_snapshot(tmp_path: Path) -> None:
    leaf = tmp_path / "security_list" / "market=SZ" / "date=20260715"
    leaf.mkdir(parents=True)
    pd.DataFrame(
        {
            "code": ["000001", "301001", "399001", "159001"],
            "name": ["平安银行", "创业股票", "深证成指", "基金"],
        }
    ).to_parquet(leaf / "data.parquet", index=False)
    config = FinanceCapitalSyncConfig(
        data_root=tmp_path,
        symbols=(),
        universe="all-a-shares",
        workers=1,
    )
    assert resolve_symbols(config) == ["000001", "301001"]


def test_run_sync_returns_report(tmp_path: Path) -> None:
    report_path = tmp_path / "report.json"
    config = FinanceCapitalSyncConfig(
        data_root=tmp_path,
        symbols=("000001.SZ", "600000.SH"),
        workers=2,
        report=report_path,
    )
    report = run_sync(config, downloader_factory=_FakeDownloader)

    assert report["requested"] == 2
    assert report["succeeded"] == 2
    assert report["failed"] == 0
    assert report_path.exists()
