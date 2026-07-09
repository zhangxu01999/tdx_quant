"""股票池日线批量下载和增量更新编排测试。"""

from pathlib import Path

import pandas as pd

from scripts.data_pipeline.batch_daily import DailySyncConfig, resolve_symbols, run_sync


class _FakeDownloader:
    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root

    def update_daily(self, code: str, *, history_bars: int, refresh_bars: int) -> pd.DataFrame:
        suffix = "SH" if code.startswith("6") else "SZ"
        return pd.DataFrame(
            [
                {
                    "ts_code": f"{code}.{suffix}",
                    "trade_date": "20260708",
                }
            ]
        )

    def download_index(self, code: str, *, market: int, max_bars: int) -> pd.DataFrame:
        return pd.DataFrame([{"trade_date": "20260708"}])


def test_resolve_configured_symbols_normalizes_suffix_and_deduplicates(tmp_path: Path) -> None:
    config = DailySyncConfig(
        data_root=tmp_path,
        symbols=("000001.SZ", "600000", "000001"),
        workers=1,
    )
    assert resolve_symbols(config) == ["000001", "600000"]


def test_all_a_share_universe_comes_from_security_snapshot(tmp_path: Path) -> None:
    leaf = tmp_path / "security_list" / "market=SZ" / "date=20260708"
    leaf.mkdir(parents=True)
    pd.DataFrame(
        {
            "code": ["000001", "300750", "399001", "159001"],
            "name": ["平安银行", "宁德时代", "深证成指", "基金"],
        }
    ).to_parquet(leaf / "data.parquet", index=False)
    config = DailySyncConfig(
        data_root=tmp_path,
        symbols=(),
        universe="all-a-shares",
        workers=1,
    )
    assert resolve_symbols(config) == ["000001", "300750"]


def test_run_sync_returns_stock_and_index_report(tmp_path: Path) -> None:
    report_path = tmp_path / "report.json"
    config = DailySyncConfig(
        data_root=tmp_path,
        symbols=("000001.SZ", "600000.SH"),
        workers=2,
        indices=(("000001", 1),),
        report=report_path,
    )
    report = run_sync(config, downloader_factory=_FakeDownloader)

    assert report["requested"] == 2
    assert report["succeeded"] == 2
    assert report["failed"] == 0
    assert report["indices"][0]["status"] == "updated"
    assert report_path.exists()
