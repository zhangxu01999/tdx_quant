"""股票池日线批量下载和增量更新编排测试。"""

from pathlib import Path

import pandas as pd

from scripts.data_pipeline.batch_daily import DailySyncConfig, resolve_symbols, run_sync


class _FakeDownloader:
    updated_codes: list[str] = []

    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root

    def update_daily(self, code: str, *, history_bars: int, refresh_bars: int) -> pd.DataFrame:
        type(self).updated_codes.append(code)
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
    _FakeDownloader.updated_codes = []
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
    assert report["skipped"] == 0
    assert report["target_trade_date"] == "20260708"
    assert report["indices"][0]["status"] == "updated"
    assert sorted(_FakeDownloader.updated_codes) == ["000001", "600000"]
    assert report_path.exists()


def _write_daily(tmp_path: Path, ts_code: str, trade_date: str) -> None:
    leaf = tmp_path / "daily" / f"ts_code={ts_code}"
    leaf.mkdir(parents=True)
    pd.DataFrame([{"trade_date": trade_date}]).to_parquet(
        leaf / "data.parquet",
        index=False,
    )


def test_run_sync_skips_local_symbol_already_at_index_target(tmp_path: Path) -> None:
    _FakeDownloader.updated_codes = []
    _write_daily(tmp_path, "000001.SZ", "20260708")
    config = DailySyncConfig(
        data_root=tmp_path,
        symbols=("000001.SZ", "600000.SH"),
        workers=1,
        indices=(("000001", 1),),
    )

    report = run_sync(config, downloader_factory=_FakeDownloader)

    assert report["skipped"] == 1
    assert _FakeDownloader.updated_codes == ["600000"]
    skipped = next(item for item in report["stocks"] if item["code"] == "000001")
    assert skipped["skip_reason"] == "local_data_is_current"


class _SuspendedFakeDownloader(_FakeDownloader):
    def update_daily(self, code: str, *, history_bars: int, refresh_bars: int) -> pd.DataFrame:
        type(self).updated_codes.append(code)
        suffix = "SH" if code.startswith("6") else "SZ"
        return pd.DataFrame(
            [
                {
                    "ts_code": f"{code}.{suffix}",
                    "trade_date": "20260707",
                }
            ]
        )


class _NoStockRequestFakeDownloader(_FakeDownloader):
    def update_daily(self, code: str, *, history_bars: int, refresh_bars: int) -> pd.DataFrame:
        raise AssertionError(f"unexpected repeated stock request: {code}")


def test_checkpoint_skips_symbol_checked_while_suspended(tmp_path: Path) -> None:
    _SuspendedFakeDownloader.updated_codes = []
    _write_daily(tmp_path, "000001.SZ", "20260707")
    config = DailySyncConfig(
        data_root=tmp_path,
        symbols=("000001.SZ",),
        workers=1,
        indices=(("000001", 1),),
    )

    first = run_sync(config, downloader_factory=_SuspendedFakeDownloader)
    second = run_sync(config, downloader_factory=_NoStockRequestFakeDownloader)

    assert first["skipped"] == 0
    assert _SuspendedFakeDownloader.updated_codes == ["000001"]
    assert second["skipped"] == 1
    assert second["stocks"][0]["skip_reason"] == "checked_for_target_trade_date"


def test_checkpoint_expires_when_index_target_advances(tmp_path: Path) -> None:
    checkpoint = tmp_path / ".daily-sync-checkpoint.json"
    checkpoint.write_text(
        '{"schema_version": 1, "target_trade_date": "20260707", '
        '"checked_symbols": ["000001"]}',
        encoding="utf-8",
    )
    _FakeDownloader.updated_codes = []
    _write_daily(tmp_path, "000001.SZ", "20260707")
    config = DailySyncConfig(
        data_root=tmp_path,
        symbols=("000001.SZ",),
        workers=1,
        indices=(("000001", 1),),
    )

    report = run_sync(config, downloader_factory=_FakeDownloader)

    assert report["skipped"] == 0
    assert _FakeDownloader.updated_codes == ["000001"]
