"""DuckDB 查询层的分区裁剪、日期过滤和股票列表测试。"""

from pathlib import Path

import pandas as pd
import pytest

from scripts.data_pipeline.query import DuckDBMarketStore


def _write_bars(root: Path, symbol: str, closes: list[float]) -> None:
    dates = pd.date_range("2026-01-01", periods=len(closes), freq="D")
    frame = pd.DataFrame(
        {
            "datetime": dates,
            "trade_date": dates.strftime("%Y%m%d"),
            "code": symbol[:6],
            "open": closes,
            "high": [value + 0.1 for value in closes],
            "low": [value - 0.1 for value in closes],
            "close": closes,
            "vol": [100] * len(closes),
        }
    )
    path = root / "daily" / f"ts_code={symbol}" / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _write_security_list(root: Path) -> None:
    frame = pd.DataFrame(
        [
            {"code": "000001", "name": "平安银行", "ts_code": "000001.SZ", "pre_close": 10.0},
            {"code": "600519", "name": "贵州茅台", "ts_code": "600519.SH", "pre_close": 1500.0},
            {"code": "510300", "name": "沪深300ETF", "ts_code": "510300.SH", "pre_close": 4.0},
        ]
    )
    path = root / "security_list" / "market=SZ" / "date=20260105" / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.iloc[[0]].to_parquet(path, index=False)
    path = root / "security_list" / "market=SH" / "date=20260105" / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.iloc[1:].to_parquet(path, index=False)


def test_query_bars_filters_symbol_date_and_returns_latest_limit(tmp_path):
    _write_bars(tmp_path, "000001.SZ", [10, 11, 12, 13, 14])
    _write_bars(tmp_path, "600519.SH", [100, 101, 102, 103, 104])
    with DuckDBMarketStore(tmp_path) as store:
        result = store.query_bars("000001", start="2026-01-02", end="2026-01-05", limit=2)

    assert result["ts_code"].tolist() == ["000001.SZ", "000001.SZ"]
    assert result["close"].tolist() == [13, 14]
    assert result["trade_date"].tolist() == ["20260104", "20260105"]


def test_latest_bars_returns_one_row_per_requested_symbol(tmp_path):
    _write_bars(tmp_path, "000001.SZ", [10, 11, 12])
    _write_bars(tmp_path, "600519.SH", [100, 101, 102])
    with DuckDBMarketStore(tmp_path) as store:
        result = store.latest_bars(symbols=["000001", "600519.SH"], as_of="2026-01-02")

    assert result[["ts_code", "close"]].values.tolist() == [
        ["000001.SZ", 11],
        ["600519.SH", 101],
    ]


def test_list_symbols_defaults_to_a_shares_and_supports_search(tmp_path):
    _write_security_list(tmp_path)
    with DuckDBMarketStore(tmp_path) as store:
        all_stocks = store.list_symbols(limit=10)
        searched = store.list_symbols(search="茅台", market="SH", limit=10)

    assert set(all_stocks["ts_code"]) == {"000001.SZ", "600519.SH"}
    assert searched["ts_code"].tolist() == ["600519.SH"]


def test_invalid_inputs_and_missing_domains_fail_clearly(tmp_path):
    with DuckDBMarketStore(tmp_path) as store:
        with pytest.raises(ValueError, match="invalid mainland symbol"):
            store.query_bars("000001'; DROP TABLE x; --")
        with pytest.raises(FileNotFoundError, match="no parquet data"):
            store.query_bars("000001")
