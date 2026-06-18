"""Offline unit tests for the 指数 K 线 pipeline (no network)."""
from __future__ import annotations

from scripts.data_pipeline.extractors.tdx_index_bars import index_bars_to_dataframe

REQUIRED_COLS = {
    "datetime",
    "open",
    "high",
    "low",
    "close",
    "vol",
    "amount",
    "up_count",
    "down_count",
    "market",
    "code",
    "ts_code",
    "trade_date",
}


def _sample() -> list[dict]:
    return [
        {
            "open": 4017.86, "close": 4031.51, "high": 4060.27, "low": 4008.18,
            "vol": 7431310.0, "amount": 1537401552896.0,
            "year": 2026, "month": 6, "day": 12, "hour": 15, "minute": 0,
            "datetime": "2026-06-12 15:00", "up_count": 1700, "down_count": 622,
        }
    ]


def test_index_bars_to_dataframe_keepsUpDown_and_derives_codes() -> None:
    df = index_bars_to_dataframe(_sample(), market=1, code="000001")
    assert not df.empty
    assert REQUIRED_COLS <= set(df.columns)
    assert df.loc[0, "ts_code"] == "000001.SH"
    assert df.loc[0, "up_count"] == 1700
    assert df.loc[0, "down_count"] == 622
    assert df.loc[0, "trade_date"] == "20260612"


def test_index_bars_to_dataframe_sh_index_suffix() -> None:
    df = index_bars_to_dataframe(_sample(), market=0, code="399001")
    assert df.loc[0, "ts_code"] == "399001.SZ"


def test_index_bars_to_dataframe_empty() -> None:
    df = index_bars_to_dataframe([], market=1, code="000001")
    assert df.empty
    assert REQUIRED_COLS <= set(df.columns)
