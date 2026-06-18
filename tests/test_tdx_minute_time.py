"""Offline unit tests for the 分时 (minute-time) pipeline (no network).

pytdx 分时 rows carry only ``{price, vol}`` with no timestamp, so we keep a
0-based ``minute_idx`` (one point per session minute) rather than fabricate a
time-of-day mapping.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.data_pipeline.extractors.tdx_minute_time import minute_time_to_dataframe

REQUIRED_COLS = {"minute_idx", "price", "vol", "ts_code", "trade_date"}


def test_minute_time_to_dataframe_assigns_index_and_codes() -> None:
    payload = [
        {"price": 10.72, "vol": 24108},
        {"price": 10.75, "vol": 10426},
        {"price": 0.0, "vol": 0},
    ]
    df = minute_time_to_dataframe(payload, market=0, code="000001", trade_date="20260617")

    assert not df.empty
    assert REQUIRED_COLS <= set(df.columns)
    assert list(df["minute_idx"]) == [0, 1, 2]
    assert (df["ts_code"] == "000001.SZ").all()
    assert (df["trade_date"] == "20260617").all()
    assert df.loc[0, "price"] == 10.72


def test_minute_time_to_dataframe_empty() -> None:
    df = minute_time_to_dataframe([], market=1, code="000001", trade_date="20260617")
    assert df.empty
    assert REQUIRED_COLS <= set(df.columns)


# --------------------------------------------------------------------------- #
# job: fetch + persist (no paging — a single fetch returns the full session)
# --------------------------------------------------------------------------- #
def test_run_minute_time_job_writes_partition(tmp_path: Path) -> None:
    from scripts.data_pipeline.jobs.minute_time_job import run_minute_time_job

    def fetch() -> list[dict]:
        return [{"price": 1.0, "vol": 100}, {"price": 1.1, "vol": 200}]

    result = run_minute_time_job(
        fetch=fetch, market=0, code="000001", trade_date="20260617", output_root=tmp_path
    )

    assert result["status"] == "success"
    assert result["ts_code"] == "000001.SZ"
    assert result["records"] == 2
    assert (tmp_path / "minute_time" / "date=20260617" / "ts_code=000001.SZ" / "data.parquet").exists()
