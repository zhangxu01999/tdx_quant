"""Offline unit tests for the security-enumeration pipeline (no network).

Integration coverage lives in ``tests/test_pytdx_extended_integration.py``.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from scripts.data_pipeline.extractors.tdx_security_list import (
    security_list_to_dataframe,
)

REQUIRED_COLS = {
    "code",
    "name",
    "pre_close",
    "decimal_point",
    "volunit",
    "market",
    "ts_code",
}


def _sample_payload() -> list[dict]:
    return [
        {"code": "000001", "volunit": 100, "decimal_point": 2, "name": "平安银行", "pre_close": 11.0},
        {"code": "000002", "volunit": 100, "decimal_point": 2, "name": "万　科Ａ", "pre_close": 8.5},
    ]


# --------------------------------------------------------------------------- #
# extractor
# --------------------------------------------------------------------------- #
def test_security_list_to_dataframe_maps_columns_and_ts_code() -> None:
    df = security_list_to_dataframe(_sample_payload(), market=0)

    assert not df.empty
    assert REQUIRED_COLS <= set(df.columns)
    assert (df["market"] == 0).all()
    assert list(df["ts_code"]) == ["000001.SZ", "000002.SZ"]
    assert df.iloc[0]["name"] == "平安银行"


def test_security_list_to_dataframe_sh_market_suffix() -> None:
    df = security_list_to_dataframe(
        [{"code": "600000", "volunit": 100, "decimal_point": 2, "name": "浦发银行", "pre_close": 10.0}],
        market=1,
    )
    assert df.loc[0, "ts_code"] == "600000.SH"


def test_security_list_to_dataframe_empty_payload() -> None:
    df = security_list_to_dataframe([], market=0)
    assert df.empty
    assert REQUIRED_COLS <= set(df.columns)


# --------------------------------------------------------------------------- #
# materializer: snapshot partitioned by market + date
# --------------------------------------------------------------------------- #
def test_write_security_list_snapshot_partition_and_readback(tmp_path: Path) -> None:
    from scripts.data_pipeline.materializers.raw_writer import write_raw_by_market_date

    df = security_list_to_dataframe(_sample_payload(), market=0)
    path = write_raw_by_market_date(tmp_path, "security_list", "SZ", df, "20260618")

    assert path == tmp_path / "security_list" / "market=SZ" / "date=20260618" / "data.parquet"
    assert path.exists()

    # hive-partition read of the table root restores market/date from the path
    # (pyarrow infers the all-digit `date=` key as int — coerce for comparison)
    back = pd.read_parquet(tmp_path / "security_list")
    assert len(back) == 2
    assert set(back["market"]) == {"SZ"}
    assert set(back["date"].astype(str)) == {"20260618"}
    assert (back["ts_code"].str.endswith(".SZ")).all()


# --------------------------------------------------------------------------- #
# job: paging
# --------------------------------------------------------------------------- #
def _make_rows(n: int, prefix: str) -> list[dict]:
    return [
        {"code": f"{prefix}{i:04d}", "volunit": 100, "decimal_point": 2, "name": f"x{i}", "pre_close": 1.0}
        for i in range(n)
    ]


def test_run_security_list_job_pages_full_range_skipping_gaps(tmp_path: Path) -> None:
    # get_security_list offset space is SPARSE: empty pages occur mid-range
    # (and SH even has an empty start=0). Page [0,count) skipping empties.
    from scripts.data_pipeline.jobs.security_list_job import run_security_list_job

    total = 3500
    pages = {
        0: _make_rows(1000, "A"),
        1000: [],  # mid-range gap (empty page)
        2000: _make_rows(1000, "B"),
        3000: _make_rows(500, "C"),  # short final page, still within range
    }
    calls: list[int] = []

    def fetch_page(start: int, count: int) -> list[dict]:
        calls.append(start)
        return pages.get(start, [])

    def fetch_count() -> int:
        return total

    result = run_security_list_job(
        fetch_page=fetch_page, fetch_count=fetch_count,
        market=0, trade_date="20260618", output_root=tmp_path,
    )

    assert result["status"] == "success"
    assert result["market"] == "SZ"
    assert result["records"] == 2500  # 1000 + (gap skipped) + 1000 + 500
    assert calls == [0, 1000, 2000, 3000]  # paged [0,total) stepping 1000, gap visited
    assert (tmp_path / "security_list" / "market=SZ" / "date=20260618" / "data.parquet").exists()


def test_run_security_list_job_skips_leading_empty(tmp_path: Path) -> None:
    # SH case: start=0 is empty, real data begins at start=1000
    from scripts.data_pipeline.jobs.security_list_job import run_security_list_job

    total = 2000
    pages = {0: [], 1000: _make_rows(1000, "X")}

    result = run_security_list_job(
        fetch_page=lambda start, count: pages.get(start, []),
        fetch_count=lambda: total,
        market=1, trade_date="2026-06-18", output_root=tmp_path,
    )
    assert result["market"] == "SH"
    assert result["records"] == 1000
