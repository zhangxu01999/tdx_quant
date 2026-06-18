"""Offline unit tests for the 股本结构 (get_finance_info) pipeline (no network).

Distinct from Phase 3 financial STATEMENTS (F10) and the existing tushare
``financial_job`` — this is pytdx HQ's capital-structure snapshot (a single row).
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.data_pipeline.extractors.tdx_finance import finance_info_to_dataframe

REQUIRED_COLS = {
    "ts_code",
    "market",
    "code",
    "zongguben",
    "liutongguben",
    "guojiagu",
    "farengu",
    "bgu",
    "ipo_date",
    "industry",
    "province",
    "updated_date",
}


def _sample_dict() -> dict:
    return {
        "liutongguben": 1.9e10,
        "zongguben": 1.94e10,
        "guojiagu": 0,
        "faqirenfarengu": 0,
        "farengu": 0,
        "bgu": 0,
        "ipo_date": 19910403,
        "industry": "银行",
        "province": "广东",
        "updated_date": 20260505,
    }


def test_finance_info_to_dataframe_maps_capital_structure() -> None:
    df = finance_info_to_dataframe(_sample_dict(), market=0, code="000001")

    assert len(df) == 1
    assert REQUIRED_COLS <= set(df.columns)
    assert df.loc[0, "ts_code"] == "000001.SZ"
    assert df.loc[0, "market"] == 0
    assert df.loc[0, "zongguben"] == 1.94e10
    assert df.loc[0, "ipo_date"] == 19910403


def test_finance_info_to_dataframe_empty() -> None:
    df = finance_info_to_dataframe({}, market=1, code="600000")
    assert len(df) == 1
    assert df.loc[0, "ts_code"] == "600000.SH"


# --------------------------------------------------------------------------- #
# job: fetch + persist single-row snapshot
# --------------------------------------------------------------------------- #
def test_run_finance_capital_job(tmp_path: Path) -> None:
    from scripts.data_pipeline.jobs.finance_capital_job import run_finance_capital_job

    def fetch() -> dict:
        return _sample_dict()

    result = run_finance_capital_job(
        fetch=fetch, market=0, code="000001", output_root=tmp_path
    )

    assert result["status"] == "success"
    assert result["ts_code"] == "000001.SZ"
    assert result["records"] == 1
    assert (tmp_path / "finance_capital" / "ts_code=000001.SZ" / "data.parquet").exists()
