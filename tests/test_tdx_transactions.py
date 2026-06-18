"""Offline unit tests for the tick / 分笔成交 pipeline (no network)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from scripts.data_pipeline.extractors.tdx_transactions import (
    transactions_to_dataframe,
)

REQUIRED_COLS = {
    "time",
    "price",
    "vol",
    "num",
    "buyorsell",
    "buyorsell_label",
    "market",
    "code",
    "ts_code",
    "trade_date",
}


def _history_rows() -> list[dict]:
    # history schema: no `num`; includes a zero-vol buyorsell=8 marker row
    return [
        {"time": "09:30", "price": 11.0, "vol": 100, "buyorsell": 0},
        {"time": "09:31", "price": 11.01, "vol": 50, "buyorsell": 1},
        {"time": "09:31", "price": 0.0, "vol": 0, "buyorsell": 8},
        {"time": "09:32", "price": 11.02, "vol": 30, "buyorsell": 2},
    ]


def _today_rows() -> list[dict]:
    # today schema: extra `num`
    return [{"time": "09:30", "price": 11.0, "vol": 100, "num": 1, "buyorsell": 0}]


def test_transactions_history_schema_unifies_and_labels() -> None:
    df = transactions_to_dataframe(_history_rows(), market=0, code="000001", trade_date="20260617")

    assert not df.empty
    assert REQUIRED_COLS <= set(df.columns)
    assert (df["ts_code"] == "000001.SZ").all()
    assert (df["trade_date"] == "20260617").all()
    # num absent in history payload → filled NA (column present)
    assert df["num"].isna().all()

    labels = dict(zip(df["buyorsell"], df["buyorsell_label"]))
    assert labels[0] == "buy"
    assert labels[1] == "sell"
    assert labels[2] == "neutral"
    assert labels[8] == "other"  # zero-vol marker → distinct, not dropped


def test_transactions_keeps_zero_vol_rows() -> None:
    df = transactions_to_dataframe(_history_rows(), market=0, code="000001", trade_date="20260617")
    zero_vol = df[df["vol"] == 0]
    assert len(zero_vol) == 1
    assert (zero_vol["buyorsell_label"] == "other").all()


def test_transactions_today_schema_preserves_num() -> None:
    df = transactions_to_dataframe(_today_rows(), market=1, code="600000", trade_date="20260618")
    assert df.loc[0, "ts_code"] == "600000.SH"
    assert df.loc[0, "num"] == 1
    assert df.loc[0, "buyorsell_label"] == "buy"


def test_transactions_empty_payload() -> None:
    df = transactions_to_dataframe([], market=0, code="000001", trade_date="20260617")
    assert df.empty
    assert REQUIRED_COLS <= set(df.columns)


# --------------------------------------------------------------------------- #
# materializer: date + ts_code partition
# --------------------------------------------------------------------------- #
def test_write_raw_by_date_symbol_partition_and_readback(tmp_path: Path) -> None:
    from scripts.data_pipeline.materializers.raw_writer import write_raw_by_date_symbol

    df = transactions_to_dataframe(_history_rows(), market=0, code="000001", trade_date="20260617")
    path = write_raw_by_date_symbol(tmp_path, "tdx_transactions", "000001.SZ", df, "20260617")

    assert path == tmp_path / "tdx_transactions" / "date=20260617" / "ts_code=000001.SZ" / "data.parquet"
    assert path.exists()

    # ts_code dropped from the file (it is the partition key) and restored on read
    leaf = pd.read_parquet(path)
    assert "ts_code" not in leaf.columns

    back = pd.read_parquet(tmp_path / "tdx_transactions" / "date=20260617")
    assert len(back) == len(df)
    assert set(back["ts_code"]) == {"000001.SZ"}


# --------------------------------------------------------------------------- #
# job: full-day paging
# --------------------------------------------------------------------------- #
def _txn_rows(n: int) -> list[dict]:
    return [{"time": "09:30", "price": 11.0, "vol": 100, "buyorsell": 0} for _ in range(n)]


def test_run_transaction_job_pages_full_day(tmp_path: Path) -> None:
    from scripts.data_pipeline.jobs.transaction_job import run_transaction_job

    pages = {0: _txn_rows(2000), 2000: _txn_rows(2000), 4000: _txn_rows(539)}
    calls: list[tuple[int, int]] = []

    def fetch_page(start: int, count: int) -> list[dict]:
        calls.append((start, count))
        return pages.get(start, [])

    result = run_transaction_job(
        fetch_page=fetch_page, market=0, code="000001", trade_date="20260617", output_root=tmp_path
    )

    assert result["status"] == "success"
    assert result["ts_code"] == "000001.SZ"
    assert result["trade_date"] == "20260617"
    assert result["records"] == 4539
    assert calls == [(0, 2000), (2000, 2000), (4000, 2000)]
    assert (tmp_path / "tdx_transactions" / "date=20260617" / "ts_code=000001.SZ" / "data.parquet").exists()


def test_run_transaction_job_single_short_page(tmp_path: Path) -> None:
    from scripts.data_pipeline.jobs.transaction_job import run_transaction_job

    def fetch_page(start: int, count: int) -> list[dict]:
        assert start == 0
        return _txn_rows(3)

    result = run_transaction_job(
        fetch_page=fetch_page, market=1, code="600000", trade_date="2026-06-18", output_root=tmp_path
    )
    assert result["ts_code"] == "600000.SH"
    assert result["records"] == 3
