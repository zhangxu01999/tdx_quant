"""Real-network integration tests for the pytdx interfaces NOT yet wrapped by
``TdxDownloader``: tick/transaction data, 分时 (minute-time) data, finance
(股本结构), F10 company info, index bars, and security enumeration.

These probe the raw ``TdxHq_API`` to confirm each endpoint is reachable and
returns the expected shape, before we build extractors/normalizers around them.

Run::

    /Users/henrylin/anaconda3/bin/python3 -m pytest tests/test_pytdx_extended_integration.py -q
    /Users/henrylin/anaconda3/bin/python3 -m pytest -q -m "not integration"   # offline gate
"""
from __future__ import annotations

import pandas as pd
import pytest

from scripts.data_pipeline.connectors.pytdx_client import (
    connected_session,
    create_hq_api,
)
from scripts.data_pipeline.tdx_client import TdxDownloader

pytestmark = pytest.mark.integration

SZ, SH = 0, 1
SAMPLE = (SZ, "000001")      # 平安银行
INDEX_SH = (SH, "000001")    # 上证指数


@pytest.fixture(scope="module")
def api():
    a = create_hq_api()
    with connected_session(a) as session:
        yield session


def _recent_completed_trade_date(a) -> int:
    """Second-most-recent daily bar date — guaranteed to be a *finalized* session
    (today's intraday tick/分时 data may still be incomplete)."""
    bars = a.get_security_bars(9, SAMPLE[0], SAMPLE[1], 0, 5)
    dates = [int(pd.to_datetime(b["datetime"]).strftime("%Y%m%d")) for b in bars]
    return dates[-2]


# --------------------------------------------------------------------------- #
# 财务 / 股本结构
# --------------------------------------------------------------------------- #
def test_finance_info_returns_capital_structure(api) -> None:
    fin = api.get_finance_info(SAMPLE[0], SAMPLE[1])
    assert isinstance(fin, dict)
    # pytdx HQ finance is 股本结构 (share capital), not full income statement.
    for key in ("zongguben", "liutongguben", "ipo_date"):
        assert key in fin


# --------------------------------------------------------------------------- #
# 逐笔 / 分笔成交 (tick data)
# --------------------------------------------------------------------------- #
def test_history_transaction_data_has_real_volume(api) -> None:
    date = _recent_completed_trade_date(api)
    rows = api.get_history_transaction_data(SAMPLE[0], SAMPLE[1], 0, 30, date)
    assert rows and isinstance(rows, list)
    sample = rows[0]
    assert {"time", "price", "vol", "buyorsell"} <= set(sample)
    assert any(r["vol"] for r in rows), "all tick rows had vol=0 (expected real volume)"


def test_transaction_data_today_is_reachable(api) -> None:
    # Today's intraday tick may be empty/vol=0 before close — we only assert
    # the endpoint responds without error.
    rows = api.get_transaction_data(SAMPLE[0], SAMPLE[1], 0, 10)
    assert isinstance(rows, list)


# --------------------------------------------------------------------------- #
# 分时数据 (minute-time, one point per session minute)
# --------------------------------------------------------------------------- #
def test_history_minute_time_data_full_session(api) -> None:
    date = _recent_completed_trade_date(api)
    rows = api.get_history_minute_time_data(SAMPLE[0], SAMPLE[1], date)
    assert len(rows) >= 200  # a full A-share session ~= 240 minutes
    assert {"price", "vol"} <= set(rows[0])
    assert any(r["price"] for r in rows)


# --------------------------------------------------------------------------- #
# F10 公司资料
# --------------------------------------------------------------------------- #
def test_company_info_category_and_content(api) -> None:
    cats = api.get_company_info_category(SAMPLE[0], SAMPLE[1])
    assert cats, "no F10 categories returned"
    first = cats[0]
    assert {"name", "filename", "start", "length"} <= set(first)
    # 3rd arg is the shared F10 file; start/length select the section within it.
    content = api.get_company_info_content(
        SAMPLE[0], SAMPLE[1], first["filename"], first["start"], first["length"]
    )
    assert content and len(str(content)) > 0


# --------------------------------------------------------------------------- #
# 指数 K 线 (index bars)
# --------------------------------------------------------------------------- #
def test_index_bars_shanghai_index(api) -> None:
    rows = api.get_index_bars(9, INDEX_SH[0], INDEX_SH[1], 0, 5)
    assert rows and isinstance(rows, list)
    sample = rows[0]
    assert {"open", "high", "low", "close", "datetime"} <= set(sample)


# --------------------------------------------------------------------------- #
# 证券枚举
# --------------------------------------------------------------------------- #
def test_security_enumeration(api) -> None:
    assert api.get_security_count(SZ) > 0
    lst = api.get_security_list(SZ, 0)
    assert lst and {"code", "name"} <= set(lst[0])


# --------------------------------------------------------------------------- #
# end-to-end: TdxDownloader.download_security_list
# --------------------------------------------------------------------------- #
def test_integration_download_security_list_sz(tmp_path) -> None:
    dl = TdxDownloader(data_root=tmp_path)
    df = dl.download_security_list(0)  # SZ

    assert not df.empty
    assert (df["ts_code"].str.endswith(".SZ")).all()
    assert set(df["market"]) == {"SZ"}
    # snapshot persisted to the market+date partition and reads back intact
    on_disk = pd.read_parquet(tmp_path / "security_list" / "market=SZ")
    assert len(on_disk) == len(df)


def test_integration_download_security_list_sh(tmp_path) -> None:
    # SH start=0 is empty (sparse offsets) — guards against the 0-row regression.
    dl = TdxDownloader(data_root=tmp_path)
    df = dl.download_security_list(1)  # SH

    assert len(df) > 1000  # SH has thousands of securities, not 0
    assert (df["ts_code"].str.endswith(".SH")).all()


# --------------------------------------------------------------------------- #
# end-to-end: TdxDownloader.download_tick (历史分笔成交)
# --------------------------------------------------------------------------- #
def test_integration_download_tick(api, tmp_path) -> None:
    dl = TdxDownloader(data_root=tmp_path)
    trade_date = _recent_completed_trade_date(api)

    df = dl.download_tick("000001", trade_date)

    assert not df.empty
    assert (df["ts_code"] == "000001.SZ").all()
    assert (df["trade_date"] == str(trade_date)).all()
    assert df["vol"].gt(0).any()  # completed session has real volume
    # persisted to the date+ts_code partition and reads back intact
    on_disk = pd.read_parquet(tmp_path / "tdx_transactions" / f"date={trade_date}")
    assert len(on_disk) == len(df)


# --------------------------------------------------------------------------- #
# end-to-end: TdxDownloader.download_company_finance (F10 财务分析)
# --------------------------------------------------------------------------- #
def test_integration_download_company_finance(tmp_path) -> None:
    dl = TdxDownloader(data_root=tmp_path)
    df = dl.download_company_finance("000001")

    assert not df.empty
    assert (df["ts_code"] == "000001.SZ").all()
    assert "基本每股收益(元)" in set(df["metric"])
    # EPS is numeric-parseable for at least one period
    eps = df[df["metric"] == "基本每股收益(元)"]
    assert eps["value_num"].notna().any()


# --------------------------------------------------------------------------- #
# end-to-end: TdxDownloader.download_minute_time (分时)
# --------------------------------------------------------------------------- #
def test_integration_download_minute_time(api, tmp_path) -> None:
    dl = TdxDownloader(data_root=tmp_path)
    trade_date = _recent_completed_trade_date(api)

    df = dl.download_minute_time("000001", trade_date)

    assert not df.empty
    assert (df["ts_code"] == "000001.SZ").all()
    assert len(df) >= 200  # a full A-share session ~= 240 minutes
    assert df["price"].gt(0).any()
    on_disk = pd.read_parquet(tmp_path / "minute_time" / f"date={trade_date}")
    assert len(on_disk) == len(df)


# --------------------------------------------------------------------------- #
# end-to-end: TdxDownloader.download_finance_capital (股本结构)
# --------------------------------------------------------------------------- #
def test_integration_download_finance_capital(tmp_path) -> None:
    dl = TdxDownloader(data_root=tmp_path)
    df = dl.download_finance_capital("000001")

    assert len(df) == 1
    assert (df["ts_code"] == "000001.SZ").all()
    assert pd.notna(df.loc[0, "zongguben"])  # total share capital present


# --------------------------------------------------------------------------- #
# end-to-end: TdxDownloader.download_index (上证指数)
# --------------------------------------------------------------------------- #
def test_integration_download_index(tmp_path) -> None:
    dl = TdxDownloader(data_root=tmp_path)
    df = dl.download_index("000001", market=1, max_bars=5)  # 上证指数 (SH)

    assert not df.empty
    assert (df["ts_code"] == "000001.SH").all()
    assert "up_count" in df.columns
    assert (tmp_path / "index_daily" / "ts_code=000001.SH" / "data.parquet").exists()
