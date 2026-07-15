"""数据终端股票搜索及单股详情服务测试。"""

from pathlib import Path

import pandas as pd

from frontend.server import MarketTerminalService


def _write_partition(root: Path, domain: str, symbol: str, frame: pd.DataFrame) -> None:
    path = root / domain / f"ts_code={symbol}" / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _fixture_data(root: Path) -> None:
    security = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "Ping An Bank",
                "ts_code": "000001.SZ",
                "market": "SZ",
                "date": "20260714",
                "pre_close": 11.00,
            },
            {
                "code": "000002",
                "name": "万 科Ａ",
                "ts_code": "000002.SZ",
                "market": "SZ",
                "date": "20260714",
                "pre_close": 3.03,
            },
            {
                "code": "600000",
                "name": "Shanghai Pudong Development Bank",
                "ts_code": "600000.SH",
                "market": "SH",
                "date": "20260714",
                "pre_close": 10.00,
            },
        ]
    )
    security_path = root / "security_list" / "market=SZ" / "date=20260714" / "data.parquet"
    security_path.parent.mkdir(parents=True, exist_ok=True)
    security.loc[security["market"] == "SZ"].to_parquet(security_path, index=False)
    sh_security_path = root / "security_list" / "market=SH" / "date=20260714" / "data.parquet"
    sh_security_path.parent.mkdir(parents=True, exist_ok=True)
    security.loc[security["market"] == "SH"].to_parquet(sh_security_path, index=False)

    dates = pd.bdate_range("2026-01-01", periods=80)
    close = pd.Series([3 + index * 0.01 for index in range(len(dates))])
    bars = pd.DataFrame(
        {
            "datetime": dates,
            "trade_date": dates.strftime("%Y%m%d"),
            "code": "000002",
            "open": close - 0.01,
            "high": close + 0.03,
            "low": close - 0.03,
            "close": close,
            "vol": 100_000,
            "amount": close * 10_000_000,
        }
    )
    _write_partition(root, "daily", "000002.SZ", bars)
    _write_partition(
        root,
        "finance_capital",
        "000002.SZ",
        pd.DataFrame(
            [{"liutongguben": 1_000_000_000, "updated_date": "20260430"}]
        ),
    )
    _write_partition(
        root,
        "short_term_daily",
        "000002.SZ",
        pd.DataFrame(
            [
                {
                    "trade_date": dates[-1].strftime("%Y%m%d"),
                    "turnover_rate": 0.01,
                    "float_market_cap": float(close.iloc[-1] * 1_000_000_000),
                    "volume_ratio": 1.5,
                }
            ]
        ),
    )
    index_dates = pd.bdate_range("2026-06-01", periods=30)
    for symbol, base in (("000001.SH", 3_800), ("399001.SZ", 14_000)):
        index_close = pd.Series([base + index for index in range(len(index_dates))])
        _write_partition(
            root,
            "index_daily",
            symbol,
            pd.DataFrame(
                {
                    "datetime": index_dates,
                    "trade_date": index_dates.strftime("%Y%m%d"),
                    "open": index_close - 1,
                    "high": index_close + 2,
                    "low": index_close - 2,
                    "close": index_close,
                    "vol": 1_000_000,
                    "amount": 1_000_000_000,
                    "up_count": 1_200,
                    "down_count": 900,
                }
            ),
        )
    _write_partition(root, "minute_5m", "000002.SZ", bars.tail(20))

    transactions = pd.DataFrame(
        [
            {"time": "09:31", "price": 3.70, "vol": 100, "buyorsell_label": "buy", "trade_date": "20260714"},
            {"time": "09:32", "price": 3.71, "vol": 80, "buyorsell_label": "sell", "trade_date": "20260714"},
        ]
    )
    transaction_path = root / "tdx_transactions" / "date=20260714" / "ts_code=000002.SZ" / "data.parquet"
    transaction_path.parent.mkdir(parents=True, exist_ok=True)
    transactions.to_parquet(transaction_path, index=False)
    minute_time_path = root / "minute_time" / "date=20260714" / "ts_code=000002.SZ" / "data.parquet"
    minute_time_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"minute_idx": 1, "price": 3.70, "vol": 100}]).to_parquet(minute_time_path, index=False)
    _write_partition(
        root,
        "company_finance",
        "000002.SZ",
        pd.DataFrame(
            [
                {"metric": "基本每股收益(元)", "period": "2025-12-31", "value_num": 0.30},
                {"metric": "净利润(元)", "period": "2025-12-31", "value_num": 1_000_000},
            ]
        ),
    )
    _write_partition(
        root,
        "company_info_raw",
        "000002.SZ",
        pd.DataFrame([{"section": "公司概况", "text": "万科公司资料"}]),
    )


def test_search_symbols_ignores_spaces_in_tdx_names(tmp_path: Path) -> None:
    _fixture_data(tmp_path)
    result = MarketTerminalService(tmp_path).search_symbols("万科")

    assert result["count"] == 1
    assert result["items"][0] == {
        "code": "000002",
        "name": "万科Ａ",
        "ts_code": "000002.SZ",
        "market": "SZ",
    }


def test_search_symbols_supports_pagination(tmp_path: Path) -> None:
    _fixture_data(tmp_path)
    service = MarketTerminalService(tmp_path)

    first = service.search_symbols("", limit=2, offset=0)
    second = service.search_symbols("", limit=2, offset=2)

    assert first["count"] == 2
    assert first["has_more"] is True
    assert [item["ts_code"] for item in first["items"]] == ["000001.SZ", "000002.SZ"]
    assert second["count"] == 1
    assert second["has_more"] is False
    assert [item["ts_code"] for item in second["items"]] == ["600000.SH"]


def test_stock_detail_combines_bars_indicators_and_short_term_fields(tmp_path: Path) -> None:
    _fixture_data(tmp_path)
    detail = MarketTerminalService(tmp_path).stock_detail("000002.SZ", limit=80)

    assert detail["ts_code"] == "000002.SZ"
    assert detail["name"] == "万科Ａ"
    assert detail["bars"] == 80
    assert len(detail["dates"]) == 80
    assert detail["latest"]["ma60"] is not None
    assert detail["latest"]["turnover_rate"] == 0.01
    assert detail["latest"]["float_shares"] == 1_000_000_000
    assert detail["latest"]["volume_ratio"] == 1.5


def test_linked_stock_resources_and_market_overview(tmp_path: Path) -> None:
    _fixture_data(tmp_path)
    service = MarketTerminalService(tmp_path)

    overview = service.market_overview(limit=30)
    minute = service.minute_detail("000002.SZ", limit=20)
    ticks = service.ticks_detail("000002.SZ")
    fundamentals = service.fundamentals_detail("000002.SZ")

    assert overview["universe"] == {"total": 3, "SH": 1, "SZ": 2}
    assert len(overview["indices"]) == 2
    assert minute["ts_code"] == "000002.SZ"
    assert minute["timeframes"] == ["5m"]
    assert ticks["ts_code"] == "000002.SZ"
    assert ticks["distribution"]["buy"] == 1
    assert fundamentals["ts_code"] == "000002.SZ"
    assert fundamentals["company_info"] == "万科公司资料"


def test_frontend_uses_one_global_stock_selection_for_all_tabs() -> None:
    frontend_root = Path(__file__).parents[1] / "frontend"
    app = (frontend_root / "app.js").read_text(encoding="utf-8")
    index = (frontend_root / "index.html").read_text(encoding="utf-8")

    assert "/api/market/overview?limit=240" in app
    assert "loadStockWorkspace" in app
    assert "/api/stocks/${encoded}/${resource}" in app
    for resource in ("minute", "ticks", "fundamentals"):
        assert f"'{resource}'" in app
    assert "clearTimeout(searchTimer);" in app
    assert "limit=100&offset=${offset}" in app
    assert "stockSearchState.hasMore" in app
    assert "Promise.allSettled" in app
    assert "全局股票联动" in index
    assert 'id="min-sym"' not in index
    assert 'rel="icon" href="data:,"' in index
