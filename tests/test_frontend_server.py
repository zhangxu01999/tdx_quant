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
                "code": "000002",
                "name": "万 科Ａ",
                "ts_code": "000002.SZ",
                "market": "SZ",
                "date": "20260714",
                "pre_close": 3.03,
            }
        ]
    )
    security_path = root / "security_list" / "market=SZ" / "date=20260714" / "data.parquet"
    security_path.parent.mkdir(parents=True, exist_ok=True)
    security.to_parquet(security_path, index=False)

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
