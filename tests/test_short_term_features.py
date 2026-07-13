"""短线日频增强特征生成测试。"""

from pathlib import Path

import pandas as pd

from scripts.data_pipeline.short_term_features import build_short_term_daily_features, write_short_term_daily_features


def _write_daily(root: Path) -> None:
    leaf = root / "daily" / "ts_code=000001.SZ"
    leaf.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "trade_date": "20260101",
                "code": "000001",
                "open": 10.0,
                "high": 10.2,
                "low": 9.8,
                "close": 10.0,
                "vol": 100.0,
                "amount": 100_000.0,
            },
            {
                "trade_date": "20260102",
                "code": "000001",
                "open": 10.5,
                "high": 11.0,
                "low": 10.4,
                "close": 11.0,
                "vol": 200.0,
                "amount": 220_000.0,
            },
            {
                "trade_date": "20260103",
                "code": "000001",
                "open": 10.8,
                "high": 12.1,
                "low": 10.7,
                "close": 11.6,
                "vol": 300.0,
                "amount": 340_000.0,
            },
        ]
    ).to_parquet(leaf / "data.parquet", index=False)


def _write_capital(root: Path) -> None:
    leaf = root / "finance_capital" / "ts_code=000001.SZ"
    leaf.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "liutongguben": 1_000_000.0,
                "updated_date": "20251231",
            }
        ]
    ).to_parquet(leaf / "data.parquet", index=False)


def test_build_short_term_features_derives_turnover_market_cap_and_limit_state(tmp_path: Path) -> None:
    _write_daily(tmp_path)
    _write_capital(tmp_path)

    frame = build_short_term_daily_features(tmp_path)
    second = frame[frame["trade_date"] == "20260102"].iloc[0]
    third = frame[frame["trade_date"] == "20260103"].iloc[0]

    assert second["ts_code"] == "000001.SZ"
    assert second["amount"] == 220_000.0
    assert second["turnover_rate"] == 0.02
    assert second["float_market_cap"] == 11_000_000.0
    assert bool(second["limit_up"])
    assert bool(second["hit_limit_up"])
    assert not bool(second["bomb_limit_up"])
    assert bool(third["hit_limit_up"])
    assert not bool(third["limit_up"])
    assert bool(third["bomb_limit_up"])


def test_write_short_term_features_partitions_by_symbol(tmp_path: Path) -> None:
    _write_daily(tmp_path)

    frame = write_short_term_daily_features(tmp_path)
    output = tmp_path / "short_term_daily" / "ts_code=000001.SZ" / "data.parquet"

    assert len(frame) == 3
    assert output.exists()
    written = pd.read_parquet(output)
    assert "ts_code" not in written.columns
    assert "turnover_rate" in written.columns
