"""TDX 交易日历同步的离线构造测试。"""

from pathlib import Path

import pandas as pd

from scripts.data_pipeline.trading_calendar_sync_cli import build_calendar


def test_build_calendar_marks_missing_natural_days_closed(tmp_path: Path):
    path = tmp_path / "index.parquet"
    pd.DataFrame({"trade_date": ["20260803", "20260804", "20260806"]}).to_parquet(path)
    rows = build_calendar(path, 30)
    by_date = {row["tradeDate"]: row["tradingDay"] for row in rows}
    assert by_date["2026-08-03"] is True
    assert by_date["2026-08-05"] is False
    assert by_date["2026-08-06"] is True
