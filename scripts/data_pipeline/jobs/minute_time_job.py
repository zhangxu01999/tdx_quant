from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from scripts.data_pipeline.code_mapping import market_code_to_ts_code
from scripts.data_pipeline.extractors.tdx_minute_time import minute_time_to_dataframe
from scripts.data_pipeline.materializers.raw_writer import write_raw_by_date_symbol


def run_minute_time_job(
    *,
    fetch: Callable[[], list[dict]],
    market: int,
    code: str,
    trade_date: str,
    output_root: Path,
) -> dict:
    """Fetch a symbol's full-session 分时 (single call returns all ~240 points)
    and persist to ``data/minute_time/date=<YYYYMMDD>/ts_code=<...>/``."""
    rows = list(fetch())
    df = minute_time_to_dataframe(rows, market=market, code=code, trade_date=trade_date)
    ts_code = market_code_to_ts_code(int(market), str(code))
    path = write_raw_by_date_symbol(output_root, 'minute_time', ts_code, df, trade_date)
    return {
        'status': 'success',
        'ts_code': ts_code,
        'trade_date': trade_date.replace('-', ''),
        'records': len(df),
        'path': str(path),
    }
