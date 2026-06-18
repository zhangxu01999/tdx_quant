from __future__ import annotations

from pathlib import Path

from scripts.data_pipeline.extractors.tdx_bars import bars_to_dataframe
from scripts.data_pipeline.materializers.canonical_writer import write_canonical_by_date
from scripts.data_pipeline.materializers.raw_writer import write_raw_by_date
from scripts.data_pipeline.normalizers.canonical import to_canonical_daily_bars


def run_daily_bars_job(*, payload, market: int, code: str, trade_date: str, output_root: Path, source: str = 'pytdx') -> dict:
    raw_df = bars_to_dataframe(payload, market=market, code=code)
    raw_path = write_raw_by_date(output_root, 'tdx_bars_1d', raw_df, trade_date)
    canonical_df = to_canonical_daily_bars(raw_df, source=source)
    canonical_path = write_canonical_by_date(output_root, 'canonical_daily_bars', canonical_df, trade_date)
    return {
        'status': 'success',
        'raw_path': str(raw_path),
        'canonical_path': str(canonical_path),
        'records': len(canonical_df),
    }
