from __future__ import annotations

from pathlib import Path

from scripts.data_pipeline.extractors.tdx_quotes import quotes_to_dataframe
from scripts.data_pipeline.materializers.canonical_writer import write_canonical_by_date
from scripts.data_pipeline.materializers.raw_writer import write_raw_by_date
from scripts.data_pipeline.normalizers.canonical import to_canonical_quotes


def run_quotes_job(*, payload, trade_date: str, output_root: Path, source: str = 'pytdx') -> dict:
    raw_df = quotes_to_dataframe(payload)
    raw_df['trade_date'] = trade_date
    raw_path = write_raw_by_date(output_root, 'tdx_quotes_snapshot', raw_df, trade_date)
    canonical_df = to_canonical_quotes(raw_df, source=source, trade_date=trade_date)
    canonical_path = write_canonical_by_date(output_root, 'canonical_quotes', canonical_df, trade_date)
    return {
        'status': 'success',
        'raw_path': str(raw_path),
        'canonical_path': str(canonical_path),
        'records': len(canonical_df),
    }
