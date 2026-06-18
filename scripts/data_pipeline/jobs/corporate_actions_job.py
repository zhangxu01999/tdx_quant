from __future__ import annotations

from pathlib import Path

from scripts.data_pipeline.extractors.tdx_xdxr import normalize_xdxr_rows
from scripts.data_pipeline.materializers.canonical_writer import write_canonical_by_date
from scripts.data_pipeline.materializers.raw_writer import write_raw_by_date
from scripts.data_pipeline.normalizers.canonical import to_canonical_corporate_actions


def run_corporate_actions_job(*, payload, trade_date: str, output_root: Path, market: int | None = None, code: str | None = None, source: str = 'pytdx') -> dict:
    raw_df = normalize_xdxr_rows(payload, market=market, code=code)
    raw_path = write_raw_by_date(output_root, 'tdx_xdxr', raw_df, trade_date)
    canonical_df = to_canonical_corporate_actions(raw_df, source=source)
    canonical_path = write_canonical_by_date(output_root, 'canonical_corporate_actions', canonical_df, trade_date)
    return {
        'status': 'success',
        'raw_path': str(raw_path),
        'canonical_path': str(canonical_path),
        'records': len(canonical_df),
    }
