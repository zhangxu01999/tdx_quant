from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.data_pipeline.materializers.canonical_writer import write_canonical_by_date
from scripts.data_pipeline.materializers.raw_writer import write_raw_by_date
from scripts.data_pipeline.normalizers.canonical import to_canonical_finance_snapshot


def run_financial_statements_job(*, payload, trade_date: str, output_root: Path, source: str = 'tushare') -> dict:
    raw_df = pd.DataFrame(list(payload))
    raw_path = write_raw_by_date(output_root, 'tushare_financial_statements', raw_df, trade_date)
    canonical_df = to_canonical_finance_snapshot(raw_df, source=source)
    canonical_path = write_canonical_by_date(output_root, 'canonical_finance_snapshot', canonical_df, trade_date)
    return {
        'status': 'success',
        'raw_path': str(raw_path),
        'canonical_path': str(canonical_path),
        'records': len(canonical_df),
    }
