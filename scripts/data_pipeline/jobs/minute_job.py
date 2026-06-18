from __future__ import annotations

from pathlib import Path

from scripts.data_pipeline.extractors.tdx_bars import bars_to_dataframe, category_to_table_name
from scripts.data_pipeline.materializers.canonical_writer import write_canonical_by_date
from scripts.data_pipeline.materializers.raw_writer import write_raw_by_date
from scripts.data_pipeline.normalizers.canonical import to_canonical_minute_bars

FREQUENCY_TO_CATEGORY = {
    5: 0,
    15: 1,
    30: 2,
    60: 3,
}


def minute_frequency_to_category(frequency: int) -> int:
    try:
        return FREQUENCY_TO_CATEGORY[int(frequency)]
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f'Unsupported minute frequency: {frequency}') from exc


def run_minute_bars_job(
    *,
    payload,
    market: int,
    code: str,
    trade_date: str,
    frequency: int,
    output_root: Path,
    source: str = 'pytdx',
) -> dict:
    category = minute_frequency_to_category(frequency)
    raw_table_name = category_to_table_name(category)
    raw_df = bars_to_dataframe(payload, market=market, code=code)
    raw_path = write_raw_by_date(output_root, raw_table_name, raw_df, trade_date)
    canonical_df = to_canonical_minute_bars(raw_df, source=source, frequency=frequency)
    canonical_table_name = 'canonical_minute_bars'
    canonical_path = write_canonical_by_date(output_root, canonical_table_name, canonical_df, trade_date)
    return {
        'status': 'success',
        'raw_table_name': raw_table_name,
        'canonical_table_name': canonical_table_name,
        'raw_path': str(raw_path),
        'canonical_path': str(canonical_path),
        'records': len(canonical_df),
        'frequency': int(frequency),
    }
