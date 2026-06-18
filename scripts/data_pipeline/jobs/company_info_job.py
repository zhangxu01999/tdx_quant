from __future__ import annotations

from collections.abc import Callable
from datetime import date
from pathlib import Path

import pandas as pd

from scripts.data_pipeline.code_mapping import market_code_to_ts_code
from scripts.data_pipeline.extractors.tdx_company_info import (
    extract_updated_date,
    parse_finance_indicators,
)
from scripts.data_pipeline.materializers.symbol_writer import write_by_symbol

FINANCE_SECTION = '财务分析'


def run_company_finance_job(
    *,
    fetch_category: Callable[[], list[dict]],
    fetch_content: Callable[[str, int, int], str],
    market: int,
    code: str,
    output_root: Path,
    snapshot_date: str | None = None,
) -> dict:
    """Fetch the F10 ``财务分析`` section, parse its 主要财务指标 table, and persist
    the structured indicators (``company_finance``) plus the raw section text
    (``company_info_raw``) — both per-symbol, latest-snapshot-wins.

    ``fetch_category()`` returns the category list; ``fetch_content(filename,
    start, length)`` returns a section's text. Injected so the orchestration is
    unit-testable offline.
    """
    cats = list(fetch_category())
    section = next((c for c in cats if c.get('name') == FINANCE_SECTION), None)
    if section is None:
        raise ValueError(f'no {FINANCE_SECTION!r} F10 section for {code!r}')

    text = fetch_content(section['filename'], section['start'], section['length'])
    df = parse_finance_indicators(text, market=market, code=code)
    ts_code = market_code_to_ts_code(int(market), str(code))
    asof = snapshot_date or extract_updated_date(text) or date.today().strftime('%Y-%m-%d')

    df['asof_date'] = asof
    write_by_symbol(output_root, 'company_finance', df)

    raw_df = pd.DataFrame([
        {'section': FINANCE_SECTION, 'text': text, 'asof_date': asof, 'ts_code': ts_code}
    ])
    write_by_symbol(output_root, 'company_info_raw', raw_df)

    return {
        'status': 'success',
        'ts_code': ts_code,
        'asof_date': asof,
        'records': len(df),
    }
