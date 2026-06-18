from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from scripts.data_pipeline.code_mapping import hq_market_label
from scripts.data_pipeline.extractors.tdx_security_list import security_list_to_dataframe
from scripts.data_pipeline.materializers.raw_writer import write_raw_by_market_date

PAGE_SIZE = 1000


def run_security_list_job(
    *,
    fetch_page: Callable[[int, int], list[dict]],
    fetch_count: Callable[[], int],
    market: int,
    trade_date: str,
    output_root: Path,
    page_size: int = PAGE_SIZE,
) -> dict:
    """Page a market's full security list and persist a daily snapshot.

    ``get_security_list``'s offset space is SPARSE — empty pages occur mid-range
    (and SH even has an empty ``start=0``), so we page ``[0, count)`` stepping by
    ``page_size`` and SKIP empty pages rather than terminating on the first one.
    ``fetch_count()`` returns ``get_security_count(market)`` as the upper bound.
    """
    total = int(fetch_count())
    rows: list[dict] = []
    start = 0
    while start < total:
        page = list(fetch_page(start, page_size))
        if page:
            rows.extend(page)
        start += page_size

    df = security_list_to_dataframe(rows, market=market)
    label = hq_market_label(market)
    path = write_raw_by_market_date(output_root, 'security_list', label, df, trade_date)
    return {
        'status': 'success',
        'market': label,
        'records': len(df),
        'path': str(path),
    }
