from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from scripts.data_pipeline.code_mapping import market_code_to_ts_code
from scripts.data_pipeline.extractors.tdx_transactions import transactions_to_dataframe
from scripts.data_pipeline.materializers.raw_writer import write_raw_by_date_symbol

# HQ history-transaction server caps each call at 2000 rows.
PAGE_SIZE = 2000


def run_transaction_job(
    *,
    fetch_page: Callable[[int, int], list[dict]],
    market: int,
    code: str,
    trade_date: str,
    output_root: Path,
    page_size: int = PAGE_SIZE,
) -> dict:
    """Page a symbol's full-day 分笔成交 (``fetch_page(start, count)``) until a
    short/empty page signals the end, then persist to a date+ts_code partition.
    """
    rows: list[dict] = []
    start = 0
    while True:
        page = list(fetch_page(start, page_size))
        rows.extend(page)
        if len(page) < page_size:
            break
        start += page_size

    df = transactions_to_dataframe(rows, market=market, code=code, trade_date=trade_date)
    ts_code = market_code_to_ts_code(int(market), str(code))
    path = write_raw_by_date_symbol(output_root, 'tdx_transactions', ts_code, df, trade_date)
    return {
        'status': 'success',
        'ts_code': ts_code,
        'trade_date': trade_date.replace('-', ''),
        'records': len(df),
        'path': str(path),
    }
