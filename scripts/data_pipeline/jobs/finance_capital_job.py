from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from scripts.data_pipeline.code_mapping import market_code_to_ts_code
from scripts.data_pipeline.extractors.tdx_finance import finance_info_to_dataframe
from scripts.data_pipeline.materializers.symbol_writer import write_by_symbol


def run_finance_capital_job(
    *,
    fetch: Callable[[], dict],
    market: int,
    code: str,
    output_root: Path,
) -> dict:
    """Fetch a symbol's HQ 股本结构 snapshot (``get_finance_info``) and persist
    a single-row snapshot to ``data/finance_capital/ts_code=<...>/``."""
    payload = fetch()
    df = finance_info_to_dataframe(payload, market=market, code=code)
    ts_code = market_code_to_ts_code(int(market), str(code))
    write_by_symbol(output_root, 'finance_capital', df)
    return {
        'status': 'success',
        'ts_code': ts_code,
        'records': len(df),
    }
