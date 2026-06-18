from __future__ import annotations

import pandas as pd

from scripts.data_pipeline.code_mapping import market_code_to_ts_code

CAPITAL_COLUMNS = [
    'ts_code',
    'market',
    'code',
    'zongguben',
    'liutongguben',
    'guojiagu',
    'faqirenfarengu',
    'farengu',
    'bgu',
    'ipo_date',
    'industry',
    'province',
    'updated_date',
]


def finance_info_to_dataframe(payload: dict, *, market: int, code: str) -> pd.DataFrame:
    """Normalize a ``get_finance_info`` dict (HQ 股本结构 snapshot) into a
    single-row DataFrame. Missing keys become NaN."""
    row = dict(payload or {})
    row['market'] = int(market)
    row['code'] = str(code)
    row['ts_code'] = market_code_to_ts_code(int(market), str(code))
    return pd.DataFrame([row], columns=CAPITAL_COLUMNS)
