from __future__ import annotations

import pandas as pd

from scripts.data_pipeline.code_mapping import market_code_to_ts_code

CATEGORY_TO_TABLE = {
    9: 'tdx_bars_1d',
    0: 'tdx_bars_5m',
    1: 'tdx_bars_15m',
    2: 'tdx_bars_30m',
    3: 'tdx_bars_60m',
}

BAR_DATAFRAME_COLUMNS = [
    'datetime',
    'open',
    'high',
    'low',
    'close',
    'vol',
    'amount',
    'market',
    'code',
    'ts_code',
    'trade_date',
]


def category_to_table_name(category: int) -> str:
    return CATEGORY_TO_TABLE[category]


def bars_to_dataframe(payload, market: int, code: str) -> pd.DataFrame:
    df = pd.DataFrame(list(payload))
    if df.empty:
        return pd.DataFrame(columns=BAR_DATAFRAME_COLUMNS)
    df['market'] = int(market)
    df['code'] = str(code)
    df['ts_code'] = market_code_to_ts_code(int(market), str(code))
    df['trade_date'] = pd.to_datetime(df['datetime']).dt.strftime('%Y%m%d')
    return df
