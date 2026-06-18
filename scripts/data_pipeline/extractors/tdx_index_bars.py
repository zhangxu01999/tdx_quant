from __future__ import annotations

import pandas as pd

from scripts.data_pipeline.code_mapping import market_code_to_ts_code

INDEX_BAR_COLUMNS = [
    'datetime',
    'open',
    'high',
    'low',
    'close',
    'vol',
    'amount',
    'up_count',
    'down_count',
    'market',
    'code',
    'ts_code',
    'trade_date',
]


def index_bars_to_dataframe(payload, *, market: int, code: str) -> pd.DataFrame:
    """Normalize a ``get_index_bars`` payload into a typed DataFrame.

    Same shape as equity bars plus ``up_count`` / ``down_count`` (涨/跌家数).
    """
    df = pd.DataFrame(list(payload))
    if df.empty:
        return pd.DataFrame(columns=INDEX_BAR_COLUMNS)
    df['market'] = int(market)
    df['code'] = str(code)
    df['ts_code'] = market_code_to_ts_code(int(market), str(code))
    df['trade_date'] = pd.to_datetime(df['datetime']).dt.strftime('%Y%m%d')
    return df[INDEX_BAR_COLUMNS]
