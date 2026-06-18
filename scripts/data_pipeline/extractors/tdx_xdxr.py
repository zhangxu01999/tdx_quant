from __future__ import annotations

import pandas as pd

from scripts.data_pipeline.code_mapping import market_code_to_ts_code


def normalize_xdxr_rows(rows, *, market: int | None = None, code: str | None = None) -> pd.DataFrame:
    df = pd.DataFrame(list(rows))
    if df.empty:
        return df
    if 'market' not in df.columns:
        if market is None:
            raise KeyError('market')
        df['market'] = int(market)
    if 'code' not in df.columns:
        if code is None:
            raise KeyError('code')
        df['code'] = str(code)
    df['ts_code'] = df.apply(lambda row: market_code_to_ts_code(int(row['market']), str(row['code'])), axis=1)
    df['trade_date'] = (
        df['year'].astype(int).astype(str).str.zfill(4)
        + df['month'].astype(int).astype(str).str.zfill(2)
        + df['day'].astype(int).astype(str).str.zfill(2)
    )
    return df
