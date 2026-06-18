from __future__ import annotations

import pandas as pd

from scripts.data_pipeline.code_mapping import market_code_to_ts_code


def build_quote_records(payload):
    return [dict(item) for item in payload]


def quotes_to_dataframe(payload) -> pd.DataFrame:
    df = pd.DataFrame(build_quote_records(payload))
    if df.empty:
        return df
    if 'market' in df.columns and 'code' in df.columns:
        df['ts_code'] = df.apply(lambda row: market_code_to_ts_code(int(row['market']), str(row['code'])), axis=1)
    return df
