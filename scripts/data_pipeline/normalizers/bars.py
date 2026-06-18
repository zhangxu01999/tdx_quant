import pandas as pd

from scripts.data_pipeline.code_mapping import market_code_to_ts_code


REQUIRED_BAR_COLUMNS = [
    'ts_code',
    'trade_date',
    'open',
    'high',
    'low',
    'close',
    'vol',
    'amount',
]


def normalize_tdx_daily_bars(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()
    df['ts_code'] = df.apply(lambda row: market_code_to_ts_code(int(row['market']), str(row['code'])), axis=1)
    dt = pd.to_datetime(df['datetime'])
    df['trade_date'] = dt.dt.strftime('%Y%m%d')
    return df[REQUIRED_BAR_COLUMNS]
