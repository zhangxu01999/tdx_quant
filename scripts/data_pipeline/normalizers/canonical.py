from __future__ import annotations

import pandas as pd


CANONICAL_QUOTE_COLUMNS = [
    'ts_code',
    'trade_date',
    'price',
    'source',
]

CANONICAL_DAILY_BAR_COLUMNS = [
    'ts_code',
    'trade_date',
    'open',
    'high',
    'low',
    'close',
    'vol',
    'amount',
    'source',
]

CANONICAL_MINUTE_BAR_COLUMNS = [
    'ts_code',
    'trade_date',
    'trade_time',
    'open',
    'high',
    'low',
    'close',
    'vol',
    'amount',
    'frequency',
    'source',
]

CANONICAL_FINANCE_SNAPSHOT_COLUMNS = [
    'ts_code',
    'report_period',
    'total_assets',
    'source',
]

CANONICAL_CORPORATE_ACTION_COLUMNS = [
    'ts_code',
    'trade_date',
    'action_category',
    'cash_dividend',
    'source',
]


def to_canonical_quotes(raw_df: pd.DataFrame, source: str, trade_date: str | None = None) -> pd.DataFrame:
    df = raw_df.copy()
    if trade_date is not None and 'trade_date' not in df.columns:
        df['trade_date'] = trade_date
    df['source'] = source
    return df[CANONICAL_QUOTE_COLUMNS]


def to_canonical_daily_bars(raw_df: pd.DataFrame, source: str) -> pd.DataFrame:
    df = raw_df.copy()
    df['source'] = source
    return df[CANONICAL_DAILY_BAR_COLUMNS]


def to_canonical_minute_bars(raw_df: pd.DataFrame, source: str, frequency: int) -> pd.DataFrame:
    df = raw_df.copy()
    timestamps = pd.to_datetime(df['datetime'])
    df['trade_date'] = timestamps.dt.strftime('%Y%m%d')
    df['trade_time'] = timestamps.dt.strftime('%H:%M:%S')
    df['frequency'] = int(frequency)
    df['source'] = source
    return df[CANONICAL_MINUTE_BAR_COLUMNS]


def to_canonical_corporate_actions(raw_df: pd.DataFrame, source: str) -> pd.DataFrame:
    df = raw_df.copy()
    df = df.rename(columns={'category': 'action_category', 'fenhong': 'cash_dividend'})
    df['source'] = source
    return df[CANONICAL_CORPORATE_ACTION_COLUMNS]


def to_canonical_finance_snapshot(raw_df: pd.DataFrame, source: str) -> pd.DataFrame:
    df = raw_df.copy()
    df = df.rename(columns={'end_date': 'report_period'})
    df['source'] = source
    return df[CANONICAL_FINANCE_SNAPSHOT_COLUMNS]
