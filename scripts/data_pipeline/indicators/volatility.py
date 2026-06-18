from __future__ import annotations

import pandas as pd


def calc_boll(
    df: pd.DataFrame,
    n: int = 20,
    k: float = 2,
    *,
    column: str = 'close',
) -> pd.DataFrame:
    price = df[column]
    mb = price.rolling(n).mean()
    std = price.rolling(n).std(ddof=0)
    up = mb + k * std
    dn = mb - k * std
    return pd.DataFrame({'BOLL_MB': mb, 'BOLL_UP': up, 'BOLL_DN': dn})


def calc_atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    prev_close = df['close'].shift(1)
    tr = pd.concat(
        [
            df['high'] - df['low'],
            (df['high'] - prev_close).abs(),
            (df['low'] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    # Row 0 has no prev_close, so tr[0] = high-low (a single-bar TR) that seeds
    # the Wilder ATR; this is standard 通达信 behavior, not a full n-bar ATR.
    atr = tr.ewm(alpha=1 / n, adjust=False).mean()
    atr.name = 'ATR'
    return atr
