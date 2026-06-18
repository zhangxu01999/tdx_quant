from __future__ import annotations

import pandas as pd


def calc_rsi(
    df: pd.DataFrame,
    periods: tuple[int, ...] = (6, 12, 24),
    *,
    column: str = 'close',
) -> pd.DataFrame:
    delta = df[column].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    out = {}
    for n in periods:
        avg_gain = gain.ewm(alpha=1 / n, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / n, adjust=False).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - 100 / (1 + rs)
        # Where data exists but avg_loss == 0, RSI = 100 (all-up, div-by-zero).
        # .mask keeps warmup rows where avg_loss is NaN as NaN (not 100).
        rsi = rsi.mask(avg_loss.eq(0) & avg_loss.notna(), 100.0)
        out[f'RSI{n}'] = rsi
    return pd.DataFrame(out)


def calc_kdj(
    df: pd.DataFrame,
    n: int = 9,
    m1: int = 3,
    m2: int = 3,
) -> pd.DataFrame:
    low_n = df['low'].rolling(n).min()
    high_n = df['high'].rolling(n).max()
    rsv = (df['close'] - low_n) / (high_n - low_n) * 100
    # flat window (high_n == low_n) naturally yields NaN via 0/0 -> left as-is
    k = rsv.ewm(alpha=1 / m1, adjust=False).mean()
    d = k.ewm(alpha=1 / m2, adjust=False).mean()
    j = 3 * k - 2 * d
    return pd.DataFrame({'K': k, 'D': d, 'J': j})
