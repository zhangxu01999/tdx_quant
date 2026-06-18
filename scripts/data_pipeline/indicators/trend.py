from __future__ import annotations

import pandas as pd


def calc_ma(
    df: pd.DataFrame,
    periods: tuple[int, ...] = (5, 10, 20, 60),
    *,
    column: str = 'close',
) -> pd.DataFrame:
    price = df[column]
    return pd.DataFrame({f'MA{n}': price.rolling(n).mean() for n in periods})


def calc_ema(
    df: pd.DataFrame,
    periods: tuple[int, ...] = (5, 10, 20, 60),
    *,
    column: str = 'close',
) -> pd.DataFrame:
    price = df[column]
    return pd.DataFrame({f'EMA{n}': price.ewm(span=n, adjust=False).mean() for n in periods})


def calc_macd(
    df: pd.DataFrame,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
    *,
    column: str = 'close',
) -> pd.DataFrame:
    price = df[column]
    ema_fast = price.ewm(span=fast, adjust=False).mean()
    ema_slow = price.ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    macd = (dif - dea) * 2
    return pd.DataFrame({'DIF': dif, 'DEA': dea, 'MACD': macd})
