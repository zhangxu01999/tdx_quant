from __future__ import annotations

import pandas as pd


def calc_vol_ma(
    df: pd.DataFrame,
    periods: tuple[int, ...] = (5, 10),
    *,
    column: str = 'vol',
) -> pd.DataFrame:
    vol = df[column]
    return pd.DataFrame({f'VOL_MA{n}': vol.rolling(n).mean() for n in periods})


def calc_volume_ratio(
    df: pd.DataFrame,
    n: int = 5,
    *,
    column: str = 'vol',
) -> pd.Series:
    # numerator = today's vol; denominator = mean of the PREVIOUS n days.
    # A zero prior-window mean (suspended/illiquid days) is undefined -> NaN,
    # not inf, so it doesn't poison downstream volume_breakout screening.
    denom = df[column].shift(1).rolling(n).mean()
    ratio = df[column] / denom.where(denom.ne(0))
    ratio.name = 'VOL_RATIO'
    return ratio


def calc_turnover(
    df: pd.DataFrame,
    shares: float | int | None = None,
    *,
    column: str = 'vol',
) -> pd.Series:
    if shares is None:
        # no float-share data -> mark N/A (plan spec)
        turnover = pd.Series(float('nan'), index=df.index)
    else:
        turnover = df[column] / shares
    turnover.name = 'TURNOVER_RATE'
    return turnover
