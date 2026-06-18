from __future__ import annotations

import pandas as pd

from scripts.data_pipeline.indicators.momentum import calc_kdj, calc_rsi
from scripts.data_pipeline.indicators.trend import calc_ema, calc_ma, calc_macd
from scripts.data_pipeline.indicators.volatility import calc_atr, calc_boll
from scripts.data_pipeline.indicators.volume import (
    calc_turnover,
    calc_vol_ma,
    calc_volume_ratio,
)

# Registry: short name -> calc callable. Every calc returns either a named
# pd.Series or a DataFrame with unique column names, so attaching all of them
# to one frame never collides.
INDICATORS = {
    'ma': calc_ma,
    'ema': calc_ema,
    'macd': calc_macd,
    'rsi': calc_rsi,
    'kdj': calc_kdj,
    'boll': calc_boll,
    'atr': calc_atr,
    'vol_ma': calc_vol_ma,
    'vol_ratio': calc_volume_ratio,
    'turnover': calc_turnover,
}

# Per-indicator kwargs by timeframe. Daily = standard 通达信 defaults;
# minute = shorter windows (fewer bars per signal).
# 'turnover' is intentionally absent: it takes `shares`, not period kwargs.
DAILY_CONFIG = {
    'ma': {'periods': (5, 10, 20, 60)},
    'ema': {'periods': (5, 10, 20, 60)},
    'macd': {'fast': 12, 'slow': 26, 'signal': 9},
    'rsi': {'periods': (6, 12, 24)},
    'kdj': {'n': 9, 'm1': 3, 'm2': 3},
    'boll': {'n': 20, 'k': 2},
    'atr': {'n': 14},
    'vol_ma': {'periods': (5, 10)},
    'vol_ratio': {'n': 5},
}

MINUTE_CONFIG = {
    'ma': {'periods': (5, 10, 20)},
    'ema': {'periods': (5, 10, 20)},
    'macd': {'fast': 12, 'slow': 26, 'signal': 9},
    'rsi': {'periods': (6, 12)},
    'kdj': {'n': 9, 'm1': 3, 'm2': 3},
    'boll': {'n': 20, 'k': 2},
    'atr': {'n': 14},
    'vol_ma': {'periods': (5, 10)},
    'vol_ratio': {'n': 5},
}


def compute_all(
    df: pd.DataFrame,
    *,
    timeframe: str = 'daily',
    shares: float | int | None = None,
) -> pd.DataFrame:
    """Run every registered indicator and return a copy with all columns attached.

    Does NOT mutate the caller's DataFrame: returns a fresh copy with the
    indicator columns added alongside the original OHLCV columns.

    timeframe selects the period set ('daily' uses 通达信 standard windows,
    'minute' uses shorter windows suited to intraday bars). Any other value
    raises ``ValueError``.

    shares, when provided, is forwarded to the turnover indicator; when None
    the ``TURNOVER_RATE`` column is all-NaN.
    """
    if timeframe == 'minute':
        config = MINUTE_CONFIG
    elif timeframe == 'daily':
        config = DAILY_CONFIG
    else:
        raise ValueError(
            f"unsupported timeframe {timeframe!r}; expected 'daily' or 'minute'"
        )

    out = df.copy()
    for name, func in INDICATORS.items():
        if name == 'turnover':
            result = func(out, shares=shares)
        else:
            result = func(out, **config[name])
        if isinstance(result, pd.Series):
            out[result.name] = result
        else:
            for col in result.columns:
                out[col] = result[col]
    return out
