from __future__ import annotations

import pandas as pd

from scripts.data_pipeline.indicators.volume import calc_volume_ratio


def golden_cross(df: pd.DataFrame) -> bool:
    """MACD golden cross on the latest bar: DIF crosses ABOVE DEA.

    True iff ``DIF[-1] > DEA[-1]`` and ``DIF[-2] <= DEA[-2]``. Returns False
    when there are fewer than two bars or any involved value is NaN.
    """
    if 'DIF' not in df.columns or 'DEA' not in df.columns or len(df) < 2:
        return False
    dif, dea = df['DIF'], df['DEA']
    d0, e0 = dif.iloc[-1], dea.iloc[-1]
    d1, e1 = dif.iloc[-2], dea.iloc[-2]
    if pd.isna(d0) or pd.isna(e0) or pd.isna(d1) or pd.isna(e1):
        return False
    return bool(d0 > e0 and d1 <= e1)


def kdj_golden_cross(df: pd.DataFrame) -> bool:
    """KDJ golden cross on the latest bar: K crosses ABOVE D."""
    if 'K' not in df.columns or 'D' not in df.columns or len(df) < 2:
        return False
    k, d = df['K'], df['D']
    k0, d0 = k.iloc[-1], d.iloc[-1]
    k1, d1 = k.iloc[-2], d.iloc[-2]
    if pd.isna(k0) or pd.isna(d0) or pd.isna(k1) or pd.isna(d1):
        return False
    return bool(k0 > d0 and k1 <= d1)


def volume_breakout(df: pd.DataFrame, n: int = 5, k: float = 2) -> bool:
    """放量突破: today's n-day volume ratio > k AND close > MA20.

    The n-day volume ratio is computed inline (honouring ``n``) rather than
    read from ``VOL_RATIO`` (which uses the daily-config default of 5).
    """
    if len(df) < 2 or 'MA20' not in df.columns or 'close' not in df.columns:
        return False
    ratio = calc_volume_ratio(df, n).iloc[-1]
    ma20 = df['MA20'].iloc[-1]
    close = df['close'].iloc[-1]
    if pd.isna(ratio) or pd.isna(ma20) or pd.isna(close):
        return False
    return bool(ratio > k and close > ma20)


def rsi_oversold(df: pd.DataFrame, threshold: float = 30) -> bool:
    """Latest RSI6 below ``threshold``."""
    if 'RSI6' not in df.columns or len(df) == 0:
        return False
    rsi = df['RSI6'].iloc[-1]
    if pd.isna(rsi):
        return False
    return bool(rsi < threshold)


def near_boll_lower(df: pd.DataFrame) -> bool:
    """Latest close touches or falls below the BOLL lower band."""
    if 'BOLL_DN' not in df.columns or 'close' not in df.columns or len(df) == 0:
        return False
    close = df['close'].iloc[-1]
    dn = df['BOLL_DN'].iloc[-1]
    if pd.isna(dn) or pd.isna(close):
        return False
    return bool(close <= dn)


# Registry mapping CLI names to the condition callables.
CONDITIONS = {
    'golden_cross': golden_cross,
    'kdj_golden_cross': kdj_golden_cross,
    'volume_breakout': volume_breakout,
    'rsi_oversold': rsi_oversold,
    'near_boll_lower': near_boll_lower,
}
