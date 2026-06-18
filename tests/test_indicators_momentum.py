from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scripts.data_pipeline.indicators.momentum import calc_kdj, calc_rsi


def _rising_close(n: int = 30) -> pd.DataFrame:
    return pd.DataFrame({'close': np.arange(1, n + 1, dtype=float)})


def test_rsi_columns() -> None:
    out = calc_rsi(_rising_close())
    assert list(out.columns) == ['RSI6', 'RSI12', 'RSI24']


def test_rsi_bounded_0_to_100() -> None:
    # zig-zag prices so avg_loss is nonzero somewhere
    close = np.array([10, 12, 9, 13, 8, 14, 7, 15, 9, 12, 11, 13, 10, 14, 9], dtype=float)
    df = pd.DataFrame({'close': close})
    out = calc_rsi(df, periods=(6,))
    valid = out['RSI6'].dropna()
    assert (valid >= 0).all() and (valid <= 100).all()


def test_rsi_is_100_on_pure_rise() -> None:
    # monotonically rising close -> avg_loss == 0 after first diff -> RSI == 100
    out = calc_rsi(_rising_close(), periods=(6,))
    # skip the first row (diff is NaN there)
    vals = out['RSI6'].iloc[1:]
    assert (vals == 100).all()


def test_rsi_div_zero_guard_no_inf() -> None:
    # where avg_loss==0 we require RSI==100 exactly (no inf/nan)
    out = calc_rsi(_rising_close(), periods=(6,))
    finite = np.isfinite(out['RSI6'].to_numpy())
    assert ((out['RSI6'] == 100) | ~finite | out['RSI6'].isna()).all()
    # the non-NaN, non-inf values where avg_loss==0 must be exactly 100
    mask_guard = out['RSI6'].notna() & np.isfinite(out['RSI6'].to_numpy())
    assert (out['RSI6'][mask_guard] == 100).all()


def test_rsi_warmup_row_is_nan() -> None:
    # row 0 has no prior close -> delta is NaN -> RSI must stay NaN, not 100.
    out = calc_rsi(_rising_close(), periods=(6,))
    assert np.isnan(out['RSI6'].iloc[0])


def test_kdj_columns() -> None:
    idx = np.arange(1, 21)
    df = pd.DataFrame({'high': idx + 9.0, 'low': idx * 1.0, 'close': idx + 5.0})
    out = calc_kdj(df, n=9, m1=3, m2=3)
    assert list(out.columns) == ['K', 'D', 'J']


def test_kdj_j_equals_3k_minus_2d() -> None:
    idx = np.arange(1, 21)
    df = pd.DataFrame({'high': idx + 9.0, 'low': idx * 1.0, 'close': idx + 5.0})
    out = calc_kdj(df)
    expected = 3 * out['K'] - 2 * out['D']
    np.testing.assert_allclose(out['J'].to_numpy(), expected.to_numpy(), equal_nan=True)


def test_kdj_first_valid_k_equals_rsv() -> None:
    # with adjust=False the first ewm value equals the seed, so K[n-1] == RSV[n-1]
    idx = np.arange(1, 21)
    df = pd.DataFrame({'high': idx + 9.0, 'low': idx * 1.0, 'close': idx + 5.0})
    out = calc_kdj(df, n=9, m1=3, m2=3)
    low_n = df['low'].rolling(9).min()
    high_n = df['high'].rolling(9).max()
    rsv = (df['close'] - low_n) / (high_n - low_n) * 100
    first_valid = 8  # n-1
    assert out['K'].iloc[first_valid] == pytest.approx(rsv.iloc[first_valid])
    # D's first ewm value is K's first value
    assert out['D'].iloc[first_valid] == pytest.approx(out['K'].iloc[first_valid])


def test_kdj_flat_window_is_nan() -> None:
    # high == low everywhere -> high_n == low_n -> rsv is NaN (honest, not 50)
    flat = np.full(12, 5.0)
    df = pd.DataFrame({'high': flat, 'low': flat, 'close': flat})
    out = calc_kdj(df, n=9)
    assert out['K'].isna().all()
    assert out['D'].isna().all()
    assert out['J'].isna().all()
