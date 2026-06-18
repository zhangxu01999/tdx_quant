from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scripts.data_pipeline.indicators.trend import calc_ema, calc_ma, calc_macd


def _trend_df(n: int = 30) -> pd.DataFrame:
    close = np.arange(1, n + 1, dtype=float)
    return pd.DataFrame({'close': close})


def test_ma_first_valid_and_value() -> None:
    df = _trend_df()
    out = calc_ma(df, periods=(5,))
    s = out['MA5']
    # first 4 rows must be NaN (need 5 observations)
    assert s.iloc[:4].isna().all()
    # MA5 at idx 4 = mean(1,2,3,4,5) = 3.0
    assert s.iloc[4] == pytest.approx(3.0)


def test_ma_default_periods_columns() -> None:
    df = _trend_df()
    out = calc_ma(df)
    assert list(out.columns) == ['MA5', 'MA10', 'MA20', 'MA60']
    # MA60 only valid once 60 obs present; with n=30 it stays NaN
    assert out['MA60'].isna().all()
    assert out['MA60'].shape[0] == 30


def test_ma_manual_mean_mid_series() -> None:
    df = _trend_df()
    out = calc_ma(df, periods=(5,))
    # idx 9 -> mean(5,6,7,8,9,10 window is close[5..9]) = mean(6..10) = 8.0
    assert out['MA5'].iloc[9] == pytest.approx(8.0)


def test_ema_first_value_and_recursion() -> None:
    df = _trend_df()
    out = calc_ema(df, periods=(5,))
    e = out['EMA5']
    # adjust=False: first value == first close
    assert e.iloc[0] == pytest.approx(1.0)
    # alpha = 2/(5+1) = 1/3 ; e[1] = 1 + (2-1)/3
    assert e.iloc[1] == pytest.approx(1.0 + (2.0 - 1.0) / 3.0)


def test_ema_default_columns() -> None:
    df = _trend_df()
    out = calc_ema(df)
    assert list(out.columns) == ['EMA5', 'EMA10', 'EMA20', 'EMA60']


def test_ema_periods_distinct_values() -> None:
    df = _trend_df()
    out = calc_ema(df)
    # different spans must yield different EMA values on a trending series
    assert out['EMA10'].iloc[-1] != pytest.approx(out['EMA20'].iloc[-1])
    assert out['EMA5'].iloc[-1] != pytest.approx(out['EMA10'].iloc[-1])


def test_macd_columns_and_relation() -> None:
    df = _trend_df()
    out = calc_macd(df)
    assert list(out.columns) == ['DIF', 'DEA', 'MACD']
    # MACD histogram == (DIF - DEA) * 2 (tongdaxin style)
    expected = (out['DIF'] - out['DEA']) * 2
    np.testing.assert_allclose(out['MACD'].to_numpy(), expected.to_numpy(), equal_nan=True)


def test_macd_dif_is_fast_minus_slow() -> None:
    df = _trend_df()
    out = calc_macd(df, fast=12, slow=26, signal=9)
    ef = df['close'].ewm(span=12, adjust=False).mean()
    es = df['close'].ewm(span=26, adjust=False).mean()
    np.testing.assert_allclose(out['DIF'].to_numpy(), (ef - es).to_numpy(), equal_nan=True)


def test_ma_respects_column_param() -> None:
    df = pd.DataFrame({'price': [1.0, 2.0, 3.0]})
    out = calc_ma(df, periods=(2,), column='price')
    assert out['MA2'].iloc[1] == pytest.approx(1.5)
