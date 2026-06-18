from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scripts.data_pipeline.indicators.volume import (
    calc_turnover,
    calc_vol_ma,
    calc_volume_ratio,
)


def test_vol_ma_columns_and_manual_mean() -> None:
    vol = pd.Series([10.0, 20.0, 30.0, 40.0, 50.0, 60.0])
    df = pd.DataFrame({'vol': vol})
    out = calc_vol_ma(df, periods=(5,))
    assert list(out.columns) == ['VOL_MA5']
    # leading NaN for first 4, then mean(10..50)=30 at idx4
    assert out['VOL_MA5'].iloc[:4].isna().all()
    assert out['VOL_MA5'].iloc[4] == pytest.approx(30.0)


def test_vol_ma_default_periods() -> None:
    df = pd.DataFrame({'vol': np.arange(1, 21, dtype=float)})
    out = calc_vol_ma(df)
    assert list(out.columns) == ['VOL_MA5', 'VOL_MA10']


def test_volume_ratio_manual_value() -> None:
    vol = pd.Series([10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 100.0])
    df = pd.DataFrame({'vol': vol})
    s = calc_volume_ratio(df, n=5)
    assert s.name == 'VOL_RATIO'
    # at idx5: today=60, prior 5 days mean = mean(10,20,30,40,50)=30 -> 2.0
    assert s.iloc[5] == pytest.approx(2.0)


def test_volume_ratio_leading_nan() -> None:
    vol = pd.Series([10.0, 20.0, 30.0, 40.0, 50.0, 60.0])
    df = pd.DataFrame({'vol': vol})
    s = calc_volume_ratio(df, n=5)
    # denominator needs shift(1).rolling(5) -> first valid at idx5
    assert s.iloc[:5].isna().all()


def test_volume_ratio_zero_denom_is_nan() -> None:
    # prior 5 days all zero vol -> mean = 0 -> ratio is undefined (NaN), not inf.
    vol = pd.Series([0.0, 0.0, 0.0, 0.0, 0.0, 60.0])
    df = pd.DataFrame({'vol': vol})
    s = calc_volume_ratio(df, n=5)
    assert np.isnan(s.iloc[5])
    assert not np.isinf(s.iloc[5])


def test_turnover_no_shares_is_all_nan() -> None:
    df = pd.DataFrame({'vol': [100.0, 200.0, 300.0]})
    s = calc_turnover(df)  # shares=None
    assert s.name == 'TURNOVER_RATE'
    assert len(s) == 3
    assert s.isna().all()


def test_turnover_with_shares() -> None:
    df = pd.DataFrame({'vol': [100.0, 200.0, 300.0]})
    s = calc_turnover(df, shares=1e6)
    np.testing.assert_allclose(s.to_numpy(), (df['vol'] / 1e6).to_numpy())
    assert s.name == 'TURNOVER_RATE'


def test_turnover_respects_column_param() -> None:
    df = pd.DataFrame({'volume': [500.0]})
    s = calc_turnover(df, shares=1000.0, column='volume')
    assert s.iloc[0] == pytest.approx(0.5)
