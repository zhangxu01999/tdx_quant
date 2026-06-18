from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scripts.data_pipeline.indicators.volatility import calc_atr, calc_boll


def _ohlc_df(n: int = 30) -> pd.DataFrame:
    rng = np.arange(1, n + 1, dtype=float)
    return pd.DataFrame({
        'high': rng + 2.0,
        'low': rng - 2.0,
        'close': rng,
    })


def test_boll_columns() -> None:
    out = calc_boll(_ohlc_df(), n=20, k=2)
    assert list(out.columns) == ['BOLL_MB', 'BOLL_UP', 'BOLL_DN']


def test_boll_ordering_up_ge_mb_ge_dn() -> None:
    out = calc_boll(_ohlc_df(), n=20, k=2)
    valid = out.dropna()
    assert (valid['BOLL_UP'] >= valid['BOLL_MB']).all()
    assert (valid['BOLL_MB'] >= valid['BOLL_DN']).all()


def test_boll_uses_population_std() -> None:
    df = _ohlc_df()
    out = calc_boll(df, n=20, k=2)
    mb = df['close'].rolling(20).mean()
    std = df['close'].rolling(20).std(ddof=0)  # population std, tongdaxin style
    np.testing.assert_allclose(out['BOLL_MB'].to_numpy(), mb.to_numpy(), equal_nan=True)
    np.testing.assert_allclose(out['BOLL_UP'].to_numpy(), (mb + 2 * std).to_numpy(), equal_nan=True)
    np.testing.assert_allclose(out['BOLL_DN'].to_numpy(), (mb - 2 * std).to_numpy(), equal_nan=True)


def test_boll_first_valid_index() -> None:
    out = calc_boll(_ohlc_df(), n=20, k=2)
    # need 20 obs -> first valid at idx 19
    assert out['BOLL_MB'].iloc[:19].isna().all()
    assert out['BOLL_MB'].iloc[19] == pytest.approx(np.arange(1, 21).mean())


def test_atr_returns_named_series() -> None:
    s = calc_atr(_ohlc_df(), n=14)
    assert isinstance(s, pd.Series)
    assert s.name == 'ATR'


def test_atr_positive_after_warmup() -> None:
    df = _ohlc_df()
    s = calc_atr(df, n=14)
    # first row has no prev_close -> TR components use NaN-prev; ewm(adjust=False) still produces a value
    valid = s.dropna()
    assert (valid > 0).all()


def test_atr_manual_first_value() -> None:
    # construct a tiny df to hand-check Wilder ATR seed
    df = pd.DataFrame({
        'high': [12.0, 15.0, 11.0],
        'low': [8.0, 10.0, 9.0],
        'close': [10.0, 13.0, 10.0],
    })
    s = calc_atr(df, n=14)
    prev_close = df['close'].shift(1)
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - prev_close).abs(),
        (df['low'] - prev_close).abs(),
    ], axis=1).max(axis=1)
    expected = tr.ewm(alpha=1 / 14, adjust=False).mean()
    np.testing.assert_allclose(s.to_numpy(), expected.to_numpy(), equal_nan=True)
    # first TR = high-low = 4 (no prev_close contribution beyond NaN), seed == 4
    assert s.iloc[0] == pytest.approx(4.0)
