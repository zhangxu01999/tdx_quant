from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scripts.data_pipeline.indicators.core import (
    DAILY_CONFIG,
    INDICATORS,
    MINUTE_CONFIG,
    compute_all,
)

ALL_DAILY_COLUMNS = [
    'MA5', 'MA10', 'MA20', 'MA60',
    'EMA5', 'EMA10', 'EMA20', 'EMA60',
    'DIF', 'DEA', 'MACD',
    'RSI6', 'RSI12', 'RSI24',
    'K', 'D', 'J',
    'BOLL_MB', 'BOLL_UP', 'BOLL_DN',
    'ATR',
    'VOL_MA5', 'VOL_MA10',
    'VOL_RATIO',
    'TURNOVER_RATE',
]


def _make_daily_df(rows: int = 120) -> pd.DataFrame:
    rng = np.random.default_rng(seed=42)
    base = 10.0 + np.arange(rows, dtype=float) * 0.1
    wiggle = rng.normal(scale=0.05, size=rows)
    close = base + wiggle
    high = close + rng.uniform(0.05, 0.3, size=rows)
    low = close - rng.uniform(0.05, 0.3, size=rows)
    open_ = close + rng.normal(scale=0.1, size=rows)
    vol = rng.uniform(1e5, 1e7, size=rows)
    amount = vol * close
    return pd.DataFrame({
        'open': open_,
        'high': high,
        'low': low,
        'close': close,
        'vol': vol,
        'amount': amount,
    })


def test_compute_all_adds_all_columns() -> None:
    df = _make_daily_df()
    out = compute_all(df)
    for col in ALL_DAILY_COLUMNS:
        assert col in out.columns, f'missing indicator column: {col}'
    # original OHLCV columns preserved
    for col in ('open', 'high', 'low', 'close', 'vol', 'amount'):
        assert col in out.columns, f'OHLCV column lost: {col}'
    # row count unchanged
    assert len(out) == len(df)


def test_compute_all_does_not_mutate_input() -> None:
    df = _make_daily_df()
    before = list(df.columns)
    _ = compute_all(df)
    assert list(df.columns) == before


def test_compute_all_minute_uses_shorter_periods() -> None:
    df = _make_daily_df()
    out = compute_all(df, timeframe='minute')
    assert 'MA20' in out.columns
    assert 'MA60' not in out.columns
    assert 'RSI12' in out.columns
    assert 'RSI24' not in out.columns


def test_compute_all_invalid_timeframe_raises() -> None:
    df = _make_daily_df()
    with pytest.raises(ValueError):
        compute_all(df, timeframe='hourly')


def test_compute_all_no_nan_pollution_past_warmup() -> None:
    df = _make_daily_df(rows=120)
    out = compute_all(df)
    warmup_bound = 60
    for col in ALL_DAILY_COLUMNS:
        if col == 'TURNOVER_RATE':
            continue
        tail = out[col].iloc[warmup_bound:]
        assert not tail.isna().any(), f'NaN pollution past warmup in {col}'


def test_compute_all_turnover_nan_when_shares_none() -> None:
    df = _make_daily_df()
    out = compute_all(df, shares=None)
    assert out['TURNOVER_RATE'].isna().all()


def test_compute_all_turnover_with_shares() -> None:
    df = _make_daily_df()
    shares = 1e8
    out = compute_all(df, shares=shares)
    np.testing.assert_allclose(
        out['TURNOVER_RATE'].to_numpy(),
        (df['vol'] / shares).to_numpy(),
    )


def test_indicators_registry_maps_all_names() -> None:
    assert set(INDICATORS.keys()) == {
        'ma', 'ema', 'macd', 'rsi', 'kdj', 'boll', 'atr',
        'vol_ma', 'vol_ratio', 'turnover',
    }
    for name, func in INDICATORS.items():
        assert callable(func), f'{name} -> not callable'


def test_daily_and_minute_configs_cover_registered_indicators() -> None:
    # every indicator except turnover has an explicit period config
    configured = set(DAILY_CONFIG.keys()) | set(MINUTE_CONFIG.keys())
    assert set(INDICATORS.keys()) - {'turnover'} <= configured
    # turnover is intentionally absent from the kwarg configs (shares-only path)
    assert 'turnover' not in DAILY_CONFIG
    assert 'turnover' not in MINUTE_CONFIG
