from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scripts.data_pipeline.indicators import compute_all
from scripts.data_pipeline.screener.conditions import (
    CONDITIONS,
    golden_cross,
    kdj_golden_cross,
    near_boll_lower,
    rsi_oversold,
    volume_breakout,
)
from scripts.data_pipeline.screener.run_screener import screen


# ---------------------------------------------------------------------------
# synthetic-frame builders
# ---------------------------------------------------------------------------
def _frame(close: np.ndarray, *, vol: np.ndarray | None = None) -> pd.DataFrame:
    """Build an OHLCV frame from a close series (high/low/open derived)."""
    n = len(close)
    if vol is None:
        vol = np.full(n, 1e6)
    return pd.DataFrame(
        {
            'open': close,
            'high': close + 0.1,
            'low': close - 0.1,
            'close': close,
            'vol': vol,
            'amount': close * vol,
        }
    )


def _golden_cross_close(n: int = 130) -> np.ndarray:
    """Long decline then a sharp up-spike on the LAST bar -> MACD golden cross."""
    decline = np.linspace(25.0, 8.0, n - 1)
    return np.concatenate([decline, [decline[-1] + 8.0]])


def _kdj_golden_close(n: int = 120) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return (close, high, low) forcing a K-crosses-above-D on the last bar."""
    rng = np.random.default_rng(1)
    decline = np.linspace(20.0, 10.0, n - 1) + rng.normal(scale=0.05, size=n - 1)
    last_close = decline[-5] + 2.0
    close = np.concatenate([decline, [last_close]])
    high = np.concatenate([decline + 0.2, [last_close + 0.5]])
    low = np.concatenate([decline - 0.2, [decline[-1] - 0.3]])
    return close, high, low


# ---------------------------------------------------------------------------
# golden_cross
# ---------------------------------------------------------------------------
def test_golden_cross_true_on_synthetic_cross() -> None:
    df = _frame(_golden_cross_close())
    ind = compute_all(df)
    assert golden_cross(ind) is True


def test_golden_cross_false_when_already_above_no_fresh_cross() -> None:
    # Monotonic uptrend: DIF stays above DEA throughout, so no fresh cross.
    up = np.linspace(8.0, 30.0, 120)
    ind = compute_all(_frame(up))
    assert ind['DIF'].iloc[-1] > ind['DEA'].iloc[-1]  # precondition: above
    assert golden_cross(ind) is False


def test_golden_cross_false_on_single_bar_frame() -> None:
    # len < 2 -> degenerate, guarded.
    df = _frame(np.array([10.0]))
    ind = compute_all(df)
    assert golden_cross(ind) is False


def test_golden_cross_nan_guarded() -> None:
    df = _frame(np.linspace(10.0, 20.0, 120))
    ind = compute_all(df)
    # Wipe the latest DIF -> must not raise, returns False.
    ind.loc[ind.index[-1], 'DIF'] = np.nan
    assert golden_cross(ind) is False


# ---------------------------------------------------------------------------
# kdj_golden_cross
# ---------------------------------------------------------------------------
def test_kdj_golden_cross_true() -> None:
    close, high, low = _kdj_golden_close()
    df = pd.DataFrame(
        {'open': close, 'high': high, 'low': low, 'close': close,
         'vol': np.full(len(close), 1e6), 'amount': close * 1e6}
    )
    ind = compute_all(df)
    assert kdj_golden_cross(ind) is True


def test_kdj_golden_cross_false_on_uptrend() -> None:
    up = np.linspace(5.0, 25.0, 120)
    ind = compute_all(_frame(up))
    assert kdj_golden_cross(ind) is False


# ---------------------------------------------------------------------------
# volume_breakout
# ---------------------------------------------------------------------------
def test_volume_breakout_true_on_spike_above_ma20() -> None:
    n = 120
    close = np.full(n, 20.0) + np.linspace(0, 2, n)  # rising -> close > MA20
    vol = np.full(n, 1e6)
    vol[-1] = 5e6  # 5x spike -> ratio ~5 > 2
    ind = compute_all(_frame(close, vol=vol))
    assert volume_breakout(ind) is True


def test_volume_breakout_false_without_spike() -> None:
    n = 120
    close = np.full(n, 20.0) + np.linspace(0, 2, n)
    vol = np.full(n, 1e6)  # flat -> ratio ~1, not > 2
    ind = compute_all(_frame(close, vol=vol))
    assert volume_breakout(ind) is False


def test_volume_breakout_false_when_close_below_ma20() -> None:
    n = 120
    close = np.full(n, 20.0) - np.linspace(0, 2, n)  # falling -> close < MA20
    vol = np.full(n, 1e6)
    vol[-1] = 5e6  # spike present, but price not above MA20
    ind = compute_all(_frame(close, vol=vol))
    assert volume_breakout(ind) is False


# ---------------------------------------------------------------------------
# rsi_oversold
# ---------------------------------------------------------------------------
def test_rsi_oversold_true_on_decline() -> None:
    decline = np.linspace(30.0, 5.0, 120)
    ind = compute_all(_frame(decline))
    assert ind['RSI6'].iloc[-1] < 30
    assert rsi_oversold(ind) is True


def test_rsi_oversold_false_on_rise() -> None:
    rise = np.linspace(5.0, 30.0, 120)
    ind = compute_all(_frame(rise))
    assert rsi_oversold(ind) is False


def test_rsi_oversold_threshold_override() -> None:
    decline = np.linspace(30.0, 28.0, 120)
    ind = compute_all(_frame(decline))
    # default threshold 30 may or may not fire; threshold=50 must fire.
    assert rsi_oversold(ind, threshold=50) is True


def test_rsi_oversold_missing_column() -> None:
    df = pd.DataFrame({'close': np.linspace(10, 20, 50)})
    assert rsi_oversold(df) is False


# ---------------------------------------------------------------------------
# near_boll_lower
# ---------------------------------------------------------------------------
def test_near_boll_lower_true() -> None:
    n = 120
    base = np.full(n, 20.0) + np.random.default_rng(2).normal(scale=0.1, size=n)
    base[-1] = 15.0  # drop below lower band
    ind = compute_all(_frame(base))
    assert ind['close'].iloc[-1] <= ind['BOLL_DN'].iloc[-1]
    assert near_boll_lower(ind) is True


def test_near_boll_lower_false_when_above_band() -> None:
    up = np.linspace(10.0, 30.0, 120)
    ind = compute_all(_frame(up))
    assert ind['close'].iloc[-1] > ind['BOLL_DN'].iloc[-1]
    assert near_boll_lower(ind) is False


# ---------------------------------------------------------------------------
# cross-day behaviour
# ---------------------------------------------------------------------------
def test_golden_cross_appears_only_on_cross_day() -> None:
    """A cross at index k fires only when the frame ends at >= k."""
    df = _frame(_golden_cross_close(n=130))
    ind = compute_all(df)
    last = len(ind) - 1
    # The cross is engineered to land on the final bar.
    assert golden_cross(ind.iloc[: last + 1]) is True
    # The day BEFORE the cross must not fire.
    assert golden_cross(ind.iloc[:last]) is False


# ---------------------------------------------------------------------------
# CONDITIONS registry
# ---------------------------------------------------------------------------
def test_conditions_registry_complete() -> None:
    assert set(CONDITIONS) == {
        'golden_cross', 'kdj_golden_cross', 'volume_breakout',
        'rsi_oversold', 'near_boll_lower',
    }
    for name, fn in CONDITIONS.items():
        assert callable(fn)
        assert fn.__name__ == name or fn.__name__.endswith(name)


# ---------------------------------------------------------------------------
# screen()
# ---------------------------------------------------------------------------
class _FakeDownloader:
    """Synthetic frames per code (daily + minute); records ``(domain, code)`` calls
    so tests can assert the cache path is preferred over download."""

    def __init__(
        self,
        frames: dict[str, pd.DataFrame] | None = None,
        *,
        minute_frames: dict[str, pd.DataFrame] | None = None,
    ) -> None:
        self.frames = frames or {}
        self.minute_frames = minute_frames or {}
        self.calls: list[tuple[str, str]] = []

    def download_daily(self, code: str, *, max_bars: int | None = None) -> pd.DataFrame:
        self.calls.append(('daily', code))
        if code not in self.frames:
            raise ValueError(f'no daily frame for {code}')
        return self.frames[code].copy()

    def download_minute(self, code: str, freq: int = 5, *, max_bars: int | None = None) -> pd.DataFrame:
        self.calls.append((f'{freq}m', code))
        if code not in self.minute_frames:
            raise ValueError(f'no minute frame for {code}')
        return self.minute_frames[code].copy()


def _fake_frame(trigger: str = 'golden_cross') -> pd.DataFrame:
    if trigger == 'golden_cross':
        return _frame(_golden_cross_close())
    if trigger == 'rsi_oversold':
        return _frame(np.linspace(30.0, 5.0, 120))
    # neutral: no signal
    return _frame(np.linspace(10.0, 12.0, 120))


def test_screen_columns_and_sort_and_ts_code(tmp_path) -> None:
    daily = {
        '000001': _fake_frame('golden_cross'),
        '600000': _fake_frame('rsi_oversold'),
        '000002': _fake_frame('neutral'),
    }
    # reuse the same synthetic frame for minute timeframes
    fake = _FakeDownloader(daily, minute_frames=daily)
    result = screen(
        ['000001', '600000', '000002'],
        [golden_cross, rsi_oversold],
        data_root=tmp_path,
        downloader=fake,
    )
    assert list(result.columns) == [
        'ts_code', 'timeframe', 'close', 'hit_count', 'matched', 'latest_trade_date',
    ]
    # one row per (code, timeframe): 3 codes x 5 timeframes
    assert len(result) == 15
    assert set(result['timeframe']) == {'daily', '5m', '15m', '30m', '60m'}
    # hit_count sorted descending
    assert list(result['hit_count']) == sorted(result['hit_count'], reverse=True)
    assert set(result['ts_code']) == {'000001.SZ', '600000.SH', '000002.SZ'}
    cond_names = {'golden_cross', 'rsi_oversold'}
    for matched in result['matched']:
        assert set(matched).issubset(cond_names)
    # 000001's golden-cross frame fires golden_cross on the daily latest bar
    gc_daily = result[(result['ts_code'] == '000001.SZ') & (result['timeframe'] == 'daily')].iloc[0]
    assert 'golden_cross' in gc_daily['matched']


def test_screen_uses_parquet_cache_instead_of_downloader(tmp_path) -> None:
    ts_code = '000001.SZ'
    cached = _frame(np.linspace(10.0, 12.0, 120))
    for domain in ('daily', 'minute_5m', 'minute_15m', 'minute_30m', 'minute_60m'):
        cache_dir = tmp_path / domain / f'ts_code={ts_code}'
        cache_dir.mkdir(parents=True)
        cached.to_parquet(cache_dir / 'data.parquet')

    fake = _FakeDownloader({})  # would raise if called
    result = screen(['000001'], [golden_cross], data_root=tmp_path, downloader=fake)
    assert fake.calls == []  # downloader never invoked (all 5 timeframes cached)
    assert len(result) == 5
    assert set(result['ts_code']) == {ts_code}


def test_screen_falls_back_to_download_when_cache_corrupt(tmp_path) -> None:
    ts_code = '000001.SZ'
    cache_dir = tmp_path / 'daily' / f'ts_code={ts_code}'
    cache_dir.mkdir(parents=True)
    (cache_dir / 'data.parquet').write_text('not a parquet file')

    frame = _frame(np.linspace(10.0, 12.0, 120))
    fake = _FakeDownloader({'000001': frame}, minute_frames={'000001': frame})
    result = screen(['000001'], [golden_cross], data_root=tmp_path, downloader=fake)
    assert ('daily', '000001') in fake.calls  # fell back after the corrupt read
    assert result.iloc[0]['ts_code'] == ts_code


def test_screen_full_pipeline_write_read_compute_condition(tmp_path) -> None:
    """End-to-end seam across all timeframes: write_by_symbol (one file per
    ts_code/domain) -> screen reads each cache dir -> compute_all -> golden_cross."""
    from scripts.data_pipeline.materializers.symbol_writer import write_by_symbol

    close = _golden_cross_close(130)
    df = _frame(close)
    df['ts_code'] = '000001.SZ'
    df['trade_date'] = pd.date_range('2023-01-02', periods=len(df), freq='D').strftime('%Y%m%d')

    written: list = []
    for domain in ('daily', 'minute_5m', 'minute_15m', 'minute_30m', 'minute_60m'):
        written += write_by_symbol(tmp_path, domain, df)
    assert len(written) == 5  # one file per (domain, ts_code)

    fake = _FakeDownloader({})  # would raise if called -> proves cache used
    result = screen(['000001'], [golden_cross], data_root=tmp_path, downloader=fake)
    assert fake.calls == []
    assert len(result) == 5
    daily_row = result[result['timeframe'] == 'daily'].iloc[0]
    assert daily_row['ts_code'] == '000001.SZ'
    assert 'golden_cross' in daily_row['matched']  # forced cross on the last bar
    assert daily_row['close'] == pytest.approx(float(close[-1]))
    assert daily_row['latest_trade_date'] == df['trade_date'].iloc[-1]


def test_screen_skips_failing_code_keeps_good_one(tmp_path, capsys) -> None:
    frames = {'000001': _fake_frame('golden_cross')}  # '600000' deliberately absent
    fake = _FakeDownloader(frames, minute_frames=frames)
    result = screen(
        ['000001', '600000'],
        [golden_cross],
        data_root=tmp_path,
        downloader=fake,
    )
    # 600000 fully excluded (all 5 timeframes failed); 000001 kept (5 rows)
    assert (result['ts_code'] == '000001.SZ').all()
    assert len(result) == 5
    assert '600000' not in set(result['ts_code'])
    captured = capsys.readouterr()
    assert 'WARNING: skip 600000' in captured.err


def test_screen_default_downloader_constructed_when_none(tmp_path, monkeypatch) -> None:
    """When downloader=None, screen builds a TdxDownloader(data_root). Patch the
    class so no real network/parquet wiring is exercised."""
    constructed: list[object] = []

    class _StubDownloader:
        def __init__(self, data_root) -> None:
            constructed.append(data_root)

        def download_daily(self, code: str) -> pd.DataFrame:
            raise RuntimeError('boom')

    import scripts.data_pipeline.screener.run_screener as mod

    monkeypatch.setattr(mod, 'TdxDownloader', _StubDownloader)
    result = screen(['000001'], [golden_cross], data_root=tmp_path)
    assert constructed == [tmp_path]  # built with data_root
    assert result.empty  # the stub raised -> code skipped


def test_screen_empty_codes_returns_empty_typed_frame(tmp_path) -> None:
    result = screen([], [golden_cross], data_root=tmp_path, downloader=_FakeDownloader({}))
    assert list(result.columns) == [
        'ts_code', 'timeframe', 'close', 'hit_count', 'matched', 'latest_trade_date',
    ]
    assert len(result) == 0


def test_screen_stable_sort_among_ties(tmp_path) -> None:
    # Two neutral frames with the same hit_count (0) -> input order preserved.
    fake = _FakeDownloader({
        '000001': _fake_frame('neutral'),
        '000002': _fake_frame('neutral'),
    })
    result = screen(['000001', '000002'], [golden_cross], data_root=tmp_path, downloader=fake)
    assert list(result['ts_code']) == ['000001.SZ', '000002.SZ']
