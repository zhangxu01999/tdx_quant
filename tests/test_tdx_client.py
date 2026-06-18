from __future__ import annotations

from collections import OrderedDict
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from scripts.data_pipeline import tdx_client as tdx_module
from scripts.data_pipeline.tdx_client import TdxDownloader


# ---------------------------------------------------------------------------
# fake pytdx HQ api
# ---------------------------------------------------------------------------
class FakeHqApi:
    """Mimics the pytdx TdxHq_API surface used by TdxDownloader.

    ``bars_for`` maps ``(category, market, code)`` -> list of bar dicts (already
    in pytdx's newest-first order). ``get_security_bars`` honours ``start`` and
    ``count`` to exercise paging.
    """

    def __init__(self) -> None:
        self.bars_for: dict[tuple[int, int, str], list[dict]] = {}
        self.xdxr_for: dict[tuple[int, str], list[dict]] = {}
        self.connected = False

    def connect(self, host: str, port: int) -> bool:
        self.connected = True
        return True

    def disconnect(self) -> None:
        self.connected = False

    def get_security_bars(
        self, category: int, market: int, code: str, start: int, count: int
    ) -> list[dict]:
        rows = self.bars_for.get((category, market, code), [])
        return rows[start:start + count]

    def get_xdxr_info(self, market: int, code: str) -> list[dict]:
        return self.xdxr_for.get((market, code), [])


def _bar(year: int, month: int, day: int, hour: int = 15, minute: int = 0) -> dict:
    """One bar dict shaped exactly like pytdx's output."""
    return OrderedDict(
        [
            ('open', 10.0),
            ('close', 11.0),
            ('high', 12.0),
            ('low', 9.0),
            ('vol', 1000),
            ('amount', 10000.0),
            ('year', year),
            ('month', month),
            ('day', day),
            ('hour', hour),
            ('minute', minute),
            ('datetime', f'{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}'),
        ]
    )


@pytest.fixture
def fake_api(tmp_path: Path, monkeypatch) -> FakeHqApi:
    api = FakeHqApi()

    @contextmanager
    def fake_connected_session(passed_api, hosts=None):
        assert passed_api is api
        yield passed_api

    monkeypatch.setattr(tdx_module, 'create_hq_api', lambda *a, **k: api)
    monkeypatch.setattr(tdx_module, 'connected_session', fake_connected_session)
    return api


# ---------------------------------------------------------------------------
# _resolve_market
# ---------------------------------------------------------------------------
def test_resolve_market_mainland_sh() -> None:
    dl = TdxDownloader()
    assert dl._resolve_market('600000') == (1, 'hq')


def test_resolve_market_mainland_sz() -> None:
    dl = TdxDownloader()
    assert dl._resolve_market('000001') == (0, 'hq')


def test_resolve_market_non_mainland() -> None:
    dl = TdxDownloader()
    assert dl._resolve_market('AAPL') == (None, 'exhq')
    assert dl._resolve_market('00700') == (None, 'exhq')


# ---------------------------------------------------------------------------
# download_daily
# ---------------------------------------------------------------------------
def test_download_daily_basic(fake_api: FakeHqApi, tmp_path: Path) -> None:
    # newest-first within page (pytdx ordering); downloader must sort ascending
    fake_api.bars_for[(9, 0, '000001')] = [
        _bar(2024, 1, 3),
        _bar(2024, 1, 2),
        _bar(2024, 1, 1),
    ]
    dl = TdxDownloader(data_root=tmp_path)
    df = dl.download_daily('000001')

    assert not df.empty
    assert (df['ts_code'] == '000001.SZ').all()
    # ascending by datetime
    dates = pd.to_datetime(df['datetime']).dt.strftime('%Y%m%d').tolist()
    assert dates == ['20240101', '20240102', '20240103']


def test_download_daily_persists_parquet(fake_api: FakeHqApi, tmp_path: Path) -> None:
    fake_api.bars_for[(9, 0, '000001')] = [
        _bar(2024, 1, 2),
        _bar(2024, 1, 1),
    ]
    dl = TdxDownloader(data_root=tmp_path)
    df = dl.download_daily('000001')

    # one file per ts_code, holding the whole history (not one file per day)
    leaf = tmp_path / 'daily' / 'ts_code=000001.SZ' / 'data.parquet'
    assert leaf.exists()
    on_disk = pd.read_parquet(leaf)
    assert len(on_disk) == len(df)
    assert set(on_disk['trade_date']) == {'20240101', '20240102'}


def test_download_daily_invalid_code_raises(fake_api: FakeHqApi, tmp_path: Path) -> None:
    # no bars_for entry -> empty page -> downloader must raise
    dl = TdxDownloader(data_root=tmp_path)
    with pytest.raises(ValueError, match='invalid code'):
        dl.download_daily('999999')


def test_download_daily_non_mainland_raises(fake_api: FakeHqApi, tmp_path: Path) -> None:
    dl = TdxDownloader(data_root=tmp_path)
    with pytest.raises(ValueError, match='mainland'):
        dl.download_daily('AAPL')


# ---------------------------------------------------------------------------
# paging + max_bars
# ---------------------------------------------------------------------------
def _bars_series(n: int) -> list[dict]:
    """Build ``n`` newest-first bars with unique datetimes spanning many days."""
    rows: list[dict] = []
    # day counter goes 1..n so every bar is unique (year/month wrap is fine here)
    for i in range(n):
        total_day = i + 1  # 1-based, monotonically increasing
        year = 2024 + (total_day - 1) // 366
        # crude month/day: spread across a synthetic calendar; uniqueness is all we need
        month = ((total_day - 1) % 12) + 1
        day = ((total_day - 1) % 28) + 1
        rows.append(_bar(year, month, day, 15, 0))
        rows[-1]['datetime'] = f'{year:04d}-{month:02d}-{day:02d} 15:00'
    # guarantee uniqueness by stamping a synthetic ordinal into the time component
    for i, row in enumerate(rows):
        # encode the global index into hour:minute so all 1000 are distinct
        row['hour'] = (i // 60) % 24
        row['minute'] = i % 60
        row['datetime'] = (
            f"{int(row['year']):04d}-{int(row['month']):02d}-{int(row['day']):02d} "
            f"{int(row['hour']):02d}:{int(row['minute']):02d}"
        )
    rows.reverse()  # newest-first to mimic pytdx
    return rows


def test_download_daily_pages_across_800(fake_api: FakeHqApi, tmp_path: Path) -> None:
    # 1000 bars newest-first -> needs 2 pages (800 + 200); all datetimes unique
    fake_api.bars_for[(9, 0, '000001')] = _bars_series(1000)

    dl = TdxDownloader(data_root=tmp_path)
    df = dl.download_daily('000001')
    assert len(df) == 1000


def test_download_daily_max_bars_truncates(fake_api: FakeHqApi, tmp_path: Path) -> None:
    fake_api.bars_for[(9, 0, '000001')] = _bars_series(1000)

    dl = TdxDownloader(data_root=tmp_path)
    df = dl.download_daily('000001', max_bars=50)
    assert len(df) == 50


# ---------------------------------------------------------------------------
# download_minute
# ---------------------------------------------------------------------------
def test_download_minute_basic(fake_api: FakeHqApi, tmp_path: Path) -> None:
    fake_api.bars_for[(0, 0, '000001')] = [
        _bar(2024, 1, 2, 9, 35),
        _bar(2024, 1, 2, 9, 30),
    ]
    dl = TdxDownloader(data_root=tmp_path)
    df = dl.download_minute('000001', freq=5)

    assert not df.empty
    assert (df['ts_code'] == '000001.SZ').all()
    assert 'trade_time' in df.columns
    # ascending -> 09:30 before 09:35
    assert df['trade_time'].tolist() == ['09:30:00', '09:35:00']
    # persisted under minute_5m domain
    assert (tmp_path / 'minute_5m' / 'ts_code=000001.SZ').exists()


def test_download_minute_invalid_freq_raises(fake_api: FakeHqApi, tmp_path: Path) -> None:
    dl = TdxDownloader(data_root=tmp_path)
    with pytest.raises(ValueError, match='Unsupported minute frequency'):
        dl.download_minute('000001', freq=7)


# ---------------------------------------------------------------------------
# download_xdxr
# ---------------------------------------------------------------------------
def test_download_xdxr_basic(fake_api: FakeHqApi, tmp_path: Path) -> None:
    fake_api.xdxr_for[(0, '000001')] = [
        OrderedDict(
            [
                ('year', 2024),
                ('month', 6),
                ('day', 15),
                ('category', 1),
                ('fenhong', 0.0),
                ('songzhuangu', 0.0),
                ('peigu', 0.0),
            ]
        ),
        OrderedDict(
            [
                ('year', 2023),
                ('month', 6),
                ('day', 15),
                ('category', 1),
                ('fenhong', 0.0),
                ('songzhuangu', 0.0),
                ('peigu', 0.0),
            ]
        ),
    ]
    dl = TdxDownloader(data_root=tmp_path)
    df = dl.download_xdxr('000001')

    assert not df.empty
    assert (df['ts_code'] == '000001.SZ').all()
    assert df['trade_date'].tolist() == ['20230615', '20240615']  # ascending
    assert (tmp_path / 'xdxr' / 'ts_code=000001.SZ').exists()


def test_download_xdxr_empty_raises(fake_api: FakeHqApi, tmp_path: Path) -> None:
    dl = TdxDownloader(data_root=tmp_path)
    with pytest.raises(ValueError, match='No xdxr'):
        dl.download_xdxr('000001')


# ---------------------------------------------------------------------------
# snapshot (live helpers monkeypatched)
# ---------------------------------------------------------------------------
def test_snapshot_mainland(fake_api: FakeHqApi, tmp_path: Path, monkeypatch) -> None:
    captured = {}

    def fake_hq(symbols):
        captured['symbols'] = symbols
        row = {
            'requested_symbol': '000001',
            'resolved_code': '000001',
            'resolved_market': 0,
            'source': 'pytdx',
            'source_channel': 'hq',
            'price': 12.34,
            'open': 12.0,
            'high': 12.5,
            'low': 11.9,
        }
        return [row], [], []

    monkeypatch.setattr(tdx_module, 'fetch_hq_snapshot_rows', fake_hq)
    dl = TdxDownloader(data_root=tmp_path)
    df = dl.snapshot('000001')

    assert captured['symbols'] == ['000001']
    assert df.loc[0, 'ts_code'] == '000001.SZ'
    assert df.loc[0, 'source_channel'] == 'hq'
    assert df.loc[0, 'price'] == 12.34
    assert not (tmp_path / 'daily').exists()  # snapshot never persists


def test_snapshot_unsupported_raises(fake_api: FakeHqApi, tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        tdx_module, 'fetch_hq_snapshot_rows', lambda symbols: ([], ['000001'], [])
    )
    dl = TdxDownloader(data_root=tmp_path)
    with pytest.raises(ValueError, match='unsupported'):
        dl.snapshot('000001')


def test_snapshot_invalid_channel_raises(tmp_path: Path) -> None:
    dl = TdxDownloader(data_root=tmp_path)
    with pytest.raises(ValueError, match='Unsupported snapshot channel'):
        dl.snapshot('000001', channel='bogus')
