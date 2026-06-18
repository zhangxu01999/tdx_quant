"""Real-network integration tests for TdxDownloader.

Marked ``integration`` so they can be skipped offline::

    /Users/henrylin/anaconda3/bin/python3 -m pytest -m integration -q
    /Users/henrylin/anaconda3/bin/python3 -m pytest -q -m "not integration"  # offline gate
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from scripts.data_pipeline.tdx_client import TdxDownloader

pytestmark = pytest.mark.integration


def test_integration_download_daily_000001(tmp_path: Path) -> None:
    dl = TdxDownloader(data_root=tmp_path)
    df = dl.download_daily('000001', max_bars=10)

    assert not df.empty
    assert (df['ts_code'] == '000001.SZ').all()
    assert 'trade_date' in df.columns
    # parquet persisted and reads back with matching row count
    parquets = list(tmp_path.glob('daily/ts_code=000001.SZ/**/*.parquet'))
    assert parquets, 'no daily parquet written'
    on_disk = pd.concat([pd.read_parquet(p) for p in parquets], ignore_index=True)
    assert len(on_disk) == len(df)


def test_integration_download_minute_5m(tmp_path: Path) -> None:
    dl = TdxDownloader(data_root=tmp_path)
    df = dl.download_minute('000001', freq=5, max_bars=20)

    assert not df.empty
    assert (df['ts_code'] == '000001.SZ').all()
    assert df['trade_time'].str.match(r'^\d{2}:\d{2}:\d{2}$').all()
