from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from scripts.data_pipeline.materializers.symbol_writer import (
    build_symbol_partition_path,
    write_by_symbol,
)


def _bars_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            'ts_code': ['000001.SZ', '000001.SZ', '600000.SH'],
            'trade_date': ['20240102', '20240103', '20240102'],
            'open': [1.0, 2.0, 3.0],
            'close': [1.1, 2.1, 3.1],
        }
    )


def test_write_by_symbol_one_file_per_ts_code(tmp_path: Path) -> None:
    df = _bars_df()
    written = write_by_symbol(tmp_path, 'daily', df)

    # one file per ts_code (not per day) -> 2 distinct stocks
    assert len(written) == 2

    # ts_code is the partition key (in the path); read the whole domain to
    # recover it via hive partitioning and confirm a full round-trip.
    domain = pd.read_parquet(tmp_path / 'daily')
    assert len(domain) == len(df)
    assert set(domain['ts_code']) == {'000001.SZ', '600000.SH'}
    # 000001.SZ carries both its rows in its single file
    assert len(domain[domain['ts_code'] == '000001.SZ']) == 2
    assert len(domain[domain['ts_code'] == '600000.SH']) == 1


def test_write_by_symbol_omits_ts_code_and_domain_read_works(tmp_path: Path) -> None:
    """ts_code is the partition key: not stored in the file, and reading the
    whole domain (all stocks at once) must work without a type collision."""
    df = _bars_df()
    write_by_symbol(tmp_path, 'daily', df)

    file_path = tmp_path / 'daily' / 'ts_code=000001.SZ' / 'data.parquet'
    assert 'ts_code' not in pd.read_parquet(file_path).columns

    domain = pd.read_parquet(tmp_path / 'daily')
    assert set(domain['ts_code']) == {'000001.SZ', '600000.SH'}
    assert len(domain) == len(df)


def test_write_by_symbol_partition_layout(tmp_path: Path) -> None:
    df = _bars_df()
    write_by_symbol(tmp_path, 'minute_5m', df)

    expected = tmp_path / 'minute_5m' / 'ts_code=000001.SZ' / 'data.parquet'
    assert expected.exists()


def test_write_by_symbol_overwrites_on_rewrite(tmp_path: Path) -> None:
    df = _bars_df()
    write_by_symbol(tmp_path, 'daily', df)
    # a second write replaces the existing single file, not appends alongside it
    write_by_symbol(tmp_path, 'daily', df)
    leaves = list((tmp_path / 'daily' / 'ts_code=000001.SZ').glob('*.parquet'))
    assert len(leaves) == 1


def test_write_by_symbol_requires_columns(tmp_path: Path) -> None:
    df = pd.DataFrame({'open': [1.0]})
    with pytest.raises(ValueError, match='ts_code'):
        write_by_symbol(tmp_path, 'daily', df)


def test_write_by_symbol_empty_returns_empty(tmp_path: Path) -> None:
    df = pd.DataFrame({'ts_code': [], 'trade_date': []})
    assert write_by_symbol(tmp_path, 'daily', df) == []


def test_build_symbol_partition_path() -> None:
    p = build_symbol_partition_path(Path('data'), 'daily', '000001.SZ')
    assert p == Path('data') / 'daily' / 'ts_code=000001.SZ'
