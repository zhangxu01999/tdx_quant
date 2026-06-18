from __future__ import annotations

from pathlib import Path

import pandas as pd


DATA_ROOT = Path('data')


def build_raw_output_path(table_name: str, trade_date: str, root: Path | None = None) -> Path:
    normalized = trade_date.replace('-', '')
    base_dir = Path(root) if root is not None else DATA_ROOT
    return (
        base_dir
        / table_name
        / 'daily'
        / f'year={normalized[:4]}'
        / f'month={normalized[4:6]}'
        / f'day={normalized[6:8]}'
    )


def write_raw_by_date(root: Path, table_name: str, df: pd.DataFrame, trade_date: str) -> Path:
    partition_dir = build_raw_output_path(table_name, trade_date, root=root)
    partition_dir.mkdir(parents=True, exist_ok=True)
    file_path = partition_dir / 'data.parquet'
    df.to_parquet(file_path, index=False)
    return file_path


def write_raw_by_market_date(
    root: Path,
    table_name: str,
    market_label: str,
    df: pd.DataFrame,
    trade_date: str,
) -> Path:
    """Write a per-market daily snapshot to a hive-partitioned path::

        <root>/<table_name>/market=<market_label>/date=<YYYYMMDD>/data.parquet

    The integer ``market`` column is dropped from the file — it collides with the
    ``market=`` path key on hive read-back, and is fully recoverable from the
    path / the ``ts_code`` suffix (``.SZ`` / ``.SH``).
    """
    normalized = trade_date.replace('-', '')
    partition_dir = Path(root) / table_name / f'market={market_label}' / f'date={normalized}'
    partition_dir.mkdir(parents=True, exist_ok=True)
    file_path = partition_dir / 'data.parquet'
    out = df.drop(columns=['market'], errors='ignore')
    out.to_parquet(file_path, index=False)
    return file_path


def write_raw_by_date_symbol(
    root: Path,
    table_name: str,
    ts_code: str,
    df: pd.DataFrame,
    trade_date: str,
) -> Path:
    """Write one symbol's daily payload to a date+symbol hive partition::

        <root>/<table_name>/date=<YYYYMMDD>/ts_code=<ts_code>/data.parquet

    ``ts_code`` is dropped from the file (it is the partition key, recovered on
    hive read-back). Used for high-volume per-day data such as 分笔成交.
    """
    normalized = trade_date.replace('-', '')
    partition_dir = Path(root) / table_name / f'date={normalized}' / f'ts_code={ts_code}'
    partition_dir.mkdir(parents=True, exist_ok=True)
    file_path = partition_dir / 'data.parquet'
    out = df.drop(columns=['ts_code'], errors='ignore')
    out.to_parquet(file_path, index=False)
    return file_path
