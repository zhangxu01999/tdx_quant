from __future__ import annotations

from pathlib import Path

import pandas as pd


DATA_ROOT = Path('data')


def build_canonical_output_path(table_name: str, trade_date: str, root: Path | None = None) -> Path:
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


def write_canonical_by_date(root: Path, table_name: str, df: pd.DataFrame, trade_date: str) -> Path:
    partition_dir = build_canonical_output_path(table_name, trade_date, root=root)
    partition_dir.mkdir(parents=True, exist_ok=True)
    file_path = partition_dir / 'data.parquet'
    df.to_parquet(file_path, index=False)
    return file_path
