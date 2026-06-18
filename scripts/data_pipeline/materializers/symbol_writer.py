from __future__ import annotations

from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = ('ts_code',)


def build_symbol_partition_path(root: Path, domain: str, ts_code: str) -> Path:
    """Directory holding the single parquet for ``ts_code`` under ``domain``."""
    return Path(root) / domain / f'ts_code={ts_code}'


def write_by_symbol(root: Path, domain: str, df: pd.DataFrame) -> list[Path]:
    """Write each ts_code's full history to ONE parquet file:
    ``<root>/<domain>/ts_code=<ts_code>/data.parquet``.

    Requires column ``ts_code``. Rows are grouped by ``ts_code`` only — a stock's
    entire history lands in a single file (overwrites, not appends; re-downloading
    a code replaces its file). Returns the list of written file paths.
    """
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f'DataFrame missing required columns: {missing}')

    if df.empty:
        return []

    written: list[Path] = []
    for ts_code, group in df.groupby('ts_code'):
        leaf_dir = build_symbol_partition_path(Path(root), domain, str(ts_code))
        leaf_dir.mkdir(parents=True, exist_ok=True)
        file_path = leaf_dir / 'data.parquet'
        # ts_code is the partition key (encoded in the path above); drop it from
        # the file so reading a directory doesn't hit a partition/data-column
        # type collision. It's recovered from the path via hive partitioning.
        out = group.drop(columns=['ts_code'], errors='ignore')
        out.to_parquet(file_path, index=False)
        written.append(file_path)
    return written
