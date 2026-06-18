from __future__ import annotations

import re

import pandas as pd

from scripts.data_pipeline.code_mapping import market_code_to_ts_code

# F10 财务分析 uses U+FF5C (full-width pipe) as the cell delimiter.
PIPE = '｜'
_UPDATED_RE = re.compile(r'更新日期：(\d{4}-\d{2}-\d{2})')
_DATE_CELL_RE = re.compile(r'\d{4}-\d{2}-\d{2}')

FINANCE_INDICATOR_COLUMNS = ['ts_code', 'metric', 'period', 'value_raw', 'value_num']


def extract_updated_date(text: str) -> str | None:
    """Pull the ``更新日期：YYYY-MM-DD`` snapshot date from the section header."""
    m = _UPDATED_RE.search(text or '')
    return m.group(1) if m else None


def _to_num(s: str) -> float:
    s = (s or '').strip()
    if s in ('', '-', '--', '—', 'null', 'NULL'):
        return float('nan')
    mult = 1.0
    if s.endswith('亿'):
        mult, s = 1e8, s[:-1]
    elif s.endswith('万'):
        mult, s = 1e4, s[:-1]
    try:
        return float(s) * mult
    except ValueError:
        return float('nan')


def _split_pipe_cells(line: str) -> list[str]:
    """Split a ``｜a｜b｜c｜`` row into stripped cells, dropping the two empty
    cells produced by the enclosing pipes."""
    parts = [c.strip() for c in line.split(PIPE)]
    if parts and parts[0] == '':
        parts.pop(0)
    if parts and parts[-1] == '':
        parts.pop()
    return parts


def parse_finance_indicators(text: str, *, market: int, code: str) -> pd.DataFrame:
    """Parse the ``【主要财务指标】`` table from a 财务分析 section into tidy
    long format: one row per ``(metric, period)`` with the raw text value and a
    numeric value (``亿/万`` normalized; text / ``-`` → NaN)."""
    ts_code = market_code_to_ts_code(int(market), str(code))

    start = (text or '').find('【主要财务指标】')
    if start < 0:
        return pd.DataFrame(columns=FINANCE_INDICATOR_COLUMNS)
    rest = text[start:]
    # block ends at the next subsection marker on its own line
    nxt = rest.find('\n【', 1)
    block = rest if nxt < 0 else rest[:nxt]

    periods: list[str] | None = None
    records: list[dict] = []
    for line in block.splitlines():
        if PIPE not in line:
            continue
        cells = _split_pipe_cells(line)
        if not cells:
            continue

        # a date-header row carries >=2 date cells. The FIRST sets the period
        # columns; a SUBSEQUENT one begins a second sub-table (e.g. 单季) whose
        # rows repeat the same metric names under different period columns —
        # stop there to avoid misaligning its values onto table-1's periods.
        date_cells = [c for c in cells if _DATE_CELL_RE.search(c)]
        if len(date_cells) >= 2:
            if periods is None:
                periods = date_cells
                continue
            break

        if periods is None or len(cells) < 2:
            continue
        metric = cells[0]
        values = cells[1:1 + len(periods)]
        for period, raw in zip(periods, values):
            records.append({
                'ts_code': ts_code,
                'metric': metric,
                'period': period,
                'value_raw': raw,
                'value_num': _to_num(raw),
            })

    return pd.DataFrame(records, columns=FINANCE_INDICATOR_COLUMNS)
