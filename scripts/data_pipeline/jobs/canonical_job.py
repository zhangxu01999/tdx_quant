from __future__ import annotations

SUPPORTED_CANONICAL_TABLES = [
    'canonical_quotes',
    'canonical_daily_bars',
    'canonical_minute_bars',
    'canonical_corporate_actions',
]


def list_supported_canonical_tables() -> list[str]:
    return list(SUPPORTED_CANONICAL_TABLES)


def compare_bar_row_counts(tdx_rows: int, legacy_rows: int) -> dict:
    return {
        'tdx_rows': tdx_rows,
        'legacy_rows': legacy_rows,
        'delta': tdx_rows - legacy_rows,
    }


def summarize_quality_report(*, table_name: str, tdx_rows: int, legacy_rows: int) -> dict:
    report = compare_bar_row_counts(tdx_rows, legacy_rows)
    report['table_name'] = table_name
    report['status'] = 'ok' if report['delta'] >= 0 else 'warning'
    return report
