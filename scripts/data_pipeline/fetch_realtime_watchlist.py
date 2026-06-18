from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

if __package__ in {None, ''}:
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

from scripts.data_pipeline.connectors.pytdx_client import (
    connected_session,
    create_hq_api,
    fetch_quotes_payload,
)
from scripts.data_pipeline.connectors.pytdx_exhq_client import (
    connected_exhq_session,
    create_exhq_api,
    fetch_instrument_info_payload,
    fetch_instrument_quote_payload,
)

DEFAULT_WATCHLIST = [
    '159981',
    '513350',
    '300502',
    '000001',
    '602181',
    'DJI',
    'IXIC',
    'HSZS',
    'N225',
    'AAPL',
    'NVDA',
    '00700',
    '09988',
]

SYMBOL_ALIASES = {
    'APPL': ('APPL', 'AAPL'),
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Fetch one latest pytdx realtime quote per watchlist symbol')
    parser.add_argument('symbols', nargs='*', help='Symbols to fetch; defaults to the built-in mixed watchlist')
    parser.add_argument('--trade-date', help='Trade date in YYYYMMDD format; defaults to today')
    return parser


def normalize_requested_symbols(symbols: list[str] | None = None) -> list[str]:
    requested = symbols or DEFAULT_WATCHLIST
    normalized = []
    for item in requested:
        value = str(item).strip().upper()
        if value:
            normalized.append(value)
    return normalized


def symbol_candidates(symbol: str) -> list[str]:
    candidates = [symbol]
    for candidate in SYMBOL_ALIASES.get(symbol, ()):  # pragma: no branch
        if candidate not in candidates:
            candidates.append(candidate)
    return candidates


def is_mainland_symbol(symbol: str) -> bool:
    return symbol.isdigit() and len(symbol) == 6


def infer_hq_market(code: str) -> int:
    if code.startswith(('5', '6', '9')):
        return 1
    if code.startswith(('0', '1', '2', '3', '4', '8')):
        return 0
    raise ValueError(f'Unable to infer mainland market for code: {code}')


def _normalize_hq_row(requested_symbol: str, market: int, row: dict) -> dict:
    return {
        'requested_symbol': requested_symbol,
        'resolved_code': row.get('code', requested_symbol),
        'resolved_market': market,
        'source': 'pytdx',
        'source_channel': 'hq',
        'price': row.get('price'),
        'open': row.get('open'),
        'high': row.get('high'),
        'low': row.get('low'),
    }


def _normalize_exhq_row(requested_symbol: str, resolution: dict, row: dict) -> dict:
    return {
        'requested_symbol': requested_symbol,
        'resolved_code': resolution['code'],
        'resolved_market': resolution['market'],
        'resolved_name': resolution.get('name'),
        'resolved_category': resolution.get('category'),
        'source': 'pytdx',
        'source_channel': 'exhq',
        'price': row.get('price'),
        'open': row.get('open'),
        'high': row.get('high'),
        'low': row.get('low'),
    }


def fetch_hq_snapshot_rows(symbols: list[str]) -> tuple[list[dict], list[str], list[dict]]:
    if not symbols:
        return [], [], []

    request_symbols = [(infer_hq_market(symbol), symbol) for symbol in symbols]
    api = create_hq_api()
    try:
        with connected_session(api) as active_api:
            payload = fetch_quotes_payload(active_api, request_symbols)
    except Exception as exc:
        return [], [], [{'channel': 'hq', 'symbols': list(symbols), 'message': str(exc)}]

    rows_by_code = {str(row.get('code', '')).upper(): dict(row) for row in payload}
    rows = []
    unsupported = []
    for market, symbol in request_symbols:
        row = rows_by_code.get(symbol)
        if row is None:
            unsupported.append(symbol)
            continue
        rows.append(_normalize_hq_row(symbol, market, row))
    return rows, unsupported, []


def discover_exhq_resolutions(api, symbols: list[str], *, batch_size: int = 500) -> dict[str, dict]:
    if not symbols:
        return {}

    candidate_to_requested: dict[str, list[str]] = {}
    for symbol in symbols:
        for candidate in symbol_candidates(symbol):
            candidate_to_requested.setdefault(candidate, []).append(symbol)

    resolutions: dict[str, dict] = {}
    instrument_count = int(api.get_instrument_count() or 0)
    for start in range(0, instrument_count, batch_size):
        rows = fetch_instrument_info_payload(api, start=start, count=batch_size)
        for row in rows:
            code = str(row.get('code', '')).strip().upper()
            if code not in candidate_to_requested:
                continue
            for requested_symbol in candidate_to_requested[code]:
                if requested_symbol not in resolutions:
                    resolutions[requested_symbol] = dict(row)
        if len(resolutions) == len(symbols):
            break
    return resolutions


def fetch_exhq_snapshot_rows(symbols: list[str]) -> tuple[list[dict], list[str], list[dict]]:
    if not symbols:
        return [], [], []

    api = create_exhq_api()
    try:
        with connected_exhq_session(api) as active_api:
            resolutions = discover_exhq_resolutions(active_api, symbols)
            rows = []
            unsupported = []
            errors = []
            for symbol in symbols:
                resolution = resolutions.get(symbol)
                if resolution is None:
                    unsupported.append(symbol)
                    continue
                try:
                    payload = fetch_instrument_quote_payload(
                        active_api,
                        market=int(resolution['market']),
                        code=str(resolution['code']),
                    )
                except Exception as exc:
                    errors.append({'channel': 'exhq', 'symbol': symbol, 'message': str(exc)})
                    continue
                if not payload:
                    unsupported.append(symbol)
                    continue
                rows.append(_normalize_exhq_row(symbol, resolution, payload[0]))
            return rows, unsupported, errors
    except Exception as exc:
        return [], [], [{'channel': 'exhq', 'symbols': list(symbols), 'message': str(exc)}]


def fetch_watchlist_snapshots(symbols: list[str] | None = None, trade_date: str | None = None) -> dict:
    requested_symbols = normalize_requested_symbols(symbols)
    mainland_symbols = [symbol for symbol in requested_symbols if is_mainland_symbol(symbol)]
    extension_symbols = [symbol for symbol in requested_symbols if not is_mainland_symbol(symbol)]

    hq_rows, hq_unsupported, hq_errors = fetch_hq_snapshot_rows(mainland_symbols)
    exhq_rows, exhq_unsupported, exhq_errors = fetch_exhq_snapshot_rows(extension_symbols)

    rows_by_symbol = {
        row['requested_symbol']: row
        for row in [*hq_rows, *exhq_rows]
    }
    unsupported_lookup = set([*hq_unsupported, *exhq_unsupported])
    ordered_rows = [rows_by_symbol[symbol] for symbol in requested_symbols if symbol in rows_by_symbol]
    ordered_unsupported = [symbol for symbol in requested_symbols if symbol in unsupported_lookup]

    return {
        'trade_date': trade_date or date.today().strftime('%Y%m%d'),
        'rows': ordered_rows,
        'unsupported_symbols': ordered_unsupported,
        'errors': [*hq_errors, *exhq_errors],
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = fetch_watchlist_snapshots(args.symbols or None, trade_date=args.trade_date)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
