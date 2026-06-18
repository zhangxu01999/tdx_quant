from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

from scripts.data_pipeline.code_mapping import market_code_to_ts_code
from scripts.data_pipeline.fetch_realtime_watchlist import infer_hq_market
from scripts.data_pipeline.indicators import compute_all
from scripts.data_pipeline.tdx_client import TdxDownloader
from scripts.data_pipeline.screener.conditions import CONDITIONS

# Default cap on bars fetched per timeframe. ~200 is enough for the indicators
# (MACD needs ~26, BOLL 20, etc.); override via screen(max_bars=...) / --max-bars.
DEFAULT_MAX_BARS = 200

# (label, parquet domain, compute_all timeframe, minute freq). freq=None -> daily.
# Each entry is both downloaded/persisted AND screened against the conditions.
TIMEFRAMES = [
    ('daily', 'daily', 'daily', None),
    ('5m', 'minute_5m', 'minute', 5),
    ('15m', 'minute_15m', 'minute', 15),
    ('30m', 'minute_30m', 'minute', 30),
    ('60m', 'minute_60m', 'minute', 60),
]

# Output schema (exact order) returned by ``screen``.
RESULT_COLUMNS = ['ts_code', 'timeframe', 'close', 'hit_count', 'matched', 'latest_trade_date']


def _load(
    code: str,
    domain: str,
    data_root: Path,
    downloader: TdxDownloader,
    max_bars: int,
    freq: int | None,
) -> tuple[str, pd.DataFrame]:
    """Load one timeframe's history for ``code``; return ``(ts_code, df)``.

    Prefers a cached parquet at ``data_root/<domain>/ts_code=<ts_code>/`` (the
    single-file layout written by ``write_by_symbol``); falls back to
    ``downloader`` (daily or minute by ``freq``) if the cache is missing or
    unreadable.
    """
    ts_code = market_code_to_ts_code(infer_hq_market(code), code)
    cache_dir = Path(data_root) / domain / f'ts_code={ts_code}'
    if cache_dir.exists():
        try:
            cached = pd.read_parquet(cache_dir)
            # The indicator/condition layers assume time-ascending rows; ensure
            # the cached frame is sorted even if it was written out of order.
            if 'trade_date' in cached.columns:
                cached = cached.sort_values('trade_date', ascending=True).reset_index(drop=True)
            return ts_code, cached
        except Exception:
            # Corrupt / partial cache -> fall through to a fresh download.
            pass
    if freq is None:
        return ts_code, downloader.download_daily(code, max_bars=max_bars)
    return ts_code, downloader.download_minute(code, freq=freq, max_bars=max_bars)


def screen(
    codes: list[str],
    conditions: list,
    *,
    data_root: str | Path,
    downloader=None,
    max_bars: int = DEFAULT_MAX_BARS,
) -> pd.DataFrame:
    """Screen ``codes`` against ``conditions`` across all timeframes.

    For each code, daily + 5/15/30/60-minute bars are loaded (cache-or-download,
    capped at ``max_bars`` each), enriched with ``compute_all``, and every
    condition is evaluated on each timeframe's latest bar. One output row per
    (ts_code, timeframe).

    Parameters
    ----------
    codes:
        Bare 6-digit mainland codes (e.g. ``'000001'``).
    conditions:
        Callables ``(df_with_indicators) -> bool``; each is evaluated on every
        timeframe and contributes to that timeframe's ``hit_count``. Callables
        carrying extra kwargs use their defaults.
    data_root:
        Path-like root holding ``<domain>/ts_code=<...>/`` parquet caches.
    downloader:
        Optional downloader exposing ``download_daily``/``download_minute``.
        Defaults to ``TdxDownloader(Path(data_root))``; inject a fake for tests.
    max_bars:
        Max bars to fetch per timeframe when the cache is cold (default 200).

    Returns
    -------
    DataFrame with columns ``['ts_code','timeframe','close','hit_count',
    'matched','latest_trade_date']`` sorted by ``hit_count`` descending (stable
    sort, so equal-hit rows keep code-then-timeframe insertion order).

    Resilience note
    ---------------
    This is a *batch* screener, so a single failing (code, timeframe) must NOT
    abort the run: any per-(code, timeframe) exception is caught, a
    ``WARNING: skip <code> <tf>: <exc>`` line is printed to stderr, and that row
    is excluded. This deliberately softens the fail-loud rule that governs the
    single-call download methods.
    """
    data_root_path = Path(data_root)
    if downloader is None:
        downloader = TdxDownloader(data_root_path)

    named = _named_conditions(conditions)
    rows: list[dict] = []
    for code in codes:
        for tf_label, domain, kind, freq in TIMEFRAMES:
            try:
                ts_code, df = _load(code, domain, data_root_path, downloader, max_bars, freq)
                ind = compute_all(df, timeframe=kind)
                matched = [name for name, cond in named if cond(ind)]
                close = ind['close'].iloc[-1] if len(ind) else float('nan')
                latest_trade_date = (
                    ind['trade_date'].iloc[-1]
                    if 'trade_date' in ind.columns and len(ind)
                    else None
                )
            except Exception as exc:  # noqa: BLE001 - batch must survive a bad (code, tf)
                print(f'WARNING: skip {code} {tf_label}: {exc}', file=sys.stderr)
                continue
            rows.append(
                {
                    'ts_code': ts_code,
                    'timeframe': tf_label,
                    'close': float(close) if pd.notna(close) else float('nan'),
                    'hit_count': len(matched),
                    'matched': matched,
                    'latest_trade_date': latest_trade_date,
                }
            )

    result = pd.DataFrame(rows, columns=RESULT_COLUMNS)
    if not result.empty:
        # stable sort preserves code-then-timeframe order among equal hit_count ties
        result = result.sort_values('hit_count', ascending=False, kind='mergesort')
        result = result.reset_index(drop=True)
    return result


def _named_conditions(conditions: list) -> list[tuple[str, object]]:
    """Attach a display name to each condition callable.

    Condition callables passed by the caller (e.g. ``golden_cross``) carry
    their ``__name__``; bare lambdas fall back to ``'cond<index>'``. This name
    is what lands in the ``matched`` list — it is cosmetic and not required to
    match the ``CONDITIONS`` registry keys.
    """
    named: list[tuple[str, object]] = []
    for idx, cond in enumerate(conditions):
        name = getattr(cond, '__name__', None) or f'cond{idx}'
        named.append((name, cond))
    return named


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='run_screener',
        description='Batch-screen mainland codes across daily + 5/15/30/60-minute timeframes.',
    )
    parser.add_argument('--codes-file', help='Path to a JSON file holding a list of code strings')
    parser.add_argument('--codes', help='Comma-separated codes (inline)')
    parser.add_argument(
        '--conditions',
        required=True,
        help=f"Comma-separated condition names; one of: {sorted(CONDITIONS)}",
    )
    parser.add_argument('--data-root', default='data', help='Data root (default: data)')
    parser.add_argument(
        '--max-bars',
        type=int,
        default=DEFAULT_MAX_BARS,
        help=f'Max bars to fetch per timeframe when cold (default: {DEFAULT_MAX_BARS})',
    )
    parser.add_argument('--output', help='Optional path to write the result CSV')
    return parser


def _resolve_codes(args: argparse.Namespace) -> list[str]:
    if args.codes_file and args.codes:
        raise SystemExit('error: --codes-file and --codes are mutually exclusive')
    if not args.codes_file and not args.codes:
        raise SystemExit('error: provide --codes or --codes-file')

    if args.codes_file:
        with open(args.codes_file) as fh:
            codes = json.load(fh)
        if not isinstance(codes, list):
            raise SystemExit(f'error: --codes-file must contain a JSON list, got {type(codes).__name__}')
    else:
        codes = [c.strip() for c in args.codes.split(',') if c.strip()]
    return codes


def _resolve_conditions(spec: str) -> list:
    names = [n.strip() for n in spec.split(',') if n.strip()]
    resolved = []
    unknown = []
    for name in names:
        if name in CONDITIONS:
            resolved.append(CONDITIONS[name])
        else:
            unknown.append(name)
    if unknown:
        raise SystemExit(
            f'error: unknown condition(s): {unknown}. '
            f'Available: {sorted(CONDITIONS)}'
        )
    return resolved


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    codes = _resolve_codes(args)
    conditions = _resolve_conditions(args.conditions)

    result = screen(codes, conditions, data_root=args.data_root, max_bars=args.max_bars)

    if args.output:
        result.to_csv(args.output, index=False)
    print(result.to_string(index=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
