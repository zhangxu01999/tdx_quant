from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if __package__ in {None, ''}:
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

from scripts.data_pipeline.connectors.pytdx_client import (
    connected_session,
    create_hq_api,
    fetch_bars_payload,
    fetch_quotes_payload,
    fetch_xdxr_payload,
)
from scripts.data_pipeline.connectors.tushare_client import (
    create_pro_api,
    fetch_financial_statements_payload,
)
from scripts.data_pipeline.jobs.corporate_actions_job import run_corporate_actions_job
from scripts.data_pipeline.jobs.daily_job import run_daily_bars_job
from scripts.data_pipeline.jobs.financial_job import run_financial_statements_job
from scripts.data_pipeline.jobs.minute_job import minute_frequency_to_category, run_minute_bars_job
from scripts.data_pipeline.jobs.realtime_job import run_quotes_job
from scripts.data_pipeline.source_policy import get_domain_policy


ALIASES = {
    'realtime': 'quotes',
    'quotes': 'quotes',
    'daily_bars': 'daily_bars',
    'minute_bars': 'minute_bars',
    'financial_statements': 'financial_statements',
    'corporate_actions': 'corporate_actions',
}


def resolve_job_sources(domain: str) -> dict:
    policy = get_domain_policy(ALIASES.get(domain, domain))
    return {
        'domain': policy.domain,
        'primary': policy.primary_source,
        'fallbacks': list(policy.fallback_sources),
    }


def fetch_live_payload(domain: str, **kwargs):
    resolved = ALIASES.get(domain, domain)
    if resolved == 'financial_statements':
        pro = create_pro_api()
        return fetch_financial_statements_payload(
            pro,
            api_method=kwargs.get('api_method', 'balancesheet_vip'),
            period=kwargs['trade_date'],
        )

    api = create_hq_api()
    with connected_session(api) as active_api:
        if resolved == 'quotes':
            symbols = kwargs.get('symbols') or [(0, kwargs.get('code', '000001'))]
            return fetch_quotes_payload(active_api, symbols)
        if resolved == 'daily_bars':
            return fetch_bars_payload(
                active_api,
                category=kwargs.get('category', 9),
                market=kwargs['market'],
                code=kwargs['code'],
                start=kwargs.get('start', 0),
                count=kwargs.get('count', 800),
            )
        if resolved == 'minute_bars':
            frequency = int(kwargs['frequency'])
            return fetch_bars_payload(
                active_api,
                category=minute_frequency_to_category(frequency),
                market=kwargs['market'],
                code=kwargs['code'],
                start=kwargs.get('start', 0),
                count=kwargs.get('count', 800),
            )
        if resolved == 'corporate_actions':
            return fetch_xdxr_payload(active_api, market=kwargs['market'], code=kwargs['code'])
    raise NotImplementedError(f'Live fetch is not implemented for domain: {resolved}')


def execute_job(domain: str, *, output_root: Path | str = Path('data'), **kwargs) -> dict:
    resolved = ALIASES.get(domain, domain)
    output_root = Path(output_root)
    payload = kwargs.pop('payload', None)
    if payload is None and resolved in {'quotes', 'daily_bars', 'minute_bars', 'corporate_actions', 'financial_statements'}:
        payload = fetch_live_payload(resolved, **kwargs)

    if resolved == 'quotes':
        return run_quotes_job(
            output_root=output_root,
            payload=payload,
            trade_date=kwargs['trade_date'],
            source=kwargs.get('source', 'pytdx'),
        )
    if resolved == 'daily_bars':
        return run_daily_bars_job(
            output_root=output_root,
            payload=payload,
            market=kwargs['market'],
            code=kwargs['code'],
            trade_date=kwargs['trade_date'],
            source=kwargs.get('source', 'pytdx'),
        )
    if resolved == 'minute_bars':
        return run_minute_bars_job(
            output_root=output_root,
            payload=payload,
            market=kwargs['market'],
            code=kwargs['code'],
            trade_date=kwargs['trade_date'],
            frequency=kwargs['frequency'],
            source=kwargs.get('source', 'pytdx'),
        )
    if resolved == 'corporate_actions':
        return run_corporate_actions_job(
            output_root=output_root,
            payload=payload,
            trade_date=kwargs['trade_date'],
            market=kwargs.get('market'),
            code=kwargs.get('code'),
            source=kwargs.get('source', 'pytdx'),
        )
    if resolved == 'financial_statements':
        return run_financial_statements_job(
            output_root=output_root,
            payload=payload,
            trade_date=kwargs['trade_date'],
            source=kwargs.get('source', 'tushare'),
        )
    raise NotImplementedError(f'Execution not implemented for domain: {resolved}')


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Unified data pipeline entrypoint')
    parser.add_argument('domain', choices=sorted(ALIASES), help='Data domain to inspect or run')
    parser.add_argument('--show-policy', action='store_true', help='Print source policy as JSON')
    parser.add_argument('--dry-run', action='store_true', help='Do not execute a job; return metadata only')
    parser.add_argument('--payload-file', help='JSON file containing source payload rows')
    parser.add_argument('--market', type=int, help='Market code for bar-based jobs')
    parser.add_argument('--code', help='Security code for bar-based jobs')
    parser.add_argument('--trade-date', help='Trade date in YYYYMMDD format')
    parser.add_argument('--frequency', type=int, choices=[5, 15, 30, 60], help='Minute-bar frequency for minute_bars jobs')
    parser.add_argument('--start', type=int, default=0, help='pytdx start offset for bar fetch jobs')
    parser.add_argument('--count', type=int, default=800, help='pytdx bar count for bar fetch jobs')
    parser.add_argument('--output-root', default='data', help='Output root for raw/canonical tables')
    parser.add_argument('--api-method', default='balancesheet_vip', help='Source API method for supported jobs')
    parser.add_argument('--symbols-file', help='JSON file containing quote symbol tuples like [[0, "000001"], [1, "600300"]]')
    return parser


def _load_payload(payload_file: str | None):
    if not payload_file:
        return None
    with open(payload_file, 'r', encoding='utf-8') as handle:
        return json.load(handle)


def _load_symbols(symbols_file: str | None):
    if not symbols_file:
        return None
    with open(symbols_file, 'r', encoding='utf-8') as handle:
        return json.load(handle)


def main() -> int:
    args = build_parser().parse_args()
    payload = resolve_job_sources(args.domain)
    if args.show_policy or args.dry_run:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    source_payload = _load_payload(args.payload_file)
    symbols = _load_symbols(args.symbols_file)
    execution_kwargs = {
        'trade_date': args.trade_date,
        'output_root': Path(args.output_root),
        'api_method': args.api_method,
    }
    if source_payload is not None:
        execution_kwargs['payload'] = source_payload
    if args.market is not None:
        execution_kwargs['market'] = args.market
    if args.code is not None:
        execution_kwargs['code'] = args.code
    if args.frequency is not None:
        execution_kwargs['frequency'] = args.frequency
    execution_kwargs['start'] = args.start
    execution_kwargs['count'] = args.count
    if symbols is not None:
        execution_kwargs['symbols'] = [tuple(item) for item in symbols]

    result = execute_job(args.domain, **execution_kwargs)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
