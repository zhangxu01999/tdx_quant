from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from scripts.data_pipeline.connectors.pytdx_client import (
    connected_session,
    create_hq_api,
    fetch_bars_payload,
    fetch_company_info_category_payload,
    fetch_company_info_content_payload,
    fetch_finance_payload,
    fetch_history_transaction_payload,
    fetch_history_minute_time_payload,
    fetch_index_bars_payload,
    fetch_minute_time_payload,
    fetch_security_list_payload,
    fetch_security_count_payload,
    fetch_transaction_payload,
    fetch_xdxr_payload,
)
from scripts.data_pipeline.extractors.tdx_bars import CATEGORY_TO_TABLE, bars_to_dataframe
from scripts.data_pipeline.extractors.tdx_index_bars import index_bars_to_dataframe
from scripts.data_pipeline.extractors.tdx_xdxr import normalize_xdxr_rows
from scripts.data_pipeline.fetch_realtime_watchlist import (
    fetch_exhq_snapshot_rows,
    fetch_hq_snapshot_rows,
    infer_hq_market,
    is_mainland_symbol,
)
from scripts.data_pipeline.code_mapping import hq_market_label
from scripts.data_pipeline.jobs.minute_job import minute_frequency_to_category
from scripts.data_pipeline.jobs.security_list_job import run_security_list_job
from scripts.data_pipeline.jobs.transaction_job import run_transaction_job
from scripts.data_pipeline.jobs.minute_time_job import run_minute_time_job
from scripts.data_pipeline.jobs.company_info_job import run_company_finance_job
from scripts.data_pipeline.jobs.finance_capital_job import run_finance_capital_job
from scripts.data_pipeline.materializers.symbol_writer import write_by_symbol

DEFAULT_DATA_ROOT = Path('data')
PAGE_SIZE = 800
# pytdx category for daily bars, derived from CATEGORY_TO_TABLE (9 -> 'tdx_bars_1d').
DAILY_BAR_CATEGORY = next(cat for cat, table in CATEGORY_TO_TABLE.items() if table == 'tdx_bars_1d')


class TdxDownloader:
    """High-level pytdx download wrapper.

    Persists daily / minute / xdxr history to parquet under
    ``data_root/<domain>/ts_code=<...>/year=.../month=.../day=.../data.parquet``.
    Historical downloads are mainland (HQ) only; exHQ is snapshot-only.
    """

    def __init__(self, data_root: Path = DEFAULT_DATA_ROOT) -> None:
        self.data_root = Path(data_root)

    # ------------------------------------------------------------------
    # market resolution
    # ------------------------------------------------------------------
    def _resolve_market(self, code: str) -> tuple[int | None, str]:
        """Return ``(market, channel)``.

        Mainland 6-digit codes use the HQ channel with an inferred market.
        Non-mainland codes (HK/US/index) resolve to channel ``exhq`` with
        ``market=None`` (resolved at snapshot time).
        """
        if is_mainland_symbol(code):
            return infer_hq_market(code), 'hq'
        return None, 'exhq'

    # ------------------------------------------------------------------
    # history paging
    # ------------------------------------------------------------------
    def _fetch_bars_paged(
        self,
        api,
        *,
        category: int,
        market: int,
        code: str,
        max_bars: int | None,
        fetch=fetch_bars_payload,
    ) -> list[dict]:
        rows: list[dict] = []
        start = 0
        while True:
            count = PAGE_SIZE
            if max_bars is not None:
                remaining = max_bars - len(rows)
                if remaining <= 0:
                    break
                count = min(PAGE_SIZE, remaining)
            page = fetch(
                api,
                category=category,
                market=market,
                code=code,
                start=start,
                count=count,
            )
            if len(page) < count:
                rows.extend(page)
                break
            rows.extend(page)
            start += count
        return rows

    # ------------------------------------------------------------------
    # download: daily
    # ------------------------------------------------------------------
    def download_daily(self, code: str, *, max_bars: int | None = None) -> pd.DataFrame:
        market, channel = self._resolve_market(code)
        if channel != 'hq':
            raise ValueError(
                f'download_daily only supports mainland 6-digit codes; '
                f'{code!r} resolves to channel {channel!r} (historical bars unavailable).'
            )

        api = create_hq_api()
        with connected_session(api):
            payload = self._fetch_bars_paged(
                api,
                category=DAILY_BAR_CATEGORY,
                market=int(market),
                code=code,
                max_bars=max_bars,
            )

        if not payload:
            raise ValueError(f'No daily bars returned for code {code!r} (invalid code?)')

        df = self._normalize_bars(payload, int(market), code)
        write_by_symbol(self.data_root, 'daily', df)
        return df

    # ------------------------------------------------------------------
    # download: minute
    # ------------------------------------------------------------------
    def download_minute(
        self,
        code: str,
        freq: int = 5,
        *,
        max_bars: int | None = None,
    ) -> pd.DataFrame:
        category = minute_frequency_to_category(freq)
        market, channel = self._resolve_market(code)
        if channel != 'hq':
            raise ValueError(
                f'download_minute only supports mainland 6-digit codes; '
                f'{code!r} resolves to channel {channel!r} (historical bars unavailable).'
            )

        api = create_hq_api()
        with connected_session(api):
            payload = self._fetch_bars_paged(
                api,
                category=category,
                market=int(market),
                code=code,
                max_bars=max_bars,
            )

        if not payload:
            raise ValueError(f'No minute bars returned for code {code!r} (invalid code?)')

        df = self._normalize_bars(payload, int(market), code)
        df['trade_time'] = df['datetime'].dt.strftime('%H:%M:%S')
        write_by_symbol(self.data_root, f'minute_{freq}m', df)
        return df

    # ------------------------------------------------------------------
    # download: xdxr
    # ------------------------------------------------------------------
    def download_xdxr(self, code: str) -> pd.DataFrame:
        market, channel = self._resolve_market(code)
        if channel != 'hq':
            raise ValueError(
                f'download_xdxr only supports mainland 6-digit codes; '
                f'{code!r} resolves to channel {channel!r} (xdxr unavailable).'
            )

        api = create_hq_api()
        with connected_session(api):
            payload = fetch_xdxr_payload(api, market=int(market), code=code)

        df = normalize_xdxr_rows(payload, market=int(market), code=code)
        if df.empty:
            raise ValueError(f'No xdxr rows returned for code {code!r}')
        # sort ascending by date; dedup on the full row only (a code can have
        # multiple distinct xdxr events on the same date).
        if 'trade_date' in df.columns:
            df = df.sort_values('trade_date', ascending=True).reset_index(drop=True)
            df = df.drop_duplicates()
        write_by_symbol(self.data_root, 'xdxr', df)
        return df

    # ------------------------------------------------------------------
    # download: security list (full-market enumeration snapshot)
    # ------------------------------------------------------------------
    def download_security_list(self, market: int) -> pd.DataFrame:
        """Page the HQ security list for ``market`` (0=SZ, 1=SH) and persist a
        daily snapshot to ``data/security_list/market=<SZ|SH>/date=<YYYYMMDD>/``.
        Returns the written snapshot (market/date restored from the partition).
        """
        if market not in (0, 1):
            raise ValueError(
                f'download_security_list supports HQ markets 0 (SZ) / 1 (SH); got {market!r}'
            )

        api = create_hq_api()
        with connected_session(api):
            result = run_security_list_job(
                fetch_page=lambda start, count: fetch_security_list_payload(
                    api, market=int(market), start=start
                ),
                fetch_count=lambda: fetch_security_count_payload(api, int(market)),
                market=int(market),
                trade_date=date.today().strftime('%Y%m%d'),
                output_root=self.data_root,
            )
        # Read the leaf file directly; the ``market``/``date`` hive keys live in
        # the path (above this file), so re-attach the market label explicitly.
        df = pd.read_parquet(result['path'])
        df['market'] = hq_market_label(int(market))
        return df

    # ------------------------------------------------------------------
    # download: 分笔成交 tick (date-partitioned)
    # ------------------------------------------------------------------
    def _tick_only_mainland(self, code: str) -> tuple[int, str]:
        market, channel = self._resolve_market(code)
        if channel != 'hq':
            raise ValueError(
                f'tick download only supports mainland 6-digit codes; '
                f'{code!r} resolves to channel {channel!r}.'
            )
        return int(market), channel

    def download_tick(self, code: str, date) -> pd.DataFrame:
        """Download a symbol's full-day 分笔成交 for ``date`` (int ``YYYYMMDD``
        or ``YYYY-MM-DD``) and persist to
        ``data/tdx_transactions/date=<YYYYMMDD>/ts_code=<...>/data.parquet``.
        Returns the written rows with ``ts_code`` re-attached.
        """
        market, _ = self._tick_only_mainland(code)
        trade_date = str(date).replace('-', '')
        api = create_hq_api()
        with connected_session(api):
            result = run_transaction_job(
                fetch_page=lambda start, count: fetch_history_transaction_payload(
                    api, market=market, code=code, start=start, count=count, date=int(trade_date)
                ),
                market=market,
                code=code,
                trade_date=trade_date,
                output_root=self.data_root,
            )
        df = pd.read_parquet(result['path'])
        df['ts_code'] = result['ts_code']
        return df

    def download_tick_today(self, code: str) -> pd.DataFrame:
        """Download today's (possibly intraday-incomplete) 分笔成交 for ``code``."""
        market, _ = self._tick_only_mainland(code)
        trade_date = date.today().strftime('%Y%m%d')
        api = create_hq_api()
        with connected_session(api):
            result = run_transaction_job(
                fetch_page=lambda start, count: fetch_transaction_payload(
                    api, market=market, code=code, start=start, count=count
                ),
                market=market,
                code=code,
                trade_date=trade_date,
                output_root=self.data_root,
            )
        df = pd.read_parquet(result['path'])
        df['ts_code'] = result['ts_code']
        return df

    # ------------------------------------------------------------------
    # download: F10 财务分析 indicators (per-symbol snapshot)
    # ------------------------------------------------------------------
    def download_company_finance(self, code: str) -> pd.DataFrame:
        """Fetch the F10 ``财务分析`` section, parse 主要财务指标 into tidy long
        format, and persist to ``data/company_finance/ts_code=<...>/`` (plus raw
        text under ``data/company_info_raw/``). Returns the parsed indicators.
        """
        market, channel = self._resolve_market(code)
        if channel != 'hq':
            raise ValueError(
                f'download_company_finance only supports mainland 6-digit codes; '
                f'{code!r} resolves to channel {channel!r}.'
            )
        api = create_hq_api()
        with connected_session(api):
            result = run_company_finance_job(
                fetch_category=lambda: fetch_company_info_category_payload(
                    api, market=int(market), code=code
                ),
                fetch_content=lambda filename, start, length: fetch_company_info_content_payload(
                    api, market=int(market), code=code, filename=filename, start=start, length=length
                ),
                market=int(market),
                code=code,
                output_root=self.data_root,
            )
        df = pd.read_parquet(
            self.data_root / 'company_finance' / f'ts_code={result["ts_code"]}' / 'data.parquet'
        )
        df['ts_code'] = result['ts_code']
        return df

    # ------------------------------------------------------------------
    # download: 分时数据 (intraday minute-time line)
    # ------------------------------------------------------------------
    def download_minute_time(self, code: str, date) -> pd.DataFrame:
        """Download a symbol's full-session 分时 for ``date`` and persist to
        ``data/minute_time/date=<YYYYMMDD>/ts_code=<...>/``."""
        market, channel = self._resolve_market(code)
        if channel != 'hq':
            raise ValueError(
                f'download_minute_time only supports mainland 6-digit codes; '
                f'{code!r} resolves to channel {channel!r}.'
            )
        trade_date = str(date).replace('-', '')
        api = create_hq_api()
        with connected_session(api):
            result = run_minute_time_job(
                fetch=lambda: fetch_history_minute_time_payload(
                    api, market=int(market), code=code, date=int(trade_date)
                ),
                market=int(market),
                code=code,
                trade_date=trade_date,
                output_root=self.data_root,
            )
        df = pd.read_parquet(result['path'])
        df['ts_code'] = result['ts_code']
        return df

    def download_minute_time_today(self, code: str) -> pd.DataFrame:
        """Download today's (possibly intraday-incomplete) 分时 for ``code``."""
        market, channel = self._resolve_market(code)
        if channel != 'hq':
            raise ValueError(
                f'download_minute_time only supports mainland 6-digit codes; '
                f'{code!r} resolves to channel {channel!r}.'
            )
        trade_date = date.today().strftime('%Y%m%d')
        api = create_hq_api()
        with connected_session(api):
            result = run_minute_time_job(
                fetch=lambda: fetch_minute_time_payload(api, market=int(market), code=code),
                market=int(market),
                code=code,
                trade_date=trade_date,
                output_root=self.data_root,
            )
        df = pd.read_parquet(result['path'])
        df['ts_code'] = result['ts_code']
        return df

    # ------------------------------------------------------------------
    # download: 股本结构 (get_finance_info snapshot)
    # ------------------------------------------------------------------
    def download_finance_capital(self, code: str) -> pd.DataFrame:
        """Fetch the HQ 股本结构 snapshot (总股本/流通股本/国家股/法人股/B股 +
        IPO/行业/省份) and persist to ``data/finance_capital/ts_code=<...>/``.
        Returns the single-row snapshot."""
        market, channel = self._resolve_market(code)
        if channel != 'hq':
            raise ValueError(
                f'download_finance_capital only supports mainland 6-digit codes; '
                f'{code!r} resolves to channel {channel!r}.'
            )
        api = create_hq_api()
        with connected_session(api):
            result = run_finance_capital_job(
                fetch=lambda: fetch_finance_payload(api, market=int(market), code=code),
                market=int(market),
                code=code,
                output_root=self.data_root,
            )
        df = pd.read_parquet(
            self.data_root / 'finance_capital' / f'ts_code={result["ts_code"]}' / 'data.parquet'
        )
        df['ts_code'] = result['ts_code']
        return df

    # ------------------------------------------------------------------
    # download: 指数 K 线 (index bars)
    # ------------------------------------------------------------------
    def download_index(self, code: str, *, market: int, max_bars: int | None = None) -> pd.DataFrame:
        """Download an index's daily bars and persist to
        ``data/index_daily/ts_code=<...>/``.

        ``market`` must be given explicitly — index codes don't follow equity
        prefix rules (e.g. 上证指数 ``000001`` is SH/market=1 despite the ``000``
        prefix). category 9 = daily.
        """
        if market not in (0, 1):
            raise ValueError(
                f'download_index supports HQ markets 0 (SZ) / 1 (SH); got {market!r}'
            )
        api = create_hq_api()
        with connected_session(api):
            payload = self._fetch_bars_paged(
                api,
                category=DAILY_BAR_CATEGORY,
                market=market,
                code=code,
                max_bars=max_bars,
                fetch=fetch_index_bars_payload,
            )
        if not payload:
            raise ValueError(f'No index bars returned for code {code!r} on market {market!r}')
        df = index_bars_to_dataframe(payload, market=market, code=code)
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.sort_values('datetime', ascending=True).reset_index(drop=True)
        df = df.drop_duplicates(subset=['datetime'])
        write_by_symbol(self.data_root, 'index_daily', df)
        return df

    # ------------------------------------------------------------------
    # snapshot (live, not persisted)
    # ------------------------------------------------------------------
    def snapshot(self, code: str, *, channel: str = 'auto') -> pd.DataFrame:
        """Return a single live quote row (not persisted).

        ``trade_date`` is the snapshot/request date (today), not necessarily a
        trading date — it may fall on a weekend or holiday.
        """
        symbol = code.strip().upper()
        if channel == 'auto':
            _, resolved_channel = self._resolve_market(symbol)
        else:
            resolved_channel = channel

        if resolved_channel == 'hq':
            rows, unsupported, errors = fetch_hq_snapshot_rows([symbol])
        elif resolved_channel == 'exhq':
            rows, unsupported, errors = fetch_exhq_snapshot_rows([symbol])
        else:
            raise ValueError(f'Unsupported snapshot channel: {channel!r}')

        if errors:
            raise RuntimeError(f'Snapshot failed for {symbol!r}: {errors}')
        if not rows or unsupported:
            raise ValueError(
                f'No snapshot for symbol {symbol!r} on channel {resolved_channel!r} '
                f'(unsupported: {unsupported})'
            )

        row = rows[0]
        ts_code = row.get('resolved_code', symbol)
        if row.get('resolved_market') is not None:
            from scripts.data_pipeline.code_mapping import market_code_to_ts_code

            ts_code = market_code_to_ts_code(int(row['resolved_market']), str(ts_code))
        record = {
            'ts_code': ts_code,
            'price': row.get('price'),
            'open': row.get('open'),
            'high': row.get('high'),
            'low': row.get('low'),
            'source_channel': row.get('source_channel', resolved_channel),
            'trade_date': date.today().strftime('%Y%m%d'),
        }
        return pd.DataFrame([record])

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _normalize_bars(
        payload: Iterable[dict[str, Any]],
        market: int,
        code: str,
    ) -> pd.DataFrame:
        df = bars_to_dataframe(payload, market=market, code=code)
        # Ensure `datetime` is a real Timestamp dtype so .dt accessors (sorting,
        # time-of-day extraction) work regardless of the raw payload's dtype.
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.sort_values('datetime', ascending=True).reset_index(drop=True)
        df = df.drop_duplicates(subset=['datetime'])
        return df
