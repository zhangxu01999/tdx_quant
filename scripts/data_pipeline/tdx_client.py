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
    fetch_xdxr_payload,
)
from scripts.data_pipeline.extractors.tdx_bars import CATEGORY_TO_TABLE, bars_to_dataframe
from scripts.data_pipeline.extractors.tdx_xdxr import normalize_xdxr_rows
from scripts.data_pipeline.fetch_realtime_watchlist import (
    fetch_exhq_snapshot_rows,
    fetch_hq_snapshot_rows,
    infer_hq_market,
    is_mainland_symbol,
)
from scripts.data_pipeline.jobs.minute_job import minute_frequency_to_category
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
            page = fetch_bars_payload(
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
