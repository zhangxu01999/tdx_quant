"""复用单条 pytdx 长连接轮询观察池，并在盘后读取日线对账。

实时服务不会为每只股票重新建立 TCP 连接。观察池按批次调用
``get_security_quotes``，连接异常时才整体重连并进行有界重试。
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from scripts.data_pipeline.connectors.pytdx_client import (
    connect_first_available,
    create_hq_api,
    fetch_bars_payload,
    fetch_quotes_payload,
)
from scripts.data_pipeline.fetch_realtime_watchlist import infer_hq_market

from .models import QuoteSnapshot


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
DAILY_CATEGORY = 9


def _chunks(values: list[Any], size: int) -> Iterable[list[Any]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


class PytdxWatchlistProvider:
    """盘中观察池使用的单连接 pytdx 行情提供器。"""

    def __init__(
        self,
        *,
        batch_size: int = 80,
        retries: int = 3,
        retry_backoff_seconds: float = 1.0,
        api_factory: Callable[[], Any] = create_hq_api,
        clock: Callable[[], datetime] | None = None,
    ):
        if batch_size < 1:
            raise ValueError("quote batch_size must be positive")
        self.batch_size = batch_size
        self.retries = max(0, retries)
        self.retry_backoff_seconds = max(0.0, retry_backoff_seconds)
        self.api_factory = api_factory
        self.clock = clock or (lambda: datetime.now(SHANGHAI_TZ).replace(tzinfo=None))
        self.api: Any | None = None

    def connect(self) -> None:
        self.close()
        api = self.api_factory()
        connect_first_available(api)
        self.api = api

    def close(self) -> None:
        if self.api is not None:
            try:
                self.api.disconnect()
            finally:
                self.api = None

    def __enter__(self) -> "PytdxWatchlistProvider":
        self.connect()
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()

    def _run_with_reconnect(self, operation: Callable[[Any], Any]) -> Any:
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                if self.api is None:
                    self.connect()
                return operation(self.api)
            except Exception as exc:
                last_error = exc
                self.close()
                if attempt < self.retries and self.retry_backoff_seconds:
                    time.sleep(self.retry_backoff_seconds * (attempt + 1))
        raise ConnectionError("pytdx watchlist request failed after retries") from last_error

    def fetch_quotes(self, symbols: Iterable[str]) -> list[QuoteSnapshot]:
        requested = []
        for symbol in symbols:
            value = str(symbol).strip().upper()
            code = value[:6]
            requested.append((infer_hq_market(code), code))
        rows: list[QuoteSnapshot] = []
        for batch in _chunks(requested, self.batch_size):
            def request(api, batch=batch):
                payload = fetch_quotes_payload(api, batch)
                if not payload:
                    raise RuntimeError("pytdx returned an empty realtime quote batch")
                return payload

            payload = self._run_with_reconnect(request)
            received_at = self.clock()
            by_code = {str(row.get("code") or ""): dict(row) for row in payload}
            for market, code in batch:
                row = by_code.get(code)
                if row is None:
                    continue
                snapshot = QuoteSnapshot.from_pytdx(
                    row,
                    received_at=received_at,
                    requested_market=market,
                    requested_code=code,
                )
                if snapshot is not None:
                    rows.append(snapshot)
        return rows

    def fetch_latest_daily(self, symbols: Iterable[str]) -> dict[str, dict[str, Any]]:
        """读取每只观察证券最新一根日 K，供盘后对账，不改写日线 Parquet。"""

        result: dict[str, dict[str, Any]] = {}
        for symbol in symbols:
            canonical = str(symbol).strip().upper()
            code = canonical[:6]
            market = infer_hq_market(code)
            try:
                payload = self._run_with_reconnect(
                    lambda api, market=market, code=code: fetch_bars_payload(
                        api,
                        category=DAILY_CATEGORY,
                        market=market,
                        code=code,
                        start=0,
                        count=2,
                    )
                )
            except ConnectionError:
                continue
            if not payload:
                continue
            latest = max(payload, key=lambda row: str(row.get("datetime") or ""))
            result[canonical] = dict(latest)
        return result
