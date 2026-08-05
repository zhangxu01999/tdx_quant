"""盘中 pytdx 提供器的单连接、分批和字段归一化离线测试。"""

from datetime import datetime

from scripts.data_pipeline.intraday.provider import PytdxWatchlistProvider


class FakeApi:
    def __init__(self):
        self.connect_count = 0
        self.disconnect_count = 0
        self.quote_calls = []

    def connect(self, _host, _port):
        self.connect_count += 1
        return True

    def disconnect(self):
        self.disconnect_count += 1

    def get_security_quotes(self, symbols):
        self.quote_calls.append(list(symbols))
        return [
            {
                "code": code,
                "price": 10.0,
                "last_close": 9.8,
                "open": 9.9,
                "high": 10.1,
                "low": 9.9,
                "vol": 1_000,
                "amount": 1_000_000,
            }
            for _market, code in symbols
        ]

    def get_security_bars(self, _category, _market, _code, _start, _count):
        return [
            {
                "datetime": "2026-07-30 15:00:00",
                "open": 9.9,
                "high": 10.1,
                "low": 9.8,
                "close": 10.0,
                "vol": 1_000,
                "amount": 1_000_000,
            }
        ]


def test_provider_reuses_one_connection_and_batches_quotes() -> None:
    api = FakeApi()
    provider = PytdxWatchlistProvider(
        batch_size=1,
        retries=0,
        api_factory=lambda: api,
        clock=lambda: datetime(2026, 7, 30, 10, 0, 5),
    )

    with provider:
        quotes = provider.fetch_quotes(["000001.SZ", "600519.SH"])
        daily = provider.fetch_latest_daily(["000001.SZ"])

    assert api.connect_count == 1
    assert api.disconnect_count == 1
    assert len(api.quote_calls) == 2
    assert [quote.symbol for quote in quotes] == ["000001.SZ", "600519.SH"]
    assert daily["000001.SZ"]["close"] == 10.0


def test_provider_can_yield_each_quote_batch_before_the_next_request() -> None:
    api = FakeApi()
    provider = PytdxWatchlistProvider(
        batch_size=1,
        retries=0,
        api_factory=lambda: api,
        clock=lambda: datetime(2026, 7, 30, 9, 25, 5),
    )

    with provider:
        batches = provider.iter_quote_batches(["000001.SZ", "600519.SH"])
        first = next(batches)
        assert len(api.quote_calls) == 1
        second = next(batches)

    assert [value.symbol for value in first] == ["000001.SZ"]
    assert [value.symbol for value in second] == ["600519.SH"]
    assert len(api.quote_calls) == 2
