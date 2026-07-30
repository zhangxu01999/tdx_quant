"""DuckDB 分钟聚合、模拟订单撮合和 A 股 T+1 的离线测试。"""

from datetime import date, datetime

from scripts.data_pipeline.intraday.models import IntradaySignal, QuoteSnapshot
from scripts.data_pipeline.intraday.paper import PaperBroker, PaperBrokerConfig
from scripts.data_pipeline.intraday.store import IntradayDuckDBStore


def _quote(at: datetime, price: float, volume: float, amount: float) -> QuoteSnapshot:
    return QuoteSnapshot(
        at,
        at.date(),
        "000001.SZ",
        price,
        10.0,
        10.0,
        max(10.0, price),
        min(10.0, price),
        volume,
        amount,
        100,
        price - 0.01,
        price,
        100,
        100,
        "{}",
    )


def _signal(at: datetime, *, buy: bool = False, sell: bool = False) -> IntradaySignal:
    return IntradaySignal(
        at,
        at.replace(second=0, microsecond=0),
        at.date(),
        "000001.SZ",
        10.0,
        9.9,
        2.0,
        9.8,
        11.0,
        True,
        True,
        False,
        False,
        False,
        3.0,
        buy,
        "volume_intraday_breakout" if buy else None,
        sell,
        "intraday_stop_loss" if sell else None,
        {},
    )


def test_snapshot_deltas_build_finalized_one_minute_bars(tmp_path) -> None:
    with IntradayDuckDBStore(tmp_path / "paper.duckdb") as store:
        store.record_snapshot(_quote(datetime(2026, 7, 30, 9, 30, 5), 10.0, 1_000, 10_000))
        store.record_snapshot(_quote(datetime(2026, 7, 30, 9, 30, 30), 10.1, 1_200, 12_020))
        store.record_snapshot(_quote(datetime(2026, 7, 30, 9, 31, 5), 10.2, 1_500, 15_080))
        rows = store.connection.execute(
            """
            SELECT minute_start, open, high, low, close, volume, is_final
            FROM minute_bars_1m ORDER BY minute_start
            """
        ).fetchall()

    assert len(rows) == 2
    assert rows[0][1:6] == (10.0, 10.1, 10.0, 10.1, 200.0)
    assert rows[0][6] is True
    assert rows[1][5] == 300.0
    assert rows[1][6] is False


def test_paper_order_fills_on_next_quote_and_t_plus_one_blocks_same_day_sell(tmp_path) -> None:
    config = PaperBrokerConfig(
        initial_cash=100_000,
        maximum_positions=1,
        position_fraction=0.5,
        slippage_rate=0,
    )
    with IntradayDuckDBStore(tmp_path / "paper.duckdb") as store:
        broker = PaperBroker(store, config)
        first = _quote(datetime(2026, 7, 30, 10, 0, 5), 10.0, 10_000, 100_000)
        broker.initialize(first.received_at)
        created = broker.create_orders(_signal(first.received_at, buy=True), first)
        assert len(created) == 1
        assert broker.process_open_orders({first.symbol: first}) == []

        second = _quote(datetime(2026, 7, 30, 10, 0, 10), 10.0, 10_100, 101_000)
        fills = broker.process_open_orders({second.symbol: second})
        assert len(fills) == 1
        assert store.positions(config.account_id)["000001.SZ"]["quantity"] == 5_000

        same_day = broker.create_orders(_signal(second.received_at, sell=True), second)
        assert same_day == []
        next_day = _quote(datetime(2026, 7, 31, 10, 0, 5), 9.5, 5_000, 47_500)
        sell_orders = broker.create_orders(_signal(next_day.received_at, sell=True), next_day)
        assert len(sell_orders) == 1
        final_quote = _quote(datetime(2026, 7, 31, 10, 0, 10), 9.5, 5_100, 48_450)
        broker.process_open_orders({final_quote.symbol: final_quote})
        assert store.positions(config.account_id) == {}
