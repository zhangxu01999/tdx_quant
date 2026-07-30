"""盘中量比、突破、均价线、炸板和回封信号的离线测试。"""

from datetime import date, datetime

from scripts.data_pipeline.intraday.models import DailyBaseline, QuoteSnapshot
from scripts.data_pipeline.intraday.signals import IntradaySignalEngine


def _quote(
    price: float,
    *,
    high: float | None = None,
    volume: float = 6_200,
    amount: float | None = None,
) -> QuoteSnapshot:
    return QuoteSnapshot(
        datetime(2026, 7, 30, 10, 0, 5),
        date(2026, 7, 30),
        "000001.SZ",
        price,
        10.0,
        10.0,
        high or price,
        9.9,
        volume,
        amount if amount is not None else volume * price,
        100,
        price - 0.01,
        price,
        100,
        100,
        "{}",
    )


def test_volume_breakout_above_vwap_generates_buy_signal() -> None:
    previous = [
        {"high": value, "close": value - 0.02}
        for value in [9.50, 9.60, 9.70, 9.80, 9.90]
    ]
    baseline = DailyBaseline("000001.SZ", 10.0, 24_000, 240_000, 5)

    signal = IntradaySignalEngine().evaluate(_quote(10.10), baseline, previous)

    assert signal.volume_ratio is not None and signal.volume_ratio > 1.5
    assert signal.above_vwap
    assert signal.intraday_breakout
    assert signal.buy_signal
    assert signal.buy_reason == "volume_intraday_breakout"


def test_limit_up_bomb_and_reseal_are_stateful() -> None:
    baseline = DailyBaseline("000001.SZ", 10.0, 24_000, 240_000, 5)
    bomb_history = [{"high": 11.0, "close": 10.80}] * 5

    bomb = IntradaySignalEngine().evaluate(
        _quote(10.80, high=11.0),
        baseline,
        bomb_history,
    )
    reseal = IntradaySignalEngine().evaluate(
        _quote(11.0, high=11.0),
        baseline,
        bomb_history,
    )

    assert bomb.touched_limit_up and bomb.bomb_limit_up
    assert not bomb.buy_signal
    assert reseal.resealed_limit_up
    assert reseal.buy_signal
    assert reseal.buy_reason == "limit_up_reseal"


def test_reseal_can_be_detected_between_two_polls_in_same_minute() -> None:
    baseline = DailyBaseline("000001.SZ", 10.0, 24_000, 240_000, 5)
    previous_minutes = [{"high": 10.5, "close": 10.4}] * 5
    previous_snapshots = [{"high": 11.0, "price": 10.85}]

    signal = IntradaySignalEngine().evaluate(
        _quote(11.0, high=11.0),
        baseline,
        previous_minutes,
        previous_snapshots=previous_snapshots,
    )

    assert signal.resealed_limit_up
    assert signal.buy_signal
