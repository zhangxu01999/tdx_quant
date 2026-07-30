"""盘中流水线使用的数据模型和行情字段归一化。

pytdx 实时快照字段较多且部分字段可能为空。本模块把策略需要的字段
收敛为稳定结构，同时保留原始 JSON，便于以后排查数据口径。
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any, Mapping

from scripts.data_pipeline.code_mapping import market_code_to_ts_code


def _number(value: Any) -> float | None:
    """把 pytdx 可能返回的字符串/数值安全转换为浮点数。"""

    if value in {None, ""}:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result


@dataclass(frozen=True)
class WatchItem:
    """日频流水线交给盘中进程的一只观察证券。"""

    symbol: str
    source: str
    rank: int | None = None
    score: float | None = None
    name: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DailyBaseline:
    """根据历史日线计算的盘中信号基线。"""

    symbol: str
    previous_close: float | None
    average_daily_volume: float | None
    average_daily_amount: float | None
    history_days: int


@dataclass(frozen=True)
class QuoteSnapshot:
    """一只证券在一次轮询中收到的实时快照。"""

    received_at: datetime
    trade_date: date
    symbol: str
    price: float
    previous_close: float | None
    open: float | None
    high: float | None
    low: float | None
    cumulative_volume: float | None
    cumulative_amount: float | None
    current_volume: float | None
    bid1: float | None
    ask1: float | None
    bid1_volume: float | None
    ask1_volume: float | None
    raw_json: str

    @classmethod
    def from_pytdx(
        cls,
        row: Mapping[str, Any],
        *,
        received_at: datetime,
        requested_market: int | None = None,
        requested_code: str | None = None,
    ) -> "QuoteSnapshot | None":
        """把 ``get_security_quotes`` 的一行转换为稳定结构。

        休市、停牌或异常节点偶尔会返回价格为 0 的行。这类行不进入分钟
        聚合，避免制造不存在的成交价格。
        """

        code = str(row.get("code") or requested_code or "").strip()
        market_value = row.get("market", requested_market)
        if not code or market_value is None:
            return None
        price = _number(row.get("price"))
        if price is None or price <= 0:
            return None
        market = int(market_value)
        raw = dict(row)
        raw.setdefault("market", market)
        raw.setdefault("code", code)
        return cls(
            received_at=received_at,
            trade_date=received_at.date(),
            symbol=market_code_to_ts_code(market, code),
            price=price,
            previous_close=_number(row.get("last_close") or row.get("pre_close")),
            open=_number(row.get("open")),
            high=_number(row.get("high")),
            low=_number(row.get("low")),
            cumulative_volume=_number(row.get("vol") or row.get("volume")),
            cumulative_amount=_number(row.get("amount")),
            current_volume=_number(row.get("cur_vol")),
            bid1=_number(row.get("bid1")),
            ask1=_number(row.get("ask1")),
            bid1_volume=_number(row.get("bid_vol1")),
            ask1_volume=_number(row.get("ask_vol1")),
            raw_json=json.dumps(raw, ensure_ascii=False, default=str, separators=(",", ":")),
        )


@dataclass(frozen=True)
class IntradaySignal:
    """某一分钟最新一次计算得到的盘中信号。"""

    signal_at: datetime
    minute_start: datetime
    trade_date: date
    symbol: str
    price: float
    vwap: float | None
    volume_ratio: float | None
    breakout_reference: float | None
    limit_up_price: float | None
    above_vwap: bool
    intraday_breakout: bool
    touched_limit_up: bool
    bomb_limit_up: bool
    resealed_limit_up: bool
    buy_score: float
    buy_signal: bool
    buy_reason: str | None
    sell_signal: bool
    sell_reason: str | None
    details: Mapping[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        value = asdict(self)
        value["details_json"] = json.dumps(
            value.pop("details"),
            ensure_ascii=False,
            default=str,
            separators=(",", ":"),
        )
        return value
