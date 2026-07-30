"""量比、分时突破、均价线、炸板和回封信号。

所有计算只使用当前轮询时刻及此前已经收到的数据。分时突破的参考高点
明确排除当前分钟，避免用同一分钟尚未结束的数据反向确认自身。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Mapping, Sequence

from .models import DailyBaseline, IntradaySignal, QuoteSnapshot


@dataclass(frozen=True)
class IntradaySignalConfig:
    """盘中技术信号的可配置阈值。"""

    average_volume_days: int = 5
    minimum_volume_ratio: float = 1.5
    breakout_lookback_minutes: int = 20
    minimum_breakout_history_minutes: int = 5
    breakout_buffer: float = 0.001
    require_above_vwap: bool = True
    buy_score_threshold: float = 3.0
    limit_touch_tolerance: float = 0.001
    stop_loss_rate: float = 0.04
    trailing_stop_rate: float = 0.06
    vwap_exit_buffer: float = 0.01
    sell_below_vwap: bool = True
    sell_on_bomb_limit_up: bool = False

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "IntradaySignalConfig":
        payload = dict(value or {})
        allowed = set(cls.__dataclass_fields__)
        unknown = sorted(set(payload).difference(allowed))
        if unknown:
            raise ValueError(f"unknown intraday signal settings: {', '.join(unknown)}")
        return cls(**payload)


def session_elapsed_minutes(value: datetime) -> int:
    """返回 A 股连续竞价已经经过的分钟数，范围为 0～240。"""

    current = value.time()
    morning_start = time(9, 30)
    morning_end = time(11, 30)
    afternoon_start = time(13, 0)
    afternoon_end = time(15, 0)
    if current < morning_start:
        return 0
    if current <= morning_end:
        return min(120, int((value - value.replace(hour=9, minute=30, second=0, microsecond=0)).total_seconds() // 60) + 1)
    if current < afternoon_start:
        return 120
    if current <= afternoon_end:
        afternoon = int(
            (value - value.replace(hour=13, minute=0, second=0, microsecond=0)).total_seconds()
            // 60
        ) + 1
        return min(240, 120 + afternoon)
    return 240


def estimated_limit_rate(symbol: str) -> float:
    """按代码板块估算普通 A 股涨停幅度。

    ST、新股首日和复牌等特殊规则需要交易所逐日参考价；免费实时快照没有
    完整标识，因此当前结果会在信号详情中明确标记为估算值。
    """

    code = symbol[:6]
    if code.startswith(("300", "301", "688", "689")):
        return 0.20
    return 0.10


def estimated_limit_up(previous_close: float | None, symbol: str) -> float | None:
    if previous_close is None or previous_close <= 0:
        return None
    raw = Decimal(str(previous_close)) * Decimal(str(1 + estimated_limit_rate(symbol)))
    return float(raw.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def estimated_limit_down(previous_close: float | None, symbol: str) -> float | None:
    if previous_close is None or previous_close <= 0:
        return None
    raw = Decimal(str(previous_close)) * Decimal(str(1 - estimated_limit_rate(symbol)))
    return float(raw.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def cumulative_vwap(snapshot: QuoteSnapshot) -> float | None:
    """从累计成交额/成交量估算当日均价线。

    不同 TDX 节点的成交量可能以“股”或“手”表示。若直接相除比当前价高
    一个数量级，则按一手 100 股修正；原始值仍完整保存在快照表。
    """

    volume = snapshot.cumulative_volume
    amount = snapshot.cumulative_amount
    if volume is None or amount is None or volume <= 0 or amount <= 0:
        return None
    value = amount / volume
    if value > snapshot.price * 20:
        value /= 100
    if value <= 0 or value > snapshot.price * 5:
        return None
    return value


class IntradaySignalEngine:
    """根据快照、历史日线基线、分钟线和模拟持仓生成信号。"""

    def __init__(self, config: IntradaySignalConfig | None = None):
        self.config = config or IntradaySignalConfig()

    def evaluate(
        self,
        snapshot: QuoteSnapshot,
        baseline: DailyBaseline,
        previous_minutes: Sequence[Mapping[str, Any]],
        position: Mapping[str, Any] | None = None,
        previous_snapshots: Sequence[Mapping[str, Any]] = (),
    ) -> IntradaySignal:
        minute_start = snapshot.received_at.replace(second=0, microsecond=0)
        elapsed = session_elapsed_minutes(snapshot.received_at)
        volume_ratio: float | None = None
        if (
            baseline.average_daily_volume is not None
            and baseline.average_daily_volume > 0
            and snapshot.cumulative_volume is not None
            and elapsed > 0
        ):
            expected = baseline.average_daily_volume * elapsed / 240
            volume_ratio = snapshot.cumulative_volume / expected if expected > 0 else None

        recent = list(previous_minutes)[-self.config.breakout_lookback_minutes :]
        highs = [
            float(row["high"])
            for row in recent
            if row.get("high") is not None and float(row["high"]) > 0
        ]
        breakout_reference = max(highs) if highs else None
        enough_breakout_history = len(highs) >= self.config.minimum_breakout_history_minutes
        intraday_breakout = bool(
            enough_breakout_history
            and breakout_reference is not None
            and snapshot.price > breakout_reference * (1 + self.config.breakout_buffer)
        )

        vwap = cumulative_vwap(snapshot)
        above_vwap = vwap is not None and snapshot.price >= vwap
        previous_close = snapshot.previous_close or baseline.previous_close
        limit_up = estimated_limit_up(previous_close, snapshot.symbol)
        tolerance = self.config.limit_touch_tolerance
        touched_before = any(
            limit_up is not None
            and row.get("high") is not None
            and float(row["high"]) >= limit_up * (1 - tolerance)
            for row in previous_minutes
        )
        touched_in_snapshots = any(
            limit_up is not None
            and row.get("high") is not None
            and float(row["high"]) >= limit_up * (1 - tolerance)
            for row in previous_snapshots
        )
        currently_at_limit = bool(
            limit_up is not None and snapshot.price >= limit_up * (1 - tolerance)
        )
        day_high = snapshot.high or snapshot.price
        touched_limit = bool(
            limit_up is not None
            and (
                touched_before
                or touched_in_snapshots
                or day_high >= limit_up * (1 - tolerance)
            )
        )
        previously_below_after_touch = any(
            limit_up is not None
            and row.get("high") is not None
            and float(row["high"]) >= limit_up * (1 - tolerance)
            and row.get("close") is not None
            and float(row["close"]) < limit_up * (1 - tolerance)
            for row in previous_minutes
        )
        previously_below_after_touch = previously_below_after_touch or any(
            limit_up is not None
            and row.get("high") is not None
            and float(row["high"]) >= limit_up * (1 - tolerance)
            and row.get("price") is not None
            and float(row["price"]) < limit_up * (1 - tolerance)
            for row in previous_snapshots
        )
        bombed = bool(touched_limit and not currently_at_limit)
        resealed = bool(currently_at_limit and previously_below_after_touch)

        volume_ok = (
            volume_ratio is not None and volume_ratio >= self.config.minimum_volume_ratio
        )
        score = 0.0
        score += 1.0 if volume_ok else 0.0
        score += 1.0 if intraday_breakout else 0.0
        score += 1.0 if above_vwap else 0.0
        score += 2.0 if resealed else 0.0
        score -= 1.0 if bombed else 0.0
        vwap_gate = above_vwap or not self.config.require_above_vwap
        regular_breakout = volume_ok and intraday_breakout and vwap_gate
        reseal_breakout = resealed and vwap_gate
        buy_signal = bool(
            not bombed
            and score >= self.config.buy_score_threshold
            and (regular_breakout or reseal_breakout)
        )
        if reseal_breakout and buy_signal:
            buy_reason = "limit_up_reseal"
        elif regular_breakout and buy_signal:
            buy_reason = "volume_intraday_breakout"
        else:
            buy_reason = None

        sell_signal = False
        sell_reason: str | None = None
        if position:
            entry_price = float(position.get("average_price") or 0)
            highest_price = max(
                float(position.get("highest_price") or entry_price),
                snapshot.price,
            )
            if entry_price > 0 and snapshot.price <= entry_price * (1 - self.config.stop_loss_rate):
                sell_signal = True
                sell_reason = "intraday_stop_loss"
            elif highest_price > 0 and snapshot.price <= highest_price * (
                1 - self.config.trailing_stop_rate
            ):
                sell_signal = True
                sell_reason = "intraday_trailing_stop"
            elif (
                self.config.sell_below_vwap
                and vwap is not None
                and snapshot.price < vwap * (1 - self.config.vwap_exit_buffer)
            ):
                sell_signal = True
                sell_reason = "below_intraday_vwap"
            elif self.config.sell_on_bomb_limit_up and bombed:
                sell_signal = True
                sell_reason = "limit_up_bomb"

        details = {
            "elapsed_session_minutes": elapsed,
            "average_daily_volume": baseline.average_daily_volume,
            "breakout_history_minutes": len(highs),
            "volume_ratio_threshold": self.config.minimum_volume_ratio,
            "buy_score_threshold": self.config.buy_score_threshold,
            "limit_rate_estimated": estimated_limit_rate(snapshot.symbol),
            "limit_rule_is_exact": False,
        }
        return IntradaySignal(
            signal_at=snapshot.received_at,
            minute_start=minute_start,
            trade_date=snapshot.trade_date,
            symbol=snapshot.symbol,
            price=snapshot.price,
            vwap=vwap,
            volume_ratio=volume_ratio,
            breakout_reference=breakout_reference,
            limit_up_price=limit_up,
            above_vwap=above_vwap,
            intraday_breakout=intraday_breakout,
            touched_limit_up=touched_limit,
            bomb_limit_up=bombed,
            resealed_limit_up=resealed,
            buy_score=score,
            buy_signal=buy_signal,
            buy_reason=buy_reason,
            sell_signal=sell_signal,
            sell_reason=sell_reason,
            details=details,
        )
