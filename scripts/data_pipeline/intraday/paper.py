"""仅在 DuckDB 内撮合的 A 股模拟账户。

订单在信号出现后的下一次轮询快照成交，避免使用触发信号的同一个报价。
模拟账户遵守 100 股整手、佣金、印花税、滑点、最大持仓数和 T+1。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .models import IntradaySignal, QuoteSnapshot
from .signals import estimated_limit_down, estimated_limit_up
from .store import IntradayDuckDBStore


@dataclass(frozen=True)
class PaperBrokerConfig:
    account_id: str = "intraday-paper"
    initial_cash: float = 1_000_000.0
    maximum_positions: int = 4
    position_fraction: float = 0.25
    lot_size: int = 100
    commission_rate: float = 0.0003
    minimum_commission: float = 5.0
    stamp_tax_rate: float = 0.0005
    slippage_rate: float = 0.0005

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "PaperBrokerConfig":
        payload = dict(value or {})
        allowed = set(cls.__dataclass_fields__)
        unknown = sorted(set(payload).difference(allowed))
        if unknown:
            raise ValueError(f"unknown paper broker settings: {', '.join(unknown)}")
        config = cls(**payload)
        if config.maximum_positions < 1:
            raise ValueError("maximum_positions must be positive")
        if not 0 < config.position_fraction <= 1:
            raise ValueError("position_fraction must be between 0 and 1")
        if config.lot_size < 1:
            raise ValueError("lot_size must be positive")
        return config


class PaperBroker:
    """根据盘中信号创建订单，并在后续快照完成本地撮合。"""

    def __init__(self, store: IntradayDuckDBStore, config: PaperBrokerConfig):
        self.store = store
        self.config = config

    def initialize(self, now) -> None:
        self.store.ensure_account(
            account_id=self.config.account_id,
            initial_cash=self.config.initial_cash,
            updated_at=now,
        )

    def process_open_orders(self, snapshots: Mapping[str, QuoteSnapshot]) -> list[dict[str, Any]]:
        """用本轮快照撮合上一轮及更早创建的订单。"""

        fills: list[dict[str, Any]] = []
        for order in self.store.open_orders(self.config.account_id):
            snapshot = snapshots.get(order["symbol"])
            if snapshot is None or snapshot.received_at <= order["created_at"]:
                continue
            limit_up = estimated_limit_up(snapshot.previous_close, snapshot.symbol)
            limit_down = estimated_limit_down(snapshot.previous_close, snapshot.symbol)
            if order["side"] == "BUY":
                # 涨停且卖一无量时无法假设排队买单一定成交。
                if (
                    limit_up is not None
                    and snapshot.price >= limit_up - 0.005
                    and not (snapshot.ask1_volume and snapshot.ask1_volume > 0)
                ):
                    continue
                price = min(
                    snapshot.price * (1 + self.config.slippage_rate),
                    limit_up if limit_up is not None else float("inf"),
                )
                gross = price * order["quantity"]
                fee = max(gross * self.config.commission_rate, self.config.minimum_commission)
            else:
                # 跌停且买一无量时同样不能虚构卖出成交。
                if (
                    limit_down is not None
                    and snapshot.price <= limit_down + 0.005
                    and not (snapshot.bid1_volume and snapshot.bid1_volume > 0)
                ):
                    continue
                price = max(
                    snapshot.price * (1 - self.config.slippage_rate),
                    limit_down if limit_down is not None else 0.0,
                )
                gross = price * order["quantity"]
                fee = max(gross * self.config.commission_rate, self.config.minimum_commission)
                fee += gross * self.config.stamp_tax_rate
            filled = self.store.fill_order(
                order=order,
                account_id=self.config.account_id,
                filled_at=snapshot.received_at,
                price=price,
                fee=fee,
            )
            if not filled:
                continue
            fills.append(
                {
                    "order_id": order["order_id"],
                    "symbol": order["symbol"],
                    "side": order["side"],
                    "quantity": order["quantity"],
                    "price": price,
                    "fee": fee,
                }
            )
        return fills

    def _buy_quantity(self, snapshot: QuoteSnapshot) -> int:
        account = self.store.account(self.config.account_id)
        positions = self.store.positions(self.config.account_id)
        market_value = sum(
            value["quantity"] * value["last_price"] for value in positions.values()
        )
        equity = float(account["cash"]) + market_value
        budget = min(float(account["cash"]), equity * self.config.position_fraction)
        estimated_price = snapshot.price * (1 + self.config.slippage_rate)
        raw = int(budget / max(estimated_price, 0.01))
        return raw // self.config.lot_size * self.config.lot_size

    def create_orders(
        self,
        signal: IntradaySignal,
        snapshot: QuoteSnapshot,
        *,
        allow_new_entry: bool = True,
    ) -> list[dict[str, Any]]:
        """把买卖信号转换为去重后的模拟订单。"""

        positions = self.store.positions(self.config.account_id)
        position = positions.get(signal.symbol)
        created: list[dict[str, Any]] = []
        details = {
            "buy_score": signal.buy_score,
            "volume_ratio": signal.volume_ratio,
            "vwap": signal.vwap,
            "breakout_reference": signal.breakout_reference,
            "bomb_limit_up": signal.bomb_limit_up,
            "resealed_limit_up": signal.resealed_limit_up,
        }
        if signal.sell_signal and position is not None:
            # A 股当日买入的仓位不可在当天卖出。
            if position["opened_date"] >= snapshot.trade_date:
                return created
            if not self.store.has_active_order(
                self.config.account_id, signal.symbol, "SELL"
            ) and not self.store.has_order_for_signal(
                self.config.account_id,
                signal.symbol,
                "SELL",
                signal.minute_start,
            ):
                order_id = self.store.create_order(
                    account_id=self.config.account_id,
                    created_at=snapshot.received_at,
                    symbol=signal.symbol,
                    side="SELL",
                    quantity=int(position["quantity"]),
                    requested_price=snapshot.price,
                    signal_minute=signal.minute_start,
                    reason=signal.sell_reason or "intraday_exit",
                    details=details,
                )
                created.append({"order_id": order_id, "side": "SELL", "symbol": signal.symbol})
            return created

        if not allow_new_entry or not signal.buy_signal or position is not None:
            return created
        pending_buys = self.store.active_order_count(self.config.account_id, "BUY")
        if len(positions) + pending_buys >= self.config.maximum_positions:
            return created
        if self.store.has_active_order(self.config.account_id, signal.symbol, "BUY"):
            return created
        if self.store.has_order_for_signal(
            self.config.account_id,
            signal.symbol,
            "BUY",
            signal.minute_start,
        ):
            return created
        quantity = self._buy_quantity(snapshot)
        if quantity < self.config.lot_size:
            return created
        order_id = self.store.create_order(
            account_id=self.config.account_id,
            created_at=snapshot.received_at,
            symbol=signal.symbol,
            side="BUY",
            quantity=quantity,
            requested_price=snapshot.price,
            signal_minute=signal.minute_start,
            reason=signal.buy_reason or "intraday_entry",
            details=details,
        )
        created.append({"order_id": order_id, "side": "BUY", "symbol": signal.symbol})
        return created
