"""盘中轮询、分钟聚合、信号、模拟撮合和盘后对账的编排服务。"""

from __future__ import annotations

import time as time_module
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import duckdb

from .models import DailyBaseline, QuoteSnapshot, WatchItem
from .java_broker import JavaPaperBrokerClient
from .paper import PaperBroker
from .provider import PytdxWatchlistProvider, SHANGHAI_TZ
from .signals import IntradaySignalEngine
from .store import IntradayDuckDBStore


@dataclass(frozen=True)
class IntradayServiceConfig:
    poll_seconds: float = 5.0
    reconcile_time: str = "15:05"
    account_snapshot_every_polls: int = 12
    maximum_consecutive_failures: int = 5


def market_phase(value: datetime) -> str:
    """把上海本地时间映射为盘前、连续竞价、午休或盘后。"""

    if value.weekday() >= 5:
        return "closed_day"
    current = value.time()
    if current < time(9, 30):
        return "pre_open"
    if current <= time(11, 30):
        return "trading"
    if current < time(13, 0):
        return "lunch"
    if current <= time(15, 0):
        return "trading"
    return "after_close"


def _compact_date(value: Any) -> date | None:
    text = str(value or "")[:10].replace("/", "-")
    if not text:
        return None
    if "-" not in text and len(text) >= 8:
        text = f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def load_daily_baselines(
    data_root: str | Path,
    symbols: Sequence[str],
    *,
    average_days: int,
    as_of: date | None = None,
) -> dict[str, DailyBaseline]:
    """一次 DuckDB 查询加载整个观察池的日线成交量基线。"""

    root = Path(data_root).expanduser().resolve()
    # 只把观察池对应分区交给 DuckDB，避免每次盘前枚举并打开全市场 5200 个文件。
    files = [
        root / "daily" / f"ts_code={symbol}" / "data.parquet"
        for symbol in symbols
        if (root / "daily" / f"ts_code={symbol}" / "data.parquet").exists()
    ]
    if not files:
        raise FileNotFoundError(f"no TDX daily parquet found under {root / 'daily'}")
    if not symbols:
        return {}
    connection = duckdb.connect(database=":memory:")
    try:
        placeholders = ",".join("?" for _ in symbols)
        day_filter = ""
        parameters: list[Any] = [
            [path.as_posix() for path in files],
            *symbols,
        ]
        if as_of is not None:
            day_filter = "AND REPLACE(CAST(trade_date AS VARCHAR), '-', '') <= ?"
            parameters.append(as_of.strftime("%Y%m%d"))
        parameters.append(average_days)
        rows = connection.execute(
            f"""
            WITH ranked AS (
                SELECT
                    ts_code,
                    close,
                    vol,
                    amount,
                    ROW_NUMBER() OVER (
                        PARTITION BY ts_code ORDER BY datetime DESC
                    ) AS row_number
                FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
                WHERE ts_code IN ({placeholders})
                  {day_filter}
            )
            SELECT
                ts_code,
                arg_min(close, row_number) AS previous_close,
                avg(vol) FILTER (WHERE row_number <= ?) AS average_volume,
                avg(amount) FILTER (WHERE row_number <= ?) AS average_amount,
                count(*) FILTER (WHERE row_number <= ?) AS history_days
            FROM ranked
            WHERE row_number <= ?
            GROUP BY ts_code
            """,
            [*parameters, average_days, average_days, average_days],
        ).fetchall()
    finally:
        connection.close()
    result = {
        str(row[0]): DailyBaseline(
            symbol=str(row[0]),
            previous_close=float(row[1]) if row[1] is not None else None,
            average_daily_volume=float(row[2]) if row[2] is not None else None,
            average_daily_amount=float(row[3]) if row[3] is not None else None,
            history_days=int(row[4]),
        )
        for row in rows
    }
    for symbol in symbols:
        result.setdefault(symbol, DailyBaseline(symbol, None, None, None, 0))
    return result


class IntradayPaperService:
    """执行观察池一次轮询或完整交易日模拟。"""

    def __init__(
        self,
        *,
        store: IntradayDuckDBStore,
        provider: PytdxWatchlistProvider,
        watchlist: Sequence[WatchItem],
        baselines: Mapping[str, DailyBaseline],
        signal_engine: IntradaySignalEngine,
        broker: PaperBroker,
        java_broker: JavaPaperBrokerClient | None = None,
        config: IntradayServiceConfig | None = None,
        clock: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], None] = time_module.sleep,
    ):
        self.store = store
        self.provider = provider
        self.watchlist = list(watchlist)
        self.symbols = [item.symbol for item in watchlist]
        self.entry_allowed = {
            item.symbol: bool(
                item.details.get(
                    "_intraday_allow_entry",
                    item.source != "positions",
                )
            )
            for item in watchlist
        }
        self.baselines = dict(baselines)
        self.signal_engine = signal_engine
        self.broker = broker
        self.java_broker = java_broker
        self.config = config or IntradayServiceConfig()
        self.clock = clock or (lambda: datetime.now(SHANGHAI_TZ).replace(tzinfo=None))
        self.sleeper = sleeper
        self.poll_count = 0

    def process_snapshots(self, snapshots: Iterable[QuoteSnapshot]) -> dict[str, Any]:
        """处理一批快照；可由离线测试直接调用而不连接网络。"""

        values = list(snapshots)
        by_symbol = {snapshot.symbol: snapshot for snapshot in values}
        # 先用本轮报价撮合上一轮已受理订单；本轮新信号只能等待下一次报价。
        remote_fills = self.java_broker.process_quotes(values) if self.java_broker else []
        remote_state = self.java_broker.account_state() if self.java_broker else None
        remote_positions = {
            str(value["symbol"]): {
                "symbol": str(value["symbol"]),
                "quantity": int(value.get("totalQuantity") or 0),
                "available_quantity": int(value.get("availableQuantity") or 0),
                "average_price": float(value.get("averageCost") or 0),
                "highest_price": float(
                    value.get("highestPrice") or value.get("lastPrice") or 0
                ),
                "last_price": float(value.get("lastPrice") or 0),
                "updated_at": value.get("updatedAt"),
                "source": "java_authoritative_account",
            }
            for value in (remote_state or {}).get("positions", [])
            if int(value.get("totalQuantity") or 0) > 0
        }
        fills = self.broker.process_open_orders(by_symbol)
        orders: list[dict[str, Any]] = []
        remote_acceptances: list[dict[str, Any]] = []
        signals = []
        for snapshot in values:
            self.store.record_snapshot(snapshot)
            self.store.update_position_mark(self.broker.config.account_id, snapshot)
            local_position = self.store.positions(self.broker.config.account_id).get(
                snapshot.symbol
            )
            position = (
                remote_positions.get(snapshot.symbol)
                if self.java_broker and self.java_broker.enabled
                else local_position
            )
            previous = self.store.previous_minutes(
                snapshot.symbol,
                snapshot.received_at.replace(second=0, microsecond=0),
                limit=self.signal_engine.config.breakout_lookback_minutes,
            )
            previous_snapshots = self.store.previous_snapshots(
                snapshot.symbol,
                snapshot.received_at,
            )
            baseline = self.baselines.get(
                snapshot.symbol,
                DailyBaseline(snapshot.symbol, snapshot.previous_close, None, None, 0),
            )
            signal = self.signal_engine.evaluate(
                snapshot,
                baseline,
                previous,
                position,
                previous_snapshots,
            )
            self.store.record_signal(signal)
            if self.java_broker:
                acceptance = self.java_broker.publish_intraday_signal(
                    signal,
                    snapshot,
                    allow_new_entry=self.entry_allowed.get(snapshot.symbol, False),
                )
                if acceptance is not None:
                    remote_acceptances.append(acceptance)
            orders.extend(
                self.broker.create_orders(
                    signal,
                    snapshot,
                    allow_new_entry=self.entry_allowed.get(snapshot.symbol, False),
                )
            )
            signals.append(signal)

        self.poll_count += 1
        account_snapshot = None
        interval = max(1, self.config.account_snapshot_every_polls)
        if values and self.poll_count % interval == 0:
            account_snapshot = self.store.record_account_snapshot(
                account_id=self.broker.config.account_id,
                snapshot_at=max(value.received_at for value in values),
                prices={value.symbol: value.price for value in values},
                source="realtime_snapshot",
            )
        return {
            "quotes": len(values),
            "signals": len(signals),
            "buy_signals": sum(signal.buy_signal for signal in signals),
            "sell_signals": sum(signal.sell_signal for signal in signals),
            "orders": orders,
            "fills": fills,
            "remote_acceptances": remote_acceptances,
            "remote_fills": remote_fills,
            "remote_account": (remote_state or {}).get("summary"),
            "remote_position_count": len(remote_positions),
            "account": account_snapshot,
        }

    def poll_once(self) -> dict[str, Any]:
        snapshots = self.provider.fetch_quotes(self.symbols)
        return self.process_snapshots(snapshots)

    def reconcile(self, trade_date: date) -> dict[str, Any]:
        """盘后用 pytdx 最新日 K 对账，并按正式收盘价记录账户权益。"""

        positions = self.store.positions(self.broker.config.account_id)
        symbols = list(dict.fromkeys([*self.symbols, *positions]))
        daily_rows = self.provider.fetch_latest_daily(symbols)
        rows: list[dict[str, Any]] = []
        close_prices: dict[str, float] = {}
        now = self.clock()
        for symbol in symbols:
            raw = daily_rows.get(symbol)
            raw_date = _compact_date(raw.get("datetime")) if raw else None
            daily = raw if raw_date == trade_date else None
            rows.append(
                self.store.record_reconciliation(
                    reconciled_at=now,
                    symbol=symbol,
                    trade_date=trade_date,
                    daily=daily,
                )
            )
            if daily is not None and daily.get("close") is not None:
                close_prices[symbol] = float(daily["close"])
                self.store.mark_position_price(
                    self.broker.config.account_id,
                    symbol,
                    float(daily["close"]),
                    now,
                )
        self.store.finalize_minutes(trade_date)
        expired_orders = self.store.expire_orders(
            self.broker.config.account_id,
            trade_date,
        )
        account = self.store.record_account_snapshot(
            account_id=self.broker.config.account_id,
            snapshot_at=now,
            prices=close_prices,
            source="daily_reconciliation",
        )
        remote_settlement = self.java_broker.settle(trade_date) if self.java_broker else None
        return {
            "trade_date": trade_date.isoformat(),
            "matched": sum(row["status"] == "matched" for row in rows),
            "exceptions": [row for row in rows if row["status"] != "matched"],
            "expired_orders": expired_orders,
            "account": account,
            "remote_settlement": remote_settlement,
        }

    def run(self, *, once: bool = False, ignore_session: bool = False) -> dict[str, Any]:
        """持续运行至收盘；``once`` 用于安装检查和手工单次采样。"""

        self.broker.initialize(self.clock())
        latest: dict[str, Any] = {}
        consecutive_failures = 0
        while True:
            now = self.clock()
            phase = market_phase(now)
            if once:
                if phase != "trading" and not ignore_session:
                    raise RuntimeError(
                        f"market is not in continuous trading session: phase={phase}; "
                        "use --ignore-session only for connectivity testing"
                    )
                return self.poll_once()
            if phase == "closed_day":
                raise RuntimeError("today is not a weekday trading candidate")
            if phase == "trading":
                try:
                    latest = self.poll_once()
                    consecutive_failures = 0
                except Exception as exc:
                    consecutive_failures += 1
                    print(
                        f"[{now:%H:%M:%S}] intraday poll failed "
                        f"({consecutive_failures}/{self.config.maximum_consecutive_failures}): "
                        f"{type(exc).__name__}: {exc}",
                        flush=True,
                    )
                    if consecutive_failures >= self.config.maximum_consecutive_failures:
                        raise RuntimeError(
                            "intraday polling stopped after consecutive failures"
                        ) from exc
                    self.sleeper(max(0.5, self.config.poll_seconds))
                    continue
                print(
                    f"[{now:%H:%M:%S}] quotes={latest['quotes']} "
                    f"buy={latest['buy_signals']} sell={latest['sell_signals']} "
                    f"orders={len(latest['orders'])} fills={len(latest['fills'])}",
                    flush=True,
                )
                for order in latest["orders"]:
                    print(
                        f"  PAPER ORDER {order['side']} {order['symbol']} "
                        f"order_id={order['order_id']}",
                        flush=True,
                    )
                for fill in latest["fills"]:
                    print(
                        f"  PAPER FILL {fill['side']} {fill['symbol']} "
                        f"qty={fill['quantity']} price={fill['price']:.3f} "
                        f"fee={fill['fee']:.2f}",
                        flush=True,
                    )
                self.sleeper(max(0.5, self.config.poll_seconds))
                continue
            if phase in {"pre_open", "lunch"}:
                self.sleeper(min(30.0, max(0.5, self.config.poll_seconds)))
                continue
            reconcile_at = time.fromisoformat(self.config.reconcile_time)
            if now.time() < reconcile_at:
                self.sleeper(min(30.0, max(0.5, self.config.poll_seconds)))
                continue
            latest["reconciliation"] = self.reconcile(now.date())
            return latest
