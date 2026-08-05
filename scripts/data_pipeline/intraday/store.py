"""盘中模拟流水线的 DuckDB 持久化层。

数据库只由一个盘中进程写入；查询页面或研究脚本可以只读连接。所有表都
保留明确时间戳和原因字段，保证一次模拟买卖可以追溯到原始行情与信号。
"""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import duckdb

from .models import IntradaySignal, QuoteSnapshot, WatchItem


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS watchlist_history (
    loaded_at TIMESTAMP NOT NULL,
    source_as_of DATE,
    source_path VARCHAR,
    symbol VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    rank INTEGER,
    score DOUBLE,
    name VARCHAR,
    details_json VARCHAR
);

CREATE TABLE IF NOT EXISTS realtime_snapshots (
    received_at TIMESTAMP NOT NULL,
    trade_date DATE NOT NULL,
    symbol VARCHAR NOT NULL,
    price DOUBLE NOT NULL,
    previous_close DOUBLE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    cumulative_volume DOUBLE,
    cumulative_amount DOUBLE,
    current_volume DOUBLE,
    bid1 DOUBLE,
    ask1 DOUBLE,
    bid1_volume DOUBLE,
    ask1_volume DOUBLE,
    raw_json VARCHAR,
    PRIMARY KEY (received_at, symbol)
);

CREATE TABLE IF NOT EXISTS auction_snapshots (
    captured_at TIMESTAMP NOT NULL,
    trade_date DATE NOT NULL,
    symbol VARCHAR NOT NULL,
    stage VARCHAR NOT NULL,
    price DOUBLE NOT NULL,
    previous_close DOUBLE,
    open DOUBLE,
    cumulative_volume DOUBLE,
    cumulative_amount DOUBLE,
    current_volume DOUBLE,
    bid1 DOUBLE,
    ask1 DOUBLE,
    bid1_volume DOUBLE,
    ask1_volume DOUBLE,
    raw_json VARCHAR,
    PRIMARY KEY (trade_date, symbol, stage)
);

CREATE TABLE IF NOT EXISTS auction_features (
    calculated_at TIMESTAMP NOT NULL,
    trade_date DATE NOT NULL,
    symbol VARCHAR NOT NULL,
    name VARCHAR,
    board VARCHAR,
    industry VARCHAR,
    candidate_source VARCHAR,
    candidate_rank INTEGER,
    daily_score DOUBLE,
    auction_price DOUBLE NOT NULL,
    auction_gap DOUBLE,
    auction_volume_ratio DOUBLE,
    auction_amount_ratio DOUBLE,
    bid_ask_imbalance DOUBLE,
    market_gap_percentile DOUBLE,
    industry_gap_percentile DOUBLE,
    volume_ratio_percentile DOUBLE,
    amount_ratio_percentile DOUBLE,
    auction_score DOUBLE,
    combined_score DOUBLE,
    review_action VARCHAR,
    review_reason VARCHAR,
    details_json VARCHAR,
    PRIMARY KEY (trade_date, symbol)
);

CREATE TABLE IF NOT EXISTS minute_bars_1m (
    minute_start TIMESTAMP NOT NULL,
    trade_date DATE NOT NULL,
    symbol VARCHAR NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume DOUBLE NOT NULL,
    amount DOUBLE NOT NULL,
    cumulative_volume DOUBLE,
    cumulative_amount DOUBLE,
    observations INTEGER NOT NULL,
    is_final BOOLEAN NOT NULL,
    first_received_at TIMESTAMP NOT NULL,
    last_received_at TIMESTAMP NOT NULL,
    PRIMARY KEY (minute_start, symbol)
);

CREATE TABLE IF NOT EXISTS intraday_signals (
    signal_at TIMESTAMP NOT NULL,
    minute_start TIMESTAMP NOT NULL,
    trade_date DATE NOT NULL,
    symbol VARCHAR NOT NULL,
    price DOUBLE NOT NULL,
    vwap DOUBLE,
    volume_ratio DOUBLE,
    breakout_reference DOUBLE,
    limit_up_price DOUBLE,
    above_vwap BOOLEAN NOT NULL,
    intraday_breakout BOOLEAN NOT NULL,
    touched_limit_up BOOLEAN NOT NULL,
    bomb_limit_up BOOLEAN NOT NULL,
    resealed_limit_up BOOLEAN NOT NULL,
    buy_score DOUBLE NOT NULL,
    buy_signal BOOLEAN NOT NULL,
    buy_reason VARCHAR,
    sell_signal BOOLEAN NOT NULL,
    sell_reason VARCHAR,
    details_json VARCHAR,
    PRIMARY KEY (minute_start, symbol)
);

CREATE TABLE IF NOT EXISTS paper_account (
    account_id VARCHAR PRIMARY KEY,
    initial_cash DOUBLE NOT NULL,
    cash DOUBLE NOT NULL,
    realized_pnl DOUBLE NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_positions (
    account_id VARCHAR NOT NULL,
    symbol VARCHAR NOT NULL,
    quantity BIGINT NOT NULL,
    average_price DOUBLE NOT NULL,
    opened_date DATE NOT NULL,
    highest_price DOUBLE NOT NULL,
    last_price DOUBLE NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (account_id, symbol)
);

CREATE TABLE IF NOT EXISTS paper_orders (
    order_id VARCHAR PRIMARY KEY,
    account_id VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    trade_date DATE NOT NULL,
    symbol VARCHAR NOT NULL,
    side VARCHAR NOT NULL,
    quantity BIGINT NOT NULL,
    requested_price DOUBLE NOT NULL,
    status VARCHAR NOT NULL,
    signal_minute TIMESTAMP,
    reason VARCHAR NOT NULL,
    details_json VARCHAR,
    filled_at TIMESTAMP,
    filled_price DOUBLE,
    fee DOUBLE,
    rejection_reason VARCHAR
);

CREATE TABLE IF NOT EXISTS paper_trades (
    trade_id VARCHAR PRIMARY KEY,
    order_id VARCHAR NOT NULL,
    account_id VARCHAR NOT NULL,
    filled_at TIMESTAMP NOT NULL,
    trade_date DATE NOT NULL,
    symbol VARCHAR NOT NULL,
    side VARCHAR NOT NULL,
    quantity BIGINT NOT NULL,
    price DOUBLE NOT NULL,
    fee DOUBLE NOT NULL,
    reason VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_account_snapshots (
    snapshot_at TIMESTAMP NOT NULL,
    trade_date DATE NOT NULL,
    account_id VARCHAR NOT NULL,
    cash DOUBLE NOT NULL,
    market_value DOUBLE NOT NULL,
    total_equity DOUBLE NOT NULL,
    source VARCHAR NOT NULL,
    PRIMARY KEY (snapshot_at, account_id)
);

CREATE TABLE IF NOT EXISTS daily_reconciliation (
    reconciled_at TIMESTAMP NOT NULL,
    trade_date DATE NOT NULL,
    symbol VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    snapshot_open DOUBLE,
    snapshot_high DOUBLE,
    snapshot_low DOUBLE,
    snapshot_close DOUBLE,
    snapshot_volume DOUBLE,
    snapshot_amount DOUBLE,
    daily_open DOUBLE,
    daily_high DOUBLE,
    daily_low DOUBLE,
    daily_close DOUBLE,
    daily_volume DOUBLE,
    daily_amount DOUBLE,
    close_difference DOUBLE,
    volume_difference DOUBLE,
    amount_difference DOUBLE,
    details_json VARCHAR,
    PRIMARY KEY (trade_date, symbol)
);
"""


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))


class IntradayDuckDBStore:
    """封装盘中数据库写入、分钟聚合和模拟账户原子更新。"""

    def __init__(self, database: str | Path):
        path = Path(database).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path.resolve()
        self.connection = duckdb.connect(str(self.path))
        self.connection.execute(SCHEMA_SQL)

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "IntradayDuckDBStore":
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()

    def record_watchlist(
        self,
        items: Sequence[WatchItem],
        *,
        loaded_at: datetime,
        source_as_of: date | None,
        source_path: str | None,
    ) -> None:
        rows = [
            (
                loaded_at,
                source_as_of,
                source_path,
                item.symbol,
                item.source,
                item.rank,
                item.score,
                item.name,
                _json(dict(item.details)),
            )
            for item in items
        ]
        if rows:
            self.connection.executemany(
                """
                INSERT INTO watchlist_history
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def latest_snapshot(self, symbol: str, trade_date: date) -> dict[str, Any] | None:
        row = self.connection.execute(
            """
            SELECT received_at, cumulative_volume, cumulative_amount, price
            FROM realtime_snapshots
            WHERE symbol = ? AND trade_date = ?
            ORDER BY received_at DESC
            LIMIT 1
            """,
            [symbol, trade_date],
        ).fetchone()
        if row is None:
            return None
        return {
            "received_at": row[0],
            "cumulative_volume": row[1],
            "cumulative_amount": row[2],
            "price": row[3],
        }

    def record_snapshot(self, snapshot: QuoteSnapshot) -> None:
        """保存快照并增量更新当前 1 分钟 K 线。"""

        minute_start = snapshot.received_at.replace(second=0, microsecond=0)
        previous = self.latest_snapshot(snapshot.symbol, snapshot.trade_date)
        volume_delta = 0.0
        amount_delta = 0.0
        if previous is not None:
            if (
                snapshot.cumulative_volume is not None
                and previous["cumulative_volume"] is not None
            ):
                volume_delta = max(
                    0.0,
                    snapshot.cumulative_volume - float(previous["cumulative_volume"]),
                )
            if (
                snapshot.cumulative_amount is not None
                and previous["cumulative_amount"] is not None
            ):
                amount_delta = max(
                    0.0,
                    snapshot.cumulative_amount - float(previous["cumulative_amount"]),
                )

        values = [
            snapshot.received_at,
            snapshot.trade_date,
            snapshot.symbol,
            snapshot.price,
            snapshot.previous_close,
            snapshot.open,
            snapshot.high,
            snapshot.low,
            snapshot.cumulative_volume,
            snapshot.cumulative_amount,
            snapshot.current_volume,
            snapshot.bid1,
            snapshot.ask1,
            snapshot.bid1_volume,
            snapshot.ask1_volume,
            snapshot.raw_json,
        ]
        self.connection.execute("BEGIN TRANSACTION")
        try:
            self.connection.execute(
                """
                INSERT OR REPLACE INTO realtime_snapshots
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )
            self.connection.execute(
                """
                UPDATE minute_bars_1m
                SET is_final = true
                WHERE symbol = ? AND trade_date = ? AND minute_start < ?
                """,
                [snapshot.symbol, snapshot.trade_date, minute_start],
            )
            existing = self.connection.execute(
                """
                SELECT open, high, low, volume, amount, observations, first_received_at
                FROM minute_bars_1m
                WHERE symbol = ? AND minute_start = ?
                """,
                [snapshot.symbol, minute_start],
            ).fetchone()
            if existing is None:
                self.connection.execute(
                    """
                    INSERT INTO minute_bars_1m
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        minute_start,
                        snapshot.trade_date,
                        snapshot.symbol,
                        snapshot.price,
                        snapshot.price,
                        snapshot.price,
                        snapshot.price,
                        volume_delta,
                        amount_delta,
                        snapshot.cumulative_volume,
                        snapshot.cumulative_amount,
                        1,
                        False,
                        snapshot.received_at,
                        snapshot.received_at,
                    ],
                )
            else:
                self.connection.execute(
                    """
                    UPDATE minute_bars_1m
                    SET high = ?,
                        low = ?,
                        close = ?,
                        volume = ?,
                        amount = ?,
                        cumulative_volume = ?,
                        cumulative_amount = ?,
                        observations = ?,
                        last_received_at = ?,
                        is_final = false
                    WHERE symbol = ? AND minute_start = ?
                    """,
                    [
                        max(float(existing[1]), snapshot.price),
                        min(float(existing[2]), snapshot.price),
                        snapshot.price,
                        float(existing[3]) + volume_delta,
                        float(existing[4]) + amount_delta,
                        snapshot.cumulative_volume,
                        snapshot.cumulative_amount,
                        int(existing[5]) + 1,
                        snapshot.received_at,
                        snapshot.symbol,
                        minute_start,
                    ],
                )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def record_auction_snapshots(
        self,
        snapshots: Sequence[QuoteSnapshot],
        *,
        stage: str = "final",
    ) -> None:
        """保存集合竞价快照，不把9:25累计量误算为连续竞价分钟K线。"""

        rows = [
            (
                snapshot.received_at,
                snapshot.trade_date,
                snapshot.symbol,
                stage,
                snapshot.price,
                snapshot.previous_close,
                snapshot.open,
                snapshot.cumulative_volume,
                snapshot.cumulative_amount,
                snapshot.current_volume,
                snapshot.bid1,
                snapshot.ask1,
                snapshot.bid1_volume,
                snapshot.ask1_volume,
                snapshot.raw_json,
            )
            for snapshot in snapshots
        ]
        if rows:
            self.connection.executemany(
                """
                INSERT OR REPLACE INTO auction_snapshots
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def record_auction_features(self, records: Sequence[Mapping[str, Any]]) -> None:
        """原子替换一天的竞价评分，重复运行不会制造重复证券。"""

        columns = [
            "calculated_at",
            "trade_date",
            "symbol",
            "name",
            "board",
            "industry",
            "candidate_source",
            "candidate_rank",
            "daily_score",
            "auction_price",
            "auction_gap",
            "auction_volume_ratio",
            "auction_amount_ratio",
            "bid_ask_imbalance",
            "market_gap_percentile",
            "industry_gap_percentile",
            "volume_ratio_percentile",
            "amount_ratio_percentile",
            "auction_score",
            "combined_score",
            "review_action",
            "review_reason",
            "details_json",
        ]
        rows = [
            [
                record.get(column)
                if column != "details_json"
                else _json(record.get("details") or {})
                for column in columns
            ]
            for record in records
        ]
        if not rows:
            return
        self.connection.execute("BEGIN TRANSACTION")
        try:
            self.connection.executemany(
                f"""
                INSERT OR REPLACE INTO auction_features
                ({", ".join(columns)})
                VALUES ({", ".join("?" for _ in columns)})
                """,
                rows,
            )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def finalize_minutes(self, trade_date: date) -> None:
        self.connection.execute(
            "UPDATE minute_bars_1m SET is_final = true WHERE trade_date = ?",
            [trade_date],
        )

    def previous_minutes(
        self,
        symbol: str,
        minute_start: datetime,
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT minute_start, open, high, low, close, volume, amount
            FROM minute_bars_1m
            WHERE symbol = ? AND minute_start < ? AND is_final
            ORDER BY minute_start DESC
            LIMIT ?
            """,
            [symbol, minute_start, limit],
        ).fetchall()
        return [
            {
                "minute_start": row[0],
                "open": row[1],
                "high": row[2],
                "low": row[3],
                "close": row[4],
                "volume": row[5],
                "amount": row[6],
            }
            for row in reversed(rows)
        ]

    def previous_snapshots(
        self,
        symbol: str,
        received_at: datetime,
        *,
        limit: int = 120,
    ) -> list[dict[str, Any]]:
        """读取当前报价前的近期快照，用于识别同一分钟内炸板后回封。"""

        rows = self.connection.execute(
            """
            SELECT received_at, price, high
            FROM realtime_snapshots
            WHERE symbol = ? AND trade_date = ? AND received_at < ?
            ORDER BY received_at DESC
            LIMIT ?
            """,
            [symbol, received_at.date(), received_at, limit],
        ).fetchall()
        return [
            {"received_at": row[0], "price": row[1], "high": row[2]}
            for row in reversed(rows)
        ]

    def record_signal(self, signal: IntradaySignal) -> None:
        row = signal.to_record()
        columns = [
            "signal_at",
            "minute_start",
            "trade_date",
            "symbol",
            "price",
            "vwap",
            "volume_ratio",
            "breakout_reference",
            "limit_up_price",
            "above_vwap",
            "intraday_breakout",
            "touched_limit_up",
            "bomb_limit_up",
            "resealed_limit_up",
            "buy_score",
            "buy_signal",
            "buy_reason",
            "sell_signal",
            "sell_reason",
            "details_json",
        ]
        self.connection.execute(
            f"""
            INSERT OR REPLACE INTO intraday_signals
            ({", ".join(columns)})
            VALUES ({", ".join("?" for _ in columns)})
            """,
            [row[column] for column in columns],
        )

    def ensure_account(
        self,
        *,
        account_id: str,
        initial_cash: float,
        updated_at: datetime,
    ) -> None:
        existing = self.connection.execute(
            "SELECT initial_cash FROM paper_account WHERE account_id = ?",
            [account_id],
        ).fetchone()
        if existing is None:
            self.connection.execute(
                "INSERT INTO paper_account VALUES (?, ?, ?, 0, ?)",
                [account_id, initial_cash, initial_cash, updated_at],
            )
        elif abs(float(existing[0]) - initial_cash) > 0.01:
            raise ValueError(
                f"account {account_id!r} already exists with initial_cash={existing[0]}; "
                "change account_id before using a different initial cash"
            )

    def account(self, account_id: str) -> dict[str, Any]:
        row = self.connection.execute(
            """
            SELECT account_id, initial_cash, cash, realized_pnl, updated_at
            FROM paper_account WHERE account_id = ?
            """,
            [account_id],
        ).fetchone()
        if row is None:
            raise KeyError(f"paper account does not exist: {account_id}")
        return {
            "account_id": row[0],
            "initial_cash": row[1],
            "cash": row[2],
            "realized_pnl": row[3],
            "updated_at": row[4],
        }

    def positions(self, account_id: str) -> dict[str, dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT symbol, quantity, average_price, opened_date,
                   highest_price, last_price, updated_at
            FROM paper_positions WHERE account_id = ?
            ORDER BY symbol
            """,
            [account_id],
        ).fetchall()
        return {
            row[0]: {
                "symbol": row[0],
                "quantity": int(row[1]),
                "average_price": float(row[2]),
                "opened_date": row[3],
                "highest_price": float(row[4]),
                "last_price": float(row[5]),
                "updated_at": row[6],
            }
            for row in rows
        }

    def update_position_mark(
        self,
        account_id: str,
        snapshot: QuoteSnapshot,
    ) -> None:
        self.connection.execute(
            """
            UPDATE paper_positions
            SET highest_price = GREATEST(highest_price, ?),
                last_price = ?,
                updated_at = ?
            WHERE account_id = ? AND symbol = ?
            """,
            [
                snapshot.price,
                snapshot.price,
                snapshot.received_at,
                account_id,
                snapshot.symbol,
            ],
        )

    def mark_position_price(
        self,
        account_id: str,
        symbol: str,
        price: float,
        updated_at: datetime,
    ) -> None:
        """用盘后日线收盘价更新持仓估值，不改变历史最高盘中价。"""

        self.connection.execute(
            """
            UPDATE paper_positions
            SET last_price = ?, updated_at = ?
            WHERE account_id = ? AND symbol = ?
            """,
            [price, updated_at, account_id, symbol],
        )

    def has_active_order(self, account_id: str, symbol: str, side: str) -> bool:
        row = self.connection.execute(
            """
            SELECT COUNT(*)
            FROM paper_orders
            WHERE account_id = ? AND symbol = ? AND side = ?
              AND status IN ('NEW', 'OPEN')
            """,
            [account_id, symbol, side],
        ).fetchone()
        return bool(row and row[0])

    def active_order_count(self, account_id: str, side: str | None = None) -> int:
        if side is None:
            row = self.connection.execute(
                """
                SELECT COUNT(*) FROM paper_orders
                WHERE account_id = ? AND status IN ('NEW', 'OPEN')
                """,
                [account_id],
            ).fetchone()
        else:
            row = self.connection.execute(
                """
                SELECT COUNT(*) FROM paper_orders
                WHERE account_id = ? AND side = ? AND status IN ('NEW', 'OPEN')
                """,
                [account_id, side],
            ).fetchone()
        return int(row[0]) if row else 0

    def has_order_for_signal(
        self,
        account_id: str,
        symbol: str,
        side: str,
        signal_minute: datetime,
    ) -> bool:
        row = self.connection.execute(
            """
            SELECT COUNT(*)
            FROM paper_orders
            WHERE account_id = ? AND symbol = ? AND side = ? AND signal_minute = ?
            """,
            [account_id, symbol, side, signal_minute],
        ).fetchone()
        return bool(row and row[0])

    def create_order(
        self,
        *,
        account_id: str,
        created_at: datetime,
        symbol: str,
        side: str,
        quantity: int,
        requested_price: float,
        signal_minute: datetime,
        reason: str,
        details: Mapping[str, Any],
    ) -> str:
        order_id = uuid.uuid4().hex
        self.connection.execute(
            """
            INSERT INTO paper_orders (
                order_id, account_id, created_at, trade_date, symbol, side,
                quantity, requested_price, status, signal_minute, reason,
                details_json, filled_at, filled_price, fee, rejection_reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'NEW', ?, ?, ?, NULL, NULL, NULL, NULL)
            """,
            [
                order_id,
                account_id,
                created_at,
                created_at.date(),
                symbol,
                side,
                quantity,
                requested_price,
                signal_minute,
                reason,
                _json(dict(details)),
            ],
        )
        return order_id

    def open_orders(self, account_id: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT order_id, created_at, trade_date, symbol, side, quantity,
                   requested_price, signal_minute, reason, details_json
            FROM paper_orders
            WHERE account_id = ? AND status IN ('NEW', 'OPEN')
            ORDER BY created_at, order_id
            """,
            [account_id],
        ).fetchall()
        return [
            {
                "order_id": row[0],
                "created_at": row[1],
                "trade_date": row[2],
                "symbol": row[3],
                "side": row[4],
                "quantity": int(row[5]),
                "requested_price": float(row[6]),
                "signal_minute": row[7],
                "reason": row[8],
                "details_json": row[9],
            }
            for row in rows
        ]

    def reject_order(self, order_id: str, reason: str) -> None:
        self.connection.execute(
            """
            UPDATE paper_orders
            SET status = 'REJECTED', rejection_reason = ?
            WHERE order_id = ?
            """,
            [reason, order_id],
        )

    def expire_orders(self, account_id: str, trade_date: date) -> int:
        """收盘后撤销未成交的当日模拟委托，避免隔夜自动成交。"""

        before = self.active_order_count(account_id)
        self.connection.execute(
            """
            UPDATE paper_orders
            SET status = 'EXPIRED', rejection_reason = 'end_of_day_unfilled'
            WHERE account_id = ? AND trade_date = ? AND status IN ('NEW', 'OPEN')
            """,
            [account_id, trade_date],
        )
        return before - self.active_order_count(account_id)

    def fill_order(
        self,
        *,
        order: Mapping[str, Any],
        account_id: str,
        filled_at: datetime,
        price: float,
        fee: float,
    ) -> bool:
        """在一个事务内更新订单、成交、现金和持仓。"""

        side = str(order["side"])
        symbol = str(order["symbol"])
        quantity = int(order["quantity"])
        account = self.account(account_id)
        position = self.positions(account_id).get(symbol)
        gross = price * quantity
        self.connection.execute("BEGIN TRANSACTION")
        try:
            if side == "BUY":
                total = gross + fee
                if float(account["cash"]) + 1e-9 < total:
                    self.connection.execute("ROLLBACK")
                    self.reject_order(str(order["order_id"]), "insufficient_cash_at_fill")
                    return False
                if position is not None:
                    self.connection.execute("ROLLBACK")
                    self.reject_order(str(order["order_id"]), "position_already_exists")
                    return False
                self.connection.execute(
                    """
                    UPDATE paper_account
                    SET cash = cash - ?, updated_at = ?
                    WHERE account_id = ?
                    """,
                    [total, filled_at, account_id],
                )
                self.connection.execute(
                    """
                    INSERT INTO paper_positions
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        account_id,
                        symbol,
                        quantity,
                        price,
                        filled_at.date(),
                        price,
                        price,
                        filled_at,
                    ],
                )
            elif side == "SELL":
                if position is None or int(position["quantity"]) < quantity:
                    self.connection.execute("ROLLBACK")
                    self.reject_order(str(order["order_id"]), "position_missing_at_fill")
                    return False
                proceeds = gross - fee
                realized = (price - float(position["average_price"])) * quantity - fee
                self.connection.execute(
                    """
                    UPDATE paper_account
                    SET cash = cash + ?, realized_pnl = realized_pnl + ?, updated_at = ?
                    WHERE account_id = ?
                    """,
                    [proceeds, realized, filled_at, account_id],
                )
                if int(position["quantity"]) == quantity:
                    self.connection.execute(
                        "DELETE FROM paper_positions WHERE account_id = ? AND symbol = ?",
                        [account_id, symbol],
                    )
                else:
                    self.connection.execute(
                        """
                        UPDATE paper_positions
                        SET quantity = quantity - ?, last_price = ?, updated_at = ?
                        WHERE account_id = ? AND symbol = ?
                        """,
                        [quantity, price, filled_at, account_id, symbol],
                    )
            else:
                raise ValueError(f"unsupported paper order side: {side}")

            self.connection.execute(
                """
                UPDATE paper_orders
                SET status = 'FILLED', filled_at = ?, filled_price = ?, fee = ?
                WHERE order_id = ?
                """,
                [filled_at, price, fee, order["order_id"]],
            )
            self.connection.execute(
                """
                INSERT INTO paper_trades
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    uuid.uuid4().hex,
                    order["order_id"],
                    account_id,
                    filled_at,
                    filled_at.date(),
                    symbol,
                    side,
                    quantity,
                    price,
                    fee,
                    order["reason"],
                ],
            )
            self.connection.execute("COMMIT")
            return True
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def record_account_snapshot(
        self,
        *,
        account_id: str,
        snapshot_at: datetime,
        prices: Mapping[str, float],
        source: str,
    ) -> dict[str, float]:
        account = self.account(account_id)
        positions = self.positions(account_id)
        market_value = sum(
            value["quantity"] * float(prices.get(symbol, value["last_price"]))
            for symbol, value in positions.items()
        )
        total = float(account["cash"]) + market_value
        self.connection.execute(
            """
            INSERT OR REPLACE INTO paper_account_snapshots
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                snapshot_at,
                snapshot_at.date(),
                account_id,
                account["cash"],
                market_value,
                total,
                source,
            ],
        )
        return {
            "cash": float(account["cash"]),
            "market_value": market_value,
            "total_equity": total,
        }

    def intraday_ohlcv(self, symbol: str, trade_date: date) -> dict[str, float] | None:
        row = self.connection.execute(
            """
            SELECT
                coalesce(arg_max(open, received_at), arg_min(price, received_at)) AS open,
                coalesce(max(high), max(price)) AS high,
                coalesce(min(low), min(price)) AS low,
                arg_max(price, received_at) AS close,
                arg_max(cumulative_volume, received_at) AS volume,
                arg_max(cumulative_amount, received_at) AS amount
            FROM realtime_snapshots
            WHERE symbol = ? AND trade_date = ?
            """,
            [symbol, trade_date],
        ).fetchone()
        if row is None or row[0] is None:
            return None
        return {
            "open": float(row[0]),
            "high": float(row[1]),
            "low": float(row[2]),
            "close": float(row[3]),
            "volume": float(row[4]) if row[4] is not None else 0.0,
            "amount": float(row[5]) if row[5] is not None else 0.0,
        }

    def record_reconciliation(
        self,
        *,
        reconciled_at: datetime,
        symbol: str,
        trade_date: date,
        daily: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        intraday = self.intraday_ohlcv(symbol, trade_date)
        status = "matched"
        if intraday is None:
            status = "missing_intraday"
        elif daily is None:
            status = "missing_daily"
        close_difference = (
            float(intraday["close"]) - float(daily["close"])
            if intraday is not None and daily is not None
            else None
        )
        volume_difference = (
            float(intraday["volume"]) - float(daily.get("vol") or 0)
            if intraday is not None and daily is not None
            else None
        )
        amount_difference = (
            float(intraday["amount"]) - float(daily.get("amount") or 0)
            if intraday is not None and daily is not None
            else None
        )
        if status == "matched" and close_difference is not None and abs(close_difference) > 0.011:
            status = "close_mismatch"
        values = [
            reconciled_at,
            trade_date,
            symbol,
            status,
            *(intraday.get(key) if intraday else None for key in ("open", "high", "low", "close", "volume", "amount")),
            *(daily.get(key) if daily else None for key in ("open", "high", "low", "close", "vol", "amount")),
            close_difference,
            volume_difference,
            amount_difference,
            _json(
                {
                    "snapshot_is_partial": True,
                    "daily_source": "pytdx_daily_bar",
                    "note": "快照累计字段用于对账；本地1分钟序列在进程启动前的部分仍不完整。",
                }
            ),
        ]
        self.connection.execute(
            """
            INSERT OR REPLACE INTO daily_reconciliation
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )
        return {
            "symbol": symbol,
            "trade_date": trade_date.isoformat(),
            "status": status,
            "close_difference": close_difference,
            "volume_difference": volume_difference,
            "amount_difference": amount_difference,
        }

    def table_counts(self) -> dict[str, int]:
        tables: Iterable[str] = (
            "watchlist_history",
            "realtime_snapshots",
            "auction_snapshots",
            "auction_features",
            "minute_bars_1m",
            "intraday_signals",
            "paper_orders",
            "paper_trades",
            "paper_positions",
            "paper_account_snapshots",
            "daily_reconciliation",
        )
        return {
            table: int(self.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in tables
        }

    def recent_orders(self, account_id: str, limit: int = 20) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT order_id, created_at, symbol, side, quantity, requested_price,
                   status, reason, filled_at, filled_price, fee, rejection_reason
            FROM paper_orders
            WHERE account_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            [account_id, limit],
        ).fetchall()
        return [
            {
                "order_id": row[0],
                "created_at": row[1],
                "symbol": row[2],
                "side": row[3],
                "quantity": int(row[4]),
                "requested_price": float(row[5]),
                "status": row[6],
                "reason": row[7],
                "filled_at": row[8],
                "filled_price": row[9],
                "fee": row[10],
                "rejection_reason": row[11],
            }
            for row in rows
        ]

    def recent_signals(self, limit: int = 20) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT signal_at, symbol, price, vwap, volume_ratio,
                   intraday_breakout, bomb_limit_up, resealed_limit_up,
                   buy_score, buy_signal, buy_reason, sell_signal, sell_reason
            FROM intraday_signals
            ORDER BY signal_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        return [
            {
                "signal_at": row[0],
                "symbol": row[1],
                "price": row[2],
                "vwap": row[3],
                "volume_ratio": row[4],
                "intraday_breakout": row[5],
                "bomb_limit_up": row[6],
                "resealed_limit_up": row[7],
                "buy_score": row[8],
                "buy_signal": row[9],
                "buy_reason": row[10],
                "sell_signal": row[11],
                "sell_reason": row[12],
            }
            for row in rows
        ]
