"""Pig 模拟券商 HTTP 客户端的离线契约测试。"""

from __future__ import annotations

import os
import unittest
from datetime import date, datetime
from unittest.mock import patch

from scripts.data_pipeline.intraday.java_broker import (
    JavaBrokerConfig,
    JavaPaperBrokerClient,
)
from scripts.data_pipeline.intraday.models import IntradaySignal, QuoteSnapshot


def _snapshot() -> QuoteSnapshot:
    return QuoteSnapshot(
        received_at=datetime(2026, 8, 8, 9, 31),
        trade_date=date(2026, 8, 8),
        symbol="002396.SZ",
        price=21.0,
        previous_close=20.0,
        open=20.5,
        high=21.0,
        low=20.4,
        cumulative_volume=10000,
        cumulative_amount=210000,
        current_volume=1000,
        bid1=20.99,
        ask1=21.01,
        bid1_volume=800,
        ask1_volume=500,
        raw_json="{}",
    )


class JavaBrokerClientTests(unittest.TestCase):
    def test_enabled_client_requires_secret_from_environment(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "QUANT_SIGNAL_API_KEY"):
                JavaPaperBrokerClient(JavaBrokerConfig(enabled=True))

    def test_quote_and_intraday_signal_follow_pig_envelope_contract(self):
        calls = []

        def transport(url, headers, body, timeout):
            calls.append((url, headers, body, timeout))
            if url.endswith("/quotes"):
                return {"code": 0, "data": []}
            if url.endswith("/state"):
                return {"code": 0, "data": {"summary": {}, "positions": []}}
            return {"code": 0, "data": {"status": "ACCEPTED"}}

        with patch.dict(os.environ, {"QUANT_SIGNAL_API_KEY": "test-secret-12345"}):
            client = JavaPaperBrokerClient(
                JavaBrokerConfig(enabled=True), transport=transport
            )
            snapshot = _snapshot()
            self.assertEqual([], client.process_quotes([snapshot]))
            signal = IntradaySignal(
                signal_at=snapshot.received_at,
                minute_start=snapshot.received_at.replace(second=0, microsecond=0),
                trade_date=snapshot.trade_date,
                symbol=snapshot.symbol,
                price=snapshot.price,
                vwap=20.8,
                volume_ratio=2.0,
                breakout_reference=20.9,
                limit_up_price=22.0,
                above_vwap=True,
                intraday_breakout=True,
                touched_limit_up=False,
                bomb_limit_up=False,
                resealed_limit_up=False,
                buy_score=3.0,
                buy_signal=True,
                buy_reason="volume_intraday_breakout",
                sell_signal=False,
                sell_reason=None,
                details={"volume_ratio": 2.0},
            )
            accepted = client.publish_intraday_signal(signal, snapshot, allow_new_entry=True)

        self.assertEqual("ACCEPTED", accepted["status"])
        quote_body = calls[0][2][0]
        self.assertEqual(64, len(quote_body["quoteId"]))
        self.assertEqual(22.0, quote_body["limitUpPrice"])
        self.assertEqual(18.0, quote_body["limitDownPrice"])
        signal_call = calls[1]
        self.assertEqual("BUY_READY", signal_call[2]["action"])
        self.assertEqual("BUY", signal_call[2]["side"])
        self.assertEqual(signal_call[1]["Idempotency-Key"], signal_call[2]["signalId"])
        self.assertEqual("test-secret-12345", signal_call[1]["X-Quant-Api-Key"])

    def test_account_state_uses_external_paper_ledger(self):
        def transport(url, headers, body, timeout):
            return {
                "code": 0,
                "data": {
                    "summary": {"status": "ACTIVE"},
                    "positions": [{"symbol": "002396.SZ", "totalQuantity": 100}],
                },
            }

        with patch.dict(os.environ, {"QUANT_SIGNAL_API_KEY": "test-secret-12345"}):
            client = JavaPaperBrokerClient(
                JavaBrokerConfig(enabled=True), transport=transport
            )
            state = client.account_state()

        self.assertEqual("ACTIVE", state["summary"]["status"])
        self.assertEqual(100, state["positions"][0]["totalQuantity"])

    def test_auction_sell_watch_is_not_auto_sell_by_default(self):
        calls = []

        def transport(url, headers, body, timeout):
            calls.append(body)
            return {"code": 0, "data": {"status": "ACCEPTED"}}

        features = [
            {
                "symbol": "002396.SZ",
                "name": "星网锐捷",
                "review_action": "SELL_WATCH",
                "review_reason": "auction_gap_below_sell_watch",
                "auction_price": 20.0,
            }
        ]
        with patch.dict(os.environ, {"QUANT_SIGNAL_API_KEY": "test-secret-12345"}):
            client = JavaPaperBrokerClient(
                JavaBrokerConfig(enabled=True), transport=transport
            )
            self.assertEqual([], client.publish_auction_features(features, calculated_at=datetime(2026, 8, 8, 9, 26)))
        self.assertEqual([], calls)

    def test_non_finite_factor_values_are_sent_as_null(self):
        calls = []

        def transport(url, headers, body, timeout):
            calls.append(body)
            return {"code": 0, "data": {"status": "ACCEPTED"}}

        feature = {
            "symbol": "002396.SZ",
            "name": "星网锐捷",
            "review_action": "BUY_ALLOWED",
            "review_reason": "auction_strength_passed",
            "auction_price": 20.0,
            "combined_score": float("nan"),
            "auction_volume_ratio": float("inf"),
        }
        with patch.dict(os.environ, {"QUANT_SIGNAL_API_KEY": "test-secret-12345"}):
            client = JavaPaperBrokerClient(
                JavaBrokerConfig(enabled=True), transport=transport
            )
            client.publish_auction_features(
                [feature], calculated_at=datetime(2026, 8, 8, 9, 26)
            )

        self.assertIsNone(calls[0]["score"])
        self.assertIsNone(calls[0]["factors"]["auction_volume_ratio"])


if __name__ == "__main__":
    unittest.main()
