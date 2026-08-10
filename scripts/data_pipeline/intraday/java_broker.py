"""Pig 模拟交易后端客户端。

Python 只产生可审计信号和行情快照；账户、风控、委托、成交与持仓由 Java
端维护。密钥只从环境变量读取，配置文件中不保存任何凭据。
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any, Callable, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .models import IntradaySignal, QuoteSnapshot
from .signals import estimated_limit_down, estimated_limit_up


@dataclass(frozen=True)
class JavaBrokerConfig:
    """Java 模拟券商连接和信号发布参数。"""

    enabled: bool = False
    base_url: str = "http://127.0.0.1:9999/admin"
    api_key_env: str = "QUANT_SIGNAL_API_KEY"
    account_id: int = 1
    strategy_name: str = "aggressive-short-term"
    strategy_version: str = "1"
    target_weight: float = 0.25
    maximum_notional: float | None = None
    timeout_seconds: float = 5.0
    publish_auction_buy_ready: bool = True
    publish_auction_sell_watch: bool = False

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "JavaBrokerConfig":
        payload = dict(value or {})
        config = cls(
            **{
                name: payload[name]
                for name in cls.__dataclass_fields__
                if name in payload
            }
        )
        if not 0 < config.target_weight <= 1:
            raise ValueError("java_broker.target_weight must be in (0, 1]")
        if config.account_id <= 0:
            raise ValueError("java_broker.account_id must be positive")
        if config.timeout_seconds <= 0:
            raise ValueError("java_broker.timeout_seconds must be positive")
        return config


def _stable_id(*parts: object) -> str:
    raw = "|".join(str(part) for part in parts).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _json_default(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _json_safe(value: Any) -> Any:
    """把 pandas/numpy 标量和非有限浮点转换为严格 JSON 可接受的值。"""

    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        return _json_safe(value.item())
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


class JavaPaperBrokerClient:
    """使用 Pig 外部接口发布信号、报价和日终结算请求。"""

    def __init__(
        self,
        config: JavaBrokerConfig,
        *,
        transport: Callable[[str, Mapping[str, str], Any, float], Any] | None = None,
    ):
        self.config = config
        self._transport = transport or self._http_post
        self._api_key = os.environ.get(config.api_key_env, "").strip()
        if config.enabled and len(self._api_key) < 16:
            raise RuntimeError(
                f"Java broker is enabled but environment variable {config.api_key_env} "
                "is empty or shorter than 16 characters"
            )

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    def process_quotes(self, snapshots: Sequence[QuoteSnapshot]) -> list[dict[str, Any]]:
        """推送本轮报价；Java 只会撮合早于本轮报价产生的等待订单。"""

        if not self.enabled or not snapshots:
            return []
        payload = [self._quote_payload(snapshot) for snapshot in snapshots]
        response = self._post("/quant/external/quotes", payload)
        return list(response or [])

    def account_state(self) -> dict[str, Any] | None:
        """读取 Java 权威资金和持仓；启用后不得静默退回本地模拟持仓。"""

        if not self.enabled:
            return None
        response = self._post(
            f"/quant/external/accounts/{self.config.account_id}/state",
            {},
        )
        if not isinstance(response, Mapping):
            raise RuntimeError("Pig broker account state is not an object")
        return dict(response)

    def reconcile(self) -> dict[str, Any] | None:
        """盘前核对 Java 权威账务；异常时服务端自动停止账户并撤销等待委托。"""

        if not self.enabled:
            return None
        response = self._post(f"/quant/external/accounts/{self.config.account_id}/reconcile", {})
        if not isinstance(response, Mapping):
            raise RuntimeError("Pig broker reconciliation report is not an object")
        return dict(response)

    def publish_intraday_signal(
        self,
        signal: IntradaySignal,
        snapshot: QuoteSnapshot,
        *,
        allow_new_entry: bool,
    ) -> dict[str, Any] | None:
        """只发布明确的 B/S；普通观察状态继续留在 DuckDB，不制造委托。"""

        if not self.enabled:
            return None
        if signal.sell_signal:
            action, side, reason = "SELL_READY", "SELL", signal.sell_reason or "intraday_exit"
        elif signal.buy_signal and allow_new_entry:
            action, side, reason = "BUY_READY", "BUY", signal.buy_reason or "intraday_entry"
        else:
            return None
        minute = signal.minute_start.replace(second=0, microsecond=0)
        signal_id = _stable_id(
            self.config.strategy_name,
            self.config.strategy_version,
            signal.trade_date,
            signal.symbol,
            action,
            minute.isoformat(),
        )
        body = self._signal_payload(
            signal_id=signal_id,
            trade_date=signal.trade_date,
            symbol=signal.symbol,
            symbol_name=None,
            action=action,
            side=side,
            reference_price=snapshot.price,
            score=signal.buy_score * 20.0 if side == "BUY" else None,
            daily_score=None,
            auction_score=None,
            generated_at=signal.signal_at,
            valid_after=signal.signal_at,
            expires_at=datetime.combine(signal.trade_date, time(14, 57)),
            reason_code=reason,
            reason=reason,
            factors=dict(signal.details),
        )
        return self._post(
            "/quant/external/signals",
            body,
            idempotency_key=signal_id,
        )

    def publish_auction_features(
        self,
        features: Sequence[Mapping[str, Any]],
        *,
        calculated_at: datetime,
    ) -> list[dict[str, Any]]:
        """把 9:25 复核结果转为等待 9:30 后报价成交的订单。

        ``SELL_WATCH`` 默认只观察，必须显式开启才会转换成 SELL_READY。
        """

        if not self.enabled:
            return []
        results: list[dict[str, Any]] = []
        trade_date = calculated_at.date()
        for feature in features:
            review_action = str(feature.get("review_action") or "")
            if review_action == "BUY_ALLOWED" and self.config.publish_auction_buy_ready:
                action, side = "BUY_READY", "BUY"
            elif review_action == "SELL_WATCH" and self.config.publish_auction_sell_watch:
                action, side = "SELL_READY", "SELL"
            else:
                continue
            symbol = str(feature["symbol"])
            signal_id = _stable_id(
                self.config.strategy_name,
                self.config.strategy_version,
                trade_date,
                symbol,
                action,
                "auction-0925",
            )
            body = self._signal_payload(
                signal_id=signal_id,
                trade_date=trade_date,
                symbol=symbol,
                symbol_name=feature.get("name"),
                action=action,
                side=side,
                reference_price=float(feature["auction_price"]),
                score=feature.get("combined_score"),
                daily_score=feature.get("daily_score"),
                auction_score=feature.get("auction_score"),
                generated_at=calculated_at,
                valid_after=datetime.combine(trade_date, time(9, 30)),
                expires_at=datetime.combine(trade_date, time(10, 0)),
                reason_code=str(feature.get("review_reason") or "auction_review"),
                reason="9:25集合竞价强度复核通过，等待9:30后的下一可成交报价",
                factors={
                    key: feature.get(key)
                    for key in (
                        "auction_gap",
                        "auction_volume_ratio",
                        "auction_amount_ratio",
                        "bid_ask_imbalance",
                        "market_gap_percentile",
                        "industry_gap_percentile",
                    )
                },
            )
            results.append(
                self._post("/quant/external/signals", body, idempotency_key=signal_id)
            )
        return results

    def settle(self, trade_date: date) -> Any:
        if not self.enabled:
            return None
        return self._post(
            f"/quant/external/accounts/{self.config.account_id}/settlements/{trade_date.isoformat()}",
            {},
        )

    def _signal_payload(self, **values: Any) -> dict[str, Any]:
        return {
            "signalId": values["signal_id"],
            "accountId": self.config.account_id,
            "strategyName": self.config.strategy_name,
            "strategyVersion": self.config.strategy_version,
            "tradeDate": values["trade_date"],
            "symbol": values["symbol"],
            "symbolName": values["symbol_name"],
            "action": values["action"],
            "side": values["side"],
            "orderType": "MARKET",
            "requestedQuantity": None,
            "targetWeight": self.config.target_weight if values["side"] == "BUY" else None,
            "maximumNotional": self.config.maximum_notional if values["side"] == "BUY" else None,
            "limitPrice": None,
            "referencePrice": values["reference_price"],
            "score": values["score"],
            "dailyScore": values["daily_score"],
            "auctionScore": values["auction_score"],
            "generatedAt": values["generated_at"],
            "validAfter": values["valid_after"],
            "expiresAt": values["expires_at"],
            "reasonCode": values["reason_code"],
            "reason": values["reason"],
            "factors": values["factors"],
        }

    def _quote_payload(self, snapshot: QuoteSnapshot) -> dict[str, Any]:
        previous_close = snapshot.previous_close
        return {
            "quoteId": _stable_id(
                "quote",
                snapshot.symbol,
                snapshot.received_at.isoformat(),
                snapshot.price,
                snapshot.bid1,
                snapshot.ask1,
            ),
            "symbol": snapshot.symbol,
            "quoteAt": snapshot.received_at,
            "price": snapshot.price,
            "bid1": snapshot.bid1,
            "ask1": snapshot.ask1,
            "bid1Volume": snapshot.bid1_volume,
            "ask1Volume": snapshot.ask1_volume,
            "limitUpPrice": estimated_limit_up(previous_close, snapshot.symbol),
            "limitDownPrice": estimated_limit_down(previous_close, snapshot.symbol),
        }

    def _post(self, path: str, body: Any, *, idempotency_key: str | None = None) -> Any:
        headers = {"X-Quant-Api-Key": self._api_key}
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        envelope = self._transport(
            f"{self.config.base_url.rstrip('/')}{path}",
            headers,
            _json_safe(body),
            self.config.timeout_seconds,
        )
        if not isinstance(envelope, Mapping):
            raise RuntimeError("Pig broker returned a non-object response")
        if int(envelope.get("code", 1)) != 0:
            raise RuntimeError(f"Pig broker rejected request: {envelope.get('msg') or envelope}")
        return envelope.get("data")

    @staticmethod
    def _http_post(url: str, headers: Mapping[str, str], body: Any, timeout: float) -> Any:
        payload = json.dumps(
            body,
            ensure_ascii=False,
            default=_json_default,
            allow_nan=False,
        ).encode("utf-8")
        request = Request(
            url,
            data=payload,
            method="POST",
            headers={"Content-Type": "application/json", **dict(headers)},
        )
        try:
            with urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Pig broker HTTP {exc.code}: {detail}") from exc
        except URLError as exc:
            raise RuntimeError(f"Pig broker connection failed: {exc.reason}") from exc
