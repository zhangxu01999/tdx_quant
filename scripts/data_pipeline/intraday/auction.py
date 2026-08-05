"""集合竞价横截面特征和日频候选复核。

本模块只处理9:25之后已经观察到的行情，不负责成交。输出的买入/卖出
建议默认是影子信号；真正模拟成交必须等到9:30后的下一次实时快照，避免
把事后看到的最终竞价价格当成自己已经成交的价格。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Sequence

import pandas as pd

from .models import DailyBaseline, QuoteSnapshot, WatchItem


@dataclass(frozen=True)
class AuctionScoringConfig:
    """竞价评分、日频融合和影子复核阈值。"""

    market_gap_weight: float = 0.25
    industry_gap_weight: float = 0.20
    volume_ratio_weight: float = 0.20
    amount_ratio_weight: float = 0.25
    order_imbalance_weight: float = 0.10
    daily_score_weight: float = 0.60
    auction_score_weight: float = 0.40
    minimum_entry_gap: float = -0.03
    maximum_entry_gap: float = 0.05
    minimum_buy_score: float = 60.0
    sell_watch_gap: float = -0.03
    sell_watch_score: float = 25.0
    minimum_industry_members: int = 5

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "AuctionScoringConfig":
        payload = dict(value or {})
        return cls(
            **{
                field: payload[field]
                for field in cls.__dataclass_fields__
                if field in payload
            }
        )

    def validate(self) -> None:
        weights = (
            self.market_gap_weight,
            self.industry_gap_weight,
            self.volume_ratio_weight,
            self.amount_ratio_weight,
            self.order_imbalance_weight,
        )
        if any(weight < 0 for weight in weights):
            raise ValueError("auction factor weights cannot be negative")
        if sum(weights) <= 0:
            raise ValueError("at least one auction factor weight must be positive")
        if self.daily_score_weight < 0 or self.auction_score_weight < 0:
            raise ValueError("daily/auction score weights cannot be negative")
        if self.daily_score_weight + self.auction_score_weight <= 0:
            raise ValueError("daily/auction score weights cannot both be zero")
        if self.minimum_entry_gap > self.maximum_entry_gap:
            raise ValueError("minimum_entry_gap cannot exceed maximum_entry_gap")
        if not 0 <= self.minimum_buy_score <= 100:
            raise ValueError("minimum_buy_score must be between 0 and 100")
        if not 0 <= self.sell_watch_score <= 100:
            raise ValueError("sell_watch_score must be between 0 and 100")
        if self.minimum_industry_members < 1:
            raise ValueError("minimum_industry_members must be positive")


def _percentile(series: pd.Series) -> pd.Series:
    """把有效横截面值转换成0～100百分位，缺失值继续保持缺失。"""

    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.rank(method="average", pct=True) * 100.0


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return max(0.0, float(numerator)) / float(denominator)


def _imbalance(snapshot: QuoteSnapshot) -> float | None:
    bid = snapshot.bid1_volume
    ask = snapshot.ask1_volume
    if bid is None or ask is None or bid + ask <= 0:
        return None
    return max(-1.0, min(1.0, (bid - ask) / (bid + ask)))


def _auction_price(snapshot: QuoteSnapshot) -> float:
    """9:25后优先使用开盘字段；节点缺失时保留当前价格并在详情中审计。"""

    return float(snapshot.open) if snapshot.open is not None and snapshot.open > 0 else snapshot.price


def _candidate_lookup(items: Sequence[WatchItem]) -> dict[str, WatchItem]:
    return {item.symbol: item for item in items}


def build_auction_features(
    snapshots: Sequence[QuoteSnapshot],
    *,
    baselines: Mapping[str, DailyBaseline],
    security_metadata: Mapping[str, Mapping[str, Any]],
    candidates: Sequence[WatchItem],
    calculated_at: datetime,
    config: AuctionScoringConfig | None = None,
) -> list[dict[str, Any]]:
    """为全市场快照计算百分位，并为日频候选生成影子买卖复核。

    竞价强弱必须放在同一天的横截面中比较，因此先对全市场计算百分位，
    再读取候选角色。没有进入日频观察池的证券也会落库，但不会生成操作建议。
    """

    settings = config or AuctionScoringConfig()
    settings.validate()
    candidate_by_symbol = _candidate_lookup(candidates)
    rows: list[dict[str, Any]] = []
    for snapshot in snapshots:
        baseline = baselines.get(snapshot.symbol)
        metadata = dict(security_metadata.get(snapshot.symbol) or {})
        candidate = candidate_by_symbol.get(snapshot.symbol)
        auction_price = _auction_price(snapshot)
        previous_close = snapshot.previous_close
        if (previous_close is None or previous_close <= 0) and baseline is not None:
            previous_close = baseline.previous_close
        gap = (
            auction_price / previous_close - 1.0
            if previous_close is not None and previous_close > 0
            else None
        )
        rows.append(
            {
                "calculated_at": calculated_at,
                "trade_date": snapshot.trade_date,
                "symbol": snapshot.symbol,
                "name": metadata.get("name") or (candidate.name if candidate else None),
                "board": metadata.get("board"),
                "industry": metadata.get("industry"),
                "candidate_source": candidate.source if candidate else None,
                "candidate_rank": candidate.rank if candidate else None,
                "daily_score": candidate.score if candidate else None,
                "auction_price": auction_price,
                "auction_gap": gap,
                "auction_volume_ratio": _ratio(
                    snapshot.cumulative_volume,
                    baseline.average_daily_volume if baseline else None,
                ),
                "auction_amount_ratio": _ratio(
                    snapshot.cumulative_amount,
                    baseline.average_daily_amount if baseline else None,
                ),
                "bid_ask_imbalance": _imbalance(snapshot),
                "price_field": "open"
                if snapshot.open is not None and snapshot.open > 0
                else "price",
                "candidate_details": dict(candidate.details) if candidate else {},
            }
        )

    if not rows:
        return []
    frame = pd.DataFrame(rows)
    frame["market_gap_percentile"] = _percentile(frame["auction_gap"])
    frame["volume_ratio_percentile"] = _percentile(frame["auction_volume_ratio"])
    frame["amount_ratio_percentile"] = _percentile(frame["auction_amount_ratio"])
    frame["industry_gap_percentile"] = frame["market_gap_percentile"]
    valid_industry = frame["industry"].notna() & frame["industry"].ne("")
    if valid_industry.any():
        industry_sizes = frame.loc[valid_industry].groupby("industry")["symbol"].transform("size")
        eligible = industry_sizes >= settings.minimum_industry_members
        indexes = industry_sizes.index[eligible]
        frame.loc[indexes, "industry_gap_percentile"] = (
            frame.loc[indexes].groupby("industry")["auction_gap"].rank(method="average", pct=True)
            * 100.0
        )

    # 买一/卖一不平衡度天然位于[-1, 1]，映射为0～100后再参与统一加权。
    imbalance_score = ((frame["bid_ask_imbalance"] + 1.0) * 50.0).fillna(50.0)
    factors = {
        "market_gap_percentile": settings.market_gap_weight,
        "industry_gap_percentile": settings.industry_gap_weight,
        "volume_ratio_percentile": settings.volume_ratio_weight,
        "amount_ratio_percentile": settings.amount_ratio_weight,
    }
    numerator = pd.Series(0.0, index=frame.index)
    denominator = pd.Series(0.0, index=frame.index)
    for column, weight in factors.items():
        valid = frame[column].notna()
        numerator.loc[valid] += frame.loc[valid, column] * weight
        denominator.loc[valid] += weight
    numerator += imbalance_score * settings.order_imbalance_weight
    denominator += settings.order_imbalance_weight
    frame["auction_score"] = (numerator / denominator).clip(lower=0.0, upper=100.0)

    blend_total = settings.daily_score_weight + settings.auction_score_weight
    daily = pd.to_numeric(frame["daily_score"], errors="coerce")
    frame["combined_score"] = frame["auction_score"]
    has_daily = daily.notna()
    frame.loc[has_daily, "combined_score"] = (
        daily.loc[has_daily].clip(lower=0.0, upper=100.0) * settings.daily_score_weight
        + frame.loc[has_daily, "auction_score"] * settings.auction_score_weight
    ) / blend_total

    result: list[dict[str, Any]] = []
    for record in frame.to_dict("records"):
        candidate = candidate_by_symbol.get(str(record["symbol"]))
        action = "MARKET_ONLY"
        reason = "not_in_daily_watchlist"
        roles = set(candidate.details.get("_intraday_roles") or []) if candidate else set()
        allow_entry = bool(candidate and candidate.details.get("_intraday_allow_entry", candidate.source != "positions"))
        gap = record.get("auction_gap")
        score = float(record["auction_score"])
        if candidate is not None and (candidate.source == "positions" or "position" in roles):
            if gap is not None and gap <= settings.sell_watch_gap:
                action, reason = "SELL_WATCH", "auction_gap_below_sell_watch"
            elif score <= settings.sell_watch_score:
                action, reason = "SELL_WATCH", "auction_score_below_sell_watch"
            else:
                action, reason = "HOLD", "position_auction_not_weak"
        elif candidate is not None and allow_entry:
            if gap is None:
                action, reason = "BUY_REJECTED", "missing_previous_close"
            elif gap < settings.minimum_entry_gap:
                action, reason = "BUY_REJECTED", "auction_gap_too_low"
            elif gap > settings.maximum_entry_gap:
                action, reason = "BUY_REJECTED", "auction_gap_too_high"
            elif score >= settings.minimum_buy_score:
                action, reason = "BUY_ALLOWED", "auction_strength_confirmed"
            else:
                action, reason = "WATCH", "auction_strength_not_confirmed"
        elif candidate is not None:
            action, reason = "HOLD", "market_gate_disallows_new_entries"

        details = {
            "mode": "shadow",
            "price_field": record.pop("price_field"),
            "candidate_roles": sorted(roles),
            "candidate_details": record.pop("candidate_details"),
            "execution_note": "9:25复核仅生成影子建议；模拟成交必须使用9:30后的下一行情。",
        }
        record["review_action"] = action
        record["review_reason"] = reason
        record["details"] = details
        # Pandas使用NaN表达缺失，写JSON/DuckDB前统一还原为None。
        result.append(
            {
                key: None if pd.isna(value) else value
                for key, value in record.items()
            }
        )
    return result


def build_auction_report(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """生成适合人工检查和后续模拟交易消费的轻量JSON。"""

    if not records:
        raise ValueError("auction feature records cannot be empty")
    candidates = [
        dict(record)
        for record in records
        if record.get("candidate_source") is not None
    ]
    candidates.sort(
        key=lambda value: (
            -(float(value.get("combined_score") or 0.0)),
            int(value.get("candidate_rank") or 999_999),
            str(value.get("symbol")),
        )
    )
    actions: dict[str, int] = {}
    for candidate in candidates:
        action = str(candidate.get("review_action"))
        actions[action] = actions.get(action, 0) + 1
    captured = [record["calculated_at"] for record in records]
    trade_date = records[0]["trade_date"]
    valid_gaps = [float(record["auction_gap"]) for record in records if record.get("auction_gap") is not None]
    return {
        "schema_version": "1.0",
        "mode": "shadow",
        "trade_date": trade_date.isoformat() if hasattr(trade_date, "isoformat") else str(trade_date),
        "calculated_at": max(captured).isoformat(),
        "universe": {
            "snapshot_count": len(records),
            "valid_gap_count": len(valid_gaps),
            "average_gap": sum(valid_gaps) / len(valid_gaps) if valid_gaps else None,
            "advance_ratio": (
                sum(value > 0 for value in valid_gaps) / len(valid_gaps)
                if valid_gaps
                else None
            ),
        },
        "candidate_summary": {
            "count": len(candidates),
            "actions": actions,
        },
        "candidates": candidates,
        "notes": [
            "竞价结果当前只作为影子复核，不会自动创建模拟订单。",
            "9:25之后观察到的竞价价格不得作为本账户已经成交的价格。",
            "正式成交复核必须使用9:30之后的下一条实时行情。",
        ],
    }
