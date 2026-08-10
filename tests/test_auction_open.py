"""集合竞价横截面评分、候选复核和DuckDB落库的离线测试。"""

from datetime import date, datetime

import pandas as pd

from scripts.data_pipeline.auction_open_cli import _server_time, run_capture
from scripts.data_pipeline.intraday.auction import (
    AuctionScoringConfig,
    build_auction_features,
    build_auction_report,
)
from scripts.data_pipeline.intraday.models import DailyBaseline, QuoteSnapshot, WatchItem
from scripts.data_pipeline.intraday.store import IntradayDuckDBStore


def _snapshot(
    symbol: str,
    *,
    price: float,
    previous_close: float = 10.0,
    volume: float = 1_000,
    amount: float = 10_000,
    bid_volume: float = 200,
    ask_volume: float = 100,
) -> QuoteSnapshot:
    captured = datetime(2026, 8, 4, 9, 25, 5)
    return QuoteSnapshot(
        captured,
        captured.date(),
        symbol,
        price,
        previous_close,
        price,
        price,
        price,
        volume,
        amount,
        volume,
        price,
        price + 0.01,
        bid_volume,
        ask_volume,
        "{}",
    )


def test_auction_features_rank_market_and_review_daily_candidates() -> None:
    snapshots = [
        _snapshot("000001.SZ", price=10.4, volume=3_000, amount=31_200),
        _snapshot("000002.SZ", price=9.5, volume=500, amount=4_750, bid_volume=50, ask_volume=300),
        _snapshot("600000.SH", price=10.0, volume=1_000, amount=10_000),
    ]
    baselines = {
        symbol: DailyBaseline(symbol, 10.0, 100_000, 1_000_000, 5)
        for symbol in ("000001.SZ", "000002.SZ", "600000.SH")
    }
    metadata = {
        "000001.SZ": {"name": "强势候选", "board": "SZ_MAIN", "industry": "电子"},
        "000002.SZ": {"name": "已有持仓", "board": "SZ_MAIN", "industry": "电子"},
        "600000.SH": {"name": "市场样本", "board": "SH_MAIN", "industry": "银行"},
    }
    candidates = [
        WatchItem(
            "000001.SZ",
            "target",
            rank=1,
            score=80,
            details={"_intraday_roles": ["target"], "_intraday_allow_entry": True},
        ),
        WatchItem(
            "000002.SZ",
            "positions",
            rank=1,
            score=70,
            details={"_intraday_roles": ["position"], "_intraday_allow_entry": False},
        ),
    ]
    config = AuctionScoringConfig(
        minimum_buy_score=50,
        sell_watch_gap=-0.03,
        minimum_industry_members=2,
    )

    records = build_auction_features(
        snapshots,
        baselines=baselines,
        security_metadata=metadata,
        candidates=candidates,
        calculated_at=datetime(2026, 8, 4, 9, 26),
        config=config,
    )
    by_symbol = {record["symbol"]: record for record in records}

    assert by_symbol["000001.SZ"]["review_action"] == "BUY_ALLOWED"
    assert by_symbol["000001.SZ"]["combined_score"] > 50
    assert by_symbol["000002.SZ"]["review_action"] == "SELL_WATCH"
    assert by_symbol["600000.SH"]["review_action"] == "MARKET_ONLY"
    assert by_symbol["000001.SZ"]["market_gap_percentile"] > by_symbol["000002.SZ"]["market_gap_percentile"]

    report = build_auction_report(records)
    assert report["mode"] == "shadow"
    assert report["universe"]["snapshot_count"] == 3
    assert report["candidate_summary"]["actions"] == {
        "BUY_ALLOWED": 1,
        "SELL_WATCH": 1,
    }


def test_auction_snapshots_and_features_are_idempotent_per_day(tmp_path) -> None:
    snapshot = _snapshot("000001.SZ", price=10.2)
    record = {
        "calculated_at": datetime(2026, 8, 4, 9, 26),
        "trade_date": date(2026, 8, 4),
        "symbol": "000001.SZ",
        "name": "平安银行",
        "board": "SZ_MAIN",
        "industry": "银行",
        "candidate_source": "target",
        "candidate_rank": 1,
        "daily_score": 80.0,
        "auction_price": 10.2,
        "auction_gap": 0.02,
        "auction_volume_ratio": 0.01,
        "auction_amount_ratio": 0.01,
        "bid_ask_imbalance": 0.2,
        "market_gap_percentile": 80.0,
        "industry_gap_percentile": 75.0,
        "volume_ratio_percentile": 70.0,
        "amount_ratio_percentile": 72.0,
        "auction_score": 74.0,
        "combined_score": 77.6,
        "review_action": "BUY_ALLOWED",
        "review_reason": "auction_strength_confirmed",
        "details": {"mode": "shadow"},
    }

    with IntradayDuckDBStore(tmp_path / "paper.duckdb") as store:
        store.record_auction_snapshots([snapshot])
        store.record_auction_snapshots([snapshot])
        store.record_auction_features([record])
        store.record_auction_features([record])
        counts = store.table_counts()
        saved = store.connection.execute(
            "SELECT review_action, auction_score FROM auction_features"
        ).fetchone()

    assert counts["auction_snapshots"] == 1
    assert counts["auction_features"] == 1
    assert saved == ("BUY_ALLOWED", 74.0)


def test_capture_pipeline_uses_local_master_and_writes_shadow_report(tmp_path) -> None:
    data_root = tmp_path / "data"
    master_path = data_root / "security_master_daily" / "trade_date=20260803" / "data.parquet"
    master_path.parent.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "symbol": "000001.SZ",
                "name": "平安银行",
                "board": "SZ_MAIN",
                "industry": "银行",
                "is_listed": True,
            },
            {
                "symbol": "600000.SH",
                "name": "浦发银行",
                "board": "SH_MAIN",
                "industry": "银行",
                "is_listed": True,
            },
        ]
    ).to_parquet(master_path, index=False)
    for symbol in ("000001.SZ", "600000.SH"):
        daily_path = data_root / "daily" / f"ts_code={symbol}" / "data.parquet"
        daily_path.parent.mkdir(parents=True)
        pd.DataFrame(
            [
                {
                    "datetime": datetime(2026, 8, day, 15),
                    "trade_date": f"2026080{day}",
                    "close": 10.0,
                    "vol": 100_000.0,
                    "amount": 1_000_000.0,
                }
                for day in (1, 2, 3)
            ]
        ).to_parquet(daily_path, index=False)

    watchlist_path = tmp_path / "intraday-watchlist.json"
    watchlist_path.write_text(
        """{
          "as_of": "2026-08-03",
          "allow_new_entries": true,
          "symbols": [
            {"symbol": "000001.SZ", "name": "平安银行", "rank": 1,
             "score": 80, "roles": ["target"]}
          ]
        }""",
        encoding="utf-8",
    )
    manifest_path = tmp_path / "latest-manifest.json"
    manifest_path.write_text(
        """{
          "status": "success",
          "steps": [
            {"name": "intraday_watchlist", "output": "%s"}
          ]
        }""" % watchlist_path.as_posix(),
        encoding="utf-8",
    )

    class FakeProvider:
        def __init__(self, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def iter_quote_batches(self, symbols):
            yield [
                _snapshot(symbol, price=10.2 if symbol == "000001.SZ" else 10.0)
                for symbol in symbols
            ]

    output = data_root / "auction-review-latest.json"
    payload = {
        "data_root": str(data_root),
        "database": str(data_root / "intraday-paper.duckdb"),
        "output": str(output),
        "universe": {"mode": "all-a-shares", "allowed_boards": []},
        "watchlist": {
            "manifest": str(manifest_path),
            "maximum_symbols": 20,
            # 本测试验证采集/落库，不重复测试观察池时效；避免固定日期随当前日期失效。
            "maximum_age_days": None,
        },
        "quote_provider": {"minimum_coverage": 1.0, "progress_every_batches": 0},
        "baseline_days": 3,
        "scoring": {"minimum_buy_score": 0, "minimum_industry_members": 2},
    }

    report = run_capture(payload, ignore_session=True, provider_factory=FakeProvider)

    assert report["status"] == "success"
    assert report["capture"]["single_connection"] is True
    assert report["capture"]["received_symbols"] == 2
    assert report["candidate_summary"]["actions"] == {"BUY_ALLOWED": 1}
    assert output.exists()


def test_tdx_server_time_is_parsed_for_stale_market_guard() -> None:
    snapshot = _snapshot("000001.SZ", price=10.0)
    snapshot = QuoteSnapshot(
        **{
            **snapshot.__dict__,
            "raw_json": '{"servertime":"9:25:05.500"}',
        }
    )

    parsed = _server_time(snapshot)

    assert parsed is not None
    assert (parsed.hour, parsed.minute, parsed.second, parsed.microsecond) == (9, 25, 5, 500_000)
