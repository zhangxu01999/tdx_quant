from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.data_pipeline.historical_auction_cli import (
    HistoricalAuctionSyncConfig,
    status,
    sync_day,
)
from scripts.data_pipeline.jobs.historical_auction_job import fetch_final_auction


def _row(time: str, *, price: float = 10.0, vol: int = 10, buyorsell: int = 2) -> dict:
    return {"time": time, "price": price, "vol": vol, "buyorsell": buyorsell}


def test_fetch_final_auction_skips_latest_page_and_stops_at_0925() -> None:
    pages = {
        3: [_row("10:30"), _row("11:00"), _row("14:00")],
        6: [_row("09:15", vol=0, buyorsell=8), _row("09:25", price=11.37, vol=3632)],
    }
    calls: list[tuple[int, int]] = []

    def fetch_page(start: int, count: int) -> list[dict]:
        calls.append((start, count))
        return pages.get(start, [])

    result = fetch_final_auction(fetch_page, page_size=3, initial_offset=3)

    assert result.status == "success"
    assert result.record == _row("09:25", price=11.37, vol=3632)
    assert result.pages_requested == 2
    assert result.records_scanned == 5
    assert calls == [(3, 3), (6, 3)]


def test_fetch_final_auction_falls_back_to_zero_for_illiquid_symbol() -> None:
    calls: list[int] = []

    def fetch_page(start: int, count: int) -> list[dict]:
        calls.append(start)
        if start == 0:
            return [_row("09:15", vol=0, buyorsell=8), _row("09:25", price=8.2, vol=100)]
        return []

    result = fetch_final_auction(fetch_page, page_size=3, initial_offset=3)

    assert result.status == "success"
    assert result.record["price"] == 8.2
    assert calls == [3, 0]


def test_fetch_final_auction_stops_when_fallback_page_reaches_known_empty_offset() -> None:
    calls: list[int] = []

    def fetch_page(start: int, count: int) -> list[dict]:
        calls.append(start)
        if start == 0:
            return [_row("09:30"), _row("10:00"), _row("14:00")]
        return []

    result = fetch_final_auction(fetch_page, page_size=3, initial_offset=3)

    assert result.status == "no_auction"
    assert result.pages_requested == 2
    assert result.records_scanned == 3
    assert calls == [3, 0]


def test_fetch_final_auction_does_not_fallback_after_a_later_empty_page() -> None:
    calls: list[int] = []

    def fetch_page(start: int, count: int) -> list[dict]:
        calls.append(start)
        if start == 3:
            return [_row("09:30"), _row("10:00")]
        return []

    result = fetch_final_auction(fetch_page, page_size=3, initial_offset=3)

    assert result.status == "no_auction"
    assert result.pages_requested == 2
    assert result.records_scanned == 2
    assert calls == [3, 5]


def test_fetch_final_auction_prefers_last_executed_record_over_marker() -> None:
    rows = [
        _row("09:25", price=10.0, vol=20),
        _row("09:25", price=10.1, vol=30),
        _row("09:25", price=10.2, vol=0, buyorsell=8),
    ]

    result = fetch_final_auction(lambda _start, _count: rows, initial_offset=0)

    assert result.status == "success"
    assert result.record == rows[1]


def test_fetch_final_auction_reports_no_auction_when_boundary_is_present() -> None:
    result = fetch_final_auction(
        lambda _start, _count: [_row("09:15", vol=0), _row("09:24", vol=0), _row("09:30")],
        initial_offset=0,
    )

    assert result.status == "no_auction"
    assert result.pages_requested == 1


class _FakeProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    def fetch_page(
        self,
        *,
        market: int,
        code: str,
        trade_date: str,
        start: int,
        count: int,
    ) -> list[dict]:
        self.calls.append((code, start))
        if code == "000001" and start == 0:
            return [_row("09:25", price=11.0, vol=100)]
        return []


def _master(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "symbol": "000001.SZ",
                "name": "平安银行",
                "exchange": "SZ",
                "board": "SZ_MAIN",
                "industry": "银行",
                "is_listed": True,
                "is_suspended": False,
            },
            {
                "symbol": "600000.SH",
                "name": "浦发银行",
                "exchange": "SH",
                "board": "SH_MAIN",
                "industry": "银行",
                "is_listed": True,
                "is_suspended": True,
            },
        ]
    ).to_parquet(path, index=False)


def test_sync_day_writes_one_row_per_point_in_time_symbol_and_is_resumable(tmp_path: Path) -> None:
    master_path = tmp_path / "master.parquet"
    _master(master_path)
    config = HistoricalAuctionSyncConfig(
        data_root=tmp_path,
        page_size=3,
        initial_offset=3,
        request_delay_seconds=0,
        checkpoint_every=1,
        progress_every=1,
    )
    provider = _FakeProvider()

    result = sync_day(config, "20250923", master_path, provider)

    assert result["status"] == "completed"
    output = tmp_path / "auction_history_daily" / "trade_date=20250923" / "data.parquet"
    frame = pd.read_parquet(output).sort_values("ts_code").reset_index(drop=True)
    assert list(frame["ts_code"]) == ["000001.SZ", "600000.SH"]
    assert list(frame["status"]) == ["success", "no_data"]
    assert frame.loc[0, "auction_volume_lots"] == 100
    assert frame.loc[0, "auction_volume_shares"] == 10_000
    assert frame.loc[0, "auction_amount"] == 110_000
    assert not (tmp_path / ".auction-history-checkpoints" / "trade_date=20250923.parquet").exists()

    calls_before = list(provider.calls)
    second = sync_day(config, "20250923", master_path, provider)
    assert second["status"] == "skipped"
    assert provider.calls == calls_before

    summary = status(config)
    assert summary["completed_trade_dates"] == 1
    assert summary["rows"] == 2
    assert summary["status_counts"] == {"success": 1, "no_data": 1}
