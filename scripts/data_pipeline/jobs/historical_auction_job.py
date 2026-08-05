"""Extract the final 09:25 call-auction record from TDX historical trades."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import time
from typing import Any


AUCTION_MINUTE = time(9, 25)
PAGE_SIZE = 1800
INITIAL_OFFSET = 1800
MAX_PAGES = 100


@dataclass(frozen=True)
class HistoricalAuctionResult:
    status: str
    record: dict[str, Any] | None
    pages_requested: int
    records_scanned: int


def _minute(value: Any) -> time | None:
    text = str(value or "").strip()
    pieces = text.split(":")
    if len(pieces) < 2:
        return None
    try:
        return time(int(pieces[0]), int(pieces[1]))
    except ValueError:
        return None


def _positive(value: Any) -> bool:
    try:
        return float(value) > 0
    except (TypeError, ValueError):
        return False


def _choose_final_record(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    executed = [row for row in rows if _positive(row.get("price")) and _positive(row.get("vol"))]
    return dict((executed or rows)[-1])


def fetch_final_auction(
    fetch_page: Callable[[int, int], list[dict]],
    *,
    page_size: int = PAGE_SIZE,
    initial_offset: int = INITIAL_OFFSET,
    maximum_pages: int = MAX_PAGES,
) -> HistoricalAuctionResult:
    """Fetch only enough reverse-ordered history pages to reach 09:25.

    TDX history pages start at the close and move towards the open as ``start``
    increases.  Starting at one page offset deliberately skips the newest page;
    an illiquid symbol falls back to offset zero when that probe is empty.
    Different nodes cap pages differently, so offsets advance by the number of
    rows actually returned and a short page is not considered end-of-data.
    """

    if page_size < 1 or initial_offset < 0 or maximum_pages < 1:
        raise ValueError("page_size and maximum_pages must be positive; initial_offset cannot be negative")

    start = initial_offset
    fell_back_to_zero = start == 0
    pages_requested = 0
    records_scanned = 0
    visited: set[int] = set()
    known_empty_offset: int | None = None

    while pages_requested < maximum_pages:
        if start in visited:
            raise RuntimeError(f"historical auction paging repeated offset {start}")
        visited.add(start)
        page = list(fetch_page(start, page_size))
        pages_requested += 1

        if not page:
            if pages_requested == 1 and start == initial_offset and initial_offset > 0:
                known_empty_offset = start
                start = 0
                fell_back_to_zero = True
                continue
            return HistoricalAuctionResult(
                status="no_data" if records_scanned == 0 else "no_auction",
                record=None,
                pages_requested=pages_requested,
                records_scanned=records_scanned,
            )

        records_scanned += len(page)
        auction_rows = [row for row in page if _minute(row.get("time")) == AUCTION_MINUTE]
        if auction_rows:
            record = _choose_final_record(auction_rows)
            status = (
                "success"
                if _positive(record.get("price")) and _positive(record.get("vol"))
                else "no_auction_trade"
            )
            return HistoricalAuctionResult(
                status=status,
                record=record,
                pages_requested=pages_requested,
                records_scanned=records_scanned,
            )

        minutes = [minute for row in page if (minute := _minute(row.get("time"))) is not None]
        if not minutes:
            raise ValueError("TDX historical transaction page contains no parseable time values")
        if min(minutes) <= AUCTION_MINUTE:
            return HistoricalAuctionResult(
                status="no_auction",
                record=None,
                pages_requested=pages_requested,
                records_scanned=records_scanned,
            )
        next_start = start + len(page)
        if known_empty_offset is not None and next_start >= known_empty_offset:
            return HistoricalAuctionResult(
                status="no_auction",
                record=None,
                pages_requested=pages_requested,
                records_scanned=records_scanned,
            )
        start = next_start

    raise RuntimeError(
        f"historical auction paging exceeded {maximum_pages} pages before reaching 09:25"
    )
