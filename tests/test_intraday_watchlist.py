"""研究流水线观察池加载、去重、时效和风险关闭测试。"""

import json
from datetime import datetime

import pytest

from scripts.data_pipeline.intraday.watchlist import load_watchlist


def _manifest(tmp_path, *, allow_new_entries: bool, as_of: str = "2026-07-30"):
    source = tmp_path / "intraday-watchlist.json"
    source.write_text(
        json.dumps(
            {
                "as_of": as_of,
                "allow_new_entries": allow_new_entries,
                "symbols": [
                    {"symbol": "000001.SZ", "roles": ["observation"], "rank": 1, "score": 80},
                    {"symbol": "600519.SH", "roles": ["position", "target"], "rank": 2, "score": 70},
                ],
            }
        ),
        encoding="utf-8",
    )
    manifest = tmp_path / "latest-manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "status": "success",
                "steps": [{"name": "intraday_watchlist", "output": str(source)}],
            }
        ),
        encoding="utf-8",
    )
    return manifest


def test_stable_watchlist_prioritizes_positions_and_deduplicates(tmp_path) -> None:
    loaded = load_watchlist(
        manifest=_manifest(tmp_path, allow_new_entries=True),
        manual_symbols=["000001.SZ", "002396"],
        now=datetime(2026, 7, 30, 18),
    )

    assert [item.symbol for item in loaded.items] == [
        "600519.SH",
        "000001.SZ",
        "002396.SZ",
    ]
    assert loaded.items[0].source == "positions"
    assert loaded.items[0].details["_intraday_allow_entry"] is True


def test_risk_off_watchlist_keeps_positions_but_drops_new_entries(tmp_path) -> None:
    loaded = load_watchlist(
        manifest=_manifest(tmp_path, allow_new_entries=False),
        now=datetime(2026, 7, 30, 18),
    )

    assert [item.symbol for item in loaded.items] == ["600519.SH"]
    assert loaded.items[0].details["_intraday_allow_entry"] is False


def test_stale_watchlist_fails_clearly(tmp_path) -> None:
    with pytest.raises(RuntimeError, match="watchlist is stale"):
        load_watchlist(
            manifest=_manifest(
                tmp_path,
                allow_new_entries=True,
                as_of="2026-07-20",
            ),
            now=datetime(2026, 7, 30, 18),
            maximum_age_days=5,
        )
