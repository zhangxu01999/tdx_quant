"""从一键研究流水线产物加载盘中观察池。

优先读取稳定的 ``intraday_watchlist`` 清单；为兼容升级前已生成的报告，
也支持从 ``daily-report.json`` 或 ``backtest.json`` 恢复观察、目标和持仓。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, replace
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from .models import WatchItem


_CANONICAL = re.compile(r"^\d{6}\.(?:SH|SZ)$")


def _symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    if re.fullmatch(r"\d{6}", text):
        suffix = "SH" if text.startswith(("5", "6", "9")) else "SZ"
        return f"{text}.{suffix}"
    if _CANONICAL.fullmatch(text):
        return text
    raise ValueError(f"intraday watchlist only supports Shanghai/Shenzhen symbols: {value!r}")


@dataclass(frozen=True)
class LoadedWatchlist:
    items: list[WatchItem]
    source_as_of: date | None
    source_path: Path | None


def _payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"watchlist source does not exist: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"watchlist source must contain a JSON object: {path}")
    return value


def _as_date(value: Any) -> date | None:
    if value in {None, ""}:
        return None
    return date.fromisoformat(str(value)[:10])


def _engine_root(manifest_path: Path) -> Path:
    for parent in manifest_path.resolve().parents:
        if parent.name == "output":
            return parent.parent
    return manifest_path.resolve().parent


def _manifest_output_path(
    manifest_path: Path,
    step: Mapping[str, Any],
    *keys: str,
) -> Path | None:
    root = _engine_root(manifest_path)
    for key in keys:
        raw = step.get(key)
        if not raw:
            continue
        path = Path(str(raw)).expanduser()
        if path.exists():
            return path.resolve()
        candidate = root / path
        if candidate.exists():
            return candidate.resolve()
    return None


def _item(raw: Mapping[str, Any], source: str, rank: int | None = None) -> WatchItem | None:
    raw_symbol = raw.get("symbol") or raw.get("ts_code")
    if not raw_symbol:
        return None
    symbol = _symbol(raw_symbol)
    supplied_rank = raw.get("rank")
    score = raw.get("score")
    details = raw.get("details")
    if not isinstance(details, Mapping):
        details = {
            key: value
            for key, value in raw.items()
            if key not in {"symbol", "ts_code", "source", "rank", "score", "name"}
        }
    return WatchItem(
        symbol=symbol,
        source=source,
        rank=int(supplied_rank or rank) if supplied_rank or rank else None,
        score=float(score) if score is not None else None,
        name=str(raw.get("name")) if raw.get("name") else None,
        details=dict(details),
    )


def _items(values: Iterable[Any], source: str) -> list[WatchItem]:
    result: list[WatchItem] = []
    for rank, raw in enumerate(values, start=1):
        if isinstance(raw, str):
            raw = {"symbol": raw}
        if not isinstance(raw, Mapping):
            continue
        item = _item(raw, source, rank)
        if item is not None:
            result.append(item)
    return result


def _load_stable_watchlist(path: Path, sections: set[str]) -> LoadedWatchlist:
    value = _payload(path)
    allow_new_entries = bool(value.get("allow_new_entries", True))
    items: list[WatchItem] = []
    for rank, raw in enumerate(value.get("symbols") or [], start=1):
        if not isinstance(raw, Mapping):
            continue
        roles = {str(role) for role in raw.get("roles") or []}
        allowed_roles = roles.intersection(
            {
                "position" if section == "positions" else section
                for section in sections
            }
        )
        if not allowed_roles:
            continue
        if "position" in allowed_roles:
            source = "positions"
        elif "target" in allowed_roles:
            source = "target"
        else:
            source = "observation"
        if not allow_new_entries and source != "positions":
            continue
        enriched = dict(raw)
        details = dict(raw.get("details") or {})
        details["_intraday_roles"] = sorted(allowed_roles)
        details["_intraday_allow_entry"] = bool(
            allow_new_entries
            and ("target" in allowed_roles or "observation" in allowed_roles)
        )
        enriched["details"] = details
        item = _item(enriched, source, rank)
        if item is not None:
            items.append(item)
    return LoadedWatchlist(
        items,
        _as_date(value.get("as_of")),
        path.resolve(),
    )


def _load_daily_report(path: Path, sections: set[str]) -> LoadedWatchlist:
    value = _payload(path)
    portfolio = value.get("portfolio") if isinstance(value.get("portfolio"), Mapping) else {}
    section_map = {
        "observation": "observation_candidates",
        "target": "target",
        "positions": "current_positions",
    }
    items: list[WatchItem] = []
    for section, key in section_map.items():
        if (
            not bool(portfolio.get("allow_new_entries", True))
            and section != "positions"
        ):
            continue
        if section in sections:
            items.extend(_items(portfolio.get(key) or [], section))
    as_of_value = value.get("as_of")
    if isinstance(as_of_value, Mapping):
        as_of_value = as_of_value.get("decision_date") or as_of_value.get("market_date")
    return LoadedWatchlist(items, _as_date(as_of_value), path.resolve())


def _load_backtest(path: Path, sections: set[str]) -> LoadedWatchlist:
    value = _payload(path)
    decisions = value.get("rebalance_decisions") or []
    latest = decisions[-1] if decisions and isinstance(decisions[-1], Mapping) else {}
    snapshot = value.get("portfolio_snapshot")
    snapshot = snapshot if isinstance(snapshot, Mapping) else {}
    items: list[WatchItem] = []
    if "observation" in sections:
        if bool(latest.get("allow_new_entries", True)):
            items.extend(_items(latest.get("observed") or [], "observation"))
    if "target" in sections:
        if bool(latest.get("allow_new_entries", True)):
            items.extend(_items(latest.get("selected") or [], "target"))
    if "positions" in sections:
        items.extend(_items(snapshot.get("positions") or [], "positions"))
    as_of = latest.get("decision_date") or snapshot.get("as_of")
    return LoadedWatchlist(items, _as_date(as_of), path.resolve())


def _load_from_manifest(manifest_path: Path, sections: set[str]) -> LoadedWatchlist:
    manifest = _payload(manifest_path)
    if manifest.get("status") != "success":
        raise RuntimeError(f"research pipeline manifest is not successful: {manifest_path}")
    steps = [
        step for step in manifest.get("steps", []) if isinstance(step, Mapping)
    ]
    intraday = next(
        (step for step in steps if step.get("name") == "intraday_watchlist"),
        None,
    )
    if intraday is not None:
        path = _manifest_output_path(manifest_path, intraday, "output")
        if path is not None:
            return _load_stable_watchlist(path, sections)
    daily = next((step for step in steps if step.get("name") == "daily_report"), None)
    if daily is not None:
        path = _manifest_output_path(manifest_path, daily, "json")
        if path is not None:
            return _load_daily_report(path, sections)
    backtest = next((step for step in steps if step.get("name") == "backtest"), None)
    if backtest is not None:
        path = _manifest_output_path(manifest_path, backtest, "output")
        if path is not None:
            return _load_backtest(path, sections)
    raise RuntimeError(f"manifest contains no readable watchlist source: {manifest_path}")


def load_watchlist(
    *,
    manifest: str | Path | None,
    manual_symbols: Iterable[str] = (),
    sections: Iterable[str] = ("observation", "target", "positions"),
    maximum_symbols: int = 200,
    now: datetime | None = None,
    maximum_age_days: int | None = 5,
) -> LoadedWatchlist:
    """合并日频观察池与手工关注代码，并按证券代码去重。

    来源优先级为当前持仓、目标、观察、手工补充。手工代码不会覆盖已有
    评分和原因。若启用时效门禁，陈旧日频结果会明确报错，防止拿旧股票池
    继续进行看似实时的模拟交易。
    """

    if maximum_symbols < 1:
        raise ValueError("maximum_symbols must be positive")
    selected_sections = {str(item) for item in sections}
    unknown = selected_sections.difference({"observation", "target", "positions"})
    if unknown:
        raise ValueError(f"unknown watchlist sections: {', '.join(sorted(unknown))}")
    loaded = LoadedWatchlist([], None, None)
    if manifest:
        loaded = _load_from_manifest(Path(manifest).expanduser().resolve(), selected_sections)
        if maximum_age_days is not None and loaded.source_as_of is not None:
            current = (now or datetime.now()).date()
            age = (current - loaded.source_as_of).days
            if age > maximum_age_days:
                raise RuntimeError(
                    f"intraday watchlist is stale: as_of={loaded.source_as_of}, "
                    f"age={age} days, maximum={maximum_age_days}; "
                    "run the daily research pipeline first"
                )

    manual = _items(({"symbol": symbol} for symbol in manual_symbols), "manual")
    priority = {"positions": 0, "target": 1, "observation": 2, "manual": 3}
    merged: dict[str, WatchItem] = {}
    entry_allowed: dict[str, bool] = {}
    all_items = [*loaded.items, *manual]
    for item in all_items:
        entry_allowed[item.symbol] = entry_allowed.get(item.symbol, False) or bool(
            item.details.get(
                "_intraday_allow_entry",
                item.source != "positions",
            )
        )
    for item in sorted(
        all_items,
        key=lambda value: (
            priority.get(value.source, 9),
            value.rank if value.rank is not None else 999_999,
            -(value.score if value.score is not None else float("-inf")),
            value.symbol,
        ),
    ):
        merged.setdefault(item.symbol, item)
    result = [
        replace(
            item,
            details={
                **dict(item.details),
                "_intraday_allow_entry": entry_allowed[item.symbol],
            },
        )
        for item in merged.values()
    ][:maximum_symbols]
    if not result:
        raise ValueError("intraday watchlist is empty")
    return LoadedWatchlist(result, loaded.source_as_of, loaded.source_path)
