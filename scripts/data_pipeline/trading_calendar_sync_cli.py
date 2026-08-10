"""从本地 TDX 上证指数日线生成交易日历并同步到 Pig，不依赖自然工作日猜测。"""

from __future__ import annotations

import argparse
import json
import os
from datetime import timedelta
from pathlib import Path
from urllib.request import Request, urlopen

import pandas as pd


def build_calendar(path: Path, history_days: int) -> list[dict[str, object]]:
    frame = pd.read_parquet(path, columns=["trade_date"])
    dates = {pd.Timestamp(value).date() for value in frame["trade_date"].dropna()}
    if not dates:
        raise RuntimeError(f"TDX index calendar is empty: {path}")
    end = max(dates)
    start = max(min(dates), end - timedelta(days=max(30, history_days) - 1))
    rows: list[dict[str, object]] = []
    current = start
    while current <= end:
        rows.append({"tradeDate": current.isoformat(), "tradingDay": current in dates, "source": "TDX:000001.SH"})
        current += timedelta(days=1)
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sync local TDX trading calendar to Pig")
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--history-days", type=int, default=400)
    parser.add_argument("--base-url", default=os.environ.get("QUANT_OPERATIONS_BASE_URL", "http://127.0.0.1:9999/admin"))
    args = parser.parse_args(argv)
    api_key = os.environ.get("QUANT_SIGNAL_API_KEY", "").strip()
    if len(api_key) < 16:
        raise RuntimeError("QUANT_SIGNAL_API_KEY is required and must contain at least 16 characters")
    path = Path(args.data_root).resolve() / "index_daily" / "ts_code=000001.SH" / "data.parquet"
    rows = build_calendar(path, args.history_days)
    endpoint = args.base_url.rstrip("/") + "/quant/external/operations/calendar"
    for offset in range(0, len(rows), 200):
        request = Request(endpoint, data=json.dumps(rows[offset : offset + 200]).encode("utf-8"),
                          headers={"Content-Type": "application/json", "X-Quant-Api-Key": api_key}, method="POST")
        with urlopen(request, timeout=15) as response:
            response.read()
    print(json.dumps({"status": "ok", "rows": len(rows), "latest": rows[-1]["tradeDate"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
