#!/usr/bin/env python3
"""data_export.py — read data/*.parquet and emit clean JSON assets for the static frontend.

Deterministic, side-effect-free apart from writing frontend/assets/*.json.
Every numeric is coerced to a native Python type and NaN/inf -> null (JSON-safe).
Designed so app.js can render charts with zero further transformation.
"""
from __future__ import annotations
import json
import math
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parents[1] / "data"
OUT = Path(__file__).resolve().parent / "assets"
OUT.mkdir(exist_ok=True)

INDEX_NAMES = {"000001.SH": "上证指数", "399001.SZ": "深证成指"}
STOCK_NAMES = {"000001.SZ": "平安银行", "000002.SZ": "万科A", "600000.SH": "浦发银行"}


# ---------- helpers ----------
def clean(obj):
    """Recursively convert numpy/pandas scalars to JSON-native; NaN/inf -> None."""
    if isinstance(obj, dict):
        return {str(k): clean(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [clean(v) for v in obj]
    if hasattr(obj, "item"):  # numpy scalar
        try:
            obj = obj.item()
        except Exception:
            return None
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if obj is pd.NaT:
        return None
    return obj


def write(name: str, payload: dict) -> None:
    path = OUT / f"{name}.json"
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(clean(payload), fh, ensure_ascii=False, separators=(",", ":"))
    print(f"  wrote {path.name}  ({path.stat().st_size:,} bytes)")


def first_parquet(domain: str) -> Path:
    files = sorted((BASE / domain).rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"no parquet under {domain}")
    return files[0]


def parquet_files(domain: str):
    return sorted((BASE / domain).rglob("*.parquet"))


def code_name_map() -> dict:
    """ts_code -> name from security_list if present, else fallback to STOCK_NAMES."""
    out = dict(STOCK_NAMES)
    try:
        sl = pd.read_parquet(BASE / "security_list")
        if "ts_code" in sl.columns and "name" in sl.columns:
            for _, r in sl.drop_duplicates("ts_code").iterrows():
                name = str(r["name"]).strip()
                if name and name != "nan":
                    out[str(r["ts_code"])] = name
    except Exception:
        pass
    return out


# ---------- exports ----------
def export_overview():
    """Index closes + market breadth (advancing/declining) + universe counts."""
    indices = []
    for f in parquet_files("index_daily"):
        ts = f.relative_to(BASE / "index_daily").parts[0].split("=")[1]
        df = pd.read_parquet(f).sort_values("trade_date")
        pts = []
        for _, r in df.iterrows():
            pts.append({
                "trade_date": str(r["trade_date"]),
                "close": r["close"],
                "up_count": r.get("up_count"),
                "down_count": r.get("down_count"),
            })
        indices.append({
            "ts_code": ts,
            "name": INDEX_NAMES.get(ts, ts),
            "points": pts,
        })

    universe = {}
    try:
        sl = pd.read_parquet(BASE / "security_list")
        if "ts_code" in sl.columns:
            vc = sl["ts_code"].astype(str).str[-2:].value_counts().to_dict()
            for k, v in vc.items():
                universe[k] = int(v)
            universe["total"] = int(len(sl))
    except Exception:
        pass

    write("overview", {"indices": indices, "universe": universe})


def export_kline(names: dict):
    """Daily candlestick + MA/BOLL/MACD/RSI/KDJ for the stock with the richest indicator file."""
    ind_path = BASE / "000001.SZ_indicators.parquet"
    df = pd.read_parquet(ind_path).sort_values("datetime").reset_index(drop=True)
    dates = [d.strftime("%Y-%m-%d") for d in pd.to_datetime(df["datetime"])]

    def col(c):
        return [None if pd.isna(v) else float(v) for v in df[c].tolist()] if c in df else [None] * len(df)

    ohlc = list(zip(
        [round(o, 3) for o in df["open"]],
        [round(c, 3) for c in df["close"]],
        [round(l, 3) for l in df["low"]],
        [round(h, 3) for h in df["high"]],
    ))
    ts_code = "000001.SZ"
    write("kline_daily", {
        "ts_code": ts_code,
        "name": names.get(ts_code, ts_code),
        "bars": len(df),
        "dates": dates,
        "ohlc": ohlc,
        "vol": df["vol"].astype(float).tolist(),
        "ma": {"MA5": col("MA5"), "MA10": col("MA10"), "MA20": col("MA20"), "MA60": col("MA60")},
        "boll": {"UP": col("BOLL_UP"), "MB": col("BOLL_MB"), "DN": col("BOLL_DN")},
        "macd": {"DIF": col("DIF"), "DEA": col("DEA"), "HIST": col("MACD")},
        "rsi": {"RSI6": col("RSI6"), "RSI12": col("RSI12"), "RSI24": col("RSI24")},
        "kdj": {"K": col("K"), "D": col("D"), "J": col("J")},
        "latest": {
            "close": round(float(df["close"].iloc[-1]), 3),
            "ma5": _last(col("MA5")), "ma10": _last(col("MA10")), "ma20": _last(col("MA20")),
            "rsi6": _last(col("RSI6")), "macd_hist": _last(col("MACD")),
        },
    })


def _last(arr):
    arr = [x for x in (arr or []) if x is not None]
    return round(arr[-1], 4) if arr else None


def export_minute(names: dict):
    """All symbols x {5m,15m,30m,60m} candlesticks."""
    tfs = ["5m", "15m", "30m", "60m"]
    symbols = []
    data = {}
    for tf in tfs:
        domain = f"minute_{tf}"
        for f in parquet_files(domain):
            ts = f.relative_to(BASE / domain).parts[0].split("=")[1]
            symbols.append(ts)
            df = pd.read_parquet(f).sort_values("datetime").reset_index(drop=True)
            dates = [d.strftime("%m-%d %H:%M") for d in pd.to_datetime(df["datetime"])]
            ohlc = list(zip(
                [round(o, 3) for o in df["open"]],
                [round(c, 3) for c in df["close"]],
                [round(l, 3) for l in df["low"]],
                [round(h, 3) for h in df["high"]],
            ))
            data.setdefault(ts, {})[tf] = {
                "dates": dates,
                "ohlc": ohlc,
                "vol": df["vol"].astype(float).tolist(),
            }
    symbols = sorted(set(symbols))
    write("minute", {
        "symbols": symbols,
        "names": {s: names.get(s, s) for s in symbols},
        "timeframes": tfs,
        "data": data,
    })


def export_ticks(names: dict):
    """Tick order-flow: per-minute buy/sell volume + 1-min price curve + distribution."""
    f = first_parquet("tdx_transactions")
    ts = f.relative_to(BASE / "tdx_transactions").parts[0].split("=")[1]
    tx = pd.read_parquet(f).copy()
    tx["trade_date"] = tx["trade_date"].astype(str)
    date = str(tx["trade_date"].iloc[0])

    # time -> minute index (HHMM -> 0..239 relative to 09:30 open)
    t = tx["time"].astype(str)
    hhmm = t.str.replace(":", "").str[:4].astype(int)
    def to_min(hm):
        hh, mm = divmod(hm, 100)
        base = (hh - 9) * 60 + (mm - 30)  # 09:30 -> 0
        if hh >= 13:  # afternoon shift: 13:00 is index 120 (lunch break at 11:30)
            base = 120 + (hh - 13) * 60 + mm
        return base
    tx["midx"] = hhmm.map(to_min).clip(0, 239).astype(int)

    side = tx["buyorsell_label"].fillna("other").astype(str).str.lower()
    tx["side"] = side.map(lambda s: "buy" if s == "buy" else ("sell" if s == "sell" else "other"))
    grp = tx.groupby("midx")
    flow = []
    for midx, g in grp:
        flow.append({
            "minute": int(midx),
            "buy_vol": float(g.loc[g.side == "buy", "vol"].sum()),
            "sell_vol": float(g.loc[g.side == "sell", "vol"].sum()),
        })
    flow.sort(key=lambda x: x["minute"])

    dist = tx["side"].value_counts().reindex(["buy", "sell", "neutral", "other"]).fillna(0).astype(int).to_dict()

    # 1-min price curve from minute_time
    price_curve = []
    try:
        mt = pd.read_parquet(first_parquet("minute_time")).sort_values("minute_idx")
        price_curve = [
            {"minute": int(r["minute_idx"]), "price": round(float(r["price"]), 3), "vol": float(r["vol"])}
            for _, r in mt.iterrows()
        ]
    except Exception as e:
        print(f"  (warn) minute_time: {e}")

    write("ticks", {
        "ts_code": ts,
        "name": names.get(ts, ts),
        "date": date,
        "n_ticks": int(len(tx)),
        "distribution": dist,
        "price_range": [round(float(tx["price"].min()), 3), round(float(tx["price"].max()), 3)],
        "flow": flow,
        "price_curve": price_curve,
    })


def export_fundamentals(names: dict):
    """Finance-metric trends (long -> pivoted) + capital structure + F10 text."""
    ts = "000001.SZ"
    fin = pd.read_parquet(first_parquet("company_finance"))
    periods = sorted(fin["period"].dropna().astype(str).unique().tolist())
    pivot = {}
    for metric, g in fin.groupby("metric"):
        vals = {}
        for _, r in g.iterrows():
            v = r.get("value_num")
            vals[str(r["period"])] = None if pd.isna(v) else float(v)
        if any(x is not None for x in vals.values()):
            pivot[str(metric)] = vals

    capital = {}
    try:
        cap = pd.read_parquet(first_parquet("finance_capital")).iloc[0]
        capital = {
            "zongguben": float(cap["zongguben"]) if not pd.isna(cap.get("zongguben")) else None,
            "liutongguben": float(cap["liutongguben"]) if not pd.isna(cap.get("liutongguben")) else None,
            "ipo_date": str(cap.get("ipo_date")),
            "industry_code": str(cap.get("industry")),
            "province_code": str(cap.get("province")),
        }
    except Exception as e:
        print(f"  (warn) finance_capital: {e}")

    company_text = ""
    try:
        info = pd.read_parquet(first_parquet("company_info_raw")).iloc[0]
        company_text = str(info.get("text", ""))
    except Exception as e:
        print(f"  (warn) company_info_raw: {e}")

    write("fundamentals", {
        "ts_code": ts,
        "name": names.get(ts, ts),
        "periods": periods,
        "metrics": pivot,
        "capital": capital,
        "company_info": company_text[:3000],
    })


def main():
    print(f"reading parquet from {BASE}")
    names = code_name_map()
    print(f"resolved {len(names)} symbol names")
    export_overview()
    export_kline(names)
    export_minute(names)
    export_ticks(names)
    export_fundamentals(names)
    print("done.")


if __name__ == "__main__":
    main()
