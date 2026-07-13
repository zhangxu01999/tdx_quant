"""从本地日线和股本数据生成 A 股短线日频增强特征。"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from scripts.data_pipeline.materializers.symbol_writer import write_by_symbol


SHORT_TERM_DOMAIN = "short_term_daily"
_SH_CHINEXT_PREFIXES = ("300", "301")
_SH_STAR_PREFIXES = ("688", "689")
_BJ_PREFIXES = ("4", "8")


def _parquet_root(data_root: Path, domain: str) -> Path:
    root = data_root / domain
    if not root.exists() or next(root.rglob("*.parquet"), None) is None:
        raise FileNotFoundError(f"no parquet data found for {domain!r}: {root}")
    return root


def _normalize_trade_date(frame: pd.DataFrame) -> pd.Series:
    values = frame["trade_date"].astype(str).str.replace("-", "", regex=False)
    if values.str.len().ne(8).any():
        raise ValueError("trade_date must be YYYYMMDD or YYYY-MM-DD")
    return values


def _limit_rate_for_code(code: object) -> float:
    """按常见 A 股板块给出基础涨跌停幅度；ST 等特殊状态后续再接精确数据源。"""

    value = str(code).zfill(6)
    if value.startswith(_BJ_PREFIXES):
        return 0.30
    if value.startswith(_SH_CHINEXT_PREFIXES) or value.startswith(_SH_STAR_PREFIXES):
        return 0.20
    return 0.10


def _round_price(value: pd.Series) -> pd.Series:
    """把理论涨跌停价格四舍五入到分，符合 A 股报价最小单位。"""

    return (value * 100).round() / 100


def build_short_term_daily_features(
    data_root: Path,
    *,
    symbols: list[str] | None = None,
    price_tolerance: float = 0.005,
) -> pd.DataFrame:
    """合成短线策略常用日频字段。

    当前只依赖已落盘的 ``daily`` 与可选 ``finance_capital``：
    - ``amount`` 直接来自通达信日线成交额；
    - ``turnover_rate`` 用成交量股数除以流通股本；
    - ``float_market_cap`` 用收盘价乘流通股本；
    - 涨跌停/炸板先按板块涨跌停幅度从 OHLC 推导。
    """

    data_root = Path(data_root)
    daily = pd.read_parquet(_parquet_root(data_root, "daily"))
    if daily.empty:
        raise ValueError("daily parquet is empty")
    required = {"ts_code", "trade_date", "code", "open", "high", "low", "close", "vol", "amount"}
    missing = required.difference(daily.columns)
    if missing:
        raise ValueError(f"daily data is missing required columns: {sorted(missing)}")

    daily = daily.copy()
    daily["ts_code"] = daily["ts_code"].astype(str).str.upper()
    if symbols:
        wanted = {value.strip().upper() for value in symbols}
        daily = daily[daily["ts_code"].isin(wanted)]
    if daily.empty:
        raise ValueError("no daily rows matched requested symbols")

    daily["trade_date"] = _normalize_trade_date(daily)
    daily = daily.sort_values(["ts_code", "trade_date"])
    daily["volume_shares"] = pd.to_numeric(daily["vol"], errors="coerce") * 100
    daily["amount"] = pd.to_numeric(daily["amount"], errors="coerce")
    daily["close"] = pd.to_numeric(daily["close"], errors="coerce")
    daily["high"] = pd.to_numeric(daily["high"], errors="coerce")
    daily["low"] = pd.to_numeric(daily["low"], errors="coerce")
    daily["previous_close"] = daily.groupby("ts_code")["close"].shift(1)
    daily["limit_rate"] = daily["code"].map(_limit_rate_for_code)

    capital_path = data_root / "finance_capital"
    if capital_path.exists() and next(capital_path.rglob("*.parquet"), None) is not None:
        capital = pd.read_parquet(capital_path)
        if "ts_code" in capital.columns and "liutongguben" in capital.columns:
            capital = capital.copy()
            capital["ts_code"] = capital["ts_code"].astype(str).str.upper()
            sort_columns = ["updated_date"] if "updated_date" in capital.columns else []
            if sort_columns:
                capital = capital.sort_values(["ts_code", *sort_columns])
            capital = capital.drop_duplicates("ts_code", keep="last")
            capital = capital[["ts_code", "liutongguben"]].rename(columns={"liutongguben": "float_shares"})
            daily = daily.merge(capital, on="ts_code", how="left")
        else:
            daily["float_shares"] = pd.NA
    else:
        daily["float_shares"] = pd.NA

    daily["float_shares"] = pd.to_numeric(daily["float_shares"], errors="coerce")
    daily["turnover_rate"] = daily["volume_shares"] / daily["float_shares"]
    daily["float_market_cap"] = daily["close"] * daily["float_shares"]

    limit_up_price = _round_price(daily["previous_close"] * (1 + daily["limit_rate"]))
    limit_down_price = _round_price(daily["previous_close"] * (1 - daily["limit_rate"]))
    daily["limit_up_price"] = limit_up_price
    daily["limit_down_price"] = limit_down_price
    daily["hit_limit_up"] = daily["high"] >= limit_up_price - price_tolerance
    daily["limit_up"] = daily["close"] >= limit_up_price - price_tolerance
    daily["hit_limit_down"] = daily["low"] <= limit_down_price + price_tolerance
    daily["limit_down"] = daily["close"] <= limit_down_price + price_tolerance
    daily["bomb_limit_up"] = daily["hit_limit_up"] & ~daily["limit_up"]
    daily.loc[daily["previous_close"].isna(), ["hit_limit_up", "limit_up", "hit_limit_down", "limit_down", "bomb_limit_up"]] = False

    base_volume = daily.groupby("ts_code")["volume_shares"].transform(lambda series: series.shift(1).rolling(20).mean())
    daily["volume_ratio"] = daily["volume_shares"] / base_volume
    daily["amount_growth"] = daily.groupby("ts_code")["amount"].pct_change()

    return daily[
        [
            "ts_code",
            "trade_date",
            "amount",
            "turnover_rate",
            "float_market_cap",
            "float_shares",
            "limit_rate",
            "limit_up_price",
            "limit_down_price",
            "limit_up",
            "limit_down",
            "hit_limit_up",
            "hit_limit_down",
            "bomb_limit_up",
            "volume_ratio",
            "amount_growth",
        ]
    ].copy()


def write_short_term_daily_features(
    data_root: Path,
    *,
    symbols: list[str] | None = None,
    price_tolerance: float = 0.005,
) -> pd.DataFrame:
    """生成并覆盖写入 ``data/short_term_daily``，返回写入前的完整 DataFrame。"""

    frame = build_short_term_daily_features(data_root, symbols=symbols, price_tolerance=price_tolerance)
    write_by_symbol(data_root, SHORT_TERM_DOMAIN, frame)
    return frame


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build short-term daily feature parquet from local TDX data")
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument("--symbols", nargs="+", help="optional canonical symbols such as 000001.SZ")
    parser.add_argument("--price-tolerance", type=float, default=0.005)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    frame = write_short_term_daily_features(
        args.data_root,
        symbols=args.symbols,
        price_tolerance=args.price_tolerance,
    )
    print(
        f"short-term features complete: rows={len(frame)} "
        f"symbols={frame['ts_code'].nunique()} -> {args.data_root / SHORT_TERM_DOMAIN}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
