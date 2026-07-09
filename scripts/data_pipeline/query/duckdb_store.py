"""使用 DuckDB 直接查询 Parquet，不复制全市场行情到传统数据库。"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import duckdb
import pandas as pd

from scripts.data_pipeline.code_mapping import market_code_to_ts_code
from scripts.data_pipeline.fetch_realtime_watchlist import infer_hq_market


TIMEFRAME_DOMAINS = {
    "daily": "daily",
    "5m": "minute_5m",
    "15m": "minute_15m",
    "30m": "minute_30m",
    "60m": "minute_60m",
    "index": "index_daily",
}
ALLOWED_DOMAINS = {
    *TIMEFRAME_DOMAINS.values(),
    "company_finance",
    "company_info_raw",
    "finance_capital",
    "minute_time",
    "security_list",
    "tdx_transactions",
    "xdxr",
}
_CANONICAL_SYMBOL = re.compile(r"^\d{6}\.(?:SH|SZ|BJ)$")
_BARE_SYMBOL = re.compile(r"^\d{6}$")
# 沪深 A 股常见代码段；排除指数、基金、债券和 B 股。
_A_SHARE_CODE = r"^(000|001|002|003|300|301|600|601|603|605|688|689)[0-9]{3}$"


def _canonical_symbol(symbol: str) -> str:
    value = symbol.strip().upper()
    if _CANONICAL_SYMBOL.fullmatch(value):
        return value
    if _BARE_SYMBOL.fullmatch(value):
        return market_code_to_ts_code(infer_hq_market(value), value)
    raise ValueError(f"invalid mainland symbol: {symbol!r}")


def _compact_date(value: str | None) -> str | None:
    if value is None:
        return None
    compact = str(value).replace("-", "")
    if not re.fullmatch(r"\d{8}", compact):
        raise ValueError(f"date must be YYYY-MM-DD or YYYYMMDD: {value!r}")
    return compact


def _checked_limit(limit: int, maximum: int = 10_000) -> int:
    if not 1 <= limit <= maximum:
        raise ValueError(f"limit must be between 1 and {maximum}")
    return limit


class DuckDBMarketStore:
    """为前端和选股器提供受限、参数化的 Parquet 查询接口。"""

    def __init__(self, data_root: str | Path = "data", database: str = ":memory:"):
        self.data_root = Path(data_root).expanduser().resolve()
        self.connection = duckdb.connect(database=database)

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "DuckDBMarketStore":
        return self

    def __exit__(self, *_args) -> None:
        self.close()

    def _parquet_glob(self, domain: str) -> str:
        if domain not in ALLOWED_DOMAINS:
            raise ValueError(f"unsupported data domain: {domain!r}")
        directory = self.data_root / domain
        if not directory.exists() or next(directory.rglob("*.parquet"), None) is None:
            raise FileNotFoundError(f"no parquet data found for domain {domain!r}: {directory}")
        return (directory / "**" / "*.parquet").as_posix()

    @staticmethod
    def _scan_sql() -> str:
        return "read_parquet(?, hive_partitioning = true, union_by_name = true)"

    def query_bars(
        self,
        symbol: str,
        *,
        timeframe: str = "daily",
        start: str | None = None,
        end: str | None = None,
        limit: int = 1_000,
    ) -> pd.DataFrame:
        """读取单只证券指定周期的最近若干根 K 线，并按时间升序返回。"""

        if timeframe not in TIMEFRAME_DOMAINS:
            raise ValueError(f"unsupported timeframe: {timeframe!r}")
        canonical = _canonical_symbol(symbol)
        start_date = _compact_date(start)
        end_date = _compact_date(end)
        if start_date and end_date and start_date > end_date:
            raise ValueError("start date must not be after end date")
        row_limit = _checked_limit(limit)
        path = self._parquet_glob(TIMEFRAME_DOMAINS[timeframe])

        filters = ["ts_code = ?"]
        parameters: list[object] = [path, canonical]
        if start_date:
            filters.append("CAST(trade_date AS VARCHAR) >= ?")
            parameters.append(start_date)
        if end_date:
            filters.append("CAST(trade_date AS VARCHAR) <= ?")
            parameters.append(end_date)
        parameters.append(row_limit)
        sql = f"""
            SELECT *
            FROM (
                SELECT *
                FROM {self._scan_sql()}
                WHERE {' AND '.join(filters)}
                ORDER BY datetime DESC
                LIMIT ?
            ) recent
            ORDER BY datetime ASC
        """
        return self.connection.execute(sql, parameters).fetchdf()

    def latest_bars(
        self,
        *,
        timeframe: str = "daily",
        symbols: list[str] | None = None,
        as_of: str | None = None,
        limit: int = 6_000,
    ) -> pd.DataFrame:
        """返回股票池中每只证券不晚于指定日期的最新一根 K 线。"""

        if timeframe not in TIMEFRAME_DOMAINS:
            raise ValueError(f"unsupported timeframe: {timeframe!r}")
        path = self._parquet_glob(TIMEFRAME_DOMAINS[timeframe])
        row_limit = _checked_limit(limit)
        filters: list[str] = []
        parameters: list[object] = [path]
        if symbols:
            canonical = [_canonical_symbol(symbol) for symbol in symbols]
            filters.append("ts_code IN (" + ",".join("?" for _ in canonical) + ")")
            parameters.extend(canonical)
        compact = _compact_date(as_of)
        if compact:
            filters.append("CAST(trade_date AS VARCHAR) <= ?")
            parameters.append(compact)
        where = "WHERE " + " AND ".join(filters) if filters else ""
        parameters.append(row_limit)
        sql = f"""
            SELECT * EXCLUDE (_row_number)
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY ts_code ORDER BY datetime DESC
                ) AS _row_number
                FROM {self._scan_sql()}
                {where}
            ) ranked
            WHERE _row_number = 1
            ORDER BY ts_code
            LIMIT ?
        """
        return self.connection.execute(sql, parameters).fetchdf()

    def list_symbols(
        self,
        *,
        search: str | None = None,
        market: str | None = None,
        stocks_only: bool = True,
        limit: int = 100,
    ) -> pd.DataFrame:
        """读取最新证券快照；默认只返回沪深 A 股，支持代码或名称搜索。"""

        path = self._parquet_glob("security_list")
        row_limit = _checked_limit(limit, maximum=6_000)
        filters: list[str] = []
        parameters: list[object] = [path]
        if stocks_only:
            filters.append("regexp_matches(code, ?)")
            parameters.append(_A_SHARE_CODE)
        if market:
            normalized_market = market.strip().upper()
            if normalized_market not in {"SH", "SZ"}:
                raise ValueError("market must be SH or SZ")
            filters.append("market = ?")
            parameters.append(normalized_market)
        if search:
            filters.append("(code ILIKE ? OR name ILIKE ?)")
            pattern = f"%{search.strip()}%"
            parameters.extend([pattern, pattern])
        where = "WHERE " + " AND ".join(filters) if filters else ""
        parameters.append(row_limit)
        sql = f"""
            SELECT code, name, ts_code, market, date, pre_close
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY ts_code ORDER BY date DESC
                ) AS _row_number
                FROM {self._scan_sql()}
                {where}
            ) latest
            WHERE _row_number = 1
            ORDER BY code
            LIMIT ?
        """
        return self.connection.execute(sql, parameters).fetchdf()

    def domain_summary(self) -> pd.DataFrame:
        """统计本地已存在数据域的行数和证券数，便于监控全市场落盘进度。"""

        rows: list[dict[str, object]] = []
        for domain in sorted(ALLOWED_DOMAINS):
            try:
                path = self._parquet_glob(domain)
            except FileNotFoundError:
                continue
            columns = {
                item[0]
                for item in self.connection.execute(
                    f"DESCRIBE SELECT * FROM {self._scan_sql()}", [path]
                ).fetchall()
            }
            symbol_sql = "COUNT(DISTINCT ts_code)" if "ts_code" in columns else "NULL"
            count, symbols = self.connection.execute(
                f"SELECT COUNT(*), {symbol_sql} FROM {self._scan_sql()}", [path]
            ).fetchone()
            rows.append({"domain": domain, "rows": count, "symbols": symbols})
        return pd.DataFrame(rows, columns=["domain", "rows", "symbols"])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Query partitioned market Parquet through DuckDB")
    parser.add_argument("--data-root", default="data")
    subparsers = parser.add_subparsers(dest="command", required=True)

    symbols = subparsers.add_parser("symbols", help="list latest A-share security snapshots")
    symbols.add_argument("--search")
    symbols.add_argument("--market", choices=["SH", "SZ"])
    symbols.add_argument("--limit", type=int, default=20)

    bars = subparsers.add_parser("bars", help="query one symbol's bars")
    bars.add_argument("symbol")
    bars.add_argument("--timeframe", choices=sorted(TIMEFRAME_DOMAINS), default="daily")
    bars.add_argument("--start")
    bars.add_argument("--end")
    bars.add_argument("--limit", type=int, default=300)

    subparsers.add_parser("summary", help="show local Parquet domain coverage")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    with DuckDBMarketStore(args.data_root) as store:
        if args.command == "symbols":
            result = store.list_symbols(search=args.search, market=args.market, limit=args.limit)
        elif args.command == "bars":
            result = store.query_bars(
                args.symbol,
                timeframe=args.timeframe,
                start=args.start,
                end=args.end,
                limit=args.limit,
            )
        else:
            result = store.domain_summary()
    print(result.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
