"""为数据终端提供静态页面及按股票查询的本地 JSON API。"""

from __future__ import annotations

import argparse
import json
import math
import re
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import pandas as pd

from scripts.data_pipeline.indicators import compute_all
from scripts.data_pipeline.query import DuckDBMarketStore


FRONTEND_ROOT = Path(__file__).resolve().parent
CANONICAL_SYMBOL = re.compile(r"^\d{6}\.(?:SH|SZ|BJ)$")


def _number(value: Any) -> float | int | None:
    """把 pandas/numpy 数值转换为浏览器可安全解析的 JSON 数值。"""

    if value is None or pd.isna(value):
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _series(frame: pd.DataFrame, column: str) -> list[float | int | None]:
    if column not in frame.columns:
        return [None] * len(frame)
    return [_number(value) for value in frame[column].tolist()]


def _canonical_symbol(symbol: str) -> str:
    value = symbol.strip().upper()
    if not CANONICAL_SYMBOL.fullmatch(value):
        raise ValueError(f"invalid mainland symbol: {symbol!r}")
    return value


class MarketTerminalService:
    """封装终端需要的搜索与单股详情，避免前端直接读取巨型 JSON。"""

    def __init__(self, data_root: str | Path = "data") -> None:
        self.data_root = Path(data_root).expanduser().resolve()

    def search_symbols(
        self,
        query: str = "",
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, object]:
        """按代码或名称搜索证券列表；返回结果很小，适合下拉框即时查询。"""

        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        if not 0 <= offset <= 100_000:
            raise ValueError("offset must be between 0 and 100000")
        with DuckDBMarketStore(self.data_root) as store:
            frame = store.list_symbols(search=query or None, limit=limit + 1, offset=offset)
        has_more = len(frame) > limit
        frame = frame.iloc[:limit]
        items = [
            {
                "code": str(row.code),
                "name": str(row.name).replace(" ", "").replace("　", ""),
                "ts_code": str(row.ts_code),
                "market": str(row.market),
            }
            for row in frame.itertuples(index=False)
        ]
        return {
            "query": query,
            "offset": offset,
            "count": len(items),
            "has_more": has_more,
            "items": items,
        }

    def _partition(self, domain: str, symbol: str) -> pd.DataFrame:
        domain_root = self.data_root / domain
        direct = domain_root / f"ts_code={symbol}" / "data.parquet"
        paths = [direct] if direct.exists() else sorted(
            domain_root.rglob(f"ts_code={symbol}/data.parquet")
        )
        if not paths:
            return pd.DataFrame()
        frames = [pd.read_parquet(path) for path in paths]
        return pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]

    def _symbol_name(self, symbol: str) -> str:
        code = symbol.split(".", 1)[0]
        try:
            result = self.search_symbols(code, limit=20)["items"]
        except FileNotFoundError:
            return symbol
        for item in result:
            if item["ts_code"] == symbol:
                return str(item["name"])
        return symbol

    def market_overview(self, *, limit: int = 240) -> dict[str, object]:
        """返回沪深指数、市场宽度与当前 A 股证券数量。"""

        if not 20 <= limit <= 2_000:
            raise ValueError("limit must be between 20 and 2000")
        definitions = (("000001.SH", "上证指数"), ("399001.SZ", "深证成指"))
        indices: list[dict[str, object]] = []
        with DuckDBMarketStore(self.data_root) as store:
            for symbol, name in definitions:
                frame = store.query_bars(symbol, timeframe="index", limit=limit)
                if frame.empty:
                    continue
                frame = frame.sort_values("datetime")
                indices.append(
                    {
                        "ts_code": symbol,
                        "name": name,
                        "points": [
                            {
                                "trade_date": str(row.trade_date),
                                "close": _number(row.close),
                                "up_count": _number(getattr(row, "up_count", None)),
                                "down_count": _number(getattr(row, "down_count", None)),
                            }
                            for row in frame.itertuples(index=False)
                        ],
                    }
                )
            symbols = store.list_symbols(limit=6_000)
        if len(indices) < 2:
            raise FileNotFoundError("沪深指数数据尚未同步完整")
        markets = symbols["market"].astype(str).value_counts().to_dict()
        return {
            "indices": indices,
            "universe": {
                "total": int(len(symbols)),
                "SH": int(markets.get("SH", 0)),
                "SZ": int(markets.get("SZ", 0)),
            },
        }

    def stock_detail(self, symbol: str, *, limit: int = 800) -> dict[str, object]:
        """查询一只股票的日线并即时计算指标，再合并短线及股本快照。"""

        if not 60 <= limit <= 2_000:
            raise ValueError("limit must be between 60 and 2000")
        with DuckDBMarketStore(self.data_root) as store:
            bars = store.query_bars(symbol, timeframe="daily", limit=limit)
        if bars.empty:
            raise FileNotFoundError(f"no daily bars found for {symbol}")

        bars = bars.sort_values("datetime").reset_index(drop=True)
        canonical = str(bars["ts_code"].iloc[-1])
        capital = self._partition("finance_capital", canonical)
        capital_row = capital.iloc[-1] if not capital.empty else None
        float_shares = (
            _number(capital_row.get("liutongguben")) if capital_row is not None else None
        )
        enriched = compute_all(bars, timeframe="daily", shares=float_shares)

        short_term = self._partition("short_term_daily", canonical)
        short_row = None
        last_trade_date = str(enriched["trade_date"].iloc[-1])
        if not short_term.empty and "trade_date" in short_term.columns:
            exact = short_term[short_term["trade_date"].astype(str) == last_trade_date]
            if not exact.empty:
                short_row = exact.iloc[-1]

        close = pd.to_numeric(enriched["close"], errors="coerce")
        latest_close = _number(close.iloc[-1])
        previous_close = _number(close.iloc[-2]) if len(close) > 1 else None
        change_pct = None
        if latest_close is not None and previous_close not in {None, 0}:
            change_pct = (latest_close / previous_close - 1) * 100

        def latest(column: str, *, source: pd.Series | None = None) -> Any:
            row = source if source is not None else enriched.iloc[-1]
            return _number(row.get(column))

        latest_short = short_row if short_row is not None else pd.Series(dtype=object)
        amount = latest("amount")
        if amount is None:
            amount = latest("amount", source=latest_short)
        turnover_rate = latest("turnover_rate", source=latest_short)
        float_market_cap = latest("float_market_cap", source=latest_short)
        volume_ratio = latest("volume_ratio", source=latest_short)
        if float_shares not in {None, 0}:
            if turnover_rate is None:
                # pytdx 日线 vol 的单位为“手”，乘 100 后才是成交股数。
                turnover_rate = latest("vol") * 100 / float_shares
            if float_market_cap is None and latest_close is not None:
                float_market_cap = latest_close * float_shares
        if volume_ratio is None:
            volume_ratio = latest("VOL_RATIO")

        dates = pd.to_datetime(enriched["datetime"]).dt.strftime("%Y-%m-%d").tolist()
        ohlc = [
            [
                _number(row.open),
                _number(row.close),
                _number(row.low),
                _number(row.high),
            ]
            for row in enriched.itertuples(index=False)
        ]
        return {
            "ts_code": canonical,
            "name": self._symbol_name(canonical),
            "bars": len(enriched),
            "dates": dates,
            "ohlc": ohlc,
            "vol": _series(enriched, "vol"),
            "amount": _series(enriched, "amount"),
            "ma": {key: _series(enriched, key) for key in ("MA5", "MA10", "MA20", "MA60")},
            "boll": {
                "UP": _series(enriched, "BOLL_UP"),
                "MB": _series(enriched, "BOLL_MB"),
                "DN": _series(enriched, "BOLL_DN"),
            },
            "macd": {
                "DIF": _series(enriched, "DIF"),
                "DEA": _series(enriched, "DEA"),
                "HIST": _series(enriched, "MACD"),
            },
            "rsi": {key: _series(enriched, key) for key in ("RSI6", "RSI12", "RSI24")},
            "kdj": {key: _series(enriched, key) for key in ("K", "D", "J")},
            "latest": {
                "trade_date": dates[-1],
                "close": latest_close,
                "change_pct": change_pct,
                "amount": amount,
                "ma5": latest("MA5"),
                "ma10": latest("MA10"),
                "ma20": latest("MA20"),
                "ma60": latest("MA60"),
                "rsi6": latest("RSI6"),
                "macd_hist": latest("MACD"),
                "turnover_rate": turnover_rate,
                "float_market_cap": float_market_cap,
                "float_shares": float_shares,
                "volume_ratio": volume_ratio,
                "capital_updated_date": (
                    str(capital_row.get("updated_date"))
                    if capital_row is not None and not pd.isna(capital_row.get("updated_date"))
                    else None
                ),
            },
        }

    def minute_detail(self, symbol: str, *, limit: int = 800) -> dict[str, object]:
        """按股票返回本地已同步的 5/15/30/60 分钟 K 线。"""

        if not 20 <= limit <= 2_000:
            raise ValueError("limit must be between 20 and 2000")
        data: dict[str, dict[str, object]] = {}
        canonical = _canonical_symbol(symbol)
        with DuckDBMarketStore(self.data_root) as store:
            for timeframe in ("5m", "15m", "30m", "60m"):
                try:
                    frame = store.query_bars(symbol, timeframe=timeframe, limit=limit)
                except FileNotFoundError:
                    continue
                if frame.empty:
                    continue
                frame = frame.sort_values("datetime").reset_index(drop=True)
                canonical = str(frame["ts_code"].iloc[-1])
                data[timeframe] = {
                    "dates": pd.to_datetime(frame["datetime"]).dt.strftime("%m-%d %H:%M").tolist(),
                    "ohlc": [
                        [_number(row.open), _number(row.close), _number(row.low), _number(row.high)]
                        for row in frame.itertuples(index=False)
                    ],
                    "vol": _series(frame, "vol"),
                    "amount": _series(frame, "amount"),
                }
        if not data:
            raise FileNotFoundError(f"no minute bars found for {symbol}")
        return {
            "ts_code": canonical,
            "name": self._symbol_name(canonical),
            "timeframes": list(data),
            "data": data,
        }

    def ticks_detail(self, symbol: str) -> dict[str, object]:
        """按股票返回逐笔买卖分布、分钟资金流和分时成交价。"""

        canonical = _canonical_symbol(symbol)
        transactions = self._partition("tdx_transactions", canonical)
        if transactions.empty:
            raise FileNotFoundError(f"no transaction data found for {symbol}")
        transactions = transactions.copy()
        transactions["trade_date"] = transactions["trade_date"].astype(str)

        def minute_index(value: Any) -> int:
            digits = "".join(character for character in str(value) if character.isdigit())[:4]
            if len(digits) < 4:
                return 0
            hour, minute = divmod(int(digits), 100)
            if hour >= 13:
                return max(0, min(239, 120 + (hour - 13) * 60 + minute))
            return max(0, min(239, (hour - 9) * 60 + minute - 30))

        transactions["minute_idx"] = transactions["time"].map(minute_index)
        labels = transactions.get("buyorsell_label", pd.Series(index=transactions.index, dtype=object))
        transactions["side"] = labels.fillna("other").astype(str).str.lower().map(
            lambda value: value if value in {"buy", "sell"} else "other"
        )
        flow = [
            {
                "minute": int(index),
                "buy_vol": _number(group.loc[group["side"] == "buy", "vol"].sum()),
                "sell_vol": _number(group.loc[group["side"] == "sell", "vol"].sum()),
            }
            for index, group in transactions.groupby("minute_idx")
        ]
        distribution = (
            transactions["side"]
            .value_counts()
            .reindex(["buy", "sell", "neutral", "other"])
            .fillna(0)
            .astype(int)
            .to_dict()
        )
        minute_time = self._partition("minute_time", canonical)
        price_curve: list[dict[str, object]] = []
        if not minute_time.empty:
            minute_time = minute_time.sort_values("minute_idx")
            price_curve = [
                {
                    "minute": int(row.minute_idx),
                    "price": _number(row.price),
                    "vol": _number(row.vol),
                }
                for row in minute_time.itertuples(index=False)
            ]
        prices = pd.to_numeric(transactions["price"], errors="coerce").dropna()
        if prices.empty:
            raise FileNotFoundError(f"transaction prices missing for {symbol}")
        return {
            "ts_code": canonical,
            "name": self._symbol_name(canonical),
            "date": str(transactions["trade_date"].iloc[-1]),
            "n_ticks": int(len(transactions)),
            "distribution": distribution,
            "price_range": [_number(prices.min()), _number(prices.max())],
            "flow": flow,
            "price_curve": price_curve,
        }

    def fundamentals_detail(self, symbol: str) -> dict[str, object]:
        """按股票返回财务指标、股本结构和 F10 文本。"""

        canonical = _canonical_symbol(symbol)
        finance = self._partition("company_finance", canonical)
        if finance.empty:
            raise FileNotFoundError(f"no fundamentals found for {symbol}")
        periods = sorted(finance["period"].dropna().astype(str).unique().tolist())
        metrics: dict[str, dict[str, float | int | None]] = {}
        for metric, group in finance.groupby("metric"):
            values = {
                str(row.period): _number(row.value_num)
                for row in group.itertuples(index=False)
            }
            if any(value is not None for value in values.values()):
                metrics[str(metric)] = values

        capital_frame = self._partition("finance_capital", canonical)
        capital: dict[str, object] = {}
        if not capital_frame.empty:
            row = capital_frame.iloc[-1]
            capital = {
                "zongguben": _number(row.get("zongguben")),
                "liutongguben": _number(row.get("liutongguben")),
                "ipo_date": str(row.get("ipo_date", "")),
                "industry_code": str(row.get("industry", "")),
                "province_code": str(row.get("province", "")),
            }
        information = self._partition("company_info_raw", canonical)
        company_text = ""
        if not information.empty and "text" in information.columns:
            company_text = "\n\n".join(information["text"].dropna().astype(str).tolist())[:3_000]
        return {
            "ts_code": canonical,
            "name": self._symbol_name(canonical),
            "periods": periods,
            "metrics": metrics,
            "capital": capital,
            "company_info": company_text,
        }


class TerminalRequestHandler(SimpleHTTPRequestHandler):
    """同一端口同时提供网页文件和只读数据 API。"""

    server_version = "TdxQuantTerminal/1.0"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, directory=str(FRONTEND_ROOT), **kwargs)

    def end_headers(self) -> None:
        """本地开发终端始终读取最新 HTML/JS，避免浏览器继续显示旧静态页面。"""

        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    @property
    def service(self) -> MarketTerminalService:
        return self.server.service  # type: ignore[attr-defined]

    def _send_json(self, payload: object, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - http.server 约定的方法名
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/api/health":
                self._send_json({"status": "ok", "data_root": str(self.service.data_root)})
                return
            if parsed.path == "/api/market/overview":
                params = parse_qs(parsed.query)
                limit = int(params.get("limit", ["240"])[0])
                self._send_json(self.service.market_overview(limit=limit))
                return
            if parsed.path == "/api/symbols":
                params = parse_qs(parsed.query)
                query = params.get("q", [""])[0]
                limit = int(params.get("limit", ["100"])[0])
                offset = int(params.get("offset", ["0"])[0])
                self._send_json(self.service.search_symbols(query, limit=limit, offset=offset))
                return
            if parsed.path.startswith("/api/stocks/"):
                remainder = unquote(parsed.path.removeprefix("/api/stocks/")).strip("/")
                symbol, separator, resource = remainder.partition("/")
                params = parse_qs(parsed.query)
                if separator and resource == "minute":
                    limit = int(params.get("limit", ["800"])[0])
                    self._send_json(self.service.minute_detail(symbol, limit=limit))
                elif separator and resource == "ticks":
                    self._send_json(self.service.ticks_detail(symbol))
                elif separator and resource == "fundamentals":
                    self._send_json(self.service.fundamentals_detail(symbol))
                elif not separator:
                    limit = int(params.get("limit", ["800"])[0])
                    self._send_json(self.service.stock_detail(symbol, limit=limit))
                else:
                    raise FileNotFoundError(f"unknown stock resource: {resource}")
                return
        except (ValueError, FileNotFoundError) as exc:
            status = HTTPStatus.NOT_FOUND if isinstance(exc, FileNotFoundError) else HTTPStatus.BAD_REQUEST
            self._send_json({"error": str(exc)}, status)
            return
        except Exception as exc:  # noqa: BLE001 - API 必须返回可读错误而不是断开连接
            self._send_json({"error": f"{type(exc).__name__}: {exc}"}, HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        super().do_GET()


def create_server(
    bind: str,
    port: int,
    *,
    data_root: str | Path = "data",
) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((bind, port), TerminalRequestHandler)
    server.service = MarketTerminalService(data_root)  # type: ignore[attr-defined]
    return server


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Serve the DuckDB-backed A-share data terminal")
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    server = create_server(args.bind, args.port, data_root=args.data_root)
    print(f"A股数据终端已启动：http://{args.bind}:{args.port}/")
    print(f"数据目录：{Path(args.data_root).expanduser().resolve()}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n数据终端已停止。")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
