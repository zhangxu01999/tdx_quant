"""
通达信 MCP 数据增补器

将 TDX MCP 能提供、但当前 parquet 快照缺失的高价值数据批量拉取并保存。

当前系统缺口（本脚本填补）：
  1. 概念板块标签   (concept_tags)    — 完全缺失，TDX 最多 47 个概念/只
  2. 北向资金持仓   (north_money)     — 完全缺失，陆股通净买量/成交额
  3. 机构基金持仓   (inst_holdings)   — 完全缺失，基金持仓比例/机构数
  4. 分析师评级     (analyst_ratings) — 完全缺失，目标价/综合评级/机构数
  5. 筹码分布增强   (chip_enhanced)   — 有 winner_rate 但缺 集中度/获利比例/平均成本

用法：
    # 增补全部（耗时较长，自动保存进度）
    python -m scripts.tdx_mcp.tdx_data_enricher --all

    # 只增补概念标签（最常用，约 2~5 分钟）
    python -m scripts.tdx_mcp.tdx_data_enricher --concepts

    # 只增补北向 + 机构持仓（行情维度）
    python -m scripts.tdx_mcp.tdx_data_enricher --market

    # 只增补分析师评级（指定股票列表）
    python -m scripts.tdx_mcp.tdx_data_enricher --ratings --codes 600519,300750,000001

    # 查看本次可增补的字段清单
    python -m scripts.tdx_mcp.tdx_data_enricher --dry-run

输出：
    data/tdx_concepts.json          — 个股概念映射
    data/tdx_north_money.json       — 北向资金个股
    data/tdx_inst_holdings.json     — 机构基金持仓
    data/tdx_analyst_ratings.json   — 分析师评级
    data/tdx_chip_enhanced.json     — 筹码分布增强
    data/tdx_enriched_summary.json  — 增补元数据（时间/覆盖率等）
"""

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from scripts.tdx_mcp.tdx_client import TdxMcpClient

API_KEY = os.getenv("TDX_API_KEY", "")
# 本子包比参考源深一层（scripts/tdx_mcp/ → 项目根），需多退一层父目录
DATA_DIR = Path(__file__).parent.parent.parent / "data"

# --------------------------------------------------------------------------
# 批量查询策略
# --------------------------------------------------------------------------

class TdxDataEnricher:
    def __init__(self, api_key: str = API_KEY, workers: int = 3, delay: float = 0.4):
        self.client = TdxMcpClient(api_key)
        self.workers = workers
        self.delay = delay
        DATA_DIR.mkdir(exist_ok=True)

    # -----------------------------------------------------------------------
    # 1. 概念板块标签（全市场批量）
    # -----------------------------------------------------------------------

    def fetch_concepts(self, page_size: int = 50, max_pages: int = 150) -> dict[str, dict]:
        """
        全市场个股概念标签批量拉取。

        返回：{ts_code: {"name": ..., "industry": ..., "concepts": [...], "concept_count": N}}
        """
        print("▶ 拉取全市场概念标签...")
        result = self.client.query(
            "全市场个股 所属概念 概念个数",
            size=page_size, page=1
        )
        if not result.ok():
            print(f"  首页查询失败: {result.message}")
            return {}

        total = result.total
        total_pages = min(max_pages, -(-total // page_size))
        print(f"  总计 {total} 只股票，预计 {total_pages} 页")

        all_rows = list(result.data)
        for p in range(2, total_pages + 1):
            time.sleep(self.delay)
            r = self.client.query("全市场个股 所属概念 概念个数", size=page_size, page=p)
            if r.ok() and r.data:
                all_rows.extend(r.data)
            if p % 10 == 0:
                print(f"  已获取 {len(all_rows)}/{total} 只...")

        # 解析
        headers = result.headers
        out: dict[str, dict] = {}
        for row in all_rows:
            d = dict(zip(headers, row))
            code = d.get("sec_code", "")
            market = d.get("market", "")
            # 拼成 ts_code 格式（0=SZ深, 1=SH沪, 2=BJ北）
            suffix = {0: "SZ", "0": "SZ", 1: "SH", "1": "SH", 2: "BJ", "2": "BJ"}.get(market, "")
            ts_code = f"{code}.{suffix}" if suffix else code

            raw_concepts = d.get("所属概念", "")
            concepts = _parse_concepts(raw_concepts)

            out[ts_code] = {
                "sec_code": code,
                "name": d.get("sec_name", ""),
                "industry": d.get("所属行业", "").replace("@", ""),
                "concepts": concepts,
                "concept_count": len(concepts),
                "raw_concepts": raw_concepts,
            }

        print(f"  ✅ 概念标签完成，覆盖 {len(out)} 只股票")
        return out

    # -----------------------------------------------------------------------
    # 2. 北向资金（全市场批量）
    # -----------------------------------------------------------------------

    def fetch_north_money(self, page_size: int = 50) -> dict[str, dict]:
        """
        陆股通今日活跃个股净买量 + 成交额。

        注意：TDX 只返回当日陆股通成交排行（约 20~50 只），
        不是全量持仓列表。覆盖今日外资主动买卖的重点标的。

        返回：{ts_code: {"name": ..., "hsgt_amount": ..., "north_net_buy": ...}}
        """
        print("▶ 拉取北向资金（今日陆股通活跃标的）...")
        result = self.client.query_all(
            "今日陆股通成交个股列表 成交额排行 北向资金净买入",
            page_size=page_size,
            max_pages=5,
            delay=self.delay,
        )
        if not result.ok():
            print(f"  查询失败: {result.message}")
            return {}

        out: dict[str, dict] = {}
        for row in result.to_dicts():
            code = row.get("sec_code", "")
            market = row.get("market", "")
            suffix = {0: "SZ", "0": "SZ", 1: "SH", "1": "SH", 2: "BJ"}.get(market, "")
            ts_code = f"{code}.{suffix}" if suffix else code

            # 字段名可能含日期后缀
            hsgt_amount = _find_field(row, "陆股通成交额")
            north_net = _find_field(row, "主力净买量")

            out[ts_code] = {
                "sec_code": code,
                "name": row.get("sec_name", ""),
                "hsgt_amount": _to_float(hsgt_amount),
                "north_net_buy": _to_float(north_net),
                "trade_date": _find_field(row, "交易日期"),
            }

        print(f"  ✅ 北向资金完成，覆盖 {len(out)} 只股票")
        return out

    # -----------------------------------------------------------------------
    # 3. 机构基金持仓（全市场批量）
    # -----------------------------------------------------------------------

    def fetch_inst_holdings(self, page_size: int = 50) -> dict[str, dict]:
        """
        机构/基金持仓比例及家数。

        返回：{ts_code: {"fund_hold_ratio": ..., "inst_count": ..., "fund_count": ...}}
        """
        print("▶ 拉取机构基金持仓...")
        result = self.client.query_all(
            "机构持仓比例 基金持流通A股比例 机构总量 基金机构数",
            page_size=page_size,
            max_pages=130,
            delay=self.delay,
        )
        if not result.ok():
            print(f"  查询失败: {result.message}")
            return {}

        out: dict[str, dict] = {}
        for row in result.to_dicts():
            code = row.get("sec_code", "")
            market = row.get("market", "")
            suffix = {0: "SZ", "0": "SZ", 1: "SH", "1": "SH", 2: "BJ"}.get(market, "")
            ts_code = f"{code}.{suffix}" if suffix else code

            out[ts_code] = {
                "sec_code": code,
                "name": row.get("sec_name", ""),
                # 机构维度（含基金）
                "inst_total_shares": _to_float(_find_field(row, "机构持股总量")),
                "inst_hold_ratio": _to_float(_find_field(row, "机构持流通A股比例")),
                "inst_hold_total_ratio": _to_float(_find_field(row, "机构持总股本比例")),
                "inst_count": _to_int(_find_field(row, "机构总量")),
                # 基金维度
                "fund_hold_ratio": _to_float(_find_field(row, "基金持流通A股比例")),
                "fund_hold_total_ratio": _to_float(_find_field(row, "基金持总股本比例")),
                "fund_total_shares": _to_float(_find_field(row, "基金持股数")),
                "fund_count": _to_int(_find_field(row, "基金机构数")),
            }

        print(f"  ✅ 机构持仓完成，覆盖 {len(out)} 只股票")
        return out

    # -----------------------------------------------------------------------
    # 4. 分析师评级（个股并发）
    # -----------------------------------------------------------------------

    def fetch_analyst_ratings(
        self,
        codes: list[str] | None = None,
        max_stocks: int = 500,
    ) -> dict[str, dict]:
        """
        分析师综合评级、目标价、EPS 预测。

        Args:
            codes: 指定代码列表（ts_code 格式），None 则取项目 data/daily 内前 max_stocks 只
        """
        if codes is None:
            codes = _get_top_codes(max_stocks)
        print(f"▶ 拉取分析师评级（{len(codes)} 只）...")

        def query_one(ts_code: str) -> tuple[str, dict | None]:
            code = ts_code.split(".")[0]
            try:
                r = self.client.query(f"{code} 研究报告 分析师评级 目标价", size=1)
                if r.ok() and r.data:
                    row = r.to_dicts()[0]
                    return ts_code, {
                        "target_price": _to_float(_find_field(row, "目标价")),
                        "consensus_rating": _to_float(_find_field(row, "综合评级")),
                        "rating_firm_count": _to_int(_find_field(row, "评级机构家数")),
                        "forecast_eps": _to_float(_find_field(row, "预测每股收益")),
                        "forecast_profit": _to_float(_find_field(row, "预测净利润")),
                        "forecast_revenue": _to_float(_find_field(row, "预测营业收入")),
                        "forecast_roe": _to_float(_find_field(row, "预测净资产收益率")),
                        "analysts": _find_field(row, "分析师"),
                        "star_analyst": _find_field(row, "明星分析师"),
                    }
            except Exception:
                pass
            return ts_code, None

        out: dict[str, dict] = {}
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = [pool.submit(query_one, c) for c in codes]
            for i, fut in enumerate(as_completed(futures), 1):
                ts_code, data = fut.result()
                if data:
                    out[ts_code] = data
                if i % 50 == 0:
                    print(f"  已完成 {i}/{len(codes)}...")
                time.sleep(self.delay / self.workers)

        print(f"  ✅ 分析师评级完成，覆盖 {len(out)} 只股票")
        return out

    # -----------------------------------------------------------------------
    # 5. 筹码分布增强（个股并发）
    # -----------------------------------------------------------------------

    def fetch_chip_enhanced(
        self,
        codes: list[str] | None = None,
        max_stocks: int = 800,
    ) -> dict[str, dict]:
        """
        筹码集中度 + 获利比例 + 平均成本（补充现有 winner_rate/cost_50pct）。
        """
        if codes is None:
            codes = _get_top_codes(max_stocks)
        print(f"▶ 拉取筹码分布增强（{len(codes)} 只）...")

        def query_one(ts_code: str) -> tuple[str, dict | None]:
            code = ts_code.split(".")[0]
            try:
                r = self.client.query(f"{code} 筹码分布 集中度 获利比例 平均成本", size=1)
                if r.ok() and r.data:
                    row = r.to_dicts()[0]
                    return ts_code, {
                        "chip_concentration_90": _to_float(_find_field(row, "集中度90")),
                        "chip_concentration_70": _to_float(_find_field(row, "集中度70")),
                        "chip_profit_ratio": _to_float(_find_field(row, "获利比例")),
                        "chip_avg_cost": _to_float(_find_field(row, "平均成本")),
                    }
            except Exception:
                pass
            return ts_code, None

        out: dict[str, dict] = {}
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = [pool.submit(query_one, c) for c in codes]
            for i, fut in enumerate(as_completed(futures), 1):
                ts_code, data = fut.result()
                if data:
                    out[ts_code] = data
                if i % 100 == 0:
                    print(f"  已完成 {i}/{len(codes)}...")
                time.sleep(self.delay / self.workers)

        print(f"  ✅ 筹码增强完成，覆盖 {len(out)} 只股票")
        return out

    # -----------------------------------------------------------------------
    # 保存 & 汇总
    # -----------------------------------------------------------------------

    def save(self, filename: str, data: dict) -> Path:
        path = DATA_DIR / filename
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        size_kb = path.stat().st_size // 1024
        print(f"  💾 已保存 {path.name}（{len(data)} 条，{size_kb} KB）")
        return path

    def save_summary(self, results: dict[str, int]) -> None:
        summary = {
            "generated_at": datetime.now().isoformat(),
            "coverage": results,
            "files": {k: str(DATA_DIR / f"tdx_{k}.json") for k in results},
        }
        self.save("tdx_enriched_summary.json", summary)


# --------------------------------------------------------------------------
# 辅助函数
# --------------------------------------------------------------------------

def _parse_concepts(raw: str) -> list[str]:
    """【@DeepSeek@】;【@人工智能@】 → ["DeepSeek", "人工智能"]"""
    import re
    return [
        c.replace("@", "").strip()
        for c in re.split(r"[;；【】]", raw)
        if c.replace("@", "").strip()
    ]


def _find_field(row: dict, key: str) -> str:
    """模糊匹配字段名（字段名可能含日期后缀）。"""
    for k, v in row.items():
        if key in k:
            return str(v) if v is not None else ""
    return ""


def _to_float(v: str) -> float | None:
    try:
        return float(v) if v else None
    except (ValueError, TypeError):
        return None


def _to_int(v: str) -> int | None:
    try:
        return int(float(v)) if v else None
    except (ValueError, TypeError):
        return None


def _get_top_codes(n: int) -> list[str]:
    """取代码全集，取前 n。

    与参考源不同：tdx_quant 没有合并的 data.parquet，也没有流通市值列；
    日 K 是 hive 分区存储（data/daily/ts_code=*/）。这里直接枚举分区目录
    作为股票池，按 ts_code 排序取前 n。--codes 显式传入时本函数不会被调用。
    """
    try:
        daily_root = DATA_DIR / "daily"
        codes = sorted(
            p.name.split("=", 1)[1]
            for p in daily_root.glob("ts_code=*")
            if p.is_dir()
        )
        return codes[:n]
    except Exception:
        return []


def _load_existing(filename: str) -> dict:
    path = DATA_DIR / filename
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="通达信 MCP 数据增补器",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python -m scripts.tdx_mcp.tdx_data_enricher --all
  python -m scripts.tdx_mcp.tdx_data_enricher --concepts
  python -m scripts.tdx_mcp.tdx_data_enricher --market
  python -m scripts.tdx_mcp.tdx_data_enricher --ratings --codes 600519,300750
  python -m scripts.tdx_mcp.tdx_data_enricher --dry-run
        """
    )
    parser.add_argument("--all", action="store_true", help="增补全部数据类型")
    parser.add_argument("--concepts", action="store_true", help="只增补概念板块标签")
    parser.add_argument("--market", action="store_true", help="只增补北向资金 + 机构持仓")
    parser.add_argument("--ratings", action="store_true", help="只增补分析师评级")
    parser.add_argument("--chip", action="store_true", help="只增补筹码分布增强")
    parser.add_argument("--codes", help="指定代码（逗号分隔，如 600519,300750）")
    parser.add_argument("--max-stocks", type=int, default=500, help="评级/筹码最多查询股票数")
    parser.add_argument("--workers", type=int, default=3, help="并发线程数（默认 3）")
    parser.add_argument("--delay", type=float, default=0.4, help="翻页间隔秒（默认 0.4）")
    parser.add_argument("--dry-run", action="store_true", help="显示字段清单，不执行查询")
    parser.add_argument("--api-key", default=API_KEY)
    args = parser.parse_args()

    if args.dry_run:
        _print_field_plan()
        return

    enricher = TdxDataEnricher(args.api_key, workers=args.workers, delay=args.delay)
    codes = [c.strip() for c in args.codes.split(",")] if args.codes else None
    coverage: dict[str, int] = {}

    t0 = time.time()

    if args.all or args.concepts:
        data = enricher.fetch_concepts()
        enricher.save("tdx_concepts.json", data)
        coverage["concepts"] = len(data)

    if args.all or args.market:
        data = enricher.fetch_north_money()
        enricher.save("tdx_north_money.json", data)
        coverage["north_money"] = len(data)

        data = enricher.fetch_inst_holdings()
        enricher.save("tdx_inst_holdings.json", data)
        coverage["inst_holdings"] = len(data)

    if args.all or args.ratings:
        data = enricher.fetch_analyst_ratings(codes=codes, max_stocks=args.max_stocks)
        enricher.save("tdx_analyst_ratings.json", data)
        coverage["analyst_ratings"] = len(data)

    if args.all or args.chip:
        data = enricher.fetch_chip_enhanced(codes=codes, max_stocks=args.max_stocks)
        enricher.save("tdx_chip_enhanced.json", data)
        coverage["chip_enhanced"] = len(data)

    enricher.save_summary(coverage)

    elapsed = time.time() - t0
    print(f"\n✅ 全部完成，耗时 {elapsed:.0f}s")
    print(f"   数据保存在 {DATA_DIR}/")
    for k, v in coverage.items():
        print(f"   {k}: {v} 条")


def _print_field_plan():
    plan = {
        "1. 概念板块标签 (tdx_concepts.json)": {
            "查询方式": "全市场批量，约 124 页",
            "新增字段": ["concepts (list)", "concept_count (int)", "raw_concepts (str)"],
            "价值": "支持概念板块筛选、轮动分析、热点追踪",
        },
        "2. 北向资金 (tdx_north_money.json)": {
            "查询方式": "全市场批量，约 30 页",
            "新增字段": ["hsgt_amount (元)", "north_net_buy (手)"],
            "价值": "追踪外资动向，识别外资重仓/减仓个股",
        },
        "3. 机构基金持仓 (tdx_inst_holdings.json)": {
            "查询方式": "全市场批量，约 130 页",
            "新增字段": [
                "fund_hold_ratio (基金持流通A股%)",
                "inst_count (机构总家数)",
                "fund_count (基金家数)",
                "fund_hold_total_ratio (基金持总股本%)",
            ],
            "价值": "识别机构重仓股、判断机构拥挤度",
        },
        "4. 分析师评级 (tdx_analyst_ratings.json)": {
            "查询方式": "个股并发，data/daily 内前 max-stocks 只（或 --codes 指定）",
            "新增字段": [
                "target_price (目标价)",
                "consensus_rating (综合评级 1~5)",
                "rating_firm_count (机构家数)",
                "forecast_eps (预测每股收益)",
                "forecast_profit (预测净利润)",
                "forecast_revenue (预测营收)",
                "forecast_roe (预测ROE)",
            ],
            "价值": "反映机构预期，高评级+低估值=潜在标的",
        },
        "5. 筹码分布增强 (tdx_chip_enhanced.json)": {
            "查询方式": "个股并发，data/daily 内前 max-stocks 只（或 --codes 指定）",
            "新增字段": [
                "chip_concentration_90 (90%成本区间宽度)",
                "chip_concentration_70 (70%成本区间宽度)",
                "chip_profit_ratio (获利比例%)",
                "chip_avg_cost (平均持仓成本)",
            ],
            "价值": "判断筹码松散/紧张，套牢盘压力，支撑位估算",
        },
    }
    print("\n通达信 MCP 数据增补计划")
    print("=" * 60)
    for title, info in plan.items():
        print(f"\n{title}")
        for k, v in info.items():
            if isinstance(v, list):
                print(f"  {k}:")
                for item in v:
                    print(f"    - {item}")
            else:
                print(f"  {k}: {v}")
    print("\n运行 --all 开始全量增补，或选择单项 --concepts / --market / --ratings / --chip")


if __name__ == "__main__":
    main()
