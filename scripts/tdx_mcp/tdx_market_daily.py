"""
通达信每日市场概览

一键获取市场整体情绪、板块热度、资金动向，适合收盘后复盘或开盘前预判。

用法：
    python -m scripts.tdx_mcp.tdx_market_daily
    python -m scripts.tdx_mcp.tdx_market_daily --json
    python -m scripts.tdx_mcp.tdx_market_daily --section breadth   # 只看涨跌家数
    python -m scripts.tdx_mcp.tdx_market_daily --section sectors   # 只看板块热度
    python -m scripts.tdx_mcp.tdx_market_daily --section capital   # 只看资金动向
    python -m scripts.tdx_mcp.tdx_market_daily --section sentiment # 只看市场情绪
"""

import argparse
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from scripts.tdx_mcp.tdx_client import TdxMcpClient

API_KEY = os.getenv("TDX_API_KEY", "")

SECTIONS = {
    "breadth": "市场涨跌家数 今日上涨家数 下跌家数 涨停家数 跌停家数 平盘家数",
    "limit_up": "今日涨停股列表 涨停原因 连板天数 封单金额",
    "top_sectors": "今日行业板块涨幅排行 前10",
    "hot_concepts": "今日涨停股热点概念 板块轮动",
    "north_flow": "今日北向资金净流入 沪股通 深股通",
    "main_capital": "今日主力净流入排行 大盘主力资金 超大单净流入",
    "index": "上证指数 深证成指 创业板指 今日行情",
}


def fetch_all_sections(client: TdxMcpClient) -> dict[str, object]:
    results = {}
    with ThreadPoolExecutor(max_workers=len(SECTIONS)) as pool:
        futures = {pool.submit(client.query, q, "AG", 10): key
                   for key, q in SECTIONS.items()}
        for fut in as_completed(futures):
            key = futures[fut]
            try:
                results[key] = fut.result()
            except Exception as e:
                print(f"  [!] {key} 查询失败: {e}", file=sys.stderr)
    return results


def print_overview(results: dict) -> None:
    date_str = datetime.now().strftime("%Y-%m-%d")
    print(f"\n{'='*60}")
    print(f"  通达信市场每日概览  {date_str}")
    print(f"{'='*60}")

    section_labels = {
        "index":        "▸ 主要指数",
        "breadth":      "▸ 涨跌家数",
        "limit_up":     "▸ 今日涨停（前10）",
        "top_sectors":  "▸ 行业板块涨幅榜",
        "hot_concepts": "▸ 热点概念",
        "north_flow":   "▸ 北向资金",
        "main_capital": "▸ 主力资金",
    }

    for key, label in section_labels.items():
        result = results.get(key)
        if result is None:
            continue
        print(f"\n{label}")
        print("-" * 50)
        if not result.ok() or not result.data:
            print("  （暂无数据）")
            continue

        rows = result.to_dicts()
        for row in rows[:10]:
            name = row.get("sec_name", "")
            chg = row.get("chg", row.get("chg0#", ""))
            price = row.get("now_price", "")
            industry = row.get("所属行业", "").replace("@", "")
            concept = row.get("所属通达信概念", row.get("所属通达信风格", "")).replace("@", "")
            reason = row.get("涨停原因", "")[:40] if row.get("涨停原因") else ""

            line_parts = [f"  {name:<10}"]
            if price:
                line_parts.append(f"  {price:>8}")
            if chg:
                line_parts.append(f"  {str(chg):>7}%")
            if industry:
                line_parts.append(f"  {industry}")
            if reason:
                line_parts.append(f"  {reason}")
            elif concept:
                line_parts.append(f"  {concept}")
            print("".join(line_parts))

    print(f"\n{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(description="通达信每日市场概览")
    parser.add_argument("--json", action="store_true", help="输出原始 JSON")
    parser.add_argument(
        "--section",
        choices=list(SECTIONS.keys()),
        help="只查询特定板块",
    )
    parser.add_argument("--api-key", default=API_KEY)
    args = parser.parse_args()

    client = TdxMcpClient(args.api_key)
    print("正在并发查询市场数据…")

    if args.section:
        result = client.query(SECTIONS[args.section], size=20)
        if args.json:
            print(json.dumps(result.raw, ensure_ascii=False, indent=2))
        else:
            result.print_table()
    else:
        results = fetch_all_sections(client)
        if args.json:
            print(json.dumps(
                {k: v.raw for k, v in results.items()},
                ensure_ascii=False,
                indent=2,
            ))
        else:
            print_overview(results)


if __name__ == "__main__":
    main()
