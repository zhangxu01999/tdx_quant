"""
通达信个股完整诊断

用法：
    python -m scripts.tdx_mcp.tdx_stock_analyzer 600519
    python -m scripts.tdx_mcp.tdx_stock_analyzer 贵州茅台
    python -m scripts.tdx_mcp.tdx_stock_analyzer 600519 --json
    TDX_API_KEY=TDX-xxx python -m scripts.tdx_mcp.tdx_stock_analyzer 600519

输出：行情 / 技术面 / 基本面 / 资金面 四维分析报告
"""

import argparse
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# 允许从项目根目录直接运行（python scripts/tdx_mcp/tdx_stock_analyzer.py）
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from scripts.tdx_mcp.tdx_client import TdxMcpClient, TdxQueryResult

API_KEY = os.getenv("TDX_API_KEY", "")


def analyze_stock(stock: str, client: TdxMcpClient) -> dict:
    """
    对单只股票并发查询四个维度，返回结构化分析数据。

    Args:
        stock: 股票代码或名称（如 "600519" 或 "贵州茅台"）
    """
    queries = {
        "quote":     f"{stock} 最新行情 现价 涨跌幅 成交量 换手率 成交额 振幅 总市值",
        "technical": f"{stock} 技术指标 MACD KDJ RSI 均线 MA5 MA10 MA20 MA60",
        "financial": f"{stock} 财务指标 市盈率PE 市净率PB ROE 净利润 营业收入 负债率",
        "capital":   f"{stock} 资金流向 主力净流入 超大单 大单 中单 小单",
    }

    results: dict[str, TdxQueryResult] = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(client.query, q): key for key, q in queries.items()}
        for fut in as_completed(futures):
            key = futures[fut]
            try:
                results[key] = fut.result()
            except Exception as e:
                print(f"  [!] {key} 查询异常: {e}", file=sys.stderr)

    # 整理输出
    report = {"stock": stock, "sections": {}}

    if "quote" in results and results["quote"].ok() and results["quote"].data:
        row = results["quote"].to_dicts()[0]
        report["name"] = row.get("sec_name", stock)
        report["code"] = row.get("sec_code", stock)
        report["industry"] = row.get("所属行业", "")
        report["concepts"] = row.get("所属概念", "")
        report["sections"]["行情"] = {k: v for k, v in row.items()
                                       if k not in ("POS", "market", "show_url",
                                                    "sec_code", "sec_name", "index_market",
                                                    "index_code")}

    for key, label in [("technical", "技术面"), ("financial", "基本面"), ("capital", "资金面")]:
        if key in results and results[key].ok() and results[key].data:
            row = results[key].to_dicts()[0]
            report["sections"][label] = {k: v for k, v in row.items()
                                          if k not in ("POS", "market", "show_url",
                                                       "sec_code", "sec_name", "index_market",
                                                       "index_code")}

    return report


def print_report(report: dict) -> None:
    name = report.get("name", report["stock"])
    code = report.get("code", "")
    industry = report.get("industry", "").replace("@", "")
    concepts_raw = report.get("concepts", "")
    concepts = concepts_raw.replace("【", "").replace("】", "").replace("@", "").replace(";", " | ")

    print("=" * 60)
    print(f"  {name}（{code}）  |  {industry}")
    print("=" * 60)

    if concepts:
        print(f"\n【概念标签】{concepts[:120]}{'…' if len(concepts) > 120 else ''}")

    for section, data in report.get("sections", {}).items():
        print(f"\n── {section} {'─' * (50 - len(section))}")
        for k, v in data.items():
            if v and str(v) not in ("nan", "None", ""):
                # 简化字段名（去掉日期后缀）
                clean_key = k.split("<br>")[0].split(".前复权")[0].strip()
                print(f"  {clean_key:<28} {v}")

    print("\n" + "=" * 60)


def main():
    parser = argparse.ArgumentParser(description="通达信个股完整诊断")
    parser.add_argument("stock", help="股票代码或名称，如 600519 / 贵州茅台")
    parser.add_argument("--json", action="store_true", help="输出原始 JSON")
    parser.add_argument("--api-key", default=API_KEY, help="通达信 API Key")
    args = parser.parse_args()

    client = TdxMcpClient(args.api_key)
    print(f"正在查询 {args.stock} …")

    report = analyze_stock(args.stock, client)

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print_report(report)


if __name__ == "__main__":
    main()
