"""
通达信概念板块分析工具

用法：
    # 查询某概念板块成分股（按涨幅排序）
    python -m scripts.tdx_mcp.tdx_concept_board --concept "DeepSeek"

    # 查询某概念板块成分股，显示全部
    python -m scripts.tdx_mcp.tdx_concept_board --concept "人形机器人" --all

    # 今日概念热度排行（哪些概念涨停股最多）
    python -m scripts.tdx_mcp.tdx_concept_board --hot

    # 查某只股票属于哪些概念
    python -m scripts.tdx_mcp.tdx_concept_board --stock 600519

    # 跨概念对比（分别查询后汇总）
    python -m scripts.tdx_mcp.tdx_concept_board --compare "DeepSeek" "人工智能" "人形机器人"
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from scripts.tdx_mcp.tdx_client import TdxMcpClient, TdxQueryResult

API_KEY = os.getenv("TDX_API_KEY", "")


def get_concept_stocks(
    client: TdxMcpClient,
    concept: str,
    top_n: int = 20,
    fetch_all: bool = False,
) -> TdxQueryResult:
    """获取概念板块成分股列表（按涨幅降序）。"""
    question = f"{concept}概念板块成分股 今日行情 涨跌幅"
    if fetch_all:
        return client.query_all(question, page_size=50)
    return client.query(question, size=top_n)


def get_concept_hot_ranking(client: TdxMcpClient, top_n: int = 20) -> TdxQueryResult:
    """今日热门概念板块涨幅排行（通过涨停股统计推断）。"""
    return client.query(
        "今日涨停概念热点板块 涨停家数 主力净流入",
        size=top_n,
    )


def get_stock_concepts(client: TdxMcpClient, stock: str) -> dict:
    """查询个股所属全部概念标签。"""
    result = client.query(f"{stock} 所属概念板块")
    if result.ok() and result.data:
        row = result.to_dicts()[0]
        name = row.get("sec_name", stock)
        raw = row.get("所属概念", row.get("所属通达信概念", ""))
        # 清洗格式：【@概念@】;【@概念@】 → list
        concepts = [
            c.replace("【", "").replace("】", "").replace("@", "").strip()
            for c in raw.split(";")
            if c.strip()
        ]
        return {"name": name, "code": row.get("sec_code", stock), "concepts": concepts}
    return {"name": stock, "code": stock, "concepts": []}


def compare_concepts(client: TdxMcpClient, concepts: list[str]) -> list[dict]:
    """对比多个概念板块的今日表现。"""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    summaries = []

    def fetch_one(concept: str) -> dict:
        result = client.query(f"{concept}概念板块成分股 今日涨跌幅", size=100)
        if not result.ok() or not result.data:
            return {"concept": concept, "count": 0, "avg_chg": 0, "up": 0, "down": 0}

        rows = result.to_dicts()
        changes = []
        for row in rows:
            for k, v in row.items():
                if "涨跌幅" in k:
                    try:
                        changes.append(float(v))
                    except (ValueError, TypeError):
                        pass
                    break

        up = sum(1 for c in changes if c > 0)
        down = sum(1 for c in changes if c < 0)
        avg = sum(changes) / len(changes) if changes else 0
        return {
            "concept": concept,
            "total": result.total,
            "sample": len(changes),
            "avg_chg": round(avg, 2),
            "up": up,
            "down": down,
            "up_ratio": f"{up / len(changes):.0%}" if changes else "N/A",
        }

    with ThreadPoolExecutor(max_workers=len(concepts)) as pool:
        futures = {pool.submit(fetch_one, c): c for c in concepts}
        for fut in as_completed(futures):
            try:
                summaries.append(fut.result())
            except Exception as e:
                print(f"  [!] 查询异常: {e}", file=sys.stderr)

    summaries.sort(key=lambda x: x["avg_chg"], reverse=True)
    return summaries


def print_concept_stocks(result: TdxQueryResult, concept: str) -> None:
    print(f"\n【{concept}】概念板块  共 {result.total} 只股票")
    print("-" * 65)
    # 显示关键字段
    key_fields = ["sec_name", "sec_code", "所属行业", "now_price", "chg"]
    rows = result.to_dicts()
    # 动态找字段（字段名可能含日期后缀）
    header_map = {}
    if rows:
        for display in key_fields:
            for k in rows[0].keys():
                if display in k:
                    header_map[display] = k
                    break
    cols = [header_map.get(f, f) for f in key_fields if header_map.get(f, f) in (rows[0] if rows else {})]
    labels = ["股票名称", "代码", "所属行业", "现价", "涨跌幅%"]
    widths = [10, 8, 10, 8, 8]
    fmt = "".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*labels[:len(cols)]))
    print("-" * 65)
    for row in rows:
        vals = [str(row.get(c, ""))[:w - 1] for c, w in zip(cols, widths)]
        print(fmt.format(*vals))
    print(f"\n本页 {len(rows)} 条（共 {result.total} 条）")


def print_compare(summaries: list[dict]) -> None:
    print("\n概念板块对比（今日表现）")
    print("-" * 60)
    fmt = "{:<16} {:>8} {:>8} {:>6} {:>6} {:>8}"
    print(fmt.format("概念", "成分股数", "平均涨跌%", "上涨", "下跌", "上涨比例"))
    print("-" * 60)
    for s in summaries:
        print(fmt.format(
            s["concept"][:14],
            s["total"],
            s["avg_chg"],
            s["up"],
            s["down"],
            s["up_ratio"],
        ))


def main():
    parser = argparse.ArgumentParser(description="通达信概念板块分析")
    parser.add_argument("--concept", help="查询指定概念成分股，如 'DeepSeek'")
    parser.add_argument("--all", action="store_true", help="获取全部成分股（自动翻页）")
    parser.add_argument("--top", type=int, default=20, help="显示前 N 条（默认 20）")
    parser.add_argument("--hot", action="store_true", help="今日概念热度排行")
    parser.add_argument("--stock", help="查询个股所属全部概念")
    parser.add_argument("--compare", nargs="+", help="对比多个概念，如 --compare DeepSeek 人工智能")
    parser.add_argument("--api-key", default=API_KEY)
    args = parser.parse_args()

    client = TdxMcpClient(args.api_key)

    if args.concept:
        result = get_concept_stocks(client, args.concept, top_n=args.top, fetch_all=args.all)
        print_concept_stocks(result, args.concept)

    elif args.hot:
        result = get_concept_hot_ranking(client, top_n=args.top)
        print("\n今日热门概念（涨停股统计）")
        result.print_table(["sec_name", "chg", "所属概念"])

    elif args.stock:
        info = get_stock_concepts(client, args.stock)
        print(f"\n{info['name']}（{info['code']}）所属概念（共 {len(info['concepts'])} 个）：")
        for i, c in enumerate(info["concepts"], 1):
            print(f"  {i:3}. {c}")

    elif args.compare:
        summaries = compare_concepts(client, args.compare)
        print_compare(summaries)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
