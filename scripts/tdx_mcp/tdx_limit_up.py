"""
通达信涨停板追踪工具

追踪今日涨停、连板梯队、板块集中度，适合做涨停板策略的投资者。

用法：
    # 今日全部涨停股（含原因）
    python -m scripts.tdx_mcp.tdx_limit_up

    # 只看连板 >= 2 的
    python -m scripts.tdx_mcp.tdx_limit_up --min-boards 2

    # 按概念分组统计
    python -m scripts.tdx_mcp.tdx_limit_up --by-concept

    # 一字板（未开板）筛选
    python -m scripts.tdx_mcp.tdx_limit_up --unbroken

    # 导出 JSON
    python -m scripts.tdx_mcp.tdx_limit_up --json
"""

import argparse
import json
import os
import re
import sys
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from scripts.tdx_mcp.tdx_client import TdxMcpClient, TdxQueryResult

API_KEY = os.getenv("TDX_API_KEY", "")


def fetch_limit_up(client: TdxMcpClient, top_n: int = 100) -> TdxQueryResult:
    """获取今日涨停股列表（含封单、原因、连板数）。"""
    return client.query(
        "今日涨停股列表 封单金额 首次涨停时间 涨停原因 连续涨停天数 板型 封成比",
        size=top_n,
    )


def fetch_multi_boards(client: TdxMcpClient, min_boards: int = 2) -> TdxQueryResult:
    """获取连板股（>= min_boards 板）。"""
    return client.query(
        f"连续涨停 {min_boards}板以上 封单金额 涨停原因 连板天数",
        size=50,
    )


def fetch_limit_down(client: TdxMcpClient) -> TdxQueryResult:
    """今日跌停股。"""
    return client.query("今日跌停股列表 跌停原因", size=50)


def parse_boards(row: dict) -> int:
    """从行数据中提取连板天数（字段名不固定，做容错处理）。"""
    for k, v in row.items():
        if "连续涨停" in k or "连板" in k or "几板" in k:
            try:
                return int(float(str(v)))
            except (ValueError, TypeError):
                pass
    return 1


def parse_concept(row: dict) -> list[str]:
    """提取所属概念列表。"""
    raw = row.get("所属通达信风格", row.get("所属概念", ""))
    return [
        c.replace("@", "").strip()
        for c in re.split(r"[;；【】]", raw)
        if c.replace("@", "").strip()
    ]


def filter_rows(rows: list[dict], min_boards: int = 1, unbroken: bool = False) -> list[dict]:
    filtered = []
    for row in rows:
        boards = parse_boards(row)
        if boards < min_boards:
            continue
        if unbroken:
            board_type = str(row.get("板型", "")).lower()
            # 一字板 / 未开板的封成比通常很高
            if "一字" not in board_type and "未开" not in board_type:
                # 通过封成比判断（封成比 > 5 视为强封）
                try:
                    seal_ratio = float(row.get("封成比", 0))
                    if seal_ratio < 5:
                        continue
                except (ValueError, TypeError):
                    continue
        filtered.append(row)
    return filtered


def print_limit_up_table(rows: list[dict]) -> None:
    fmt = "{:<10} {:<8} {:<6} {:<6} {:<12} {:<8} {}"
    print(fmt.format("股票名称", "代码", "现价", "涨跌%", "封单额(万)", "连板数", "涨停原因"))
    print("-" * 80)
    for row in rows:
        name = row.get("sec_name", "")[:10]
        code = row.get("sec_code", "")
        price = row.get("now_price", "")
        chg = row.get("chg", row.get("chg0#", ""))
        seal = row.get("涨停最大封单额(万)", row.get("封单金额", ""))
        try:
            seal = f"{float(seal)/10000:.0f}" if seal else ""
        except (ValueError, TypeError):
            seal = str(seal)
        boards = parse_boards(row)
        reason = str(row.get("涨停原因", ""))[:40]
        print(fmt.format(name, code, price, chg, seal, boards, reason))


def print_concept_summary(rows: list[dict]) -> None:
    counter: Counter = Counter()
    for row in rows:
        for c in parse_concept(row):
            counter[c] += 1

    print("\n概念板块涨停集中度（今日）")
    print("-" * 40)
    for concept, count in counter.most_common(20):
        bar = "█" * min(count, 20)
        print(f"  {concept:<16} {count:>3}只  {bar}")


def print_ladder(rows: list[dict]) -> None:
    """打印连板梯队分布。"""
    ladder: Counter = Counter()
    for row in rows:
        b = parse_boards(row)
        ladder[b] += 1

    print("\n连板梯队分布")
    print("-" * 30)
    for boards in sorted(ladder.keys(), reverse=True):
        label = f"{boards}板"
        count = ladder[boards]
        bar = "█" * min(count, 30)
        print(f"  {label:<6} {count:>3}只  {bar}")


def main():
    parser = argparse.ArgumentParser(description="通达信涨停板追踪")
    parser.add_argument("--min-boards", type=int, default=1, help="最低连板数（默认 1）")
    parser.add_argument("--unbroken", action="store_true", help="只看未开板（强封）")
    parser.add_argument("--by-concept", action="store_true", help="按概念分组统计")
    parser.add_argument("--ladder", action="store_true", help="显示连板梯队")
    parser.add_argument("--limit-down", action="store_true", help="同时显示跌停股")
    parser.add_argument("--top", type=int, default=50, help="最多显示条数")
    parser.add_argument("--json", action="store_true", help="输出原始 JSON")
    parser.add_argument("--api-key", default=API_KEY)
    args = parser.parse_args()

    client = TdxMcpClient(args.api_key)

    print("正在获取涨停数据…")
    result = fetch_limit_up(client, top_n=args.top)

    if not result.ok():
        print(f"查询失败: {result.message}")
        sys.exit(1)

    rows = result.to_dicts()
    filtered = filter_rows(rows, min_boards=args.min_boards, unbroken=args.unbroken)

    if args.json:
        print(json.dumps(filtered, ensure_ascii=False, indent=2))
        return

    print(f"\n今日涨停  共 {result.total} 只  "
          f"{'（已过滤：' + str(len(filtered)) + '只）' if len(filtered) < len(rows) else ''}")

    print_limit_up_table(filtered[:args.top])

    if args.ladder or args.min_boards > 1:
        print_ladder(rows)

    if args.by_concept:
        print_concept_summary(filtered)

    if args.limit_down:
        print("\n正在获取跌停数据…")
        ld = fetch_limit_down(client)
        if ld.ok() and ld.data:
            print(f"\n今日跌停  共 {ld.total} 只")
            print_limit_up_table(ld.to_dicts()[:20])


if __name__ == "__main__":
    main()
