"""Offline unit tests for the F10 财务分析 parser (no network).

The fixture mirrors the real full-width-pipe table layout captured from pytdx.
"""
from __future__ import annotations

import math

import pytest

from scripts.data_pipeline.extractors.tdx_company_info import (
    extract_updated_date,
    parse_finance_indicators,
)

# realistic slice of the 财务分析 section (｜ = U+FF5C, CRLF line endings)
_FIXTURE = (
    "☆财务分析☆ ◇000001 平安银行 更新日期：2026-05-05◇ 港澳资讯 灵通V9.0\r\n"
    "★本栏包括【1.财务指标】【2.报表摘要】【3.异动科目】【4.环比分析】★\r\n"
    "\r\n"
    "【1.财务指标】\r\n"
    "【主要财务指标】\r\n"
    "┌───────────┬───────┬───────┬───────┐\r\n"
    "｜财务指标              ｜    2026-03-31｜    2025-12-31｜    2024-12-31｜\r\n"
    "├───────────┼───────┼───────┼───────┤\r\n"
    "｜审计意见              ｜      未经审计｜标准无保留意见｜标准无保留意见｜\r\n"
    "├───────────┼───────┼───────┼───────┤\r\n"
    "｜净利润(元)            ｜      145.23亿｜      426.33亿｜      445.08亿｜\r\n"
    "｜基本每股收益(元)      ｜          0.67｜          2.07｜          2.15｜\r\n"
    "｜每股净资产(元)        ｜         23.91｜         23.25｜         21.89｜\r\n"
    "｜营业毛利率            ｜       71.9109｜       69.9738｜       71.3276｜\r\n"
    "├───────────┼───────┼───────┼───────┤\r\n"
    # a SECOND sub-table (单季/single-quarter) follows with its own date header
    # and repeats the same metric names — must NOT be merged into table 1.
    "｜财务指标              ｜    2026-03-31｜    2025-12-31｜    2025-09-30｜\r\n"
    "├───────────┼───────┼───────┼───────┤\r\n"
    "｜基本每股收益(元)      ｜          0.67｜          2.07｜          1.87｜\r\n"
    "├───────────┼───────┼───────┼───────┤\r\n"
    "【2.报表摘要】\r\n"
)


def test_extract_updated_date() -> None:
    assert extract_updated_date(_FIXTURE) == "2026-05-05"
    assert extract_updated_date("no date here") is None


def test_parse_finance_indicators_stops_at_second_subtable() -> None:
    df = parse_finance_indicators(_FIXTURE, market=0, code="000001")

    # only table-1 (累计) periods emitted; 单季-only period 2025-09-30 excluded
    assert "2025-09-30" not in set(df["period"])
    # no (metric, period) duplicates — 基本每股收益 appears once per period
    eps = df[df["metric"] == "基本每股收益(元)"]
    assert len(eps) == eps["period"].nunique()
    # the 单季 value 1.87 must NOT leak in (its period column is excluded)
    assert not (
        (df["metric"] == "基本每股收益(元)") & (df["value_num"] == pytest.approx(1.87))
    ).any()


def test_parse_finance_indicators_long_format() -> None:
    df = parse_finance_indicators(_FIXTURE, market=0, code="000001")

    assert {"ts_code", "metric", "period", "value_raw", "value_num"} <= set(df.columns)
    assert (df["ts_code"] == "000001.SZ").all()
    assert set(df["period"]) == {"2026-03-31", "2025-12-31", "2024-12-31"}

    def get(metric: str, period: str):
        row = df[(df["metric"] == metric) & (df["period"] == period)].iloc[0]
        return row["value_raw"], row["value_num"]

    raw, num = get("基本每股收益(元)", "2026-03-31")
    assert raw == "0.67"
    assert num == pytest.approx(0.67)

    raw, num = get("净利润(元)", "2026-03-31")
    assert raw == "145.23亿"
    assert num == pytest.approx(145.23e8)

    raw, num = get("营业毛利率", "2025-12-31")
    assert num == pytest.approx(69.9738)

    # text-valued metric → value_num NaN, value_raw preserved
    raw, num = get("审计意见", "2026-03-31")
    assert raw == "未经审计"
    assert math.isnan(num)


# --------------------------------------------------------------------------- #
# job: orchestrate category lookup + section fetch + parse + persist
# --------------------------------------------------------------------------- #
def test_run_company_finance_job(tmp_path) -> None:
    from scripts.data_pipeline.jobs.company_info_job import run_company_finance_job

    cats = [{"name": "财务分析", "filename": "000001.txt", "start": 22069, "length": 52509}]

    def fetch_category() -> list[dict]:
        return cats

    def fetch_content(filename: str, start: int, length: int) -> str:
        return _FIXTURE

    result = run_company_finance_job(
        fetch_category=fetch_category,
        fetch_content=fetch_content,
        market=0,
        code="000001",
        output_root=tmp_path,
    )

    assert result["status"] == "success"
    assert result["ts_code"] == "000001.SZ"
    assert result["asof_date"] == "2026-05-05"  # parsed from 更新日期
    assert result["records"] > 0

    # structured indicators persisted per-symbol; EPS metric present
    import pandas as pd

    back = pd.read_parquet(tmp_path / "company_finance" / "ts_code=000001.SZ" / "data.parquet")
    assert "基本每股收益(元)" in set(back["metric"])

    # raw 财务分析 text persisted alongside for re-parsing
    raw = pd.read_parquet(tmp_path / "company_info_raw" / "ts_code=000001.SZ" / "data.parquet")
    assert (raw["section"] == "财务分析").all()
    assert raw.iloc[0]["text"].startswith("☆财务分析☆")
