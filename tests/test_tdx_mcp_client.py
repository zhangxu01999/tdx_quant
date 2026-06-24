"""通达信 MCP 客户端纯逻辑离线测试（不触网、不需 TDX_API_KEY）。"""

from __future__ import annotations

import pytest

from scripts.tdx_mcp.tdx_client import TdxMcpClient, TdxQueryResult
from scripts.tdx_mcp.tdx_data_enricher import (
    _find_field,
    _parse_concepts,
    _to_float,
    _to_int,
)
from scripts.tdx_mcp.tdx_limit_up import parse_boards, parse_concept


# --------------------------------------------------------------------------
# TdxQueryResult
# --------------------------------------------------------------------------

def _sample_raw() -> dict:
    return {
        "meta": {"code": 0, "total": 2, "message": ""},
        "headers": ["sec_name", "now_price"],
        "data": [["贵州茅台", "1500.0"], ["五粮液", "200.0"]],
        "summary": "ok",
    }


def test_query_result_ok_and_fields() -> None:
    r = TdxQueryResult(_sample_raw())
    assert r.ok() is True
    assert r.total == 2
    assert r.headers == ["sec_name", "now_price"]
    assert len(r.data) == 2
    assert r.summary == "ok"


def test_query_result_to_dicts_zips_headers() -> None:
    r = TdxQueryResult(_sample_raw())
    assert r.to_dicts() == [
        {"sec_name": "贵州茅台", "now_price": "1500.0"},
        {"sec_name": "五粮液", "now_price": "200.0"},
    ]


def test_query_result_not_ok_when_code_nonzero() -> None:
    r = TdxQueryResult({"meta": {"code": 1, "message": "bad question"}})
    assert r.ok() is False
    assert r.message == "bad question"


def test_query_result_defaults_on_empty_raw() -> None:
    r = TdxQueryResult({})
    assert r.ok() is False
    assert r.code == -1
    assert r.total == 0
    assert r.headers == []
    assert r.data == []
    assert r.to_dicts() == []


def test_query_result_print_table_empty(capfd: pytest.CaptureFixture[str]) -> None:
    TdxQueryResult({"meta": {"code": 0}, "headers": ["a"], "data": []}).print_table()
    out, _ = capfd.readouterr()
    assert "无数据" in out


# --------------------------------------------------------------------------
# TdxMcpClient._parse_sse
# --------------------------------------------------------------------------

def test_parse_sse_extracts_result_line() -> None:
    text = 'event: message\ndata: {"jsonrpc":"2.0","result":{"x":1}}\n\n'
    assert TdxMcpClient._parse_sse(text) == {"jsonrpc": "2.0", "result": {"x": 1}}


def test_parse_sse_extracts_error_line() -> None:
    text = 'data: {"error":{"code":-1}}\n'
    assert TdxMcpClient._parse_sse(text) == {"error": {"code": -1}}


def test_parse_sse_returns_empty_when_no_result_or_error() -> None:
    # data 行存在但既无 result 也无 error
    assert TdxMcpClient._parse_sse('data: {"hello":1}\n') == {}
    # 完全不含 data 行
    assert TdxMcpClient._parse_sse("event: ping\n") == {}


def test_parse_sse_skips_malformed_lines() -> None:
    # 第一行非 JSON 应跳过，取到第二行
    text = 'data: not-json\ndata: {"result": 42}\n'
    assert TdxMcpClient._parse_sse(text) == {"result": 42}


# --------------------------------------------------------------------------
# 缺 key 客户端构造即报错
# --------------------------------------------------------------------------

def test_client_raises_without_api_key() -> None:
    with pytest.raises(ValueError, match="TDX_API_KEY"):
        TdxMcpClient("")


# --------------------------------------------------------------------------
# enricher 辅助函数
# --------------------------------------------------------------------------

def test_find_field_matches_substring_with_date_suffix() -> None:
    row = {"主力净流入(万元)\n2026.06.21": "28450.3", "sec_name": "人形机器人"}
    assert _find_field(row, "主力净流入") == "28450.3"


def test_find_field_missing_returns_empty() -> None:
    assert _find_field({"sec_name": "茅台"}, "主力净流入") == ""


def test_find_field_none_value_returns_empty() -> None:
    assert _find_field({"a": None}, "a") == ""


@pytest.mark.parametrize("raw, expected", [
    ("1.5", 1.5),
    ("0", 0.0),
    ("", None),
    ("abc", None),
])
def test_to_float(raw: str, expected) -> None:
    assert _to_float(raw) == expected


@pytest.mark.parametrize("raw, expected", [
    ("3.0", 3),
    ("12", 12),
    ("", None),
    ("x", None),
])
def test_to_int(raw: str, expected) -> None:
    assert _to_int(raw) == expected


def test_parse_concepts_strips_markup() -> None:
    assert _parse_concepts("【@DeepSeek@】;【@人工智能@】") == ["DeepSeek", "人工智能"]
    assert _parse_concepts("") == []


# --------------------------------------------------------------------------
# limit_up 解析器
# --------------------------------------------------------------------------

def test_parse_boards_from_varying_field_names() -> None:
    assert parse_boards({"连续涨停天数": "3"}) == 3
    assert parse_boards({"连板": "5"}) == 5
    assert parse_boards({"几板": "2"}) == 2


def test_parse_boards_fallback_to_one() -> None:
    # 无连板字段时默认 1 板
    assert parse_boards({"sec_name": "茅台", "now_price": "1500"}) == 1
    # 无效值也回退 1
    assert parse_boards({"连续涨停天数": "N/A"}) == 1


def test_parse_concept_extracts_list() -> None:
    out = parse_concept({"所属概念": "【@DeepSeek@】;【@人工智能@】"})
    assert out == ["DeepSeek", "人工智能"]
    # 字段缺失 → 空列表
    assert parse_concept({}) == []
