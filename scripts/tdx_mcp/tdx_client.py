"""
通达信 MCP 基础客户端

直接通过 HTTP 调用通达信问小达 MCP 服务（无需 Claude），
支持 Streamable HTTP 和 SSE 两种响应格式。

环境变量：
    TDX_API_KEY  — 通达信 API Key，未设置则使用构造函数传入值
"""

import json
import os
import time
from typing import Any

import httpx

MCP_URL = "https://mcp.tdx.com.cn:3001/mcp"
DEFAULT_API_KEY = os.getenv("TDX_API_KEY", "")


class TdxQueryResult:
    """解析后的查询结果"""

    def __init__(self, raw: dict):
        self.raw = raw
        self.code: int = raw.get("meta", {}).get("code", -1)
        self.total: int = raw.get("meta", {}).get("total", 0)
        self.message: str = raw.get("meta", {}).get("message", "")
        self.headers: list[str] = raw.get("headers", [])
        self.data: list[list] = raw.get("data", [])
        self.summary: str = raw.get("summary", "")

    def ok(self) -> bool:
        return self.code == 0

    def to_dicts(self) -> list[dict]:
        """将 data 行转换为字段名 -> 值的字典列表，方便处理"""
        return [dict(zip(self.headers, row)) for row in self.data]

    def print_table(self, cols: list[str] | None = None) -> None:
        """简单的终端表格打印"""
        headers = cols if cols else self.headers
        if not self.data:
            print("（无数据）")
            return
        col_idx = [self.headers.index(h) for h in headers if h in self.headers]
        widths = [max(len(str(h)), max((len(str(row[i])) for row in self.data), default=0))
                  for i, h in zip(col_idx, headers)]
        fmt = "  ".join(f"{{:<{w}}}" for w in widths)
        print(fmt.format(*[self.headers[i] for i in col_idx]))
        print("-" * (sum(widths) + 2 * len(widths)))
        for row in self.data:
            print(fmt.format(*[str(row[i]) for i in col_idx]))
        print(f"\n共 {self.total} 条，本页 {len(self.data)} 条")


class TdxMcpClient:
    """
    通达信 MCP 客户端

    用法：
        client = TdxMcpClient(api_key="TDX-xxx")
        result = client.query("贵州茅台600519最新行情")
        print(result.to_dicts())
    """

    def __init__(self, api_key: str = DEFAULT_API_KEY, timeout: int = 30):
        if not api_key:
            raise ValueError("缺少 TDX_API_KEY，请设置环境变量或传入 api_key 参数")
        self.timeout = timeout
        self._headers = {
            "tdx-api-key": api_key,
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        self._session_id: str | None = None
        self._req_id = 0

    # ------------------------------------------------------------------
    # 内部：JSON-RPC over HTTP
    # ------------------------------------------------------------------

    def _next_id(self) -> int:
        self._req_id += 1
        return self._req_id

    def _post(self, payload: dict) -> dict:
        """发送 JSON-RPC 请求，自动处理 JSON / SSE 两种响应。"""
        headers = dict(self._headers)
        if self._session_id:
            headers["Mcp-Session-Id"] = self._session_id

        with httpx.Client(timeout=self.timeout) as client:
            resp = client.post(MCP_URL, json=payload, headers=headers)
            resp.raise_for_status()

            # 保存 session id（如果服务器返回）
            if "Mcp-Session-Id" in resp.headers:
                self._session_id = resp.headers["Mcp-Session-Id"]

            ct = resp.headers.get("content-type", "")
            if "text/event-stream" in ct:
                return self._parse_sse(resp.text)
            return resp.json()

    @staticmethod
    def _parse_sse(text: str) -> dict:
        """从 SSE 流中提取第一个含 result 的 data 行。"""
        for line in text.splitlines():
            if line.startswith("data: "):
                try:
                    obj = json.loads(line[6:])
                    if "result" in obj or "error" in obj:
                        return obj
                except json.JSONDecodeError:
                    continue
        return {}

    # ------------------------------------------------------------------
    # MCP 握手
    # ------------------------------------------------------------------

    def initialize(self) -> dict:
        """执行 MCP initialize + initialized 通知。"""
        result = self._post({
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "tdx-python-client", "version": "1.0"},
            },
        })
        # initialized 通知（无 id，不需要响应）
        try:
            headers = dict(self._headers)
            if self._session_id:
                headers["Mcp-Session-Id"] = self._session_id
            httpx.post(
                MCP_URL,
                json={"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
                headers=headers,
                timeout=10,
            )
        except Exception:
            pass
        return result

    # ------------------------------------------------------------------
    # 工具调用
    # ------------------------------------------------------------------

    def call_tool(self, name: str, arguments: dict) -> dict:
        """
        直接调用 MCP tool，返回原始响应 dict。
        如果未 initialize，会自动先执行初始化。
        """
        if self._session_id is None:
            self.initialize()

        resp = self._post({
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        })

        # 从 MCP content 数组中提取文本结果
        if "result" in resp:
            for item in resp["result"].get("content", []):
                if item.get("type") == "text":
                    try:
                        return json.loads(item["text"])
                    except json.JSONDecodeError:
                        return {"raw_text": item["text"]}
        if "error" in resp:
            raise RuntimeError(f"MCP 错误: {resp['error']}")
        return resp

    def query(
        self,
        question: str,
        range: str = "AG",
        size: int = 10,
        page: int = 1,
    ) -> TdxQueryResult:
        """
        调用 tdx_wenda_quotes 工具，返回 TdxQueryResult。

        Args:
            question: 自然语言查询（见文档示例）
            range:    市场 — AG(A股) / HK-GP(港股) / JJ(基金) / ZS(指数)
            size:     每页条数（1~100）
            page:     页码（从 1 开始）
        """
        raw = self.call_tool(
            "tdx_wenda_quotes",
            {"question": question, "range": range, "size": size, "page": page},
        )
        return TdxQueryResult(raw)

    def query_all(
        self,
        question: str,
        range: str = "AG",
        page_size: int = 50,
        max_pages: int = 10,
        delay: float = 0.3,
    ) -> TdxQueryResult:
        """
        自动翻页，合并所有结果（最多 max_pages 页）。

        Args:
            delay: 翻页间隔秒数，避免请求过频
        """
        first = self.query(question, range, size=page_size, page=1)
        if not first.ok() or first.total <= page_size:
            return first

        all_data = list(first.data)
        total_pages = min(max_pages, -(-first.total // page_size))  # ceil div

        for p in range(2, total_pages + 1):
            time.sleep(delay)
            page_result = self.query(question, range, size=page_size, page=p)
            if not page_result.ok() or not page_result.data:
                break
            all_data.extend(page_result.data)

        merged = dict(first.raw)
        merged["data"] = all_data
        return TdxQueryResult(merged)


# ------------------------------------------------------------------
# 便利函数：无需实例化
# ------------------------------------------------------------------

_default_client: TdxMcpClient | None = None


def get_client(api_key: str = DEFAULT_API_KEY) -> TdxMcpClient:
    """获取（或创建）全局默认客户端。"""
    global _default_client
    if _default_client is None:
        _default_client = TdxMcpClient(api_key)
    return _default_client


def query(question: str, range: str = "AG", size: int = 10, page: int = 1) -> TdxQueryResult:
    """快捷函数，使用全局客户端查询。"""
    return get_client().query(question, range, size, page)


if __name__ == "__main__":
    import sys

    api_key = os.getenv("TDX_API_KEY", "")
    q = sys.argv[1] if len(sys.argv) > 1 else "贵州茅台600519最新行情"

    client = TdxMcpClient(api_key)
    result = client.query(q)

    if result.ok():
        result.print_table()
    else:
        print(f"查询失败: {result.message}")
