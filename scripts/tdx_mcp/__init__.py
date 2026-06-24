"""通达信 MCP（HTTP/SSE 自然语言数据接口）示例脚本。

与 pytdx 历史数据管道互补：MCP 提供通达信问小达的实时自然语言查询，
补 pytdx 拿不到的数据——概念板块、封单/封成比、主力资金流、首次涨停
预警、竞价、北向资金、机构/基金持仓、分析师评级、筹码分布等。

环境：
    pip install httpx
    export TDX_API_KEY=TDX-your-api-key   # 必填，不入仓

用法见 README「5. 通达信 MCP」一节。
"""

from __future__ import annotations

from scripts.tdx_mcp.tdx_client import TdxMcpClient, TdxQueryResult

__all__ = ["TdxMcpClient", "TdxQueryResult"]
