"""检查 Pig 模拟交易后端、服务密钥、账户和权威持仓接口是否可用。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

from scripts.data_pipeline.intraday.java_broker import JavaBrokerConfig, JavaPaperBrokerClient


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a JSON object")
    return dict(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check the Pig paper broker account endpoint")
    parser.add_argument("--config", default="configs/intraday-paper.json")
    args = parser.parse_args(argv)
    path = Path(args.config).expanduser().resolve()
    payload = _mapping(json.loads(path.read_text(encoding="utf-8")), "config")
    config = JavaBrokerConfig.from_mapping(_mapping(payload.get("java_broker"), "java_broker"))
    if not config.enabled:
        raise RuntimeError("java_broker.enabled is false; paper broker health check is not active")
    client = JavaPaperBrokerClient(config)
    state = client.account_state()
    reconciliation = client.reconcile()
    summary = _mapping((state or {}).get("summary"), "account summary")
    if summary.get("mode") != "PAPER":
        raise RuntimeError(f"unexpected account mode: {summary.get('mode')}")
    if not bool((reconciliation or {}).get("healthy")):
        raise RuntimeError(
            "Pig account reconciliation failed: "
            + "; ".join(str(value) for value in (reconciliation or {}).get("issues") or [])
        )
    result = {
        "status": "ok",
        "base_url": config.base_url,
        "account_id": config.account_id,
        "account_status": summary.get("status"),
        "total_equity": summary.get("totalEquity"),
        "positions": len((state or {}).get("positions") or []),
        "reconciliation_healthy": (reconciliation or {}).get("healthy"),
        "reconciliation_issues": (reconciliation or {}).get("issues") or [],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
