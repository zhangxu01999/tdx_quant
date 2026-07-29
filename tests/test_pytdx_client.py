"""pytdx 连接器的纯离线测试。"""

from scripts.data_pipeline.connectors.pytdx_client import rotate_hosts


def test_rotate_hosts_distributes_connection_priority() -> None:
    hosts = [
        ("a", "127.0.0.1", 7709),
        ("b", "127.0.0.2", 7709),
        ("c", "127.0.0.3", 7709),
    ]

    assert [item[0] for item in rotate_hosts(hosts, 0)] == ["a", "b", "c"]
    assert [item[0] for item in rotate_hosts(hosts, 1)] == ["b", "c", "a"]
    assert [item[0] for item in rotate_hosts(hosts, 4)] == ["b", "c", "a"]
