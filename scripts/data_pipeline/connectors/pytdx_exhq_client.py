from __future__ import annotations

from collections.abc import Iterable
from contextlib import contextmanager

DEFAULT_EXHQ_HOSTS = (
    ('ex_default', '106.14.95.149', 7727),
    ('ex_backup_1', '112.74.214.43', 7727),
    ('ex_backup_2', '119.147.86.171', 7727),
    ('ex_backup_3', '61.152.107.141', 7727),
)


def create_exhq_api(**overrides):
    from pytdx.exhq import TdxExHq_API

    options = {
        'heartbeat': True,
        'auto_retry': True,
        'raise_exception': False,
    }
    options.update(overrides)
    return TdxExHq_API(**options)


def normalize_exhq_hosts(hosts: Iterable[tuple[str, str, int]] | None = None) -> list[tuple[str, str, int]]:
    normalized = []
    for name, host, port in (hosts or DEFAULT_EXHQ_HOSTS):
        normalized.append((str(name), str(host), int(port)))
    return normalized


def connect_first_available_exhq(api, hosts: Iterable[tuple[str, str, int]] | None = None) -> tuple[str, str, int]:
    last_error = None
    for name, host, port in normalize_exhq_hosts(hosts):
        try:
            if api.connect(host, port):
                return (name, host, port)
        except Exception as exc:
            last_error = exc
    if last_error is not None:
        raise ConnectionError('No available pytdx exhq hosts') from last_error
    raise ConnectionError('No available pytdx exhq hosts')


@contextmanager
def connected_exhq_session(api, hosts: Iterable[tuple[str, str, int]] | None = None):
    connect_first_available_exhq(api, hosts)
    try:
        yield api
    finally:
        api.disconnect()


def fetch_instrument_info_payload(api, *, start: int, count: int = 500):
    return list(api.get_instrument_info(start, count) or [])


def fetch_instrument_quote_payload(api, *, market: int, code: str):
    return list(api.get_instrument_quote(market, code) or [])
