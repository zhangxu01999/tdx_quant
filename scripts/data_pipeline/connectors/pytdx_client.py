from __future__ import annotations

from collections.abc import Iterable
from contextlib import contextmanager

DEFAULT_HOSTS = (
    ('default', '119.147.212.81', 7709),
    ('server_1', '115.238.56.198', 7709),
    ('server_2', '115.238.90.165', 7709),
    ('server_3', '180.153.18.170', 7709),
)


def create_hq_api(**overrides):
    from pytdx.hq import TdxHq_API

    options = {
        'heartbeat': True,
        'auto_retry': True,
        'raise_exception': False,
    }
    options.update(overrides)
    return TdxHq_API(**options)


def normalize_hosts(hosts: Iterable[tuple[str, str, int]] | None = None) -> list[tuple[str, str, int]]:
    normalized = []
    for name, host, port in (hosts or DEFAULT_HOSTS):
        normalized.append((str(name), str(host), int(port)))
    return normalized


def connect_first_available(api, hosts: Iterable[tuple[str, str, int]] | None = None) -> tuple[str, str, int]:
    last_error = None
    for name, host, port in normalize_hosts(hosts):
        try:
            if api.connect(host, port):
                return (name, host, port)
        except Exception as exc:
            last_error = exc
    if last_error is not None:
        raise ConnectionError('No available pytdx hosts') from last_error
    raise ConnectionError('No available pytdx hosts')


@contextmanager
def connected_session(api, hosts: Iterable[tuple[str, str, int]] | None = None):
    connect_first_available(api, hosts)
    try:
        yield api
    finally:
        api.disconnect()


def fetch_quotes_payload(api, symbols):
    return list(api.get_security_quotes(symbols) or [])


def fetch_bars_payload(api, *, category: int, market: int, code: str, start: int = 0, count: int = 800):
    return list(api.get_security_bars(category, market, code, start, count) or [])


def fetch_xdxr_payload(api, *, market: int, code: str):
    return list(api.get_xdxr_info(market, code) or [])


def fetch_security_count_payload(api, market: int) -> int:
    return int(api.get_security_count(market) or 0)


def fetch_security_list_payload(api, *, market: int, start: int):
    # get_security_list returns up to 1000 rows per call; it takes no count arg.
    return list(api.get_security_list(market, start) or [])


def fetch_transaction_payload(api, *, market: int, code: str, start: int, count: int):
    return list(api.get_transaction_data(market, code, start, count) or [])


def fetch_history_transaction_payload(
    api, *, market: int, code: str, start: int, count: int, date: int
):
    return list(api.get_history_transaction_data(market, code, start, count, date) or [])


def fetch_company_info_category_payload(api, *, market: int, code: str):
    return list(api.get_company_info_category(market, code) or [])


def fetch_company_info_content_payload(
    api, *, market: int, code: str, filename: str, start: int, length: int
) -> str:
    # get_company_info_content returns the section TEXT (not a list).
    return api.get_company_info_content(market, code, filename, start, length) or ''


def fetch_minute_time_payload(api, *, market: int, code: str):
    return list(api.get_minute_time_data(market, code) or [])


def fetch_history_minute_time_payload(api, *, market: int, code: str, date: int):
    return list(api.get_history_minute_time_data(market, code, date) or [])


def fetch_finance_payload(api, *, market: int, code: str) -> dict:
    # get_finance_info returns a dict (股本结构 snapshot), not a list.
    return dict(api.get_finance_info(market, code) or {})


def fetch_index_bars_payload(api, *, category: int, market: int, code: str, start: int = 0, count: int = 800):
    return list(api.get_index_bars(category, market, code, start, count) or [])
