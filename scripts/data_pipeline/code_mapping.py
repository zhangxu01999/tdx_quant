def market_code_to_ts_code(market: int, code: str) -> str:
    suffix = 'SZ' if market == 0 else 'SH'
    return f'{code}.{suffix}'


def hq_market_label(market: int) -> str:
    """HQ market id (0=SZ, 1=SH) to exchange label."""
    return 'SZ' if int(market) == 0 else 'SH'
