def market_code_to_ts_code(market: int, code: str) -> str:
    suffix = 'SZ' if market == 0 else 'SH'
    return f'{code}.{suffix}'
