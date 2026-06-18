from __future__ import annotations


def create_pro_api():
    try:
        from demo.db_utils import DatabaseUtils
    except ImportError:
        from db_utils import DatabaseUtils

    return DatabaseUtils.init_tushare_api()


def fetch_financial_statements_payload(pro, *, api_method: str = 'balancesheet_vip', period: str):
    api = getattr(pro, api_method)
    data = api(period=period)
    if hasattr(data, 'to_dict'):
        return data.to_dict(orient='records')
    return list(data or [])
