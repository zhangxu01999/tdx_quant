from __future__ import annotations

import pandas as pd

from scripts.data_pipeline.code_mapping import market_code_to_ts_code

SECURITY_LIST_COLUMNS = [
    "code",
    "name",
    "pre_close",
    "decimal_point",
    "volunit",
    "market",
    "ts_code",
]


def security_list_to_dataframe(payload, *, market: int) -> pd.DataFrame:
    """Normalize a ``get_security_list`` page into a typed DataFrame.

    Each raw row is ``{code, volunit, decimal_point, name, pre_close}``; we add
    the integer ``market`` and the ``ts_code`` (``<code>.SZ|.SH``).
    """
    df = pd.DataFrame(list(payload))
    if df.empty:
        return pd.DataFrame(columns=SECURITY_LIST_COLUMNS)
    df["market"] = int(market)
    df["ts_code"] = df.apply(
        lambda row: market_code_to_ts_code(int(row["market"]), str(row["code"])), axis=1
    )
    return df[SECURITY_LIST_COLUMNS]
