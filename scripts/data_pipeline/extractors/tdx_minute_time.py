from __future__ import annotations

import pandas as pd

from scripts.data_pipeline.code_mapping import market_code_to_ts_code

MINUTE_TIME_COLUMNS = ['minute_idx', 'price', 'vol', 'ts_code', 'trade_date']


def minute_time_to_dataframe(payload, *, market: int, code: str, trade_date: str) -> pd.DataFrame:
    """Normalize a 分时 payload (one ``{price, vol}`` point per session minute)
    into a typed DataFrame with a 0-based ``minute_idx``.

    The source carries no timestamp; ``minute_idx`` orders the session points
    rather than fabricating an uncertain time-of-day mapping.
    """
    df = pd.DataFrame(list(payload))
    if df.empty:
        return pd.DataFrame(columns=MINUTE_TIME_COLUMNS)
    df['minute_idx'] = range(len(df))
    df['ts_code'] = market_code_to_ts_code(int(market), str(code))
    df['trade_date'] = str(trade_date)
    return df[MINUTE_TIME_COLUMNS]
