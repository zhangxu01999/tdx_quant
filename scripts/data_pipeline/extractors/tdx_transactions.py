from __future__ import annotations

import pandas as pd

from scripts.data_pipeline.code_mapping import market_code_to_ts_code

# HQ "transaction data" is 分笔成交 (minute-resolution, ``HH:MM``), not
# second-level 逐笔 (that is ExHQ). ``buyorsell`` observed values:
#   0=买盘(buy)  1=卖盘(sell)  2=中性(neutral)  8=零量标记(other, vol==0)
BUYORSELL_LABEL = {0: 'buy', 1: 'sell', 2: 'neutral'}

TRANSACTION_COLUMNS = [
    'time',
    'price',
    'vol',
    'num',
    'buyorsell',
    'buyorsell_label',
    'market',
    'code',
    'ts_code',
    'trade_date',
]


def _buyorsell_label(value) -> str:
    try:
        return BUYORSELL_LABEL.get(int(value), 'other')
    except (TypeError, ValueError):
        return 'other'


def transactions_to_dataframe(payload, *, market: int, code: str, trade_date: str) -> pd.DataFrame:
    """Normalize a 分笔成交 payload (today or history) into a typed DataFrame.

    History rows are ``{time, price, vol, buyorsell}``; today adds ``num`` —
    missing ``num`` is filled NA so both schemas share one column set. Zero-vol
    rows (``buyorsell`` marker values like 8) are KEPT, labelled ``'other'``.
    """
    df = pd.DataFrame(list(payload))
    if df.empty:
        return pd.DataFrame(columns=TRANSACTION_COLUMNS)

    df['market'] = int(market)
    df['code'] = str(code)
    df['ts_code'] = market_code_to_ts_code(int(market), str(code))
    df['trade_date'] = str(trade_date)
    if 'num' not in df.columns:
        df['num'] = pd.NA
    df['buyorsell_label'] = df['buyorsell'].map(_buyorsell_label)
    return df[TRANSACTION_COLUMNS]
