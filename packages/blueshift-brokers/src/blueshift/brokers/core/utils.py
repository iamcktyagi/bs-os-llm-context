from __future__ import annotations
from typing import TYPE_CHECKING
import numpy as np

from blueshift.lib.common.enums import BrokerTables
from blueshift.lib.common.ctx_mgrs import TimeoutRLock
from blueshift.config import LOCK_TIMEOUT

if TYPE_CHECKING:
    import pandas as pd
    from blueshift.calendar.trading_calendar import TradingCalendar
    from blueshift.lib.common.constants import Frequency
else:
    import blueshift.lib.common.lazy_pandas as pd

# Table locks for thread-safe operations


def make_blank_data(cols:list, tz:str) -> pd.DataFrame:
    """Create blank DataFrame with specified columns and timezone."""
    return pd.DataFrame(columns=cols, index=pd.DatetimeIndex([]).tz_localize(tz))

def make_missing_data(fields:list[str], nbars:int, frequency:Frequency, calendar:TradingCalendar):
    idx = calendar.last_n_bars(pd.Timestamp.now(calendar.tz), nbars, frequency)
    idx = pd.DatetimeIndex(idx)
    
    cols:dict[str, pd.Series] = {}

    for f in fields:
        cols[f] = pd.Series(np.nan, index=idx)
    return pd.DataFrame(cols)

__all__ = [
    'make_blank_data',
    'make_missing_data',
]

