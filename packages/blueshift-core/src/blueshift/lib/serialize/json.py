from __future__ import annotations
from typing import TYPE_CHECKING, Any
import json
from enum import Enum
import math
import numpy as np

from blueshift.lib.trades._order import Order
from blueshift.lib.trades._trade import Trade
from blueshift.lib.trades._position import Position
from blueshift.interfaces.assets._assets import MarketData
from blueshift.lib.common.constants import Frequency
from blueshift.interfaces.trading.algo_orders import IAlgoOrder

if TYPE_CHECKING:
    import pandas as pd
else:
    import blueshift.lib.common.lazy_pandas as pd

class BlueshiftJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Enum):
            return o.value
        elif isinstance(o, np.integer):
            return int(o)
        elif isinstance(o, pd.Timestamp):
            return str(o)
        elif isinstance(o, pd.DataFrame):
            if all([isinstance(c,MarketData) for c in o.columns]):
                obj = o.copy()
                obj.columns = [c.symbol for c in obj.columns]           # type: ignore
            elif all([isinstance(i,MarketData) for i in o.index]):
                obj = o.copy()
                obj.index = [i.symbol for i in obj.index]               # type: ignore
            elif isinstance(o.index, pd.MultiIndex):
                obj = o.reset_index()
                if 'level_0' in obj.columns and all([isinstance(e,MarketData) for e in obj.level_0]):
                    obj['level_0'] = [e.symbol for e in obj.level_0]
            obj = o.to_json()
            return json.loads(obj)
        elif isinstance(o, pd.Series):
            if all([isinstance(i,MarketData) for i in o.index]):
                obj = o.copy()
                obj.index = [i.symbol for i in obj.index]               # type: ignore
            obj = o.to_json()
            return json.loads(obj)
        elif isinstance(o, MarketData):
            return o.symbol
        elif isinstance(o, (Order, Trade, Position, IAlgoOrder)):
            return o.to_json()
        elif isinstance(o, Frequency):
            return o.period
        elif isinstance(o, set):
            return list(o)
        return json.JSONEncoder.default(self, o)
    
def convert_nan(data:Any):
    """ convert nan and inf in place to null. """
    try:
        if isinstance(data, dict):
            for k, v in data.items():
                if isinstance(v,float):
                    if math.isnan(v) or math.isinf(v):
                        data[k] = None
                else:
                    convert_nan(v)
        elif isinstance(data, list):
            for i, v in enumerate(data):
                if isinstance(v, float):
                    if math.isnan(v) or math.isinf(v):
                        data[i] = None
                else:
                    convert_nan(v)
        else:
            return
    except Exception:
        return