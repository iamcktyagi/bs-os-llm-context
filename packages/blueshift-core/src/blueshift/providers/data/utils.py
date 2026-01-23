from __future__ import annotations
import numpy as np
from typing import TYPE_CHECKING
    
from blueshift.lib.common.constants import Frequency
from blueshift.lib.exceptions import (
        ValidationError, DataStoreException)
from blueshift.calendar.trading_calendar import TradingCalendar
from blueshift.calendar.date_utils import filter_for_session

if TYPE_CHECKING:
    import pandas as pd
else:
    import blueshift.lib.common.lazy_pandas as pd

_agg_func_map = {
        'close':lambda x:x.iloc[-1],
        'open':lambda x:x.iloc[0],
        'high':lambda x:np.nanmax(x),
        'low':lambda x:np.nanmin(x),
        'volume':lambda x:np.nansum(x),
        'open_interest':lambda x:x.iloc[-1],
        'first':lambda x:x.iloc[0],
        'last':lambda x:x.iloc[-1],
        'max':lambda x:np.nanmax(x),
        'min':lambda x:np.nanmin(x),
        'mean':lambda x:np.nanmean(x),
        'sum':lambda x:np.nansum(x),
        'median':lambda x:np.nanmedian(x),
        }

def _get_resampler(df:pd.Series|pd.DataFrame, period:str|Frequency):
    try:
        freq = Frequency(period)
    except Exception:
        raise DataStoreException(f'Illegal period {period}.')
        
    if freq == Frequency('D'):
        label = closed = "left"
    else:
        label = closed = "right"
        
    level = None
    if isinstance(df.index, pd.MultiIndex):
        level = 1 # datetime index is the second level
        
    try:
        resampler = df.resample(freq.period, label=label, closed=closed,
                                level=level)
    except Exception as e:
        msg = "failed to resample or aggregate data: "
        msg = msg + str(e)
        raise DataStoreException(msg)
    else:
        return resampler
    
def _align(x, agg_func='last'):
    if x is None or len(x)==0:
        return np.nan
    
    func = None
    if isinstance(x.name, str):
        func = _agg_func_map.get(x.name.lower(), None)
    else:
        func = agg_func
        
    if not callable(func):
        func = _agg_func_map.get(agg_func, lambda x:x.iloc[-1])
    
    try:
        return func(x)
    except Exception:
        return np.nan

def resample(df:pd.Series|pd.DataFrame, period:str|Frequency, fill_value:float|int|None=None, 
             fill_method:str="ffill", limit:int|None=None, colname:str|None=None, 
             calendar:TradingCalendar|None=None) -> pd.Series|pd.DataFrame:
    """
        Function to resample pandas dataframe or series to
        different periods.
    """
    try:
        freq = Frequency(period)
    except Exception:
        msg = f"Illegal period, period {period} not supported."
        raise ValidationError(msg)
        
    if isinstance(df, pd.DataFrame) or isinstance(df, pd.Series):
        # we are good.
        pass
    else:
        msg = "input data not dataframe or series."
        raise ValidationError(msg)
    
    try:
        resampler = _get_resampler(df, freq)
        out = resampler.apply(_align, agg_func=colname)
        
        if fill_value is not None:
            out = out.fillna(fill_value, limit=limit) # type: ignore
        elif fill_method=='ffill':
            out = out.ffill(limit=limit)
        elif fill_method=='bfill':
            out = out.bfill(limit=limit)
        else:
            msg = f'failed to resample: unknown method {fill_value}'
            raise DataStoreException(msg)
    except Exception as e:
        msg = "failed to resample data, "
        msg = msg + str(e)
        raise DataStoreException(msg)
    else:
        if calendar and period < Frequency('D'):
            try:
                out = filter_for_session(out, period, calendar)
            except Exception as e:
                msg = "failed to resample, filter for sessions failed, "
                msg = msg + str(e)
                raise DataStoreException(msg)
        
        return out
    
def subset(x:pd.DataFrame|pd.Series, n:int) -> pd.DataFrame|pd.Series:
    if not isinstance(x.index, pd.MultiIndex):
        return x[-n:]
    
    assets = x.index.get_level_values(0).unique()
    data = {}
    for asset in assets:
        df = x.xs(asset)
        data[asset] = df[-2:]
        
    return pd.concat(data)