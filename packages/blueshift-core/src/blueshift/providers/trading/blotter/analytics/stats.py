from __future__ import annotations
from typing import TYPE_CHECKING
import warnings

import numpy as np
from blueshift.lib.common.constants import PRECISION as _PRECISION
from blueshift.lib.exceptions import DataException

if TYPE_CHECKING:
    import pandas as pd
else:
    import blueshift.lib.common.lazy_pandas as pd


USE_PYFOLIO=False

def import_empyrical():
    try:
        import empyrical as ep # type:ignore -> optional dependency
        import empyrical.stats as empst # type:ignore -> optional dependency
        return ep, empst
    except Exception:
        from blueshift.lib.common.sentinels import nan_op
        class EMPST():
            pass
        empst = EMPST()
        empst.aggregate_returns = nan_op # type: ignore
        return None, empst
    
def import_pyfolio():
    global USE_PYFOLIO
    
    try:
        import pyfolio.timeseries as pfts # type:ignore -> optional dependency
        import pyfolio.round_trips as pfrt # type:ignore -> optional dependency
        USE_PYFOLIO=True
        return pfts, pfrt
    except ImportError:
        from blueshift.lib.common.sentinels import nan_op
        class Dummy():
            pass
        pfts = Dummy()
        pfrt = Dummy()
        pfts.perf_stats = nan_op # type: ignore
        pfrt.gen_round_trip_stats = nan_op # type: ignore
        USE_PYFOLIO=False
        return pfts, pfrt

def format_timedelta(x:pd.Timedelta):
    hours = minutes = 0
    days = x.days
    seconds = int(x.seconds)
    
    if seconds > 3600:
        hours = int(seconds/3600)
        seconds = int(seconds - hours*3600)
        
    if seconds > 60:
        minutes = int(seconds/60)
        seconds = int(seconds - minutes*60)
        
    msg = ''
    if days:
        msg += f'{days} days '
    msg += f"{hours:02d}:" + f"{minutes:02d}:" + f"{seconds:02d}"
    
    return msg

def update_dtype(x, ndigits:int=4, freq:str='S'):
    try:
        x = float(x)
        if x == int(x):
            return int(x)
        return round(x, ndigits)
    except Exception:
        try:
            dt = pd.Timedelta(x).round(freq)
            return format_timedelta(dt)
        except Exception:
            return x

def compute_eod_point_stats_report(returns:pd.Series, benchmark_returns:pd.Series|None=None, 
                                   period:str='daily', risk_free:float=0.0, 
                                   required_return:float=0.0, var_cutoff:float=0.05,
                                   full:bool=True, min_len:int=5) -> dict:
    """
        Collect a bunch of point stats in a dict and returns.
        
        Args:
            
            ``returns (Series)``: Pandas Series for returns.
            
            ``benchmark_returns (Series)``: Benchmar returns.
            
            ``period (object)``: Periodicity of input returns series.
            
            ``risk_free (float)``; Risk-free rate of returns (annualized).
            
            ``required_return (float)``: Required returns (annualized).
            
            ``var_cutoff (float)``: Threshold for VaR computation.
            
            ``full (bool)``: Toggle between a full and a brief version.
            
            ``min_len (int)``: Min number of points to generate stats.
            
        Returns:
            Dict. The metrics are as below. All are based on daily returns.
            
            :sharpe_ratio: `The Sharpe ratio <https://en.wikipedia.org/wiki/Sharpe_ratio>`_.
            :sortino_ratio: `The Sortino ratio <https://en.wikipedia.org/wiki/Sortino_ratio>`_.
            :calmar_ratio: `The Calmer ratio <https://en.wikipedia.org/wiki/Calmar_ratio>`_.
            :omega_ratio: `The Omega ratio <https://en.wikipedia.org/wiki/Omega_ratio>`_.
            :tail_ratio: The Tail ratio - ratio of right and left tail of returns distribution.
            :value_at_risk: `The VaR measure <https://en.wikipedia.org/wiki/Value_at_risk>`_.
            :stability: Stability of time-series - the R-squared of cumulative returns regressd against time.
    """
    if period!='daily':
        raise DataException('stats analytics can handle only daily frequency.')
    
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = _compute_eod_point_stats_report(
                returns,benchmark_returns, period,risk_free,required_return,
                var_cutoff, full, min_len)
        
    return out

def _compute_eod_point_stats_report(returns, benchmark=None, period='daily', 
                                   risk_free=0.0, required_return=0.0, 
                                   var_cutoff=0.05, full=True, min_len=5):
    ep, empst = import_empyrical()
    out = {}
    
    if len(returns) < min_len:
        out["cagr"] = np.nan
        out["sharpe_ratio"] = np.nan
        
        if benchmark is not None:
            out["alpha"] = np.nan
            out["beta"] = np.nan
        
        if full:
            out["sortino_ratio"] = np.nan
            out["calmar_ratio"] = np.nan
            out["omega_ratio"] = np.nan
            out["stability"] = np.nan
            out["tail_ratio"] = np.nan
            out["value_at_risk"] = np.nan
        
        return out
    
    if ep:
        out["cagr"] = round(ep.cagr(returns), _PRECISION)
        out["sharpe_ratio"] = round(
                ep.sharpe_ratio(returns, risk_free = risk_free),
                _PRECISION)
        if benchmark is not None:
            out["beta"] = round(
                    ep.beta(returns, benchmark),_PRECISION)
            out["alpha"] = round(
                    ep.alpha(returns, benchmark),_PRECISION)
        else:
            out["alpha"] = np.nan
            out["beta"] = np.nan
        
        if full:
            out["sortino_ratio"] = round(
                    ep.sortino_ratio(returns, required_return=required_return),_PRECISION)
            out["calmar_ratio"] = round(
                    ep.calmar_ratio(returns, period=period),_PRECISION)
            out["omega_ratio"] = round(
                    ep.omega_ratio(returns, risk_free=risk_free, 
                                   required_return=required_return),_PRECISION)
            out["stability"] = round(
                    ep.stability_of_timeseries(returns),_PRECISION)
            out["tail_ratio"] = round(ep.tail_ratio(returns), _PRECISION)
            out["value_at_risk"] = round(
                    ep.value_at_risk(returns, cutoff=var_cutoff), _PRECISION)
    
    return out
        
def create_report(perfs:pd.DataFrame, pos=None, txns=None, round_trips=None) -> tuple[dict, pd.DataFrame]:
    ep, empst = import_empyrical()
    pfst, pfrt = import_pyfolio()
    
    if not USE_PYFOLIO:
        return {}, pd.DataFrame()
    
    stats = {}
    monthly = {}
    returns = perfs.algo_returns
    
    if 'benchmark_returns' in perfs.columns:
        benchmark_rets = perfs.benchmark_returns
    else:
        benchmark_rets = None
    
    round_trips = round_trips if round_trips is not None else pd.DataFrame()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            current = np.seterr(all='ignore')
            stats = pfst.perf_stats(returns, factor_returns=benchmark_rets) # type: ignore
            stats = stats.to_dict()
            monthly = empst.aggregate_returns(returns,'monthly') # type: ignore
            monthly = monthly.to_json()
            
            if isinstance(round_trips, pd.DataFrame) and not round_trips.empty:
                round_trips = pfrt.gen_round_trip_stats(round_trips) # type: ignore
                round_trips = pd.concat([round_trips['summary'], 
                                         round_trips['pnl'], 
                                         round_trips['duration'], 
                                         round_trips['returns']])
                
                round_trips = round_trips.map(update_dtype)
        finally:
            np.seterr(**current) # type:ignore
    
    stats = {'stats':stats,
            'monthly':monthly}
    
    return stats, round_trips
    

    