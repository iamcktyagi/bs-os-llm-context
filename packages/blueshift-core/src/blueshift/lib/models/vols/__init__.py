from __future__ import annotations
from typing import TYPE_CHECKING, cast
from numpy import exp
import pandas as pd
import numpy as np
import warnings

from blueshift.lib.common.constants import ALMOST_ZERO
from blueshift.lib.exceptions import ModelError

from blueshift.lib.trades._order_types import GREEK
from blueshift.interfaces.assets._assets import OptionType

from ._black_scholes import (
        interpolate_atmf, interpolate_atmfv, bs_price, bs_pricev,
        bs_greek, bs_greekv, bs_implied_vol, bs_implied_volv, 
        piecewise_linear_vol_smile, bs_price_to_strike)
from ._vol_models import VolModel, Volatility, QuadraticVol, SABRVol
from ._vol_dist import implied_dist, realized_dist, dist_pricer

if TYPE_CHECKING:
    from numpy.typing import NDArray

def bs_delta_to_strike(f:float, delta:float, v:float, t:float) -> float:
    """
        Convert a delta to a strike given ATMF (f), vol (v) and time 
        to expiry (t).
    """
    try:
        import scipy.stats as st
        qnorm = st.norm.ppf
    except ImportError:
        raise ImportError(f'scipy is required for this feature. Please install scipy')
    
    if delta < 0:
        option_type=OptionType.PUT
    else:
        option_type=OptionType.CALL
        
    delta = abs(delta)
    if delta < ALMOST_ZERO:
        delta = ALMOST_ZERO
    elif delta > (1-ALMOST_ZERO):
        delta = (1-ALMOST_ZERO)
        
    if option_type==OptionType.CALL:
        k = exp(-qnorm(delta)*v*(t**0.5) + ((v**2/2)*t))
    else:
        k = exp(qnorm(delta)*v*(t**0.5) + ((v**2/2)*t))
        
    return k*f

def bs_delta_to_strikev(F:np.ndarray, delta:float, V:NDArray[np.float64], T:NDArray[np.float64]) -> np.ndarray:
    """
        Convert a delta to a strike given ATMF (F), vol (V) and time 
        to expiry (T). Here F, V and T are numpy 1-D array with equal 
        length. The parameter delta is a number.
    """
    try:
        import scipy.stats as st
        qnorm = st.norm.ppf
    except ImportError:
        raise ImportError(f'scipy is required for this feature. Please install scipy')
    
    if delta < 0:
        option_type=OptionType.PUT
    else:
        option_type=OptionType.CALL
        
    delta = abs(delta)
    if delta < ALMOST_ZERO:
        delta = ALMOST_ZERO
    elif delta > (1-ALMOST_ZERO):
        delta = (1-ALMOST_ZERO)
        
    if option_type==OptionType.CALL:
        K = exp(-qnorm(delta)*V*(T**0.5) + ((V**2/2)*T))
    else:
        K = exp(qnorm(delta)*V*(T**0.5) + ((V**2/2)*T))
        
    return K*F

def calibrate_vol(atmf:float, t:float, strikes:NDArray[np.float64], vols:NDArray[np.float64], 
                  model_type:VolModel|None=None, beta:float=1, cutoff:float=1.0):
    if not model_type:
        model_type = VolModel.QUADRATIC

    model:Volatility|None = None
    
    try:
        if isinstance(model_type, str):
            model_type = VolModel[model_type.upper()]
        model_type = VolModel(model_type)
    except Exception:
        raise ModelError(f'unknown model type specified: {model_type}.')
        
    if isinstance(strikes, pd.Series):
        strikes = cast(NDArray[np.float64], strikes.values)
        
    if isinstance(vols, pd.Series):
        vols = cast(NDArray[np.float64], vols.values)
        
    if not isinstance(strikes, np.ndarray) or not isinstance(vols, np.ndarray):
        msg = f'Strikes and vols must be pandas series or numpy array.'
        raise ModelError(msg)
        
    if len(strikes) != len(vols) or len(vols) < 3:
        msg = f'vols and strikes array should be of equal size and at least '
        msg += f'have three observations.'
        raise ModelError(msg)
        
    if model_type == VolModel.QUADRATIC:
        model = QuadraticVol(beta, cutoff)
        model.calibrate(atmf, t, strikes, vols)
    elif model_type == VolModel.SABR:
        model = SABRVol(beta, cutoff)
        try:
            err = {'divide': 'ignore', 'over': 'ignore', 'under': 'ignore', 'invalid': 'ignore'}
            before = np.seterr(**err) # type: ignore
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=np.ComplexWarning)
                model.calibrate(atmf, t, strikes, vols)
        finally:
            np.seterr(**before) # type: ignore
    else:
        raise ModelError(f'model specification {model} not implemented.')
        
    return model

def get_vol(model:Volatility, strike:float|pd.Series|NDArray[np.float64]):
    if not isinstance(model, Volatility):
        msg = f'Expected a volatility model, got {type(model)}.'
        raise ModelError(msg)
        
    if not model.calibrated:
        raise ModelError(f'Model is not calibrated.')
        
    if isinstance(strike, pd.Series):
        strike = cast(NDArray[np.float64], strike.values)
        return model.get_volv(strike)
    elif isinstance(strike, np.ndarray):
        return model.get_volv(strike)
    else:
        return model.get_vol(strike)

def get_implied_dist(model, n=512, bounds=np.array([-0.2,0.2]), normalize=True, 
                     width=1):
    if not isinstance(model, Volatility):
        msg = f'Expected a volatility model, got {type(model)}.'
        raise ModelError(msg)
        
    if not isinstance(bounds, np.ndarray):
        try:
            bounds=np.array([bounds[0],bounds[1]])
        except Exception:
            msg = f'Illegal bounds, expected a lower and upper percentage.'
            raise ModelError(msg)
        
    try:
        err = {'divide': 'ignore', 'over': 'ignore', 'under': 'ignore', 'invalid': 'ignore'}
        before = np.seterr(**err) # type: ignore
        dist = implied_dist(model, n, bounds, normalize, width)
        return pd.Series(dist[:,1], index=dist[:,0], name='pdf')
    finally:
        np.seterr(**before) # type: ignore

def get_realized_dist(prices, t, n=512, bounds=np.array([-0.2,0.2]), 
                      normalize=True):
    if isinstance(prices, np.ndarray):
        prices = pd.Series(prices)
        
    if not isinstance(prices, pd.Series):
        msg = f'Expected a pandas series or a 1D ndarray of prices.'
        raise ModelError(msg)
        
    if not isinstance(bounds, np.ndarray):
        try:
            bounds=np.array([bounds[0],bounds[1]])
        except Exception:
            msg = f'Illegal bounds, expected a lower and upper percentage.'
            raise ModelError(msg)
        
    rets = prices.pct_change().dropna().values
    try:
        err = {'divide': 'ignore', 'over': 'ignore', 'under': 'ignore', 'invalid': 'ignore'}
        before = np.seterr(**err) # type: ignore
        dist = realized_dist(rets, t, n, bounds, normalize)
    finally:
        np.seterr(**before) # type: ignore
    return pd.Series(dist[:,1], index=dist[:,0], name='pdf')

def price_from_dist(atmf:float, strike:float, dist:pd.DataFrame, option_type=OptionType.CALL, 
                    discount_factor:float=1, bound:float|None=None):
    try:
        from scipy.integrate import IntegrationWarning
    except ImportError:
        raise ImportError(f'scipy is required for this feature. Please install scipy')
    
    if not isinstance(dist, pd.Series):
        msg = f'Expected a pandas series with index as normalized stock '
        msg += 'level and values as probabilities'
        raise ModelError(msg)
        
    try:
        if isinstance(option_type, str):
            option_type = OptionType[option_type.upper()]
        else:
            option_type = OptionType(int(option_type))
    except Exception:
        msg = f'Illegal option type {option_type}.'
        raise ModelError(msg)
        
    if not bound:
        bound = 0
        
    dist_arry = np.vstack([dist.index.values, dist.values]).T
    try:
        err = {'divide': 'ignore', 'over': 'ignore', 'under': 'ignore', 'invalid': 'ignore'}
        before = np.seterr(**err) # type: ignore
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=IntegrationWarning)
            return dist_pricer(
                    atmf, strike, dist_arry, option_type, discount_factor, bound)
    finally:
        np.seterr(**before) # type: ignore


__all__ = ['interpolate_atmf', 
           'interpolate_atmfv', 
           'bs_price', 
           'bs_pricev',
           'bs_greek', 
           'bs_greekv', 
           'bs_implied_vol',
           'bs_implied_volv', 
           'bs_delta_to_strike',
           'bs_delta_to_strikev',
           'bs_price_to_strike',
           'piecewise_linear_vol_smile',
           'GREEK',
           'VolModel',
           'Volatility',
           'QuadraticVol',
           'SABRVol',
           'calibrate_vol',
           'get_vol',
           'get_implied_dist',
           'get_realized_dist',
           'price_from_dist',
           ]

