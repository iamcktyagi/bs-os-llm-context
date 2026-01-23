from __future__ import annotations
from typing import TYPE_CHECKING, Any
from enum import Enum

if TYPE_CHECKING:
    import numpy as np
    from numpy.typing import NDArray
    from blueshift.interfaces.assets._assets import OptionType
    from blueshift.lib.trades._order_types import GREEK


# scalar atmf interpolation
def interpolate_atmf(futures:float, spot:float, t:float, T:float, tick:float=0.05, 
                     precision:int=2) -> float:...

# vectorized atmf interpolation
def interpolate_atmfv(futures:NDArray[np.float64], spot:NDArray[np.float64], 
                      t:NDArray[np.float64], T:NDArray[np.float64], 
                      tick:float=0.05, precision:int=2) -> NDArray[np.float64]:...

# scalar black scholes pricing function
def bs_price(F:float, K:float, v:float, T:float, option_type:OptionType) -> float:...

# vectorized black scholes pricing function
def bs_pricev(F:NDArray[np.float64], K:NDArray[np.float64], v:NDArray[np.float64], 
              T:NDArray[np.float64], option_type:OptionType) -> NDArray[np.float64]:...

# scalar option greeks function
def bs_greek(F:float, K:float, v:float, T:float, option_type:OptionType, greek:GREEK) -> float:...

# vectorized option greeks function
def bs_greekv(F:NDArray[np.float64], K:NDArray[np.float64], v:NDArray[np.float64], 
              T:NDArray[np.float64], option_type:OptionType, greek:GREEK) -> NDArray[np.float64]:...

# scalar black scholes implied vol function
def bs_implied_vol(F:float, K:float, price:float, T:float, option_type:OptionType, 
                   precision:float=1.0e-4, early_return:bool=False):...

# vectorized black scholes implied vol function
def bs_implied_volv(F:NDArray[np.float64], K:NDArray[np.float64], price:NDArray[np.float64], 
                    T:NDArray[np.float64], option_type:OptionType, precision:float=1.0e-4, 
                    early_return:bool=False) -> NDArray[np.float64]:...

# piece-wise linear vol smile interpolation
def piecewise_linear_vol_smile(
        F:float, K:float, strikes:NDArray[np.float64], vols:NDArray[np.float64]) -> float:...

# scalar black scholes price to strike
def bs_price_to_strike(F:float, price:float, V:float, T:float, option_type:OptionType, 
                       precision:float=1.0e-2, early_return:bool=False) -> float:...