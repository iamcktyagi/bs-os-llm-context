from __future__ import annotations
from typing import TYPE_CHECKING, Any
from enum import Enum

if TYPE_CHECKING:
    import numpy as np
    from numpy.typing import NDArray

class VolModel(Enum):
    UNDEFINED = 0,
    LINNEAR = 1,
    QUADRATIC = 2,
    SABR = 3

class Volatility:
    atmf:float
    atmv:float
    t:float
    beta:float
    vol_cutoff:float
    rmse:float
    calibrated:float
    model:VolModel
    
    def __init__(self, beta:float=1.0, vol_cutoff:float=0.0):...
    def calibrate(self, atmf:float, t:float, strikes:NDArray[np.float64], vols:NDArray[np.float64]):...
    def get_vol(self, strike:float) -> float:...
    def get_volv(self, strikes:NDArray[np.float64]) -> NDArray[np.float64]:...

class QuadraticVol(Volatility):
    convexity:float
    skew:float
    model:VolModel = VolModel.QUADRATIC

    def update(self, params:dict[str, Any]):...

class SABRVol(Volatility):
    alpha:float
    rho:float
    nu:float
    bounds:list[tuple[float,float]]

    def __init__(self, beta:float=1.0, vol_cutoff:float=0.0, 
                 bounds:list[tuple[float,float]]=[(-1, 1), (0, 100)]):...
    def update(self, params:dict[str, Any]):...