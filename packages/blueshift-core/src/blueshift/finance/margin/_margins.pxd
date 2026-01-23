# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
cimport numpy as np
import numpy as np

from blueshift.interfaces.assets._assets cimport Asset
from blueshift.interfaces.trading._simulation cimport ABCMarginModel
from blueshift.lib.trades._order cimport Order
from blueshift.lib.trades._position cimport Position

cdef class MarginModel(ABCMarginModel):
    cdef readonly object _library
    cdef readonly np.float64_t margins_collected
    cdef readonly dict position_wise_margins
    
    cpdef tuple calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t position, object timestamp, 
                    np.float64_t last_fx=*, np.float64_t underlying_px=*,
                    Position pos=*)
    cpdef double exposure_margin(self, Asset asset, np.float64_t exposure, 
                          Position pos, object timestamp,
                          np.float64_t last_fx=*, np.float64_t underlying_px=*)
    cdef double _calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t position, Position pos, object timestamp,
                    np.float64_t last_fx=*, np.float64_t underlying_px=*)
    cdef double _opt_calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t position, Position pos, object timestamp,
                    np.float64_t last_fx=*, np.float64_t underlying_px=*)
    
cdef class VarMargin(MarginModel):
    cdef readonly np.float64_t _factor
    cdef readonly np.float64_t _floor
    cdef readonly int _lookback
    cdef readonly object _last_dt
    cdef readonly object _vol
    
    cdef double _get_vol(self, Asset asset, object timestamp)