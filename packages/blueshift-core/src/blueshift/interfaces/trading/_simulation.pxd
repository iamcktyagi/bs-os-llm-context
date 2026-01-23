# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
cimport numpy as np
import numpy as np

from blueshift.interfaces.assets._assets cimport Asset
from blueshift.lib.trades._order cimport Order
from blueshift.lib.trades._position cimport Position

cdef class ABCSlippageModel:
    """ trade simulation model interface. """
    cpdef tuple simulate(self, Order order, object dt)

cdef class ABCChargesModel:
    """ non-brokerarge charges (e.g. regulatory charges or taxes). """
    cpdef double calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t commission=*, np.float64_t last_fx=*)

cdef class ABCCostModel:
    """ cost model interface  for calculating trading costs. """
    cpdef tuple calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t last_fx=*)
    cdef double _calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t last_fx=*)
    cpdef tuple rollcost(self, object positions)
    
cdef class ABCMarginModel:
    """ interface face for trading and position margin model. """
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