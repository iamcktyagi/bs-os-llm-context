# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
cimport numpy as np
import numpy as np

from blueshift.interfaces.trading._simulation cimport ABCSlippageModel
from blueshift.interfaces.assets._assets cimport Asset
from blueshift.lib.trades._order cimport Order

cdef class SlippageModel(ABCSlippageModel):
    cdef readonly object _freq
    cdef readonly object _library
    cpdef tuple simulate(self, Order order, object dt)
    
cdef class OrderBookSlippage(SlippageModel):
    pass

cdef class BarDataSlippage(SlippageModel):
    cdef readonly np.float64_t _max_volume
    
    cdef _calculate(self, Order order, object dt)