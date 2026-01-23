# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
cimport numpy as np
import numpy as np

from blueshift.interfaces.trading._simulation cimport ABCChargesModel
from blueshift.lib.trades._order cimport Order

cdef class ChargesModel(ABCChargesModel):
    cdef readonly np.float64_t _charges
    cdef readonly np.float64_t _cap
    cdef readonly np.float64_t _floor
    cdef readonly bool _sell_only
    
    cpdef double calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t commission=*, np.float64_t last_fx=*)
    
cdef class NoCharge(ChargesModel):
    pass