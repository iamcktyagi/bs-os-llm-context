# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
cimport numpy as np
import numpy as np

from blueshift.interfaces.assets._assets cimport Asset

cpdef enum ExitMethod:
    PRICE = 0
    MOVE = 1
    PERCENT = 2
    AMOUNT = 3
    PNL = 4

cdef class IExitHandler:
    cpdef reset_cooloff(self, object context, Asset asset)
    cpdef reset_stoploss(self, object context, Asset asset)
    cpdef reset_takeprofit(self, object context, Asset asset)
    cpdef add_stoploss(self, object context, Asset asset, 
                       ExitMethod method, np.float64_t target, 
                       object callback=*, np.float64_t trailing=*,
                       bool do_exit=*, np.float64_t entry_level=*,
                       bool rolling=*)
    cpdef add_takeprofit(self, object context, Asset asset, 
                       ExitMethod method, np.float64_t target, 
                       object callback=*, bool do_exit=*, 
                       np.float64_t entry_level=*,bool rolling=*)
    cpdef set_cooloff(self, object assets=*, bool squareoff=*, bool is_global=*)
    cpdef bool is_empty(self, object context=*)
    cpdef check_exits(self, bool reset=*)