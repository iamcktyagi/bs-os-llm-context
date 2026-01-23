# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
cimport numpy as np
import numpy as np

from blueshift.interfaces.assets._assets cimport Asset
from blueshift.lib.common.sentinels import noop

cdef class IExitHandler:
    def set_cooloff_period(self, np.float64_t cooloff):
        raise NotImplementedError
    
    def get_cooloff_period(self):
        raise NotImplementedError
    
    def reset(self, bool roll=True):
        raise NotImplementedError
    
    def remove_context(self, object ctx):
        raise NotImplementedError

    def validate_cooloff(self, Asset asset):
        raise NotImplementedError

    def validate_squareoff(self, Asset asset):
        raise NotImplementedError

    def get_stoploss(self, object context, Asset asset):
        raise NotImplementedError

    def get_takeprofit(self, object context, Asset asset):
        raise NotImplementedError

    def copy_stoploss(self, Asset old, Asset new):
        raise NotImplementedError

    def copy_takeprofit(self, Asset old, Asset new):
        raise NotImplementedError

    cpdef reset_cooloff(self, object context, Asset asset):
        raise NotImplementedError

    cpdef reset_stoploss(self, object context, Asset asset):
        raise NotImplementedError

    cpdef reset_takeprofit(self, object context, Asset asset):
        raise NotImplementedError

    cpdef add_stoploss(self, object context, Asset asset, 
                       ExitMethod method, np.float64_t target, 
                       object callback=noop, np.float64_t trailing=0,
                       bool do_exit=True, np.float64_t entry_level=np.nan,
                       bool rolling=False):
        raise NotImplementedError
        
    cpdef add_takeprofit(self, object context, Asset asset, 
                       ExitMethod method, np.float64_t target, 
                       object callback=noop, bool do_exit=True, 
                       np.float64_t entry_level=np.nan,bool rolling=False):
        raise NotImplementedError

    cpdef set_cooloff(self, object assets=None, bool squareoff=False, bool is_global=False):
        raise NotImplementedError

    cpdef bool is_empty(self, object context=None):
        raise NotImplementedError

    cpdef check_exits(self, bool reset=False):
        raise NotImplementedError