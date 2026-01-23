# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
cimport numpy as np
import numpy as np

from blueshift.interfaces.assets._assets cimport Asset
from blueshift.lib.trades._position cimport Position
from blueshift.lib.trades._accounts cimport BlotterAccount
from blueshift.interfaces.trading._exit_handler cimport IExitHandler, ExitMethod

cpdef enum ExitType:
    STOPLOSS=0
    TAKEPROFIT=1

cdef class ExitDef:
    cdef readonly object context
    cdef readonly ExitType type
    cdef readonly Asset asset
    cdef readonly ExitMethod method
    cdef readonly np.float64_t target
    cdef readonly bool trailing
    cdef readonly np.float64_t entry
    cdef readonly object callback
    cdef readonly bool do_exit
    cdef readonly bool rolling
    cdef readonly np.float64_t trailing_fraction
    
cdef class ExitHandler(IExitHandler):
    cdef readonly object _algo
    cdef readonly object _handlers
    cdef readonly object _ctx_handlers
    cdef readonly object _entry_cooloff
    cdef readonly object _ctx_entry_cooloff
    cdef readonly object _squareoff_cooloff
    cdef readonly object _ctx_squareoff_cooloff
    cdef readonly np.float64_t _cooloff_period
    cdef readonly bool _global_squareoff_cooloff
    
    cpdef set_cooloff_period(self, np.float64_t cooloff)
    cpdef reset_cooloff(self, object context, Asset asset)
    cpdef reset(self, bool roll=*)
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
    cpdef get_stoploss(self, object context, Asset asset)
    cpdef get_takeprofit(self, object context, Asset asset)
    cpdef copy_stoploss(self, Asset old, Asset new)
    cpdef copy_takeprofit(self, Asset old, Asset new)
    cpdef set_cooloff(self, object assets=*, bool squareoff=*, bool is_global=*)
    cpdef bool is_empty(self, object context=*)
    cpdef check_exits(self, bool reset=*)
    cdef _update_trailing_target_strategy(self, ExitDef sl, BlotterAccount acct)
    cdef _check_strategy_exits_in_context(self, object context)
    cdef _check_exits_in_context(self, object context)
    cdef _update_trailing_target_asset(self, ExitDef sl, Position pos)
    cdef _check_sl(self, ExitDef sl, Position pos)
    cdef _check_tp(self, ExitDef tp, Position pos)
    cdef bool _check_if_empty_context(self, object context)
    
    
    