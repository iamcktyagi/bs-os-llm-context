# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
from cpython cimport bool

cimport cython
import numpy as np
cimport numpy as np

cdef class TradingClock:
    """
        The implementation of trading clock that synchronizes algorithm. The
        emit frequency controls the ticks. For backtest, the clock issues 
        a series of nanosecond timestamps at every tick implied from the 
        start and end datte, depending on the opening and closing defined 
        in the calendar supplied. For live trading, this is extended to use
        an async queue to put the ticks (instead of streaming synchronously).
        
        Args:
            ``trading_calendar (object)``: The calendar object.
            
            ``emit_frequency (int)``: Tick frequency in minutes.
    """
    cdef readonly object trading_calendar
    cdef public int emit_frequency
    cdef public bool is_terminated
    cdef readonly np.int64_t open_nano
    cdef readonly np.int64_t close_nano
    cdef readonly np.int64_t before_trading_start_nano
    cdef readonly np.int64_t after_trading_hours_nano
    cdef readonly np.int64_t[:] intraday_nanos
    cdef readonly generate_intraday_nanos(self)
    cpdef readonly terminate(self)
    
cdef class SimulationClock(TradingClock):
    """
        Similation clock for backtest, derived from TradingClock.
        
        Args:
            ``trading_calendar (object)``: The calendar object.
            
            ``emit_frequency (int)``: Tick frequency in minutes.
            
            ``start_dt (Timestamp)``: Algo start date
            
            ``end_dt (Timestamp)``: Algo end date
    """
    cdef readonly np.int64_t start_nano
    cdef readonly np.int64_t end_nano
    cdef readonly np.int64_t[:] session_nanos
    
    cdef readonly bool init_emitted
    cdef readonly bool bts_emitted
    cdef readonly bool ath_emitted
    cdef readonly bool end_emitted
    cdef readonly int last_loc
    
    cdef _reset_clock(self)