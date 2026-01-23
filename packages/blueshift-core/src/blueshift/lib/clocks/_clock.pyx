# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
from cpython cimport bool
import numpy as np
cimport numpy as np

from ..exceptions.exceptions import ClockError
from ..common.constants import NANO_SECOND

cpdef enum BARS:
    ALGO_START = 0
    BEFORE_TRADING_START = 1
    TRADING_BAR = 2
    DAY_END = 3
    AFTER_TRADING_HOURS = 4
    ALGO_END = 5
    HEART_BEAT = 6
    
cdef class TradingClock:
    
    def __init__(self, trading_calendar, emit_frequency):
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
        from blueshift.calendar.trading_calendar import TradingCalendar
        if not isinstance(trading_calendar, TradingCalendar):
            raise ClockError(f'Clock expected a blueshift calendar, got {type(trading_calendar)}.')

        self.trading_calendar = trading_calendar
        self.emit_frequency = emit_frequency
        
        open_time = self.trading_calendar.open_time
        close_time = self.trading_calendar.close_time
        
        self.open_nano = (((open_time.hour*60 + open_time.minute)*60\
                  + open_time.second)*1000000 + 
                    open_time.microsecond)*1000
        self.close_nano = (((close_time.hour*60 + close_time.minute)*60\
                      + close_time.second)*1000000 + 
                        close_time.microsecond)*1000
        
        self.before_trading_start_nano = self.open_nano \
                                            - 1800*NANO_SECOND
        self.after_trading_hours_nano = self.close_nano \
                                            + 300*NANO_SECOND
        self.generate_intraday_nanos()
        self.is_terminated = False
    
    def __iter__(self):
        raise StopIteration
        
    cdef generate_intraday_nanos(self):
        cdef int n
        cdef np.int64_t period, open_nano
        cdef Py_ssize_t size
        
        if self.close_nano < self.open_nano:
            # offset open nano by a full day.
            open_nano = self.open_nano - 24*3600*NANO_SECOND
        else:
            open_nano = self.open_nano
        
        n= int((self.close_nano - open_nano)/NANO_SECOND/60/self.emit_frequency)
        
        period = self.emit_frequency*60*NANO_SECOND
        self.intraday_nanos = np.asarray(
                [open_nano + i*period for i in range(n+1)])

        size = len(self.intraday_nanos)
        if self.intraday_nanos[size-1] >= self.close_nano:
            self.intraday_nanos = self.intraday_nanos[:(size-1)]
            
    cpdef terminate(self):
        self.is_terminated = True

    def reset(self, *args, **kwargs):
        pass
        
cdef class SimulationClock(TradingClock):
    
    def __init__(self, object trading_calendar,
                           int emit_frequency,
                           object start_dt,
                           object end_dt):
        """
            Similation clock for backtest, derived from TradingClock.
            
            Args:
                ``trading_calendar (object)``: The calendar object.
                
                ``emit_frequency (int)``: Tick frequency in minutes.
                
                ``start_dt (Timestamp)``: Algo start date
                
                ``end_dt (Timestamp)``: Algo end date
        """
        cdef Py_ssize_t size
        super(SimulationClock,self).__init__(trading_calendar,
             emit_frequency)
        
        if start_dt > end_dt:
            msg = "start date must be less than end date."
            raise ClockError(msg=msg)
        
        sessions = trading_calendar.sessions(start_dt, end_dt)
        self.session_nanos = np.asarray([s.value for s in sessions])
        size = len(self.session_nanos)
        self.start_nano = self.session_nanos[0]
        self.end_nano = self.session_nanos[size-1] + self.after_trading_hours_nano
        
        self.init_emitted = False
        self.bts_emitted = False
        self.ath_emitted = False
        self.end_emitted = False
        self.last_loc = 0
        
    def __iter__(self):
        cdef Py_ssize_t size, size_intraday
        cdef np.int64_t session_nano, first_day, last_day, t
        cdef np.int64_t pos_session_nano, first_trading_nano
        cdef bool bts_triggered = False
        
        size = len(self.session_nanos)
        size_intraday = len(self.intraday_nanos)
        first_day = self.session_nanos[0]
        last_day = self.session_nanos[size-1]
        
        yield first_day, BARS.ALGO_START
        
        for session_nano in self.session_nanos:
            if self.is_terminated:
                break
            
            bts_triggered = False
            
            if self.before_trading_start_nano < self.close_nano:
                bts_triggered = True
                t = session_nano+self.before_trading_start_nano
                yield t, BARS.BEFORE_TRADING_START
            elif session_nano == first_day:
                yield session_nano, BARS.BEFORE_TRADING_START
            
            day_end_nano = self.intraday_nanos[size_intraday-1]
            for intraday_nano in self.intraday_nanos:
                if session_nano == first_day and intraday_nano < 0:
                    # make sure we start after midnight on first day.
                    continue
                
                if intraday_nano == day_end_nano:
                    yield session_nano+intraday_nano, BARS.DAY_END
                else:
                    yield session_nano+intraday_nano, BARS.TRADING_BAR
            
            bts_nano = session_nano + self.before_trading_start_nano
            ath_nano = session_nano+self.after_trading_hours_nano
            
            if bts_triggered:
                yield ath_nano, BARS.AFTER_TRADING_HOURS
            else:
                if bts_nano < ath_nano:
                    yield bts_nano, BARS.BEFORE_TRADING_START
                    yield ath_nano, BARS.AFTER_TRADING_HOURS
                else:
                    yield ath_nano, BARS.AFTER_TRADING_HOURS
                    if session_nano == last_day:
                        break
                    yield bts_nano, BARS.BEFORE_TRADING_START
        
        yield ath_nano, ALGO_END
        
    def emit(self, np.int64_t dt):
        cdef Py_ssize_t size
        cdef np.int64_t session_nano, bts_nano, ath_nano
        cdef int loc

        size = len(self.intraday_nanos)
        
        if dt < self.start_nano:
            return
        
        if dt >= self.end_nano or self.is_terminated:
            if self.init_emitted:
                if self.bts_emitted and not self.ath_emitted:
                    yield self.end_nano, BARS.AFTER_TRADING_HOURS
                if not self.end_emitted:
                    yield self.end_nano, BARS.ALGO_END
            self._reset_clock()
            return
                
        
        if not self.init_emitted:
            self.init_emitted = True
            yield self.start_nano, BARS.ALGO_START
        
        loc = np.searchsorted(self.session_nanos, dt)
        if loc == 0:
            # we must have emitted init already
            return
        
        loc = loc-1        
        if loc < self.last_loc:
            msg = "attempt to go back in time."
            raise ClockError(msg)
            
        if loc != self.last_loc:
            # we have a change of day, make sure the bts and ath are
            # emitted properly
            if self.bts_emitted and not self.ath_emitted:
                ath_nano = self.session_nanos[self.last_loc] + \
                                self.after_trading_hours_nano
                yield ath_nano, BARS.AFTER_TRADING_HOURS
            self.bts_emitted = False
            self.ath_emitted = False
            self.last_loc = loc
        
        session_nano = dt - self.session_nanos[loc]
        if not self.bts_emitted and \
            session_nano >= self.before_trading_start_nano:
            self.bts_emitted = True
            bts_nano = self.session_nanos[self.last_loc] + \
                        self.before_trading_start_nano
            yield bts_nano, BARS.BEFORE_TRADING_START
            
        if session_nano >= self.intraday_nanos[0] and\
            session_nano <= self.intraday_nanos[size-1]:
            yield dt, BARS.TRADING_BAR
            return
            
        if not self.ath_emitted and \
            session_nano >= self.after_trading_hours_nano:
            self.ath_emitted = True
            ath_nano = self.session_nanos[loc] + \
                                self.after_trading_hours_nano
            yield ath_nano, BARS.AFTER_TRADING_HOURS
        
        yield dt, BARS.HEART_BEAT
        
    cdef _reset_clock(self):
        self.ath_emitted = False
        self.bts_emitted = False
        self.end_emitted = False
        self.init_emitted = False
        self.last_loc = 0
        
    def reset(self, *args, **kwargs):
        self.ath_emitted = False
        self.bts_emitted = False
        self.end_emitted = False
        self.init_emitted = False
        self.last_loc = 0
    
    def __str__(self):
        tz = self.trading_calendar.tz
        return f"Blueshift Simulation Clock [tick:{self.emit_frequency},tz:{tz}]"
                    
    def __repr__(self):
        return self.__str__()
        
