from __future__ import annotations
from typing import TYPE_CHECKING, Literal
import numpy as np
from datetime import datetime, time

from blueshift.config import BLUESHIFT_CALENDAR_START
from blueshift.lib.common.constants import Frequency, NANO_SECOND as NANO
from .holidays import Holiday, holidays_range
from blueshift.lib.exceptions import (
        SessionOutofRange, InvalidDate, BlueshiftCalendarException)
from .date_utils import get_aligned_timestamp
from .date_utils import (
        get_expiries as _get_expiries, valid_timezone, make_consistent_tz
        )

if TYPE_CHECKING:
    import pandas as pd
else:
    import blueshift.lib.common.lazy_pandas as pd

# defaults for calendar creations
START_DATE = None
END_DATE = None
OPEN_TIME = (10,0,0)
CLOSE_TIME = (16,0,0)

def _init_start_dt():
    global START_DATE
    if START_DATE is None:
        START_DATE = pd.Timestamp(BLUESHIFT_CALENDAR_START)

def _init_end_dt():
    global END_DATE
    if END_DATE is None:
        END_DATE = pd.Timestamp(datetime.now().date()) + pd.Timedelta(weeks=52)

def np_search(array, value):
    '''
        return index position if a match if found, else -1
    '''
    matches = np.where(array==np.int64(value))[0]
    if len(matches)>0:
        return matches[0]
    return -1


def days_to_nano(start:pd.Timestamp|str|None=None, end:pd.Timestamp|str|None=None, 
                 dts:pd.DatetimeIndex|list[pd.Timestamp]|None=None, tz:str|None=None, 
                 weekends:list[int]|None=None, 
                 special_sessions:list[str]|None=None) -> np.ndarray:
    '''
        convert a list of bizdays to a list of sessions in nanos
    '''
    dtsn = []
    if not start:
        _init_start_dt()
        start = START_DATE
    if not end:
        _init_end_dt()
        end = END_DATE

    if weekends is None:
        weekends = []
        
    # TODO: optimize this function
    if dts is None:
        start = make_consistent_tz(start, tz)   # type: ignore
        end = make_consistent_tz(end, tz)       # type: ignore
        n_days = (end - start).days + 1
        dts = [start + pd.Timedelta(days=i) for i
               in range(n_days)
               if (start + pd.Timedelta(days=i)).weekday() not in weekends]
    else:
        dts = [pd.Timestamp(dt.date(), tz=tz) for dt in dts
               if dt.weekday() not in weekends]
        
    if special_sessions:
        special_sessions = [make_consistent_tz(dt, tz) for dt in special_sessions]  # type: ignore
        specials = [pd.Timestamp(dt.date(), tz=tz) for dt in special_sessions]      # type: ignore
        dts = dts + specials
        
    try:
        try:
            dts = pd.DatetimeIndex(dts)
        except Exception:
            dts = pd.DatetimeIndex(pd.to_datetime(dts, utc=True))
            
        dts = dts.sort_values()
        
        if dts.tz is None: # type: ignore
            dts = dts.tz_localize(tz)   # type: ignore
        else:
            dts = dts.tz_convert(tz)    # type: ignore
        dts = dts.normalize()
        dtsn = [dt.value for dt in dts]
    except Exception as e:
        raise(e)

    return np.asarray(dtsn)

def date_to_nano(dt:str|pd.Timestamp, tz:str|None) -> int:
    '''
        Convert the datetime to the given timezone and returns the
        nanosecond since the epoch
    '''
    dt = make_consistent_tz(dt, tz)
    return dt.value

def date_to_nano_midnight(dt:str|pd.Timestamp, tz:str|None) -> int:
    '''
        Convert the datetime to the given timezone =, normalize and
        returns the nanosecond since the epoch
    '''
    dt = make_consistent_tz(dt, tz)
    dt = dt.normalize()
    return dt.value

class TradingCalendar:
    '''
        A trading calendar maintains the trading sessions for a given
        trading venue (e.g. an exchange like NYSE). It has an 
        associated timezone. It tracks sessions opening/  
        closing times and holidays. Strategy can access it via the 
        context variable as :attr:`.AlgoContext.trading_calendar`.
        
        .. seealso::
            See documentation on the :ref:`Context Object`.
        
        
    '''
    def __init__(self, name:str, tz:str='Etc/UTC', opens:time|tuple=OPEN_TIME,
                 closes:time|tuple=CLOSE_TIME, start:pd.Timestamp|str|None=None, 
                 end:pd.Timestamp|str|None=None, bizdays:pd.DatetimeIndex|list[pd.Timestamp]|None=None, 
                 weekends:list[int]=[5,6], holidays:list[Holiday]|None=None,
                 special_sessions:pd.DatetimeIndex|list[pd.Timestamp]|None=None):
        """
            Args:
                `name (str)`: Name of the calendar.
                
                `tz (str)`: Timezone for the calendar.
                
                `opens (time or tuple of h,m,s)`: Opening time.
                
                `closes (time or tuple of h,m,s)`: Closing time.
                
                `bizdays (list of dates)`: A list of dates for business days.
                
                `weekends (list of tuple)`: List of weekends day of week.
                
                `holidays (list of holidays)`: A list of holidays objects.
                
                `special_sessions (list of dates)`: A list of special sessions.
        """
        self._name = name
        
        if not valid_timezone(tz):
            raise BlueshiftCalendarException('Timezone is not valid.')
        self._tz = tz
        
        self._saved_bizdays = bizdays
        self._saved_holidays = holidays
        self._saved_start = pd.Timestamp(start) if start else start
        self._saved_end = pd.Timestamp(end) if end else end
        self._weekends = weekends
        self._special_sessions = special_sessions
        self.__bizdays = None
        self.__sessions = None
        self.__opens = None
        self.__closes = None
        
        if isinstance(opens, time):
            open_time = opens
        else:
            try:
                open_time = time(*opens)
            except Exception:
                raise BlueshiftCalendarException('open time is not valid.')
        self._open_nano = (open_time.hour*60 + open_time.minute)*60*NANO
        self._open_time = open_time
        
        if isinstance(closes, time):
            close_time = closes
        else:
            try:
                close_time = time(*closes)
            except Exception:
                raise BlueshiftCalendarException('closing time is not valid.')
        self._close_nano = (close_time.hour*60 + close_time.minute)*60*NANO
        self._close_time = close_time

    @property
    def name(self) -> str:
        """ name (``str``) of the trading calendar. """
        return self._name

    @property
    def tz(self) -> str:
        """ time-zone (``str``) of the trading calendar. """
        return self._tz
    
    @property
    def start(self) -> pd.Timestamp:
        # return a 60 day offset, else bump dates and is holiday
        # go into recursion
        offset = min(int(len(self.bizdays)*0.02), 60) # type: ignore
        return pd.Timestamp(self.bizdays[offset], tz=self.tz) # type: ignore
    
    @property
    def end(self) -> pd.Timestamp:
        # return a 60 day offset, else bump dates and is holiday
        # go into recursion
        offset = min(int(len(self.bizdays)*0.02), 60) # type: ignore
        return pd.Timestamp(self.bizdays[-offset], tz=self.tz) # type: ignore
    
    @property
    def all_sessions(self) -> pd.DatetimeIndex:
        if self.__sessions is None:
            self.__sessions = pd.to_datetime(
                self.bizdays).tz_localize('Etc/UTC').tz_convert(self.tz) # type: ignore
        return pd.DatetimeIndex(self.__sessions)
    
    @property
    def opens(self) -> pd.DatetimeIndex:
        if self.__opens is None:
            self.__opens = self.bizdays + self._open_nano # type: ignore
            self.__opens = pd.to_datetime(self.__opens).\
                tz_localize('Etc/UTC').tz_convert(self.tz)
                
        return pd.DatetimeIndex(self.__opens)
                
    @property
    def closes(self) -> pd.DatetimeIndex:
        if self.__closes is None:
            self.__closes = self.bizdays + self._close_nano # type: ignore
            self.__closes = pd.to_datetime(self.__closes).\
                tz_localize('Etc/UTC').tz_convert(self.tz)
                
        return pd.DatetimeIndex(self.__closes)

    @property
    def open_time(self) -> time:
        """ session opening time in ``datetime.time`` format. """
        return self._open_time

    @property
    def close_time(self) -> time:
        """ session closing time in ``datetime.time`` format. """
        return self._close_time
    
    @property
    def minutes_per_day(self) -> int:
        """ total number of minutes (``int``) in a trading session. """
        opens = self.open_time
        closes = self.close_time
    
        opens_minute = opens.hour*60 + opens.minute
        closes_minute = closes.hour*60 + closes.minute
    
        if closes_minute > opens_minute:
            return closes_minute - opens_minute
        else:
            return 24*60 - (opens_minute - closes_minute)
        
    @property
    def bizdays(self) -> np.ndarray:
        if self.__bizdays is None:
            self.__initialize()
            
        return self.__bizdays # type: ignore

    def __str__(self):
        return "Trading Calendar [name:%s, timezone:%s]" % (self.name,
                                                       self.tz)

    def __repr__(self):
        return self.__str__()
    
    def to_timestamp(self, dt:pd.Timestamp|str) -> pd.Timestamp:
        return make_consistent_tz(dt, self.tz)

    def is_holiday(self, dt) -> bool:
        '''
            check (``bool``) if it is a holiday, given a timestamp.
        '''
        return not self.is_session(dt)

    def is_session(self, dt:pd.Timestamp|str) -> bool:
        '''
            check (``bool``) if it is a trading day, given a timestamp.
        '''
        if isinstance(dt, str):
            dt = pd.Timestamp(dt)
            
        dt = self.to_timestamp(dt)
        dtm = date_to_nano_midnight(dt,self._tz)
        if dtm > self.bizdays[-1] or dtm < self.bizdays[0]:
            raise SessionOutofRange(dt=dt)
        
        if np_search(self.bizdays,dtm) > 0:
            return True
        
        return False

    def is_open(self, dt:pd.Timestamp|str) -> bool:
        '''
            check (``bool``) for if a trading session is in progress, given a 
            timestamp.
        '''
        if isinstance(dt, str):
            dt = pd.Timestamp(dt)
            
        dt = self.to_timestamp(dt)
        dtn = date_to_nano(dt,self._tz)
        dtnm = date_to_nano_midnight(dt, self._tz)
        
        if self.is_session(dt):
            nanos = dtn - dtnm
            if self._open_nano < self._close_nano:
                if nanos >= self._open_nano and nanos <= self._close_nano:
                    return True
            else:
                if nanos <= self._close_nano:
                    return True
                next_dt = dt.normalize() + pd.Timedelta(days=1)
                if nanos >= self._open_nano and self.is_session(next_dt):
                    return True
        elif self._close_nano < self._open_nano:
            next_dt = dt + pd.Timedelta(days=1)
            if self.is_session(next_dt):
                dtn = date_to_nano(next_dt,self._tz)
                dtnm = date_to_nano_midnight(next_dt, self._tz)
                nanos = dtn - dtnm
                if nanos >= self._open_nano:
                    return True
                
        return False

    def next_open(self, dt:pd.Timestamp|str) -> pd.Timestamp:
        '''
            returns next open session time (``pandas.Timestamp``), 
            given a timestamp.
        '''
        current_dt = self.next_session(dt)
        current_dt = pd.Timestamp(current_dt.value + self._open_nano, tz=self.tz)
        
        if self._close_nano > self._open_nano:
            return current_dt
        
        return current_dt - pd.Timedelta(days=1)

    def previous_open(self, dt:pd.Timestamp|str) -> pd.Timestamp:
        '''
            returns previous open session time (``pandas.Timestamp``)
            , given a timestamp.
        '''
        current_dt = self.previous_session(dt)
        current_dt = pd.Timestamp(current_dt.value + self._open_nano, tz=self.tz)
        
        if self._close_nano > self._open_nano:
            return current_dt
        
        return current_dt - pd.Timedelta(days=1)
            
    def to_open(self, dt:pd.Timestamp|str) -> pd.Timestamp:
        '''
            returns current open session time (``pandas.Timestamp``)
            , given a timestamp. Returns the next session open if the market is 
            closed.
        '''
        current_dt = self.current_session(dt)
        current_dt = pd.Timestamp(current_dt.value + self._open_nano, tz=self.tz)
        
        if self._close_nano > self._open_nano:
            return current_dt
        
        return current_dt - pd.Timedelta(days=1)

    def next_close(self, dt:pd.Timestamp|str) -> pd.Timestamp:
        '''
            returns next close session time (``pandas.Timestamp``), 
            given a timestamp.
        '''
        next_dt = self.next_session(dt)
        return pd.Timestamp(next_dt.value + self._close_nano, tz=self.tz)

    def previous_close(self, dt:pd.Timestamp|str) -> pd.Timestamp:
        '''
            returns previous close session time (``pandas.Timestamp``).
        '''
        prev_dt = self.previous_session(dt)
        return pd.Timestamp(prev_dt.value + self._close_nano, tz=self.tz)
            
    def to_close(self, dt:pd.Timestamp|str) -> pd.Timestamp:
        '''
            returns the close time of the current session(``pandas.Timestamp``)
            , given a timestamp. Returns the next session open if the market is 
            closed.
        '''
        current_dt = self.current_session(dt)
        return pd.Timestamp(current_dt.value + self._close_nano, tz=self.tz)

    def sessions(self, start_dt:str|pd.Timestamp, end_dt:str|pd.Timestamp, 
                 convert:bool=True) -> pd.DatetimeIndex|list[pd.Timestamp]:
        '''
            list all valid sessions between dates, inclusive 
            (``pandas.DatetimeIndex``).
        '''
        if isinstance(start_dt, str):
            start_dt = pd.Timestamp(start_dt)
        if isinstance(end_dt, str):
            end_dt = pd.Timestamp(end_dt)
        
        if start_dt > end_dt:
            msg = "start date must be less than end date."
            raise InvalidDate(msg)

        dt1 = date_to_nano_midnight(start_dt,self._tz)
        dt2 = date_to_nano_midnight(end_dt,self._tz)
        idx1 = np.searchsorted(self.bizdays,dt1)
        idx2 = np.searchsorted(self.bizdays,dt2)
        
        if idx2 == len(self.bizdays):
            raise SessionOutofRange(dt=end_dt)
        
        if self.bizdays[idx2] ==  dt2:
            idx2 = idx2 + 1
        idx2 = max(idx2, idx1+1)
        dts = np.array(self.bizdays[idx1:idx2])
        dts = dts[dts>=dt1]
        dts = dts[dts<=dt2]

        if convert:
            dts = pd.to_datetime(dts).\
                    tz_localize('Etc/UTC').tz_convert(self._tz)
            return pd.DatetimeIndex(dts)
        else:
            return dts.tolist()
        
    def current_session(self, dt:pd.Timestamp|str) -> pd.Timestamp:
        """ 
            returns the current session. Returns next if not a trading 
            session.
        """
        if isinstance(dt, str):
            dt = pd.Timestamp(dt)
            
        dt = self.to_timestamp(dt)
        dtn = date_to_nano_midnight(dt,self._tz)
        idx = np.searchsorted(self.bizdays,dtn)
        
        if self._close_nano > self._open_nano:
            if idx < len(self.bizdays):
                return pd.Timestamp(self.bizdays[idx], tz=self._tz)
            raise SessionOutofRange(dt=dt)
        
        # for regular calendar the session is the same date, 
        # but for extended calendar, the session can be the next
        # day if current time is greater than open time, else the 
        # same day
        if self.bizdays[idx]==dtn and dt.time() > self.open_time:
            idx = idx + 1
                
        if idx < len(self.bizdays):
            next_dt = pd.Timestamp(
                    self.bizdays[idx], tz=self._tz)
            return next_dt
            
        raise SessionOutofRange(dt=dt)
        
    def next_session(self, dt:pd.Timestamp|str) -> pd.Timestamp:
        """ returns the next session. """
        if isinstance(dt, str):
            dt = pd.Timestamp(dt)
            
        dt = self.to_timestamp(dt)
        dtn = date_to_nano_midnight(dt,self._tz)
        idx = np.searchsorted(self.bizdays,dtn)
        
        if self._close_nano > self._open_nano:
            if self.bizdays[idx]==dtn:
                idx = idx + 1
            if idx < len(self.bizdays):
                return pd.Timestamp(self.bizdays[idx], tz=self._tz)
            raise SessionOutofRange(dt=dt)
        
        # for regular calendar the session is the same date, 
        # but for extended calendar, the session can be the next
        # day if current time is greater than open time, else the 
        # same day
        if self.bizdays[idx]==dtn and dt.time() > self.open_time \
            and self.is_open(dt):
            idx = idx + 2
        elif self.bizdays[idx]==dtn:
            idx = idx + 1
        elif self.is_open(dt):
            idx = idx + 1
                
        if idx < len(self.bizdays):
            next_dt = pd.Timestamp(
                    self.bizdays[idx], tz=self._tz)
            return next_dt
            
        raise SessionOutofRange(dt=dt)
        
    def previous_session(self, dt:pd.Timestamp|str) -> pd.Timestamp:
        """ returns the previous session. """
        if isinstance(dt, str):
            dt = pd.Timestamp(dt)
            
        dt = self.to_timestamp(dt)
        dtn = date_to_nano_midnight(dt,self._tz)
        idx = np.searchsorted(self.bizdays,dtn)
        
        if self._close_nano > self._open_nano:
            idx = idx - 1
            if idx >= 0:
                return pd.Timestamp(self.bizdays[idx], tz=self._tz)
            raise SessionOutofRange(dt=dt)
        
        # for regular calendar the session is the same date, 
        # but for extended calendar, the session can be the next
        # day if current time is greater than open time, else the 
        # same day
        if self.bizdays[idx]==dtn and dt.time() > self.open_time:
            pass
        else:
            idx = idx - 1
                
        if idx >=0 and idx < len(self.bizdays):
            next_dt = pd.Timestamp(
                    self.bizdays[idx], tz=self._tz)
            return next_dt
            
        raise SessionOutofRange(dt=dt)
        
    def last_n_sessions(self, dt:pd.Timestamp|str, n:int, convert:bool=True) -> pd.DatetimeIndex:
        """ Returns last n trading sessions, including dt. """
        if isinstance(dt, str):
            dt = pd.Timestamp(dt)
            
        dt = self.to_timestamp(dt)
        
        if self.is_session(dt):
            dt = self.current_session(dt)
        else:
            dt = self.previous_session(dt)
            
        dtn = date_to_nano_midnight(dt,self._tz)
        idx2 = np.searchsorted(self.bizdays,dtn)
        
        if idx2 == len(self.bizdays):
            raise SessionOutofRange(dt=dt)
            
        if self.bizdays[idx2] ==  dtn:
            idx2 = idx2 + 1
            
        idx1 = idx2 - n
        
        if convert:
            dts = pd.to_datetime(self.bizdays[idx1:idx2]).\
                    tz_localize('Etc/UTC').tz_convert(self._tz)
            return pd.DatetimeIndex(dts)
        else:
            return pd.DatetimeIndex(self.bizdays[idx1:idx2])
        
    def next_n_sessions(self, dt:pd.Timestamp|str, n:int, convert:bool=True) -> pd.DatetimeIndex:
        """ Returns next n trading sessions, including dt. """
        if isinstance(dt, str):
            dt = pd.Timestamp(dt)
            
        dt = self.current_session(dt)
        dtn = date_to_nano_midnight(dt,self._tz)
        idx1 = np.searchsorted(self.bizdays,dtn)
        
        if idx1 == len(self.bizdays):
            raise SessionOutofRange(dt=dt)
            
        idx2 = min(idx1 + n, len(self.bizdays)-1)
        
        if convert:
            dts = pd.to_datetime(self.bizdays[idx1:idx2]).\
                    tz_localize('Etc/UTC').tz_convert(self._tz)
            return pd.DatetimeIndex(dts)
        else:
            return pd.DatetimeIndex(self.bizdays[idx1:idx2])
        
    def last_n_minutes(self, dt:pd.Timestamp|str, n:int, convert:bool=True) -> pd.DatetimeIndex:
        if isinstance(dt, str):
            dt = pd.Timestamp(dt)
            
        dt = make_consistent_tz(dt, self.tz)
        dt = get_aligned_timestamp(dt, '1m')
        
        return self._last_n_minutes(dt, n, convert)
            
    def _last_n_minutes(self, dt, n, convert=True):
        n_days = int(n/self.minutes_per_day) + 5
        
        sessions = self.last_n_sessions(self.next_open(dt), n_days)
        dt = self.to_timestamp(dt)
        dts = []
        for session in sessions:
            start =self.to_open(session)
            end = self.to_close(session)
            dts.extend(pd.date_range(start, end, freq='min', tz=self.tz))
        
        dts = [t for t in dts if t <=dt]
        dts = pd.DatetimeIndex(dts).drop_duplicates()
        if convert:
            dts = dts.tz_convert(self.tz)
        offset = min(n, len(dts))
        return dts[-offset:]
    
    def last_trading_minute(self, dt:pd.Timestamp|str) -> pd.Timestamp:
        """ returns the last trading minute. """
        idx = self.last_n_minutes(dt, 1)
        if len(idx) > 0:
            return idx[0]
        
        msg = f'No last trading minute found for {dt}.'
        raise BlueshiftCalendarException(msg)
        
    def last_n_bars(self, dt:pd.Timestamp|str, n:int, frequency:Frequency|str) -> pd.DatetimeIndex:
        freq = Frequency(frequency)
        if freq == Frequency('1m'):
            return self.last_n_minutes(dt, n)
        elif freq == Frequency('1d'):
            return self.last_n_sessions(dt, n)
        else:
            dt = self.to_timestamp(dt)
            n_days = round(n*1.2*freq.minute_bars/self.minutes_per_day) + 5
            start_dt = dt - pd.Timedelta(days=n_days)
            idx = pd.date_range(
                    start=start_dt,end=dt,freq=freq.period, tz=self.tz)
            idx = idx[idx.indexer_between_time(
                    self.open_time, self.close_time)] # type: ignore
            return idx[-n:]
        
    def next_n_minutes(self, dt:str|pd.Timestamp, n:int, convert:bool=True) -> pd.DatetimeIndex:
        if isinstance(dt, str):
            dt = pd.Timestamp(dt)
            
        dt = make_consistent_tz(dt, self.tz)
        dt = get_aligned_timestamp(dt, '1m')
        
        return self._next_n_minutes(dt, n, convert=convert)
            
    def _next_n_minutes(self, dt, n, convert=True):
        n_days = int(n/self.minutes_per_day) + 5
        
        sessions = self.next_n_sessions(self.to_open(dt), n_days)
        dt = self.to_timestamp(dt)
        
        if not self.is_open(dt):
            dt = self.next_open(dt)
        
        dts = []
        for session in sessions:
            start = self.to_open(session)
            end = self.to_close(session)
            dts.extend(pd.date_range(start, end, freq='T', tz=self.tz))
        
        dts = [t for t in dts if t >= dt]
        dts = pd.DatetimeIndex(dts).drop_duplicates()
        if convert:
            dts = dts.tz_convert(self.tz)
        offset = min(n, len(dts))
        return dts[:offset]
    
    def next_trading_minute(self, dt:str|pd.Timestamp) -> pd.Timestamp:
        dt = self.to_timestamp(dt)
        idx = self.next_n_minutes(dt, 2)
        
        if len(idx) > 0:
            if dt < idx[0]:
                return idx[0]
            elif len(idx)>1:
                return idx[1]
        
        msg = f'No last trading minute found for {dt}.'
        raise BlueshiftCalendarException(msg)
        
    def current_trading_minute(self, dt:str|pd.Timestamp) -> pd.Timestamp:
        dt = self.to_timestamp(dt)
        idx = self.next_n_minutes(dt, 2)
        
        if len(idx) > 0:
            return idx[0]
        
        msg = f'No last trading minute found for {dt}.'
        raise BlueshiftCalendarException(msg)
        
    def next_period(self, dt:str|pd.Timestamp, period:str) -> pd.Timestamp:
        if not self.is_open(dt):
            raise BlueshiftCalendarException(
                    f'Illegal timestamp {dt}, no active session.')
            
        try:
            freq = Frequency(period)
        except Exception:
            raise BlueshiftCalendarException(f'Illegal period {period}.')
            
        nmins = int(freq.minute_bars)
        try:
            # assert freq < Frequency('D') 
            # assert freq >= Frequency('T')
            if freq >= Frequency('D') or freq < Frequency('T'):
                raise ValueError('illegal period')
            ratio = self.minutes_per_day/nmins
            #assert int(ratio) == ratio
            if int(ratio) != ratio:
                raise ValueError('illegal period')
        except Exception:
            msg = f'Period must be intraday and in multiple of minutes, '
            msg += f'got {period}.'
            raise BlueshiftCalendarException(msg)
        
        start = self.to_timestamp(dt)
        start = get_aligned_timestamp(start, '1m')
        # add the period time delta
        value = start + pd.Timedelta(freq.period)
        if self.is_open(value):
            return value
        
        current_close = self.to_close(value)
        next_open = self.next_open(start)
        
        diff = value - current_close
        return next_open + diff
        
    def bump_date(self, dt:str|pd.Timestamp, bump:str) -> pd.Timestamp:
        bump = bump.lower()
        if bump == 'previous':
            return self.bump_previous(dt)
        elif bump == 'forward':
            return self.bump_forward(dt)
        elif bump == 'mod_previous':
            return self.bump_mod_previous(dt)
        elif bump == 'mod_forward':
            return self.bump_mod_forward(dt)
        else:
            msg = f'Date bump specification {bump} is not valid. Must be '
            msg += 'one of previous, forward, mod_previous or mod_forward.'
            raise BlueshiftCalendarException(msg)
        
    def bump_previous(self, dt:str|pd.Timestamp) -> pd.Timestamp:
        """ bump to the previous date if dt is a holiday. """
        dt = make_consistent_tz(dt, self.tz)
        while self.is_holiday(dt):
            dt = dt - pd.Timedelta(days=1)
            
        return dt
    
    def bump_forward(self, dt:str|pd.Timestamp) -> pd.Timestamp:
        """ bump to the next date if dt is a holiday. """
        dt = make_consistent_tz(dt, self.tz)
        while self.is_holiday(dt):
            dt = dt + pd.Timedelta(days=1)
            
        return dt
    
    def bump_mod_previous(self, dt:str|pd.Timestamp) -> pd.Timestamp:
        """ 
            bump to previous, unless the result is in the previous 
            month, in which case, bump to the next date.
        """
        original_dt = make_consistent_tz(dt, self.tz)
        month = original_dt.month
        
        dt = original_dt
        while self.is_holiday(dt):
            dt = dt - pd.Timedelta(days=1)
            
        if dt.month == month:
            return dt
            
        dt = original_dt
        while self.is_holiday(dt):
            dt = dt + pd.Timedelta(days=1)
            
        return dt
    
    def bump_mod_forward(self, dt:str|pd.Timestamp) -> pd.Timestamp:
        """ 
            bump to next date, unless the result is in the next 
            month, in which case, bump to the previous date.
        """
        original_dt = make_consistent_tz(dt, self.tz)
        month = original_dt.month
        
        dt = original_dt
        while self.is_holiday(dt):
            dt = dt + pd.Timedelta(days=1)
            
        if dt.month == month:
            return dt
            
        dt = original_dt
        while self.is_holiday(dt):
            dt = dt - pd.Timedelta(days=1)
            
        return dt
    
    def get_expiries(self, start_dt:str|pd.Timestamp, end_dt:str|pd.Timestamp, frequency:str, 
                     weekday:int, n:int|None=None, 
                 bump:Literal['forward','previous','mod_forward','mod_previous']='previous', 
                 monthly_weekday:int|None=None, expiry_params:dict|None=None) -> list[pd.Timestamp]:
        return _get_expiries(
                self, start_dt, end_dt, frequency, weekday, nweek=n,
                bump=bump, monthly_weekday=monthly_weekday,
                expiry_params=expiry_params)

    def minutes(self, start_dt, end_dt):
        raise NotImplementedError()
        
    def add_holidays(self, dts) -> None:
        """ add a list of holidays from a file or a list of dates. """
        if isinstance(dts, str):
            try:
                dts = pd.read_csv(dts)
                dts = pd.to_datetime(dts.iloc[:,0])
            except Exception:
                pass
        self.__add_holidays(dts)
        
    def __initialize(self):
        if self.__bizdays:
            return
        
        self.__add_bizdays()
        self.__add_holidays()

    def __add_bizdays(self):
        '''
            Add business days. Expect Timestamp list.
        '''
        if self.__bizdays is not None:
            return
        
        self.__bizdays = days_to_nano(
                self._saved_start, self._saved_end, self._saved_bizdays, 
                self._tz, self._weekends, self._special_sessions) # type: ignore
        self.__bizdays = np.sort(self.__bizdays)

    def __add_holidays(self, dts=None):
        '''
            Add holiddays. Should be Timestamp or Holiday rules list.
        '''
        if self.__bizdays is None:
            self.__add_bizdays()
        
        if dts is None and not self._saved_holidays:
            return
        
        if dts is None:
            dts = self._saved_holidays
            
        try:
            if isinstance(dts[0], Holiday): # type: ignore
                dts = [pd.Timestamp(dt, tz=self.tz) for dt in \
                       holidays_range(
                               self.start.year, self.end.year, dts) ] # type: ignore
            else:
                dts = [make_consistent_tz(dt, self.tz) for dt in dts] # type: ignore
        except Exception:
            raise InvalidDate('must be list of holidays or timestamps')
        
        dts = [dt for dt in dts if dt >= self.start and dt <= self.end]
        dtsn = days_to_nano(dts=dts, tz=self._tz)
        self.__bizdays = [dt for dt in self.__bizdays if dt not in dtsn] # type: ignore