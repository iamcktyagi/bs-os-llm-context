from __future__ import annotations
from typing import TYPE_CHECKING, Callable, cast
import bisect
from time import strptime as time_strptime
from datetime import time
from pandas.tseries.offsets import MonthBegin, MonthEnd
import warnings
import weakref

from blueshift.calendar.trading_calendar import make_consistent_tz
from blueshift.calendar.date_utils import datetime_time_to_nanos
from blueshift.lib.exceptions import (
        ScheduleFunctionError, NoContextError, InternalError, BlueshiftWarning)
from blueshift.lib.common.constants import NANO_SECOND
from blueshift.lib.common.types import ListLike
from blueshift.lib.common.sentinels import noop
from blueshift.lib.common.enums import AlgoMode as MODE

if TYPE_CHECKING:
    import pandas as pd
    import datetime
    from blueshift.interfaces.context import IContext
    from blueshift.interfaces.assets._assets import Asset
    from blueshift.calendar.trading_calendar import TradingCalendar
    from blueshift.lib.common.enums import AlgoMode
    from blueshift.core.algorithm.algorithm import TradingAlgorithm
else:
    import blueshift.lib.common.lazy_pandas as pd

MAX_DAYS_AHEAD = 90
MAX_MONTH_DAY_OFFSET = 15
MAX_WEEK_DAY_OFFSET = 3
MAX_MINUTE_OFFSET = 59
MAX_HOUR_OFFSET = 23

class date_rules:
    """
        Date rules define the date part of the rules for a scheduled 
        function call. The supported functions are as below (further 
        subjected to time rule). The ``days_offset`` parameter below 
        (if applicable) must be ``int`` (positive), and it must not be greater than
        3 for ``week_start``/``week_end`` and must not be greater than 
        15 for ``month_start`` and ``month_end``. The `weekday` parameter
        can be either an integer between 0 (Monday) to 6 (Sunday), or a 
        string representing the day of week (e.g. "Fri", or "Friday").
        
        - ``every_day()``: called every day.
        - ``week_start(days_offset=0)``: `days_offet` days after the first trading day of the week.
        - ``week_end(days_offset=0)``: `days_offet` days before the last trading day of the week.
        - ``week_day(weekday=0)``: Run on given `weekday` - can be either an integer or weekday name or list of the same.
        - ``month_start(days_offset=0)``: `days_offet` days after the first trading day of the month.
        - ``month_end(days_offset=0)``: `days_offet` days before the last trading day of the month.
        - ``on(dts)``: called every day in the list `dts` (must be list of pandas Timestamps or DatetimeIndex).
        - ``expiry(context, asset, days_offset=0)``: `days_offet` days before the expiry date of `asset`.
        
        .. note::
            For the `expiry` date rule, `asset` must be a futures or an option.
            Also, if the asset is dated, only the single expiry date will be 
            considered. For a rolling asset, this will trigger based on every 
            expiry day.
    """
    
    def __init__(self):
        self.trigger_dates = None
        
    def __str__(self):
        return "Blueshift date_rules"
    
    def __repr__(self):
        return self.__str__()
        
    @classmethod
    def every_day(cls):
        def func(sessions):
            return [dt.normalize().value for dt in sessions]
        
        func.date_rule = True # type: ignore
        return func
    
    @classmethod
    def everyday(cls):
        def func(sessions):
            return [dt.normalize().value for dt in sessions]
        
        func.date_rule = True # type: ignore
        return func
    
    @classmethod
    def on(cls, dts:list[pd.Timestamp]):
        if not isinstance(dts, ListLike):
            dts = [dts] # type: ignore
        def func(sessions):
            if sessions.empty:
                return []
            start_dt = sessions[0]
            if start_dt.tz:
                dts_tz = [make_consistent_tz(dt, start_dt.tz) for dt in dts]
            else:
                dts_tz = [dt for dt in dts]
            return sorted([dt.normalize().value for dt in dts_tz])
        func.date_rule = True # type: ignore
        return func
    
    @classmethod
    def expiry(cls, context:IContext, asset:Asset, days_offset:int=0):
        days_offset = abs(int(days_offset))
        
        def func(sessions):
            cal = context.trading_calendar
            expiries = context.data_portal.get_expiries(asset, sessions[0], sessions[-1])
            dts = [cal.bump_previous(
                    dt - pd.Timedelta(days=days_offset)) for dt in expiries]
            return sorted([dt.normalize().value for dt in dts])
        
        func.date_rule = True # type: ignore
        return func
    
    @classmethod
    def week_start(cls, days_offset:int=0):
        days_offset = abs(int(days_offset))
        if days_offset > MAX_WEEK_DAY_OFFSET:
            msg = f"invalid days offset supplied, cannot be greater than {MAX_WEEK_DAY_OFFSET}."
            raise ScheduleFunctionError(msg=msg)
            
        def func(sessions):
            dts = pd.Series(sessions).groupby([sessions.year, 
                           pd.Index(sessions.isocalendar().week)]).nth(days_offset)
            return sorted([dt.normalize().value for dt in dts]) # type: ignore
        
        func.date_rule = True # type: ignore
        return func
    
    @classmethod
    def week_end(cls, days_offset:int=0):
        days_offset = abs(int(days_offset))
        if days_offset > MAX_WEEK_DAY_OFFSET:
            msg = f"invalid days offset supplied, cannot be greater than {MAX_WEEK_DAY_OFFSET}."
            raise ScheduleFunctionError(msg=msg)
        days_offset = -days_offset -1
        
        def func(sessions):
            dts = pd.Series(sessions).groupby([sessions.year, 
                           pd.Index(sessions.isocalendar().week)]).nth(days_offset)
            return sorted([dt.normalize().value for dt in dts]) # type: ignore
        
        func.date_rule = True # type: ignore
        return func
    
    @classmethod
    def week_day(cls, weekday:int|set|list=0):
        if not isinstance(weekday, ListLike):
            weekday = [weekday] # type: ignore
        weekdays = set()
        for wk in weekday: # type: ignore
            if isinstance(wk, str):
                if len(wk)==3:
                    wk = time_strptime(wk, "%a").tm_wday
                else:
                    wk = time_strptime(wk, "%A").tm_wday
                
            wk = abs(int(wk))
            if wk > 6 or wk < 0:
                # 6 is sunday
                msg = f"invalid week day {weekday}, must be between 0 (Monday) "
                msg += f"and 6 (Sunday)."
                raise ScheduleFunctionError(msg=msg)
            weekdays.add(wk)
        
        def func(sessions):
            dts = [dt for dt in sessions if dt.weekday() in weekdays]
            return [dt.normalize().value for dt in dts]
        
        func.date_rule = True # type: ignore
        return func
    
    @classmethod
    def month_start(cls, days_offset:int=0):
        days_offset = abs(int(days_offset))
        if days_offset > MAX_MONTH_DAY_OFFSET:
            msg = f"invalid days offset supplied, cannot be greater than {MAX_MONTH_DAY_OFFSET}."
            raise ScheduleFunctionError(msg=msg)
        
        def func(sessions):
            dts = pd.Series(sessions).groupby([sessions.year, 
                           sessions.month]).nth(days_offset)
            
            return sorted([dt.normalize().value for dt in dts]) # type: ignore
        
        func.date_rule = True # type: ignore
        return func
    
    @classmethod
    def month_end(cls, days_offset:int=0):
        days_offset = abs(int(days_offset))
        if days_offset > MAX_MONTH_DAY_OFFSET:
            msg = f"invalid days offset supplied, cannot be greater than {MAX_MONTH_DAY_OFFSET}."
            raise ScheduleFunctionError(msg=msg)
        days_offset = -days_offset -1
        def func(sessions):
            dts = pd.Series(sessions).groupby([sessions.year, 
                           sessions.month]).nth(days_offset)
            return sorted([dt.normalize().value for dt in dts]) # type: ignore
        
        func.date_rule = True # type: ignore
        return func
    
class time_rules:
    """
        time rules defines the time part of the rules for a scheduled 
        function call. The supported functions are as below (further 
        subjected to date rule). 
        
        The ``hours`` parameter below (if applicable) must be ``int``, 
        (positive) and it must not be greater than 23. The ``minutes`` parameter 
        below (if applicable) must be ``int`` (positive) and must not 
        be greater than 59.
        
        - ``market_open(minutes=0, hours=0)``: called after ``hours`` and ``minutes`` offset from market open.
        - ``on_open(minutes=0, hours=0)``: Alias for market_open.
        - ``market_close(minutes=0, hours=0)``: called after ``hours`` and ``minutes`` offset before market close.
        - ``on_close(minutes=0, hours=0)``: Alias for market_close.
        - ``every_nth_minute(minutes=1)``: called every n-th minute during the trading hours.
        - ``every_nth_hour(cls, hours=1)``: called every n-th hour during the trading hours.
        - ``every_hour()``: called every hours during the trading day.
        - ``at(dt)``: Called at the given time `dt` (must be a datetime.time object).
        
        .. note::
            Timestamps generated by these rules are always aligned with the 
            session start and end timings. That means, for example, the 
            `every_nth_hour` will execute aligned from market open. If the 
            scheduling happens after the market open, it will still trigger 
            aligned with open time, unless the next trigger falls in the next 
            day, in which case the first call will trigger immediately in the 
            next clock tick. For example, if a trigger is scheduled at 09:45, 
            and the algo is started at 10:00, the first call will happen in 
            the next clock tick.
        
    """
    
    def __init__(self):
        self.trigger_dates = None
        
    def __str__(self):
        return "Blueshift time_rules"
    
    def __repr__(self):
        return self.__str__()
    
    @classmethod
    def at(cls, at:str|datetime.time|tuple[int,int]):
        """ 
            schedule after a give minutes of delay from the scheduling 
            call. The parameter `at` is a datetime.time object.
        """
        if isinstance(at, str):
            # look for format hh:mm
            try:
                hh, mm = at.split(':')
                at = (int(hh), int(mm))
            except Exception:
                msg = f'Invalid time string, expected in hh:mm format.'
                raise ScheduleFunctionError(msg=msg)
        
        if isinstance(at, tuple):
            at = time(*at)
        def func(session_open, session_close):
            dt = datetime_time_to_nanos(at)
            return [dt]
        func.time_rule = True # type: ignore
        return func
    
    @classmethod
    def market_open(cls, minutes:int=0, hours:int=0):
        minutes = abs(int(minutes))
        hours = abs(int(hours))
        if minutes > MAX_MINUTE_OFFSET:
            raise ScheduleFunctionError(
                    msg="invalid minute offset supplied, must be less than 60.")
        if hours > MAX_HOUR_OFFSET:
            raise ScheduleFunctionError(
                    msg="invalid hour offset supplied, must be less than 24.")
        
        def func(session_open, session_close):
            max_hour = session_close.hour - session_open.hour - 1
            if max_hour < 0:
                max_hour = max_hour + 24
            
            if hours > max_hour:
                raise ScheduleFunctionError(msg=f"max allowed hour offset {max_hour}.")
            
            dt = datetime_time_to_nanos(session_open) + \
                datetime_time_to_nanos(time(hour=hours, minute=minutes))
            return [dt]
        
        func.time_rule = True # type: ignore
        return func
    
    @classmethod
    def on_open(cls, minutes:int=0, hours:int=0):
        return cls.market_open(minutes, hours)
    
    @classmethod
    def market_close(cls, minutes:int=2, hours:int=0):
        minutes = abs(int(minutes))
        hours = abs(int(hours))
        if minutes > MAX_MINUTE_OFFSET:
            raise ScheduleFunctionError(
                    msg="invalid minute offset supplied, must be less than 60.")
        if hours > MAX_HOUR_OFFSET:
            raise ScheduleFunctionError(
                    msg="invalid hour offset supplied, must be less than 24.")
            
        if hours==0 and minutes < 2:
            msg = 'You are scheduling a function too near to the market close.'
            msg += ' This may be executed on the next day open instead.'
            msg += ' Minute offset should be at least 2, and greater '
            msg += '5 from the market close to be safe.'
            warnings.warn(msg, BlueshiftWarning)
            
        def func(session_open, session_close):
            max_hour = session_close.hour - session_open.hour - 1
            if max_hour < 0:
                max_hour = max_hour + 24
                
            if hours > max_hour:
                raise ScheduleFunctionError(msg=f"max allowed hour offset {max_hour}.")
            
            dt = datetime_time_to_nanos(session_close) - \
                datetime_time_to_nanos(time(hour=hours, minute=minutes))
            return [dt]
        
        func.time_rule = True # type: ignore
        return func
    
    @classmethod
    def on_close(cls, minutes:int=2, hours:int=0):
        return cls.market_close(minutes, hours)
    
    @classmethod
    def every_nth_minute(cls, minutes:int=1, n:int=0):
        if n != 0:
            minutes = n
        minutes = abs(int(minutes))
        if minutes > MAX_MINUTE_OFFSET:
            h = minutes/60
            if h > MAX_HOUR_OFFSET:
                raise ScheduleFunctionError(
                        msg=f"minute offset supplied is too large {minutes}.")
        if not minutes > 0:
            raise ScheduleFunctionError(
                    msg="invalid minute offset supplied, must be less than 60.")
        
        def func(session_open, session_close):
            start_val = datetime_time_to_nanos(session_open)
            end_val = datetime_time_to_nanos(session_close)
            
            if end_val < start_val:
                start_val = start_val - 24*3600*NANO_SECOND
            
            step = NANO_SECOND*60*minutes
            dts = [dt for dt in range(start_val, end_val, step)]
            return dts
        
        func.time_rule = True # type: ignore
        return func
    
    @classmethod
    def every_minute(cls):
        return cls.every_nth_minute(1)
    
    @classmethod
    def every_nth_hour(cls, hours:int=1, n:int=0):
        if n!=0:
            hours = n
        hours = abs(int(hours))
        if hours > MAX_HOUR_OFFSET:
            raise ScheduleFunctionError(
                    msg="invalid hour offset supplied, must be less than 24.")
        if not hours > 0:
            raise ScheduleFunctionError(
                    msg="invalid hour offset supplied, must be positive.")
        
        def func(session_open, session_close):
            start_val = datetime_time_to_nanos(session_open)
            end_val = datetime_time_to_nanos(session_close)
            
            if end_val < start_val:
                start_val = start_val - 24*3600*NANO_SECOND
            
            step = NANO_SECOND*60*60*hours
            dts = [dt for dt in range(start_val, end_val, step)]
            return dts
        
        func.time_rule = True # type: ignore
        return func
    
    @classmethod
    def every_hour(cls):
        return cls.every_nth_hour(1)

class TimeRule:
    '''
        Rule object that defines the trigger date and time. It takes inputs
        of date_rule and time_rule, apply the respective functions and 
        combine them by simple arithmetic. It maintains a pre-computed buffer
        of task schedule times and re-compute if necessary. It implements 
        next method to supply the next scheduled time for its task.
    '''
    def __init__(self, dt:pd.Timestamp, dt_func:Callable, time_func:Callable, 
                 start_dt:pd.Timestamp|None=None, end_dt:pd.Timestamp|None=None,
                 trading_calendar:TradingCalendar|None = None, mode:AlgoMode=MODE.BACKTEST):
        self._current_dt = dt
        self._dt_func = dt_func
        self._time_func = time_func
        
        if not callable(dt_func) or not callable(time_func):
            msg = 'missing date rule or time rule.'
            raise ScheduleFunctionError(msg)
        
        self._trading_calendar = trading_calendar
        if not self._trading_calendar:
            raise ScheduleFunctionError(msg="rule must be supplied a calendar")
        
        self._mode = mode
        self._start_dt = start_dt
        
        if self._start_dt:
            self._start_dt = make_consistent_tz(self._start_dt,
                                                self._trading_calendar.tz)
        self._end_dt = end_dt
        if self._end_dt:
            self._end_dt = make_consistent_tz(self._end_dt, 
                                              self._trading_calendar.tz)
        
        self._trigger_dts:list[int] = []
        self._trigger_idx = -1
        
        self._calc_dts()
        
    def _calc_dts(self):
        self._trading_calendar = cast(TradingCalendar, self._trading_calendar)
        if not self._start_dt:
            self._start_dt = make_consistent_tz(pd.Timestamp.now(
                    self._trading_calendar.tz).\
                                                normalize(), 
                                                self._trading_calendar.tz)
        
        if not self._end_dt:
            self._end_dt = self._start_dt + pd.Timedelta(days=MAX_DAYS_AHEAD)

        self._trigger_dts = self._trigger_dts_calc(self._start_dt,
                                                   self._end_dt)
        if len(self._trigger_dts) < 1:
            # try again, resetting end_dates
            self._end_dt = self._start_dt + pd.Timedelta(days=MAX_DAYS_AHEAD)
            self._trigger_dts = self._trigger_dts_calc(self._start_dt, 
                                                       self._end_dt)
            # if still no trigger dates, raise StopIteration
            if len(self._trigger_dts) < 1:
                #raise ScheduleFunctionError(msg="failed to create task schedules")
                raise StopIteration('nothing to schedule - no valid dates found within range.')
            
    def _trigger_dts_calc(self, start_dt, end_dt) -> list[int]:
        self._trading_calendar = cast(TradingCalendar, self._trading_calendar)
        # bump the dates to ensure we capture month start and end
        start_dt_use = start_dt - MonthBegin()
        end_dt_use = end_dt + MonthEnd()
        sessions = self._trading_calendar.sessions(start_dt_use, end_dt_use)
        
        mkt_open = self._trading_calendar.open_time
        mkt_close = self._trading_calendar.close_time
        dt_dates = self._dt_func(sessions)
        
        # we make sure the date ranges does not fall outside the start 
        # and end dates.
        dt_dates = [dt for dt in dt_dates if dt >= start_dt.value and\
                    dt <= end_dt.value]
        
        # we cannot do the same for times, for e.g. for 24 hour calendar
        # it becomes meaningless. open may be > close.
        dt_times = self._time_func(mkt_open, mkt_close)
        dts = [dt1 + dt2 for dt1 in dt_dates for dt2 in dt_times]
        
        # for live, we cannot trade in the past, so we start with the
        # next attainable minute we can have.
        if self._mode in (MODE.LIVE, MODE.PAPER, MODE.EXECUTION):
            now = pd.Timestamp.now(tz=self._trading_calendar.tz)
        else:
            now = self._current_dt
            # for backtest, we cannot trade before the start date.
            dts = [dt for dt in dts if dt > dt_dates[0]]
            
        today_start = now.normalize().value
        today_end = now.normalize() + pd.Timedelta(days=1)
        today_end = today_end.value - 60*NANO_SECOND
        now = now.value
        
        # filter the list for future timestamps (inclusive current).
        # if filtered dts starts after today, add back any trigger
        # from today, else it remains as it is. This filtering is 
        # required for live trading and also for backtesting if schedule
        # function is called outside initialize
        filtered_dts = [dt for dt in dts if dt >= now]
        
        if filtered_dts and filtered_dts[0] > today_end:
            less_dts = [dt for dt in dts if dt >= today_start and dt < now]
            dts = less_dts[-1:] + filtered_dts
        else:
            dts = filtered_dts
        
        return dts
    
    def __next__(self) -> int:
        self._trigger_idx = self._trigger_idx + 1
        if self._trigger_idx < len(self._trigger_dts):
            return self._trigger_dts[self._trigger_idx]
        
        self._start_dt = self._end_dt
        self._end_dt = self._start_dt + pd.Timedelta(days=MAX_DAYS_AHEAD) # type: ignore
        self._calc_dts()
        self._trigger_idx = -1
        return self._trigger_dts[self._trigger_idx]
    
    def __iter__(self):
        return self
    
    def __str__(self) -> str:
        return "Blueshift Time-Rules"
    
    def __repr__(self):
        return self.__str__()
    

class TriggerOnce(TimeRule):
    """
        Time based rule that triggers only once.
    """
    def __init__(self, dt:pd.Timestamp, trading_calendar:TradingCalendar|None=None, 
                 mode:AlgoMode=MODE.BACKTEST):
        self._trading_calendar = trading_calendar
        dt = pd.Timestamp(dt)
        
        if self._trading_calendar:
            dt = make_consistent_tz(dt, self._trading_calendar.tz)
        
        self._dt = int(dt.value)
        
    def _calc_dts(self):
        raise NotImplementedError(f'not applicable for trigger-once event')
        
    def _trigger_dts_calc(self, start_dt, end_dt):
        raise NotImplementedError(f'not applicable for trigger-once event')
        
    def __next__(self) -> int:
        if self._dt:
            dt = self._dt
            self._dt = None
            return dt
        raise StopIteration()
    
    def __iter__(self):
        return self
    
    def __str__(self) -> str:
        return "Blueshift Trigger-Once"


class TimeEvent:
    '''
        Time-based event class - associates a callable with a rule and a time.
        The rule object, when called, returns the next call dt. The callable
        always has the signature of function(context, data).
    '''
    def __init__(self, ctx:str, rule:TimeRule, callback:Callable|None=None):
        self._context_name = ctx
        self._dt = next(rule)
        self._rule=rule
        self._callback=callback if callback else noop
        
    def __iter__(self):
        return self
        
    def __next__(self):
        self._dt = next(self._rule)
        return self
        
    @property
    def context_name(self):
        return self._context_name
    
    @property
    def dt(self):
        return self._dt
        
    @property
    def rule(self):
        return self._rule
    
    @property
    def callback(self):
        return self._callback
    
    def __int__(self):
        return int(self._dt)
    
    def __eq__(self, obj):
        return self.dt == int(obj)
    
    def __ne__(self, obj):
        return self.dt != int(obj)
    
    def __gt__(self, obj):
        return self.dt > int(obj)
    
    def __lt__(self, obj):
        return self.dt < int(obj)
    
    def __ge__(self, obj):
        return self.dt >= int(obj)
    
    def __le__(self, obj):
        return self.dt <= int(obj)
    
    def __str__(self):
        if hasattr(self.callback, '__name__'):
            func = self.callback.__name__
        else:
            func = str(self.callback)
        return f"Blueshift TimeEvent [{func}@{self.dt}@{self.context_name}]"
    
    def __repr__(self):
        return self.__str__()

class Scheduler:
    '''
        Class to manage scheduled events (events based on time-stamp). The 
        core parts are a queue and a method to check the queue on request. 
        The implementation uses bisect insort_left to insert an incoming 
        event to the first possible sorted position, and also uses bisect
        bisect_right to retrieve all pending tasks for a given nano. The
        event trigger method returns early in case the task queue is empty
        or the task with the nearest future nano is higher than the current
        nano. Else it removes and processes all hits and re-insert them 
        after updating the next call nano.
    '''
    
    def __init__(self, algo_instance:TradingAlgorithm):
        # list of TimeEvent events
        self._algo_instance = weakref.ref(algo_instance)
        self._events = []
        self._next_dt = None
        self._call_stack = {}
        self._eod_reset = set()
        
    def call_soon(self, callback, ctx):
        if ctx not in self._call_stack:
            self._call_stack[ctx] = set()
        
        self._call_stack[ctx].add(callback)
        return EventHandle(self, None, callback, ctx)
        
    def _trigger_call_soon(self, context, data):
        contexts = list(self._call_stack.keys())
        
        for ctx_name in contexts:
            callbacks = list(self._call_stack.pop(ctx_name,set()))
            if not callbacks:
                continue
            
            try:
                with context.switch_context(ctx_name) as ctx:
                    for callback in callbacks:
                        try:
                            callback(ctx, data)
                        except Exception as e:
                            algo = self._algo_instance()
                            if algo:
                                algo.handle_error(e, ctx.name)
            except NoContextError:
                # this always run in the main thread, we should not be here
                pass
                
        
    def add_event(self, event, eod_reset:bool=False):
        bisect.insort_left(self._events, event)
        self._next_dt = self._events[0].dt
        
        if eod_reset:
            self._eod_reset.add(str(event))
            
        return EventHandle(self, event)
            
    def eod_reset(self):
        self._events = [e for e in self._events if str(e) not in self._eod_reset]
        self._eod_reset = set()
        
    def next_event_dt(self, dt=None):
        if not self._events:
            return 0
            
        if dt is None:
            return self._events[0].dt
        else:
            pos = bisect.bisect_right(self._events, dt)
            if pos == 0:
                return 0
            callback_list = self._events[:pos]
            if not callback_list:
                return 0
            return callback_list[0].dt
        
    def trigger_events(self, context, data, dt):
        self._trigger_call_soon(context, data)
        
        if not self._events:
            return

        if dt < self._next_dt:
            return
        
        pos = bisect.bisect_right(self._events, dt)
        if pos == 0:
            return
        
        callback_list = self._events[:pos]
        self._events = self._events[pos:]

        for e in callback_list:
            try:
                self.add_event(next(e))
            except StopIteration:
                # no next trigger applicable for this event
                pass
            
            try:
                with context.switch_context(e.context_name) as ctx:
                    try:
                        e.callback(ctx, data)
                    except Exception as ee:
                        algo = self._algo_instance()
                        if algo:
                            algo.handle_error(ee, ctx.name)
            except NoContextError:
                # this always run in the main thread, we should not be here
                pass
                
    def remove_context(self, name):
        if name in self._call_stack:
            self._call_stack.pop(name)
            
        events = self._events.copy()
        for e in events:
            if e.context_name == name:
                self._events.remove(e)
        if self._events:
            self._next_dt = self._events[0].dt
    
    def __str__(self):
        return "Blueshift tasks scheduler"
    
    def __repr__(self):
        return self.__str__()

    
class EventHandle:
    """
        Event handle that can be used to cancel a scheduled event.
    """
    __slots__ = ['scheduler','evt','cb','ctx','handle','_id','cancelled']
    
    def __init__(self, scheduler:Scheduler, evt=None, cb=None, ctx=None, 
                 handle=None, id_=None):
        self.evt = evt
        self.cb = cb
        self.ctx = ctx
        self.handle = handle
        self._id = id_
        self.cancelled = False
        
        if scheduler:
            self.scheduler = weakref.ref(scheduler)
        else:
            self.scheduler = None
            
    def __str__(self):
        return f'EventHanfle[{id(self)}]'
    
    def __repr__(self):
        return self.__str__()
        
    def cancel(self):
        if self.cancelled:
            return
        
        try:
            if self.evt:
                return self._remove_scheduled_func()
            elif self.cb:
                return self._remove_scheduled_once()
            elif self.handle:
                return self._remove_asyncio_handle()
        except Exception as e:
            msg = f'Failed to cancel event: {str(e)}.'
            raise InternalError(msg)
        
    def update_handle(self, handle):
        if self.cancelled:
            return
        
        if handle:
            self.handle = handle
            
    def _remove_asyncio_handle(self):
        scheduler = self.scheduler() if self.scheduler else None
        algo = scheduler._algo_instance() if scheduler else None
        
        self.handle.cancel() # type: ignore
        if algo:
            algo._scheduled_events.pop(self._id, None)
            
        self.cancelled = True
        
    def _remove_scheduled_once(self):
        scheduler = self.scheduler() if self.scheduler else None
        
        cb, ctx = self.cb, self.ctx
        
        if scheduler and cb:
            callstack = scheduler._call_stack.get(ctx, set())
            if cb in callstack:
                callstack.remove(cb)
                
        self.cancelled = True
        
    def _remove_scheduled_func(self):
        scheduler = self.scheduler() if self.scheduler else None
        evt = self.evt
        
        if scheduler and evt:
            events = scheduler._events.copy()
            
            for idx, e in enumerate(events):
                if id(e) == id(evt):
                    scheduler._events.pop(idx)
                    if len(scheduler._events)>0:
                        scheduler._next_dt = scheduler._events[0].dt
                    
                    if str(evt) in scheduler._eod_reset:
                        scheduler._eod_reset.remove(str(evt))
            
        self.cancelled = True