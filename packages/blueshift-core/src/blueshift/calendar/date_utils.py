from __future__ import annotations
from typing import TYPE_CHECKING, Literal, cast
from datetime import date, timedelta
import datetime
from functools import lru_cache
import math
import time

from blueshift.lib.exceptions import BlueshiftCalendarException
from blueshift.lib.common.constants import Frequency, NANO_IN_SECONDS

if TYPE_CHECKING:
    import pandas as pd
    from blueshift.calendar.trading_calendar import TradingCalendar
else:
    import blueshift.lib.common.lazy_pandas as pd

_memoized_nanos = {}

def valid_timezone(tz:str) -> bool:
    '''
        check if a timezone string is valid
    '''
    import pytz
    return (str(tz) in pytz.all_timezones)

def convert_to_time(*args) -> datetime.time:
    """
        Convert a text input (like "HH:MM" or "HH:MM:SS") or a tuple of  
        numbers (hours, minutes and optionally seconds) to datetime.time 
        object.
    """
    if isinstance(args[0], str):
        parts = args[0].split(':')
        try:
            parts = [int(float(part)) for part in parts]
            time = datetime.time(*parts) # type: ignore
        except Exception:
            time = args[0]
    elif isinstance(args[0], tuple):
        try:
            time = datetime.time(*args[0])
        except Exception:
            time = args[0]
    elif isinstance(args[0], datetime.time):
        time = args[0]
    else:
        try:
            time = datetime.time(*args)
        except Exception:
            time = args
            
    if not isinstance(time, datetime.time):
        msg = f'time part must be datetime.time or a tuple or string '
        msg += f'that can be converted to same. Got {time}.'
        raise BlueshiftCalendarException(msg)
        
    return time

def combine_date_time(dt:pd.Timestamp, time:str|tuple[int,int]|datetime.time, 
                      tz:str|None=None) -> pd.Timestamp:
    """
        Convert inputs `dt` and `time` to a datetime with date part from
        `dt` and time part from `time`.
    """
    dt = pd.Timestamp(dt).normalize()

    input_tz = dt.tz
    time = convert_to_time(time)
        
    dt = pd.to_datetime(dt.strftime('%Y-%m-%d') + ' ' + str(time))
    if input_tz:
        dt = dt.tz_localize(input_tz)
        
    if tz:
        if dt.tz:
            dt = dt.tz_convert(input_tz)
        else:
            dt = dt.tz_localize(tz)
            
    return dt
        

def make_consistent_tz(dt:pd.Timestamp|datetime.date|datetime.datetime|str, tz:str|None) -> pd.Timestamp:
    '''
        Given a datetime and a timezone, ensure the datetime in the
        same timezone.
    '''
    dt = pd.Timestamp(dt)
    
    if dt.tz is None:
        dt = pd.Timestamp(dt, tz=tz)
    elif tz:
        dt = dt.tz_convert(tz=tz)
    return dt

def remove_tzinfo(dt:pd.Timestamp) -> pd.Timestamp:
    """remove tz-info from a timestamp."""
    dt = pd.Timestamp(dt)
    return dt.tz_localize(None)

def to_unix_timestamp(dt:pd.Timestamp, tz:str|None=None, unit:Literal['ms','s']='ms'):
    """convert timestamp to unix timestamp."""
    dt = make_consistent_tz(dt, tz)
    
    if dt.tz:
        dt = dt.tz_convert('Etc/UTC').tz_localize(None)
    
    if unit == 'ms':
        t = round(dt.value/1E6)
    elif unit == 's':
        t = round(dt.value/1E9)
    else:
        raise BlueshiftCalendarException(f'unit can be either ms or s.')
        
    return t

def to_timestamp(strtime:str, tz:str|None=None) -> pd.Timestamp:
    """convert date to timestamp."""
    dt = pd.Timestamp(strtime)
    
    if not tz:
        return dt
    
    if dt.tz:
        return dt.tz_localize(None).tz_localize(tz)
    else:
        return dt.tz_localize(tz)

def to_iso_time(dt:pd.Timestamp) -> str:
    """convert timestamp to iso date-time string."""
    dt = pd.Timestamp(dt)
    return dt.isoformat()[:19] + '.000Z'

@lru_cache()
def month_first_day(year:int, month:int|None=None) -> date:
    """ 
        Get the first date of the month. If month not specified, return 
        the first day of the year
    """
    if not month:
        return date(year=year, month=1, day=1)
    return date(year=year, month=month, day=1)

@lru_cache()
def month_last_day(year:int, month:int|None=None) -> date:
    """ 
        Get the last date of the month. If month not specified, return 
        the last day of the year
    """
    if not month:
        return date(year=year, month=12, day=31)
    
    guess_day = date(year=year, month=month, day=28)
    next_month = guess_day + timedelta(days=5)
    return next_month - timedelta(days=next_month.day)

@lru_cache()
def quarter_first_day(year:int, quarter:int|None=None) -> date:
    """ 
        Get the first date of the month. If month not specified, return 
        the first day of the year
    """
    mapping = {1:1,2:4,3:7,4:10}
    
    if not quarter:
        return date(year=year, month=1, day=1)
    
    month=mapping[quarter]
    return date(year=year, month=month, day=1)

@lru_cache()
def quarter_last_day(year:int, quarter:int|None=None) -> date:
    """ 
        Get the first date of the month. If month not specified, return 
        the first day of the year
    """
    mapping = {1:3,2:6,3:9,4:12}
    mapping2 = {1:31,2:30,3:30,4:31}
    
    if not quarter:
        return date(year=year, month=12, day=31)
    
    month=mapping[quarter]
    day = mapping2[quarter]
    return date(year=year, month=month, day=day)

@lru_cache()
def nth_day(year:int, month:int|None=None, n:int=1, tz:str|None=None, strict:bool=False,
            direction:Literal['forward','backward']='forward') -> pd.Timestamp:
    """
        Get the n-th day of month or the last date. If month is `None`,
        return the n-th day of the year. If `strict` is set True, returns
        NaT if n is beyond the date range. If `direction` is 'backward`, 
        counts from the last date.
    """
    dts = pd.date_range(
            start=month_first_day(year, month), end=month_last_day(
                    year,month))
    
    if direction=='forward':
        if n < len(dts):
            return dts[n-1].tz_localize(tz)
        else:
            return pd.Timestamp(None) if strict else dts[-1].tz_localize(tz) # type: ignore
    else:
        if n < len(dts):
            return dts[-(n-1)].tz_localize(tz)
        else:
            return pd.Timestamp(None) if strict else dts[0].tz_localize(tz) # type: ignore

@lru_cache()
def nth_weekday(year:int, month:int, weekday:int, n:int=1, tz:str|None=None, strict:bool=False,
                direction:Literal['forward','backward']='forward') -> pd.Timestamp:
    """
        Get the n-th day-of-week of the month or the last such date. 
        If `direction` is 'backward', counts from the last. Returns 
        NaT if out of range and `strict` is True.
    """
    if n == -1:
        return last_weekday(year, month, weekday, tz=tz)
        
    dts = pd.date_range(
            start=month_first_day(year, month), end=month_last_day(
                    year,month))
    
    dts = [dt for dt in dts if dt.weekday()==weekday]
    
    if direction == 'forward':
        if n < len(dts):
            return dts[n-1].tz_localize(tz)
        else:
            return pd.Timestamp(None) if strict else dts[-1].tz_localize(tz) # type: ignore
    else:
        if n < len(dts):
            return dts[-(n-1)].tz_localize(tz)
        else:
            return pd.Timestamp(None) if strict else dts[0].tz_localize(tz) # type: ignore

@lru_cache()
def last_weekday(year:int, month:int, weekday:int, tz:str|None=None) -> pd.Timestamp:
    """
        Get the last day-of-week of the month.
    """
    dts = pd.date_range(
            start=month_first_day(year, month), end=month_last_day(
                    year,month))
    dts = [dt for dt in dts if dt.weekday()==weekday]
    
    return dts[-1].tz_localize(tz)

def bump_sundays(dt:pd.Timestamp|datetime.datetime|date) -> pd.Timestamp|datetime.datetime|date:
    """ 
        Common way of bumping a holiday coninciding with weekends. If 
        it is a Sunday, bump it to next Monday.
    """
    if dt.weekday() == 6:
        return dt + timedelta(days=1)
    else:
        return dt
    
def bump_weekends(dt:pd.Timestamp|datetime.datetime|date) -> pd.Timestamp|datetime.datetime|date:
    """ 
        Common way of bumping a holiday coninciding with weekends. If 
        it is a Sunday, bump it to next Monday. If it is a Saturday, 
        bump it to previous Friday
    """
    if dt.weekday() == 6:
        return dt + timedelta(days=1)
    elif dt.weekday() == 5:
        return dt - timedelta(days=1)
    else:
        return dt


def get_quarterly_expiries(cal:TradingCalendar, start_dt:str|pd.Timestamp, end_dt:str|pd.Timestamp, 
                           weekday:int|None, n:int|None=None,
                           bump:Literal['forward','previous','mod_forward','mod_previous']='previous') -> list[pd.Timestamp]:
    """
        Get quarterly expiry dates. The `weekday` is the day of week 
        for the expiry dates. If `n` is specified it must be an integer
        and is the count of the weekday in the last month of the quarter.
        If n is None, last `weekday` is considered.
    """
    if weekday is None or math.isnan(weekday):
        return []
    
    dts = pd.date_range(start_dt, end_dt, freq='Q')
    quarters = sorted(list(set([(dt.year,dt.quarter) for dt in dts])))
    months = [(q[0],q[1]*3) for q in quarters]
    
    if n:
        dts = [nth_weekday(m[0],m[1],weekday=weekday,n=n) for m in months]
    else:
        dts = [last_weekday(m[0],m[1],weekday=weekday) for m in months]
        
    start_dt = cal.to_open(start_dt)
    end_dt = cal.to_close(end_dt)
    dts = [cal.bump_date(dt, bump) for dt in dts]
    dts = [cal.to_close(dt) for dt in dts]
    
    return [dt for dt in dts if dt >=start_dt and dt <=end_dt]

def get_imm_expiries(cal:TradingCalendar, start_dt:str|pd.Timestamp, end_dt:str|pd.Timestamp) -> list[pd.Timestamp]:
    """
        Get the IMM expiry dates. These are the quarterly 3rd 
        Wednesdays with a modified forward date bump.
    """
    return get_quarterly_expiries(
            cal, start_dt, end_dt,weekday=2,n=3,bump='mod_forward')
    
def get_monthly_expiries(cal:TradingCalendar, start_dt:str|pd.Timestamp, end_dt:str|pd.Timestamp, 
                         weekday:int|None, n:int|None=None,
                         bump:Literal['forward','previous','mod_forward','mod_previous']='previous') -> list[pd.Timestamp]:
    """
        Get monthly expiry dates. The `weekday` is the day of week 
        for the expiry dates. If `n` is specified it must be an integer
        and is the count of the weekday in the month. If n is None, 
        last `weekday` is considered.
    """
    if weekday is None or math.isnan(weekday):
        return []
    
    modified_end_dt = month_last_day(end_dt.year, end_dt.month) # type: ignore
    dts = pd.date_range(start_dt, modified_end_dt, freq='M')
    months = sorted(list(set([(dt.year,dt.month) for dt in dts])))
    
    if n:
        dts = [nth_weekday(m[0],m[1],weekday=weekday,n=n) for m in months]
    else:
        dts = [last_weekday(m[0],m[1],weekday=weekday) for m in months]
        
    start_dt = cal.to_open(start_dt)
    end_dt = cal.to_close(end_dt)
    dts = [cal.bump_date(dt, bump) for dt in dts]
    dts = [cal.to_close(dt) for dt in dts]
    
    return [dt for dt in dts if dt >=start_dt and dt <=end_dt]

def _get_weekly_expiries(cal:TradingCalendar, start_dt:str|pd.Timestamp, end_dt:str|pd.Timestamp, weekday:int|None, 
                        bump:Literal['forward','previous','mod_forward','mod_previous']='previous') -> list[pd.Timestamp]:
    if weekday is None or math.isnan(weekday):
        return []
    
    dts = pd.date_range(start_dt, end_dt)
    dts = [dt for dt in dts if dt.weekday()==weekday]
    
    start_dt = cal.to_open(start_dt)
    end_dt = cal.to_close(end_dt)
    dts = [cal.bump_date(dt, bump) for dt in dts]
    dts = [cal.to_close(dt) for dt in dts]
    
    return [dt for dt in dts if dt >=start_dt and dt <=end_dt]

def get_weekly_expiries(cal:TradingCalendar, start_dt:str|pd.Timestamp, end_dt:str|pd.Timestamp, 
                        weekday:int|None, 
                        bump:Literal['forward','previous','mod_forward','mod_previous']='previous', 
                        monthly_weekday:int|None=None, n:int|None=None) -> list[pd.Timestamp]:
    """
        Get monthly expiry dates. The `weekday` is the day of week 
        for the expiry dates. weekday can be different from monthly 
        weekday parameter. This happens if the monthly expiry day is 
        different from the expiry of the weeklies series.
    """
    if weekday is None or math.isnan(weekday):
        return []
    
    if monthly_weekday is None or monthly_weekday==weekday:
        return _get_weekly_expiries(cal, start_dt, end_dt, weekday, bump)
    
    weeklies = _get_weekly_expiries(cal, start_dt, end_dt, weekday, bump)
    weeklies = {str(dt.weekofyear)+'-'+str(dt.year):dt for dt in weeklies}
    monthlies = get_monthly_expiries(cal, start_dt, end_dt, monthly_weekday, n, bump)
    monthlies = {str(dt.weekofyear)+'-'+str(dt.year):dt for dt in monthlies}
    
    # make sure for the week where the monthly expiries occur the weekly 
    # expiry is replaced by monthly expiy
    weeklies = {**weeklies, **monthlies}
    return [cal.to_close(dt) for k,dt in weeklies.items()]

def _get_expiries(cal, start_dt, end_dt, freq, weekday, n=None, 
                 bump:Literal['forward','previous','mod_forward','mod_previous']='previous', 
                 monthly_weekday=None):
    if weekday is None or math.isnan(weekday):
        return []
    
    frequency = freq.period
    if frequency not in ['W','M','Q']:
        msg = f'Frequency must be weekly, monthly, quarterly or IMM.'
        raise BlueshiftCalendarException(msg)
        
    if frequency == 'W':
        return get_weekly_expiries(
                cal, start_dt, end_dt, weekday, bump=bump,
                monthly_weekday=monthly_weekday, n=n)
    elif frequency == 'M':
        return get_monthly_expiries(
                cal, start_dt, end_dt, weekday, n=n, bump=bump)
    else:
        return get_quarterly_expiries(
            cal, start_dt, end_dt, weekday,n=n,bump=bump)
        
def get_expiries(cal:TradingCalendar, start_dt:str|pd.Timestamp, end_dt:str|pd.Timestamp, frequency:str, 
                 expiry_day:int, nweek:int|None=None, 
                 bump:Literal['forward','previous','mod_forward','mod_previous']='previous', 
                 monthly_weekday:int|None=None, expiry_params:dict|None=None) -> list[pd.Timestamp]:
    """
        Compute expiries between start and end date with optional date 
        dependent expiry parameters. If `expiry_params` is supplied, it 
        must be a dictionary, keyed by the date till the the parameters 
        are applicable (including), agains values of dict of `weekday`,
        `n`, `bump` and `monthly_weekday`. If any of these parameters is
        missing, the value will be picked up from the respective named 
        parameters. If the start and end dates are tz-aware, the date keys 
        in the expiry_params must also be so and in the same timezone. If 
        the last date in the expiry_params does not cover the end date, the 
        remaining expiries (till the end date) is generated based on the 
        named parameters.
        
        Examples:..
        >>> from blueshift.calendar.date_utils import get_expiries
        >>> from blueshift.calendar import get_calendar
        >>> nse = get_calendar('NSE')
        >>> get_expiries(
                nse, '2023-08-01', '2023-09-30', 'W', expiry_day=2, 
                monthly_weekday=3, 
                expiry_params={'2023-09-03':{'expiry_day':3}})
        
        This captures the expiry change of BANKNIFTY weekly option contracts 
        from Thursdays to Wednesdays, starting Sep 04, 2023. In this case, 
        we pass the expiry params only till dates before the switch, and pass 
        the only parameter that changes (expiry day).
    """
    from blueshift.lib.common.constants import Frequency

    if isinstance(frequency, str) and frequency.upper() == 'IMM':
        # 3rd wednesday of IMM (i.e. quarterly) months
        return get_quarterly_expiries(
            cal, start_dt, end_dt,weekday=2,n=3,bump='mod_forward')
        
    try:
        freq = Frequency(frequency)
    except Exception as e:
        msg = f'Invalid frequency {frequency}:{str(e)}'
        raise BlueshiftCalendarException(msg)
        
    try:
        start_dt = pd.Timestamp(start_dt).tz_localize(None)
        end_dt = pd.Timestamp(end_dt).tz_localize(None)
    except Exception as e:
        msg = f'Invalid start or end date:{str(e)}'
        raise BlueshiftCalendarException(msg)
        
    if not expiry_params:
        return _get_expiries(cal, start_dt, end_dt, freq, expiry_day, n=nweek, 
                 bump=bump, monthly_weekday=monthly_weekday)
        
    end = start_dt - pd.Timedelta(days=1)
    out = []
    
    for key in expiry_params:
        start = end + pd.Timedelta(days=1)
        dt = pd.Timestamp(key).tz_localize(None)
        if dt < start:
            continue
        
        end = dt if dt < end_dt else end_dt
        if 'expiry_day' in expiry_params.get(key):                      # type: ignore
            weekday = expiry_params.get(key).get('expiry_day')          # type: ignore
        else:
            weekday = expiry_day
        n = expiry_params.get(key).get('nweek') or nweek                # type: ignore
        bump_day = expiry_params.get(key).get('bump') or bump           # type: ignore
        m_weekday = expiry_params.get(key).get(                         # type: ignore
                'monthly_weekday') or monthly_weekday
        out.extend(_get_expiries(cal, start, end, freq, weekday, n=n, 
                 bump=bump_day, monthly_weekday=m_weekday))
        if end >= end_dt:
            break
        
    if end < end_dt:
        start = end + pd.Timedelta(days=1)
        end = end_dt
        out.extend(_get_expiries(cal, start, end, freq, expiry_day, n=nweek, 
                 bump=bump, monthly_weekday=monthly_weekday))
        
    start_dt = make_consistent_tz(start_dt, cal.tz)
    out = [dt for dt in out if dt >=start_dt]
    return out
        
def subtract_minutes(time:str|datetime.time, minutes:int, now:pd.Timestamp|None=None) -> datetime.time:
    import datetime

    if now is None:
        now = pd.Timestamp.now()
        
    if isinstance(time, str):
        at = time.split(':')
        time = datetime.time(*[int(s) for s in at]) # type: ignore
    
    target = datetime.datetime.combine(now.date(), time)
    target = target - datetime.timedelta(minutes=minutes)
    
    return target.time()

def datetime_time_to_nanos(dt:pd.Timestamp|datetime.time) -> int:
    return (dt.hour*60 + dt.minute)*60*NANO_IN_SECONDS

def unix_time_to_timestamp(ts:int) -> pd.Timestamp:
    """ Convert a number represnting Unix timestamp to timestamp. """
    ts = int(ts)
    return pd.to_datetime(ts, unit='s').tz_localize('Etc/UTC')

def get_aligned_timestamp(timestamp:pd.Timestamp, frequency:str|Frequency) -> pd.Timestamp:
    """ round up and fetch the nearest minute. """
    if isinstance(frequency, str):
        frequency = Frequency(frequency) # type: ignore
    offset = 0
    
    if frequency == Frequency('day') and timestamp.tz:
        #offset = timestamp.utcoffset().total_seconds()*NANO_IN_SECONDS
        return timestamp.normalize()
    
    if frequency in _memoized_nanos:
        nano = _memoized_nanos[frequency]
    else:
        nano = int(frequency.minute_bars*60*NANO_IN_SECONDS) # type: ignore
        _memoized_nanos[frequency] = nano
        
    resid = timestamp.value % nano + offset
    if resid:
        # bump to next alighment for inbetween times
        return pd.Timestamp(
                timestamp.value - resid + nano, tz=timestamp.tz)
    else:
        # case of perfect alignment
        return pd.Timestamp(timestamp.value, tz=timestamp.tz)
    

def local_tznames() -> list[str]:
    """ return all matching timezone names for the local settings. """
    import pytz
    offset = time.timezone
    tznames = []
    for tz in pytz.all_timezones:
        try:
            tz_offset = pytz.timezone(tz).utcoffset(pd.Timestamp.now()).\
                        total_seconds()
        except Exception:
            continue
        else:
            if -offset == tz_offset:
                tznames.append(tz)
            
    return tznames

def local_tzname(default:str|None=None) -> str|None:
    """ return the first match for timezones given local settings. """
    tznames = local_tznames()
    if tznames:
        return tznames[0]
    elif default:
        return default
    else:
        return None

def filter_dates(df:pd.Series|pd.DataFrame, cal:TradingCalendar, 
                 use_time:bool=False) -> pd.Series|pd.DataFrame:
    """ 
        check input dataframe `df` and keep indices which are valid 
        sessions according to trading calendar `cal`. If `use_time` is
        True, will also filter out indices outside the open and close 
        of the calendar.
    """
    if not df.index.tz:                                                 # type: ignore
        df.index = df.index.tz_localize('Etc/UTC').tz_convert(cal.tz)   # type: ignore
    sessions = cal.sessions(df.index[0], df.index[-1]).date             # type: ignore
    dts = df.index.date                                                 # type: ignore
    mask = [date in sessions for date in dts]
    df = df[mask]
    
    if not use_time:
        return df
    
    return df.between_time(cal.open_time, cal.close_time)

def filter_by_calendar(df:pd.Series|pd.DataFrame, cal:TradingCalendar) -> pd.Series|pd.DataFrame:
    """ to be deprecated. """
    if not df.index.tz:                                                 # type: ignore
        df.index = df.index.tz_localize('Etc/UTC').tz_convert(cal.tz)   # type: ignore
    sessions = cal.sessions(df.index[0], df.index[-1]).date             # type: ignore
    dts = df.index.date                                                 # type: ignore
    mask = [date in sessions for date in dts]
    df = df[mask]
    return df.between_time(cal.open_time, cal.close_time)
    

def filter_expiries(dts:pd.DatetimeIndex|list[pd.Timestamp], cal:TradingCalendar, frequency:str, 
                    weekday:int, n:int|None=None, 
                    bump:Literal['forward','previous','mod_forward','mod_previous']='previous', 
                    monthly_weekday:int|None=None,
                    expiry_params:dict|None=None) -> list[pd.Timestamp]:
    """ Filter a given set of dates by a calendar. """
    return get_expiries(
            cal, dts[0], dts[-1], frequency, weekday, nweek=n, bump=bump,
            monthly_weekday=monthly_weekday, expiry_params=expiry_params)

def filter_for_session(df:pd.DataFrame|pd.Series, period:str|Frequency, 
                       calendar:TradingCalendar) -> pd.DataFrame|pd.Series:
    """
        Filter an input series for valid sessions and valid trading 
        hours based on a TradingCalendar.
        
        Args:
            ``df (dataframe or series)``: Input time-series.
            
            ``period (str)``: Target period string.
            
            ``calendar (TradingCalendar)``: Calendar for filtering.
            
        Returns:
            Series or DataFrame. Aggregated and na-filled.
            
    """
    valid_sessions = calendar.sessions(df.index[0],df.index[-1])
    freq = Frequency(period)
    
    if freq < Frequency('D'):
        dt = datetime.datetime(
            2000, 1, 1, calendar.open_time.hour, 
            calendar.open_time.minute, 
            calendar.open_time.second)
        
        start_time = dt + timedelta(minutes=int(freq.minute_bars))
        start_time = start_time.time()
        df = df.between_time(start_time, calendar.close_time)
        
    df = df[df.index.normalize().isin(valid_sessions)] # type: ignore
    return df