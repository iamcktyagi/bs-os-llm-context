from __future__ import annotations
from typing import TYPE_CHECKING, Literal, Callable, Generator, Any
import os
from abc import ABC, abstractmethod
from functools import lru_cache
from datetime import date, time
from bisect import bisect_left
from collections import namedtuple
from enum import Enum

from blueshift.lib.exceptions import BlueshiftCalendarException
from blueshift.lib.common.constants import Frequency
from blueshift.lib.common.enums import ChunkerType
from blueshift.lib.common.types import Chunker

from .calendar_dispatch import get_calendar
from .date_utils import (
        quarter_last_day, quarter_first_day, month_first_day, 
        month_last_day, get_expiries)

if TYPE_CHECKING:
    import pandas as pd
    from .trading_calendar import TradingCalendar
else:
    import blueshift.lib.common.lazy_pandas as pd

ExpiryEpoch = namedtuple('ExpiryEpoch',['start_dt','end_dt'])

def _filter_expiries(dts:pd.DatetimeIndex|list[pd.Timestamp], freq:Frequency|str='monthly') -> pd.DatetimeIndex:
    mapping = {'M':'month','W':'week','Q':'quarter'}
    freq = Frequency(freq)
    
    def check_period(dt, v, period, freq):
        if v!=period:
            return False
        
        if freq.period=='Q':
            if dt.month%3 != 0:
                return False
            
        return True
    
    def find_last_locs(dts, values, periods):
        locs = []
        for period in periods:
            subset = [i for i,v in enumerate(values) if check_period(
                    dts[i], v, period, freq)]
            if subset:
                locs.append(subset[-1])
                
        return dts[locs]
    
    if freq.period not in mapping:
        return pd.DatetimeIndex([])
    
    years = sorted(list(set([dt.year for dt in dts])))
    out = []
    for year in years:
        dts_year = pd.DatetimeIndex([dt for dt in dts if dt.year==year])
        values = [getattr(dt,mapping[freq.period]) for dt in dts_year]
        periods = sorted(list(set(values)))
        out.extend(find_last_locs(dts_year, values, periods))
    
    return pd.DatetimeIndex(sorted(out))

# exported function
filter_expiries = _filter_expiries

class DateChunker(Chunker):
    """ data chunk keys based on dates. """
    _valid_chunk_freq = set(['m','q','y'])
    
    def __init__(self, chunksize=1):
        """
            A class for managing date based chunking of data files. 
            The input frequency can be an integer or a string. Integer 
            input is treated as chunk size in years. String input can 
            be either 'y', 'q' or 'm', respectively for yearly, quarterly 
            or monthly chunking. The function `key_gen` is the main 
            interface, which takes in a start and an end date, and 
            generates a list of tuples (key, epoch). The first value is 
            the unique key for the chunk and second is the date epoch 
            for the chunk.
        """
        self._extract_chunk_spec(chunksize)
        self._make_freq_dispatch()
        
    @property
    def type(self):
        return ChunkerType.DATE
        
    def _extract_chunk_spec(self, chunksize:int|Literal['m','q','y']):
        if isinstance(chunksize, str):
            chunksize = chunksize.lower() # type: ignore
            if chunksize not in self._valid_chunk_freq:
                try:
                    chunksize = int(chunksize)
                except Exception:
                    raise BlueshiftCalendarException(f'illegal input {chunksize}.')
        else:
            try:
                chunksize = int(chunksize)
            except Exception:
                raise BlueshiftCalendarException(f'illegal input {chunksize}.')
            
        if not isinstance(chunksize, str):
            self._chunk_fraction = None
            self._chunk_year = chunksize
            self._chunk_freq = 'y'
            return
                
        if chunksize.lower() not in self._valid_chunk_freq:
            raise BlueshiftCalendarException(f'illegal input {chunksize}.')
            
        self._chunk_freq = chunksize.lower()
        if self._chunk_freq == 'y':
            self._chunk_fraction = None
            self._chunk_year = 1
        elif self._chunk_freq == 'q':
            self._chunk_fraction = 4
            self._chunk_year = None
        else:
            self._chunk_fraction = 12
            self._chunk_year = None
    
    @lru_cache()
    def _translate_index(self, index:int) -> int:
        if not self._chunk_year:
            raise BlueshiftCalendarException('chunk not based on years.')
        
        ratio = index/self._chunk_year
        int_ratio = int(ratio)
        if int_ratio == ratio:
            return int_ratio*self._chunk_year
        return (int_ratio + 1)*self._chunk_year
        
    def _make_freq_dispatch(self):
        self._freq_dispatch:dict[str, Callable[[pd.Timestamp],tuple[int,int]]] = {}
        
        def _map_year(timestamp:pd.Timestamp):
            index = self._translate_index(timestamp.year)
            return -1, index
        
        def _map_quarter(timestamp:pd.Timestamp):
            int_frac = int(timestamp.month/3)
            if int_frac == timestamp.month/3:
                quarter = int_frac
            else:
                quarter = int(timestamp.month/3)+1
            year = timestamp.year
            return quarter, year
        
        def _map_month(timestamp:pd.Timestamp):
            month = timestamp.month
            year = timestamp.year
            return month, year
        
        self._freq_dispatch['y'] = _map_year
        self._freq_dispatch['q'] = _map_quarter
        self._freq_dispatch['m'] = _map_month
        
        for key in self._valid_chunk_freq:
            if key not in self._freq_dispatch:
                raise BlueshiftCalendarException(
                        'missing frequency dispatch for {key}.')
        
    def _translate(self, timestamp:pd.Timestamp):
        return self._freq_dispatch[self._chunk_freq](timestamp)
    
    @lru_cache()
    def _key_gen(self, start_idx:int, start_year:int, end_idx:int, 
                 end_year:int) -> list[tuple[int,int,date]]:
        out:list[tuple[int,int,date]] = []
        if start_idx==-1:
            if not self._chunk_year:
                raise BlueshiftCalendarException(f'not chunk for year defined.')
            for year in range(start_year, end_year+1, self._chunk_year):
                out.append((-1, year, date(year, month=12, day=31)))
            return out
        
        if not self._chunk_fraction:
            raise BlueshiftCalendarException(f'not chunk for year fraction defined.')
        years = [y for y in range(start_year, end_year+1)]
        periods = [f for f in range(1,self._chunk_fraction+1)]
        
        n = len(years)
        for i, year in enumerate(years):
            for period in periods:
                if i==0 and period < start_idx:
                    continue
                if i==(n-1) and period > end_idx:
                    return out
                if self._chunk_freq == 'y':
                    epoch = date(year, month=12, day=31)
                elif self._chunk_freq == 'q':
                    epoch = quarter_last_day(year, period)
                else:
                    epoch = month_last_day(year, period)
                out.append((period, year, epoch))
                
        return out
        
    def key_gen(self, start_dt:pd.Timestamp, end_dt:pd.Timestamp, 
                reverse:bool=False) -> list[tuple[str,date]]:
        """
            Generate a list of tuples with key and date epoch for each
            chunks in the date range.
            
            Args:
                `start_dt (timestamp)`: Starting timestamp.
                
                `end_dt (timestamp)`: Ending timestamp (inclusive).
                
                `reverse (bool)`: If keys should be in reverse order.
                
            Returns:
                List of tuples (str, datetime.date) - chunking key and the 
                corresponding date epoch.
        """
        start_idx, start_year = self._translate(start_dt)
        end_idx, end_year = self._translate(end_dt)
        
        if isinstance(self._chunk_freq, str) and self._chunk_freq != 'y':
            freq = self._chunk_freq.upper()
            keys = [(str(year)+ freq + str(period), epoch) for \
                    period, year, epoch in self._key_gen(
                    start_idx, start_year, end_idx, end_year)]
        else:
            keys = [(str(year), epoch) for period, year, epoch in \
                    self._key_gen(
                            start_idx, start_year, end_idx, end_year)]
    
        if reverse:
            return [key for key in reversed(keys)]
        
        return keys
    
    def map_key(self, dt:pd.Timestamp, *args, **kwargs) -> str:
        """
            Map a date-time to a particular chunking key.
        """
        idx, year = self._translate(dt)
        
        if not isinstance(self._chunk_freq, str):
            return str(year)
        elif self._chunk_freq == 'y':
            return str(year)
        else:
            freq = self._chunk_freq.upper()
            return str(year)+ freq + str(idx)
        
    def get_epochs(self, key:str) -> tuple[date, date]:
        """
            Get the start and end date for a given chunking key.
        """
        if 'M' in key:
            year, month = tuple(key.split('M'))
            start_dt = month_first_day(int(year),int(month))
            end_dt = month_last_day(int(year),int(month))
        elif 'Q' in key:
            year, quarter = tuple(key.split('Q'))
            start_dt = quarter_first_day(int(year),int(quarter))
            end_dt = quarter_last_day(int(year),int(quarter))
        else:
            try:
                year = int(key)
            except Exception:
                raise BlueshiftCalendarException('not a valid key for a chunk {key}')
            
            if not self._chunk_year:
                raise BlueshiftCalendarException(f'no year chunk defined.')
            
            start_year = year - self._chunk_year + 1
            start_dt = date(year=start_year, month=1, day=1)
            end_dt = date(year=year, month=12, day=31)
            
        return start_dt, end_dt
    
class ExpiryParams():
    """
        Expiry parameters with support for changing parameters over 
        dates. The input is a dict of dict. The outer dict is keyed by
        dates till when the particular inner dict is valid. The inner dict 
        is the set of params valid till the key date. The inner dict is 
        of structure {'expiry_day':value,'nweek':value, 'bump':value,
        'monthly_weekday':value}. The expiry day is the day of week for 
        expiry. The `nweek` parameter is the week (number) of the month 
        for which we have the expiry. Finally, `monthly_weekday` is the 
        day of the week for monthly expiry. The last one is only applicable
        for a weekly expiry chunker that also incorporates a different 
        monthly expiry day of week.
        
        Note:..
            For asset dependent expiry chunkers, use ExpiryDispatch. For 
            date dependent expiry chunkers, use ExpiryParams.
    """
    def __init__(self, params:dict):
        self._params = {}
        self.tz = params.pop('tz', None)
        self.from_dict(params)
        
    @property
    def params(self) -> dict:
        return self._params
        
    def from_dict(self, data):
        """ deserialize from json dict."""
        params = {}
        for dt in data:
            if self.tz:
                key = pd.Timestamp(dt, tz=self.tz)
            else:
                key = pd.Timestamp(dt)
            params[key] = data[dt]
        self._params = params
        
    def to_dict(self) -> dict:
        """ serialize to json dict. """
        params = {}
        for key in self.params:
            if self._params[key] is None:
                continue
            params[str(key)] = self._params[key]
        
        if self.tz:
            params['tz'] = self.tz
        
        return params
    
class ExpiryChunker(Chunker):
    """ 
        Chunk keys based on expiry dates. The argument `expiries` 
        must be a pandas DatetimeIndex. This implementation is for 
        monthly/ weekly expiries and NOT for 24-hours markets. The parameter 
        `expiries` provide a list of expiry dates. If this is not 
        provided, then `calendar` and `expiry_day` must be provided to 
        compute the expiry dates based on the trading calendar. If 
        both provided, only `expiries` will be considered. If expiry_params 
        is supplied, `expiry_day`, `nweek`, `bump` and `monthly_weekday` 
        will be ignored if those keys are present.
        
        Args:
            `expiries (pd.DatetimeIndex)`: A list of expiry dates.
            
            `frequency (str)`: Frequency of the expiries.
            
            `expiry_day (int)`: Day-of-week for expiry.
            
            `roll_day (int)`: Roll day offset from expiry day.
            
            `roll_time (tuple)`: Hour, minute tuple of roll time.
            
            `calendar (TradingCalendar)`: Calendar for the expiry.
            
            `nweek (int)`: Week of the month for expiry.
            
            `bump (str)`: Date bump rules for expiry.
            
            `monthly_weekday(int)`: Weekday for monthly expiries.
            
            `expiry_params(dict)`: Expiry parameters.
    """
    
    def __init__(self, name:str, expiries=pd.DatetimeIndex([]), frequency:Frequency|str='monthly', 
                 expiry_day:int=3, roll_day:int=0, roll_time:tuple[int,int]=(23,59), 
                 calendar:TradingCalendar|None=None, nweek:int|None=None, 
                 bump:Literal['forward', 'previous', 'mod_forward', 'mod_previous']="previous", 
                 monthly_weekday:int|None=None, expiry_params:ExpiryParams|dict|None=None, 
                 save_expiries:bool=True):
        if expiry_params is None:
            expiry_params = {}
        if not isinstance(expiry_params, ExpiryParams):
            self._expiry_params = ExpiryParams(expiry_params)
        else:
            self._expiry_params = expiry_params
            
        self.name = name
        self._expiries = expiries.tz_localize(None).normalize()
        self._freq = Frequency(frequency)
        
        if self._freq.period != 'M' and calendar is None:
            raise NotImplementedError(
                    'Only monthly frequency implemented from expiries list.')
        
        self._nweek = nweek
        self._bump = bump
        self._monthly_weekday = monthly_weekday
        self._cal = calendar
        if isinstance(calendar, str):
            self._cal = get_calendar(calendar)
        
        self._save_expiries = save_expiries
        self._expiries = _filter_expiries(self._expiries, self._freq)
        self._expiries_set = set(self._expiries)
        self._expiry_day = expiry_day
        self._roll = roll_day
        
        if isinstance(roll_time, time):
            self._roll_time = roll_time
        else:
            self._roll_time = time(*roll_time)
        
        self.reset()
        
    def __str__(self) -> str:
        return f"ExpiryChunker[{self.name}]"
    
    def __repr__(self) -> str:
        return self.__str__()
    
    @property
    def type(self) -> ChunkerType:
        return ChunkerType.EXPIRY
        
    def add_expiry(self, expiry:pd.Timestamp|str):
        expiry = pd.Timestamp(expiry).tz_localize(None).normalize()
        
        if expiry in self.expiries:
            return
        
        expiries = self.expiries.tolist() + [expiry]
        self._expiries = pd.DatetimeIndex(sorted(expiries))
        self._expiries = _filter_expiries(self._expiries, self._freq)
        self._expiries_set = set(self._expiries)
        
    @property
    def expiries(self) -> pd.DatetimeIndex:
        if len(self._expiries)==0:
            try:
                self._expiries = self._calc_expiry()
            except Exception:
                pass
            else:
                self._expiries_set = set(self._expiries)
        return self._expiries
    
    @property
    def freq(self) -> Frequency:
        return self._freq
    
    @property
    def frequency(self) -> str:
        freq = self._freq
        if freq.period == 'W':
            return 'weekly'
        elif freq.period == 'M':
            return 'monthly'
        elif freq.period == 'Q':
            return 'quarterly'
        else:
            return self._freq.period
    
    @property
    def roll(self) -> tuple[int, time]:
        return (self._roll, self._roll_time)
        
    @roll.setter
    def roll(self, value:tuple[int,time]|tuple[int,tuple[int,int]]):
        roll_day, roll_time = value
        
        if isinstance(roll_time, time):
            hour, minute = roll_time.hour, roll_time.minute
        else:
            hour, minute = roll_time
        
        if not hour >=0 and hour <=23:
            raise BlueshiftCalendarException('illegal hour value {hour}')
            
        if not minute >=0 and minute <=59:
            raise BlueshiftCalendarException('illegal minute value {minute}')
        
        self._roll = roll_day
        self._roll_time = time(*(hour, minute))
        
    @property
    def roll_day(self) -> int:
        return self._roll
    
    @roll_day.setter
    def roll_day(self, value:int):
        self._roll = int(value)
    
    @property
    def roll_time(self) -> time:
        return self._roll_time
    
    @roll_time.setter
    def roll_time(self, value:time|tuple[int,int]):
        if isinstance(value, time):
            self._roll_time = value
        else:
            self._roll_time = time(*value)
        
    @property
    def expiry_day(self) -> int:
        return self._expiry_day
    
    @expiry_day.setter
    def expiry_day(self, value:int):
        self._expiry_day = value
        
    def valid_expiry(self, expiry:pd.Timestamp) -> bool:
        _ = self.expiries
        return expiry in self._expiries_set
        
    def reset(self, expiries:pd.DatetimeIndex=pd.DatetimeIndex([])):
        if len(expiries) !=0:
            expiries = expiries.tz_localize(None).normalize()
            expiries = self.expiries.tolist() + expiries.tolist() # type: ignore
            self._expiries = pd.DatetimeIndex(sorted(expiries))
            self._expiries = _filter_expiries(self._expiries, self._freq)
            self._expiries_set = set(self._expiries)
            
    def _calc_expiry(self, start_dt:pd.Timestamp|None=None, 
                     end_dt:pd.Timestamp|None=None) -> pd.DatetimeIndex:
        if not self._cal:
            return pd.DatetimeIndex([])
        
        if not start_dt:
            start_dt = self._cal.start.tz_localize(None)
        if not end_dt:
            end_dt = self._cal.end.tz_localize(None)
        
        expiries = get_expiries(self._cal, start_dt, end_dt, self._freq.period, 
                                self._expiry_day, self._nweek, self._bump, # type: ignore
                                monthly_weekday=self._monthly_weekday,
                                expiry_params=self._expiry_params.params)
        expiries = [e.normalize().tz_localize(None) for e in expiries]
        return pd.DatetimeIndex(sorted(expiries))
        
    def _get_expiry_index(self, dt:pd.Timestamp, offset=0) -> int:
        if len(self.expiries) == 0:
            raise BlueshiftCalendarException('No or illegal expiries in store.')
        
        dt = dt.tz_localize(None)
        if dt.time() > self._roll_time:
            dt = dt + pd.Timedelta(days=1)
        
        # we assume no expiry queries after market hours, so we can 
        # normalize to fetch the expiry date
        dt = dt.normalize()
        idx = bisect_left(self.expiries, dt)
        idx = idx + offset
        
        return min(idx, len(self.expiries)-1)
    
    def key_gen(self, start_dt:pd.Timestamp, end_dt:pd.Timestamp, reverse:bool=False, 
                offset:int=0) -> list[tuple[str, ExpiryEpoch]]|Generator[tuple[str, ExpiryEpoch]]:
        """
            Generate epoch keys between start and end date.
        """
        if reverse:
            return self._reverse_key_gen(start_dt, end_dt, offset=offset)
        
        start_dt = start_dt.tz_localize(None)
        end_dt = end_dt.tz_localize(None)
        
        start_offset = start_dt + pd.Timedelta(days=self._roll)
        end_offset = end_dt + pd.Timedelta(days=self._roll)
        _ = self.expiries
        
        # special case of where start_dt == end_dt == some expiry date
        # we are trying to read data for a specific expiry, and the 
        # epoch is valid for whole date range, so we send pd.NaT
        if start_dt in self._expiries_set and start_dt==end_dt:
            epoch = ExpiryEpoch(pd.NaT, pd.NaT)
            return [(end_dt.strftime('%Y-%m-%d'), epoch)]
        
        # case for continuous expiries
        start_idx = self._get_expiry_index(start_offset, offset=offset)
        end_idx = self._get_expiry_index(end_offset, offset=offset)
        start_0 = self._get_expiry_index(start_offset, offset=0)
        end_0 = self._get_expiry_index(end_offset, offset=0)
        
        expiries = [self.expiries[idx] \
                    for idx in range(start_idx, end_idx+1)]
        
        periods = [self.expiries[idx] \
                    for idx in range(start_0, end_0+1)]
        
        # compute roll date, add 1 day to match to market close to 
        # midnight. This breaks down for 24-hours markets !!
        rolls = [exp - pd.Timedelta(days=self._roll-1) for exp in periods]
        starts = [start_dt] + rolls[:-1]
        rolls[-1] = end_dt
        
        epochs = [ExpiryEpoch(start, roll) for start,roll in zip(starts, rolls)]
        
        keys = [(expiry.strftime('%Y-%m-%d'), epoch) for expiry, epoch \
                    in zip(expiries, epochs)]
        
        return keys
    
    def _reverse_key_gen(self, start_dt:pd.Timestamp, end_dt:pd.Timestamp, 
                         offset:int=0) -> Generator[tuple[str, ExpiryEpoch]]:
        """
            Generate epoch keys between start and end date.
        """
        start_dt = start_dt.tz_localize(None)
        end_dt = end_dt.tz_localize(None)
        
        start_offset = start_dt + pd.Timedelta(days=self._roll)
        end_offset = end_dt + pd.Timedelta(days=self._roll)
        _ = self.expiries
        
        # special case of where start_dt == end_dt == some expiry date
        # we are trying to read data for a specific expiry, and the 
        # epoch is valid for whole date range, so we send pd.NaT
        if start_dt in self._expiries_set and start_dt==end_dt:
            epoch = ExpiryEpoch(pd.NaT, pd.NaT)
            yield (end_dt.strftime('%Y-%m-%d'), epoch)
            return
        
        # case for continuous expiries
        start_idx = self._get_expiry_index(start_offset, offset=offset)
        end_idx = self._get_expiry_index(end_offset, offset=offset)
        start_0 = self._get_expiry_index(start_offset, offset=0)
        end_0 = self._get_expiry_index(end_offset, offset=0)
        
        end = end_dt
        for idx1, idx2 in zip(range(end_idx, start_idx-1, -1),
                              range(end_0, start_0-1, -1)):
            expiry = self.expiries[idx1]
            period = self.expiries[idx2-1]
            # compute roll date, add 1 day to match to market close to 
            # midnight. This breaks down for 24-hours markets !!
            roll = period - pd.Timedelta(days = self._roll-1)
            
            if idx2 == 0 or roll.value < start_dt.value:
                roll = start_dt
            
            epoch = ExpiryEpoch(roll, end)
            yield expiry.strftime('%Y-%m-%d'), epoch
            end = roll
        
    def map_key(self, dt:pd.Timestamp, offset:int=0, expiry:pd.Timestamp|None=None, 
                *args, **kwargs) -> str:
        if expiry:
            return pd.Timestamp(expiry).strftime('%Y-%m-%d')
        
        dt = dt.tz_localize(None)
        return self.get_expiry(dt, offset=offset).strftime('%Y-%m-%d')
    
    def get_epochs(self, key:pd.Timestamp) -> tuple[None, date]:
        return None, pd.Timestamp(key).date()
    
    def get_expiry(self, dt:pd.Timestamp, offset:int=0) -> pd.Timestamp:
        dt = pd.Timestamp(dt).tz_localize(None)
        idx = self._get_expiry_index(dt, offset)
        return self.expiries[idx]
        
    def to_dict(self, root:str|None=None) -> dict[str, Any]:
        expiry_path:str|None = None
        
        if self._save_expiries and len(self.expiries) > 0:
            expiries = pd.DataFrame(self.expiries, columns=['expiry'])
            if not root:
                msg = f'Root must be supplied to save expiry info.'
                raise BlueshiftCalendarException(msg)
            
            # save expiry and return the path to the json info
            expiry_file = self.name + '_expiry.csv'
            expiry_path = os.path.join(root, expiry_file)
            expiries.to_csv(expiry_path)
        
        calendar = None
        if self._cal:
            calendar = self._cal.name
            
        return {'name':self.name,
                'frequency':self.frequency,
                'expiry_day':self.expiry_day,
                'nweek':self._nweek,
                'bump':self._bump,
                'monthly_weekday':self._monthly_weekday,
                'expiry_params':self._expiry_params.to_dict(),
                'calendar':calendar,
                'roll_day':self._roll,
                'roll_time':(self.roll_time.hour, self.roll_time.minute),
                'expiries':expiry_path,
                'save_expiries':self._save_expiries}
        
    @classmethod
    def from_dict(cls, data:dict, calendar:TradingCalendar|None=None) -> ExpiryChunker:
        expiry_path = data['expiries']
        if isinstance(expiry_path, str):
            expiries = pd.read_csv(expiry_path).expiry.tolist()
            expiries = pd.DatetimeIndex(
                    pd.to_datetime(expiries))
            data['expiries'] = expiries
        else:
            if expiry_path is not None:
                data['expiries'] = pd.DatetimeIndex(expiry_path)
            else:
                data['expiries'] = pd.DatetimeIndex([])
        
        if 'calendar' not in data or not data['calendar']:
            data['calendar'] = calendar
        else:
            if calendar:
                if calendar.name != data['calendar']:
                    msg = f'Calendar name mismatch for expiry chunker: '
                    msg += f'Got {data["calendar"]} against {calendar.name}.'
                    raise BlueshiftCalendarException(msg)
                data['calendar'] = calendar
            
        return cls(**data)
    
class ExpiryDispatch:
    """
        A dispatch for expiry chunker (with potentially different expiry 
        cycle, date etc.) based on asset (underlying). The default expiry
        chunker is returned if a match is not found. The parameter `mapping`
        is a dict of dict, with first keyed by assets and then keyed by 
        frequency and maps a (asset, frequency) pair to a chunker. The 
        parameter `chunker` is a dict that maps a name to a chunker. Finally
        the parameter default is the name of the default chunker if an 
        asset does not match with any of the mappings.
    """
    def __init__(self, chunkers:dict[str,ExpiryChunker], 
                 asset_dispatch:dict[str,dict[str,ExpiryChunker]], 
                 default_dispatch:dict[str,ExpiryChunker], default:ExpiryChunker|str|None=None):
        self.asset_dispatch = asset_dispatch
        self.default_dispatch = default_dispatch
        self.chunkers = chunkers
        
        if default is None:
            if len(chunkers) > 0:
                self.default = list(chunkers.values())[0]
        elif isinstance(default, str):
            if default not in self.chunkers:
                msg = f'default chunker {default} not registerd.'
                raise BlueshiftCalendarException(msg)
            self.default = self.chunkers[default]
        else:
            self.chunkers[default.name] = default
            self.default = default
            
    def __str__(self):
        chunkers = '|'.join(self.chunkers.keys())
        return f'ExpiryDispatch[{chunkers}]'
    
    def __repr__(self):
        return self.__str__()
        
    def add(self, chunker:ExpiryChunker, assets:list[str]=[]):
        if chunker.name not in self.chunkers:
            self.chunkers[chunker.name] = chunker
        
        if self.default is None and not assets:
            self.default = chunker
        
        if assets:
            for asset in assets:
                if asset not in self.asset_dispatch:
                    self.asset_dispatch[asset] = {}
                self.asset_dispatch[asset][chunker.frequency] = chunker
        else:
            self.default_dispatch[chunker.frequency] = chunker
            
    def get(self, underlying:str, frequency:str|None=None, 
            expiry:pd.Timestamp|None=None) -> ExpiryChunker|None:
        if expiry:
            expiry = pd.Timestamp(expiry).tz_localize(None)
            expiry = expiry.normalize()
            
        if underlying == 'default':
            # bypass the logic for default case
            if not expiry or expiry in self.default.expiries:
                return self.default
            candidates = list(self.default_dispatch.values())
            for chunker in candidates:
                if chunker.valid_expiry(expiry):
                    return chunker
            # no match
            return
        
        if frequency:
            if underlying in self.asset_dispatch:
                chunker = self.asset_dispatch[underlying].get(frequency)
                if chunker:
                    if not expiry:
                        return chunker
                    elif expiry and chunker.valid_expiry(expiry):
                        return chunker
            
            chunker = self.default_dispatch.get(frequency)
            if not chunker:
                return
            elif not expiry:
                return chunker
            elif chunker.valid_expiry(expiry):
                return chunker
            else:
                # no match
                return
            
        if underlying not in self.asset_dispatch:
            candidates = list(self.default_dispatch.values())
        else:
            candidates = list(self.asset_dispatch[underlying].values())
            freqs = set([c.frequency for c in candidates])
            defaults = [c for _,c in self.default_dispatch.items() \
                            if c.frequency not in freqs]
            if defaults:
                candidates.extend(defaults)
        
        candidates = sorted(candidates, key=lambda x: x.freq)
        if not expiry:
            return candidates[0]
        
        for chunker in candidates:
            if chunker.valid_expiry(expiry):
                return chunker
            
        # no match
        return
        
    
    def to_dict(self, root:str|None=None) -> dict:
        mapping:dict[str,list[str]] = {}
        for asset in self.asset_dispatch:
            chunkers_dict:dict[str,ExpiryChunker] = self.asset_dispatch[asset]
            for _, chunker in chunkers_dict.items():
                if chunker.name in mapping:
                    mapping[chunker.name].append(asset)
                else:
                    mapping[chunker.name] = [asset]
        
        chunkers:list[dict] = []
        for name in self.chunkers:
            info = self.chunkers[name].to_dict(root)
            info['asset'] = mapping.get(name, [])
            chunkers.append(info)
            
        data:dict[str, Any] = {}
        if self.default:
            data['default'] = self.default.name
        else:
            data['default'] = None
            
        data['chunkers'] = chunkers
        return data
    
    @classmethod
    def from_dict(cls, data:dict, calendar:TradingCalendar|None=None):
        if isinstance(calendar, str):
            calendar = get_calendar(calendar)
            
        default:str = data['default']
        chunkers:dict[str,ExpiryChunker] = {}
        asset_dispatch:dict[str,dict[str,ExpiryChunker]] = {}
        default_dispatch:dict[str,ExpiryChunker] = {}
        
        for packet in data['chunkers']:
            cal = packet.get('calendar', None)
            if isinstance(cal, str):
                cal = get_calendar(cal)
            
            cal = cal or calendar
            if not cal:
                raise BlueshiftCalendarException(f'no calendar specified for chunker {packet}')

            assets = packet.pop('asset',[])
            chunker = ExpiryChunker.from_dict(packet, cal)
            chunkers[chunker.name] = chunker
            if assets:
                for asset in assets:
                    if asset not in asset_dispatch:
                        asset_dispatch[asset] = {}
                    asset_dispatch[asset][chunker.frequency] = chunker
            else:
                default_dispatch[chunker.frequency] = chunker
        
        return cls(chunkers, asset_dispatch, default_dispatch, default=default)