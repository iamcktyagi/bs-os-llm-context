from __future__ import annotations
from typing import TYPE_CHECKING, cast, Any

import numpy as np
import uuid
import numbers
import bisect

from pandas import Timestamp

from blueshift.calendar import TradingCalendar, get_calendar,make_consistent_tz
from blueshift.lib.common.types import ListLike
from blueshift.lib.common.constants import (
        OHLCV_COLUMNS, OHLCV_DTYPE, Frequency, OHLCVX_COLUMNS)
from blueshift.interfaces.assets._assets import MarketData
from blueshift.interfaces.data.store import DataStore, register_data_store
from blueshift.interfaces.logger import get_logger
from blueshift.providers.assets.dictdb import DictDBDriver
from blueshift.providers.data.adjs.noadjustment import NoAdjustments
from blueshift.lib.exceptions import DataStoreException, SymbolNotFound
from blueshift.calendar.date_utils import get_aligned_timestamp

if TYPE_CHECKING:
    import pandas as pd
else:
    import blueshift.lib.common.lazy_pandas as pd

def na_locf(df:pd.DataFrame|pd.Series) -> pd.DataFrame|pd.Series:
    return df.ffill()

class DataFrameStore(DataStore):
    """ 
        Pandas based in-memory datastore for OHLCV data. This is only 
        used for transient intraday data store. This dataset cannot 
        handle any corporate action adjustments and hence should be 
        reset at the start of each trading days.
    """
    _store_type = 'dataframe'
    _metafile = None
    _cols = OHLCV_COLUMNS
    _hole_tolerance = 120 # tolerate 120 minutes of gap

    def __init__(self, root=None, name=None, max_size=2500, 
                 frequency='D', max_tickers=200, tz='Etc/UTC', 
                 exchange=None, calendar=None, supported_assets=[],
                 columns=None, logger=None, **kwargs):
        self._root = None
        self._metadata = {}
        self._logger = logger or get_logger()
        self._supported_exchanges = kwargs.get('supported_exchanges', [])
        
        if name is None:
            self._name = uuid.uuid4().hex
        else:
            self._name = name
        
        self._frequency = Frequency(frequency)
        self._max_tickers = max_tickers
        self._max_size = max_size
        self._tz = tz
        self._exchange = exchange
        
        if calendar is None:
            self._calendar = TradingCalendar(
                    name=self._name, tz=self._tz)
        else:
            if isinstance(calendar, TradingCalendar):
                self._calendar = calendar
            else:
                try:
                    self._calendar = get_calendar(calendar)
                except Exception:
                    raise DataStoreException('not a valid calendar.')
            self._tz = calendar.tz
        
        self._supported_assets = supported_assets
        self._has_oi = False
        self._int_cols = ['volume']
        
        if columns:
            for c in columns:
                if c not in OHLCVX_COLUMNS:
                    msg = f'Illegal column specified:{c}.'
                    raise DataStoreException(msg)
            self._cols = columns
            if 'open_interest' in self._cols and \
                'open_interest' not in self._int_cols:
                self._int_cols.append('open_interest')
                self._has_oi = True
        
        self.initialize()
        
    def __hash__(self):
        return hash(self.name + self.type)
    
    @property
    def name(self):
        return self._name
    
    @property
    def root(self):
        return self._root
    
    @property
    def columns(self):
        if not self._has_oi:
            return OHLCV_COLUMNS
        return OHLCVX_COLUMNS
        
    @property
    def dtypes(self):
        if not self._has_oi:
            return OHLCV_DTYPE
        dtypes = OHLCV_DTYPE.copy()
        dtypes['open_interest'] = np.int32
        return dtypes
        
    @property
    def frequency(self):
        return self._frequency.period
    
    @property
    def date_range(self):
        return None, None
    
    @property
    def tz(self):
        return self._tz
    
    @property
    def calendar(self):
        return self._calendar
    
    @property
    def exchange(self):
        return self._exchange
    
    @property
    def metadata(self):
        return self._metadata
    
    @property
    def db(self):
        asset_db = {asset.exchange_ticker:asset for asset in self._db}
        return DictDBDriver(
                asset_db, self._supported_assets, 
                supported_exchanges=self._supported_exchanges)
        
    @property
    def adjustments_handler(self):
        return NoAdjustments()
    
    @property
    def max_tickers(self) -> int:
        return self._max_tickers
    
    def initialize(self):
        """ initialize the store. """
        self._db:dict[MarketData,pd.DataFrame] = {}
        self._last_timestamp = {}
        
    def connect(self):
        """ connect is a no-ops. """
        pass
    
    def close(self, auto_commit=True):
        """ commit or rollback not supported. """
        pass

    def _current_timestamp(self, timestamp):
        ts = get_aligned_timestamp(timestamp, self._frequency)
        return make_consistent_tz(pd.Timestamp(ts), tz=self._tz)

    def _ensure_size(self, asset, nrows=1):
        nrows = int(nrows)
        if asset not in self._db:
            return None

        if len(self._db[asset]) < self._max_size:
            return False

        self._db[asset].values[:-nrows,] = self._db[asset].values[nrows:,]
        self._db[asset].index.values[:-nrows] \
                            = self._db[asset].index.values[nrows:]

        return True

    def _resample(self, data, columns, nbar, frequency, dt):
        freq = Frequency(frequency)
        functions = {"open": "first", "high": "max", "low":"min",
                     "close":"last","volume":"sum"}
        
        if self._has_oi:
            functions = {"open": "first", "high": "max", "low":"min",
                     "close":"last","volume":"sum",
                     "open_interest":"last"}
            
        nbar = int(nbar)
        if not isinstance(columns, ListLike):
            columns = [columns]
        else:
            columns = cast(list, columns)
            columns = list(columns)
        
        df = data[columns]
        if dt:
            dt = pd.Timestamp(dt)
            if dt.tz is None:dt = pd.Timestamp(dt, tz=self.tz)
            df = df[df.index <= dt]
        
        if freq == self._frequency:
            if len(df) <= nbar:
                return na_locf(df)
            else:
                return na_locf(df[-nbar:])

        if freq < self._frequency:
            raise DataStoreException("cannot upsample.")
        
        funcs = {col:functions.get(col,'last') for col in columns}
        if freq.period == "D":
            label = closed = "left"
        else:
            label = closed = "right"

        ratio = freq.nano/self._frequency.nano
        nbar2 = int(nbar*ratio)
        if len(df) <= nbar2:
            return na_locf(df).resample(
                    frequency.period, label=label, closed=closed)\
                                                        .agg(funcs)
        else:
            return na_locf(df[-(nbar2+1):]).resample(
                    frequency.period, label=label, closed=closed)\
                                .agg(funcs)[-nbar:]
                                
    def _resample2(self, data, columns, start_dt, end_dt, frequency):
        freq = Frequency(frequency)
        functions = {"open": "first", "high": "max", "low":"min",
                     "close":"last","volume":"sum"}
        
        if self._has_oi:
            functions = {"open": "first", "high": "max", "low":"min",
                     "close":"last","volume":"sum",
                     "open_interest":"last"}
        
        start_dt = pd.Timestamp(start_dt)
        if start_dt.tz is None:
            start_dt = pd.Timestamp(start_dt, tz=self.tz)
        
        end_dt = pd.Timestamp(end_dt)
        if end_dt.tz is None:
            end_dt = pd.Timestamp(end_dt, tz=self.tz)
        
        if not isinstance(columns, ListLike):
            columns = [columns]
        else:
            columns = cast(list, columns)
            columns = list(columns)
        
        df = data[columns]
        df = df[(df.index >= start_dt) & (df.index <= end_dt)]
        
        if freq == self._frequency:
            return na_locf(df)

        if freq < self._frequency:
            raise DataStoreException("cannot upsample.")
        
        funcs = {col:functions.get(col,'last') for col in columns}
        if freq.period == "D":
            label = closed = "left"
        else:
            label = closed = "right"
        return na_locf(df).resample(
                    frequency.period, label=label, closed=closed)\
                                                        .agg(funcs)

    def size(self, asset:MarketData) -> int:
        """ Current size of data in the data store. """
        if asset not in self._db:
            return 0

        return len(self._db[asset])

    def has_data(self, asset:MarketData) -> bool:
        """ Returns true if an asset has data. """
        return asset in self._db

    def last_timestamp(self, asset:MarketData) -> pd.Timestamp|None:
        """ last timestamp an asset has been updated in the store. """
        if self.has_data(asset):
            return self._last_timestamp[asset]

    def write_candle(self, asset:MarketData, timestamp:pd.Timestamp, candle:dict[str, Any]|pd.Series):
        """ 
            write a whole candle at a time. The position will be 
            aligned to nearest position as per the frequency of 
            the data store.
        """
        if not self._calendar.is_open(timestamp):
            return
        
        if len(candle)==0:
            return
        
        if isinstance(candle, dict) or isinstance(candle, pd.Series):
            for c in self._int_cols:
                if c in candle and np.isnan(candle[c]):
                    candle[c] = 0
        
        current_ts = self._current_timestamp(timestamp)
        new = pd.DataFrame(candle, index = pd.DatetimeIndex(pd.to_datetime([current_ts])))

        if asset in self._db:
            existing_cols = set(self._db[asset].columns)
            new_cols = set(new.columns)
            
            if existing_cols != new_cols:
                msg = f'cannot add candles, columns do not match, exising: {existing_cols}, new: {new_cols}'
                self._logger.error(msg)
                return

        if asset not in self._db:
            if not len(self._db.keys()) < self._max_tickers:
                self._logger.error('ticker exceeds limit.')
                return
            self._db[asset] = new
            self._last_timestamp[asset] = current_ts
            return
        elif current_ts >= self._last_timestamp[asset]:
            # have we have any holes in the datafrane?
            missing_idx = None
            ts_expected = pd.date_range(
                    self._last_timestamp[asset], current_ts,
                    freq='min', tz=self._tz, inclusive=('right'))
            if len(ts_expected) > 1:
                missing_idx = ts_expected[:-1]
            # add the next candle in place
            flag = self._ensure_size(asset)
            if flag is None:
                msg = "failied to ensure dataframe size,"  
                msg = msg + " something went wrong!"
                raise DataStoreException(msg)
            
            if flag:
                self._db[asset].loc[len(self._db[asset])-1] = new
            else:
                df = self._db[asset]
                if missing_idx is not None:
                    idx = pd.to_datetime([*(df.index), *(missing_idx)])
                    idx = pd.DatetimeIndex(idx)
                    df = df.reindex(idx, method='ffill')
                self._db[asset] = pd.concat([df,new], ignore_index=True)
                self._db[asset].index = [*(df.index), current_ts]
                self._db[asset].index = pd.DatetimeIndex(
                        pd.to_datetime(self._db[asset].index))
            self._last_timestamp[asset] = current_ts


    def write_price(self, asset:MarketData, timestamp:pd.Timestamp, price:float, volume:int=0, 
                    open_interest:int=0):
        """ 
            Write price/ volume data to the store. The candles at the
            frequency of the store will be built from this point 
            input over a period of time.
        """
        if not self._calendar.is_open(timestamp):
            return
        
        current_ts = self._current_timestamp(timestamp)
        if asset not in self._db:
            if not len(self._db.keys()) < self._max_tickers:
                self._logger.error('ticker exceeds limit.')
                return
            
            # write 0 for volume and let OI be NaN if missing
            if np.isnan(volume):
                volume = 0
            
            self._db[asset] = pd.DataFrame(
                    columns=self._cols,dtype=float)
            row = {c:price for c in self._cols}
            row["volume"] = volume
            
            if self._has_oi:
                row["open_interest"] = open_interest
            
            self._db[asset].loc[0] = row # type: ignore
            self._db[asset].index = pd.DatetimeIndex(
                    pd.to_datetime([current_ts]))
            self._db[asset]['volume'] = self._db[asset]['volume'].astype(np.int64)
            
            if self._has_oi:
                self._db[asset]['open_interest'] = self._db[asset]['open_interest'].astype(np.int64)
            
            self._last_timestamp[asset] = current_ts
            return

        if current_ts < self._last_timestamp[asset]:
            # no retrospective updates
            return
        elif current_ts == self._last_timestamp[asset]:
            # update the current candle, skip column if missing
            if not np.isnan(price):
                self._db[asset].at[self._db[asset].index[-1],"close"] = price
                if self._db[asset].high.iloc[-1] < price:
                    self._db[asset].at[self._db[asset].index[-1],"high"] = price
                if self._db[asset].low.iloc[-1] > price:
                    self._db[asset].at[self._db[asset].index[-1],"low"] = price
                    
            if not np.isnan(volume):
                self._db[asset].at[self._db[asset].index[-1],"volume"] \
                                = self._db[asset].volume.iloc[-1] + volume
            
            if not np.isnan(open_interest) and self._has_oi:
#                self._db[asset].at[self._db[asset].index[-1],"open_interest"] \
#                            = self._db[asset].open_interest.iloc[-1] + open_interest
                self._db[asset].at[self._db[asset].index[-1],"open_interest"] \
                            = open_interest
        else:
            # add the next candle in place
            flag = self._ensure_size(asset)
            if flag is None:
                raise DataStoreException("something went wrong!")
                
            if np.isnan(volume):
                volume = 0
            if np.isnan(open_interest) and self._has_oi:
                # carry forward last OI
                open_interest=self._db[asset]['open_interest'].iloc[-1]
            if np.isnan(price):
                # carry forward last close
                price = self._db[asset]['close'].iloc[-1]

            row = {c:price for c in self._cols}
            row["volume"] = volume
            
            if self._has_oi:
                row["open_interest"] = open_interest

            if flag:
                self._db[asset].loc[len(self._db[asset])-1] = row       # type: ignore
            else:
                self._db[asset].loc[len(self._db[asset])] = row         # type: ignore

            self._db[asset].index.values[-1] = current_ts
            self._db[asset]['volume'] = self._db[asset]['volume'].astype(np.int64)
            
            if self._has_oi:
                self._db[asset]['open_interest'] = self._db[asset]['open_interest'].astype(np.int64)
            
            self._db[asset].index = pd.DatetimeIndex(
                    pd.to_datetime(self._db[asset].index))
            self._last_timestamp[asset] = current_ts

    def add_data(self, asset:MarketData, data:pd.DataFrame):
        """ add dataframe for an asset. """
        # assert set(data.columns) == set(self._cols),\
        #         "columns mismatch in data, cannot add."
        if set(data.columns) != set(self._cols):
            raise ValueError("columns mismatch in data, cannot add.")
                
        data = data.copy()
        for c in self._int_cols:
            #data[np.isnan(data[c])] = 0
            data.loc[:,c] = data[c].fillna(0)

        if len(data) > self._max_size:
            data = data[-self._max_size:]

        if asset not in self._db:
            if not len(self._db.keys()) < self._max_tickers:
                self._logger.error('ticker exceeds limit.')
                return
            
            self._db[asset] = data
            self._last_timestamp[asset] = data.index[-1]
        else:
            current_size = len(self._db[asset])
            last_timestamp = self._db[asset].index[-1] - pd.Timedelta(minutes=1)
            
            if current_size < len(data) and \
                last_timestamp <= data.index[-1]:
                self._db[asset] = data
                self._last_timestamp[asset] = data.index[-1]
            else:
                df = self._merge_data(data, self._db[asset])
                if df.empty:
                    self._logger.error('empty data after merge while adding chunks.')
                    self._logger.info(f'empty data after merge: new {data}, existing {self._db[asset]}')
                if not df.empty:
                    self._db[asset] = df
                    self._last_timestamp[asset] = df.index[-1]

    def _merge_data(self, df1, df2):
        # assert set(df1.columns) == set(self._cols),\
        #         "columns mismatch in data, cannot add."
        if set(df1.columns) != set(self._cols):
            raise ValueError("columns mismatch in data, cannot add.")
                
        # assert set(df2.columns) == set(self._cols),\
        #         "columns mismatch in data, cannot add."
        if set(df2.columns) != set(self._cols):
            raise ValueError("columns mismatch in data, cannot add.")
                
        gap = (df1.index[0] - df2.index[-1]).total_seconds()
        gap = gap/60/int(self._frequency)
        
        if gap > self._hole_tolerance:
            self._logger.error('chunk data timestamp exceeds gap tolerance.')
            return pd.DataFrame()
        
        df = pd.concat([df1, df2])
        df = df.sort_index()
        df = df.loc[~df.index.duplicated(keep='first')]
        idx = pd.date_range(df.index[0], df.index[-1], freq=self._frequency.period)
        df = df.reindex(idx)
        df = df.between_time(self._calendar.open_time, self._calendar.close_time)
        return df.ffill()
    
    def reset(self):
        self._db = {}
        self._last_timestamp = {}
    
    def read(self, asset:MarketData, columns:str|list[str], nbars:int, dt:pd.Timestamp|None=None, 
             adjusted:bool=True, frequency:str|None=None, **kwargs) -> pd.DataFrame|pd.Series:
        """
            Read data by asset and number of bars required.
            
            Args:
                `asset (object)`: Asset to fetch data for.
                
                `columns (list)`: Columns to read data for.
                
                `nbars (int)`: Number of bars to fetch data for.

                `adjusted(bool)`: ignored.
                
                `frequency (str)`: Frequency of data.
                
                `dt (Timestamp)`: As of timestamp.
                
            Returns:
                DataFrame.
        """
        if asset not in self._db:
            self._logger.error('asset not in store.')
            return pd.DataFrame()
        
        if not columns:
            columns = self._cols
        elif isinstance(columns, str):
            columns = [columns.lower()]
        if not set(columns).issubset(set(self._cols)):
            raise DataStoreException('illegal columns supplied.')

        if frequency is None:
            freq = self._frequency
        else:
            freq = Frequency(frequency)

        return self._resample(self._db[asset], columns, nbars, freq, dt)
    
    def read_between(self, asset:MarketData, columns:str|list[str], start_dt:pd.Timestamp, 
                     end_dt:pd.Timestamp, adjusted:bool=True, frequency:str|Frequency|None=None, 
                     **kwargs) -> pd.DataFrame|pd.Series:
        """ Read data from store for a given asset and date range. """
        if asset not in self._db:
            self._logger.error('asset not in store.')
            return pd.DataFrame()
        
        if not columns:
            columns = self._cols
        elif isinstance(columns, str):
            columns = [columns.lower()]
        if not set(columns).issubset(set(self._cols)):
            raise DataStoreException('illegal columns supplied.')

        if frequency is None:
            freq = self._frequency
        else:
            freq = Frequency(frequency)

        return self._resample2(self._db[asset], columns, start_dt, end_dt, freq)
        
    def load_raw_arrays(self, columns, start_date, end_date, sids):
        raise NotImplementedError
        
    def get_spot_value(self, asset:MarketData, dt:pd.Timestamp, column:str) -> float:
        if asset not in self._db:
            self._logger.error('asset not in store.')
            if column in self._int_cols:
                return 0
            return np.nan
        column = column.lower()
        
        try:
            df = self._db[asset]
            idx = bisect.bisect_right(df.index, dt)-1
            idx = max(len(df)-1, min(0,idx))
            return df[column].iloc[idx]
        except Exception as e:
            self._logger.error(f'cannot fetch spot value for {asset}:{str(e)}.')
            if column in self._int_cols:
                return 0
            return np.nan
        
    def sweep_bars(self, assets, start_dt, end_dt, columns='close'):
        """ iterating over bars not supported. """
        raise NotImplementedError

    def write(self, asset:MarketData, *args, **kwargs):
        """
            Write data to the databse. The format can be either 
            `asset`, `timestamp`, `price` and `volume` with volume as 
            optional input. In this case, the price is assumed to be 
            the last traded price and the candles at the given 
            frequency are built from there. Alternatively, they can 
            also be passed as keyword arguments. A whole OHLCV candle
            can also be passed instead of timestamp, price (and volume),
            in which case the whole candle is updated, replacing the 
            current values for the timestamp. Finally, bulk insert is 
            possible by passing keyword `data` and a dataframe object 
            to be inserted.
            
            Returns:
                None.
        """
        def get_volume():
            volume = 0
            try:
                if len(args) > 2 and isinstance(args[2], numbers.Number):
                    volume = np.int64(args[2]) # type: ignore
                elif 'volume' in kwargs:
                    volume = np.int64(kwargs['volume'])
            except Exception:
                pass
            return volume
        
        def get_oi():
            oi = 0
            if not self._has_oi:
                return oi
            try:
                if len(args) > 3 and isinstance(args[3], numbers.Number):
                    oi = np.int64(args[2])
                elif 'open_interest' in kwargs:
                    oi = np.int64(kwargs['open_interest'])
            except Exception:
                pass
            return oi
        
        def get_candle():
            candle = {}
            try:
                if 'close' in kwargs and len(kwargs)>3:
                    close_ = float(kwargs['close'])
                    open_ = float(kwargs.get('open', close_))
                    high_ = float(kwargs.get('high', max(open_, close_)))
                    low_ = float(kwargs.get('open', max(open_, close_)))
                    volume_ = int(kwargs.get('volume', 0))
                    candle = {'open':open_, 'high':high_, 'low':low_, 'close':close_, 'volume':volume_}
                    if 'open_interest' in kwargs and self._has_oi:
                        candle['open_interest'] = int(kwargs['open_interest'])
            except Exception:
                pass
            return candle
                
        if args:
            if isinstance(args[0], pd.Timestamp):
                timestamp = args[0]
                if len(args) > 1 and isinstance(args[1], numbers.Number):
                    price = args[1]
                    volume = get_volume()
                    oi = get_oi()
                    self.write_price(
                            asset, timestamp, price, volume,open_interest=oi) # type: ignore
                elif 'price' in kwargs:
                    price = kwargs['price']
                    volume = get_volume()
                    oi = get_oi()
                    self.write_price(asset, timestamp, price, volume, open_interest=oi) # type: ignore
                elif len(args) > 1 and isinstance(args[1], dict):
                    candle = args[1]
                    self.write_candle(asset, timestamp, candle)
                elif 'candle' in kwargs:
                    candle = kwargs['candle']
                    self.write_candle(asset, timestamp, candle)
                elif 'close' in kwargs and len(kwargs)>3:
                    candle = get_candle()
                    self.write_candle(asset, timestamp, candle)
                else:
                    raise DataStoreException('illegal inputs to write.')
            else:
                data = args[0]
                self.add_data(asset, data)
            return
        elif kwargs:
            if 'timestamp' in kwargs:
                timestamp = kwargs['timestamp']
                if 'price' in kwargs:
                    price = kwargs['price']
                    volume = get_volume()
                    oi = get_oi()
                    self.write_price(asset, timestamp, price, volume, open_interest=oi) # type: ignore
                elif 'candle' in kwargs:
                    candle = kwargs['candle']
                    self.write_candle(asset, timestamp, candle)
                elif 'close' in kwargs and len(kwargs)>4:
                    candle = get_candle()
                    self.write_candle(asset, timestamp, candle)
            elif 'data' in kwargs:
                data = kwargs['data']
                self.add_data(asset, data)
            else:
                raise DataStoreException('illegal inputs to write.')
            return
        
        raise DataStoreException('illegal inputs to write.')
        
    def commit(self):
        """ commit or rollback not supported. """
        pass
    
    def rollback(self):
        """ commit or rollback not supported. """
        pass
    
    def update_metadata(self, *args, **kwargs):
        """ update metadata from keywords not supported. """
        pass
    
    def exists(self, symbols, table=None) -> bool|list[bool]:
        """ Check if symbols exists in the db. """
        return self.db.exists(symbols)
    
    def list(self) -> pd.DataFrame:
        """ List of symbols in the index, along with start and end. """
        assets = self.db.search()
        assets = cast(list, assets)
        syms = [asset.exchange_ticker for asset in assets]
        names = [asset.name for asset in assets]
        start_dts = [asset.start_date for asset in assets]
        end_dts = [asset.end_date for asset in assets]
        
        return pd.DataFrame({'name':names,
                             'start_date':start_dts,
                             'end_date':end_dts},
                index=syms)
    
    def find(self, assets, key=None):
        """ search by assets matching symbols. """
        return self.db.find(assets)
    
    def search(self, symbols:str|list[str]|None=None, **kwargs) -> list[MarketData]:
        """ Search symbols, other arguments are ignored. """
        return self.db.search(symbols)
    
    def fuzzy_search(self, symbols, **kwargs):
        """ 
            fuzzy search is identical to search for this 
            implementation. 
        """
        return self.db.fuzzy_search(symbols)
    
    def sid(self, sec_id: int) -> MarketData:
        raise NotImplementedError
    
    def symbol(self, sym: str, dt: Timestamp | None = None, **kwargs) -> MarketData:
        assets = self.search(sym)
        
        if not assets:
            raise SymbolNotFound(f'symbol {sym} not found.')
        
        return assets[0]
    
    def update(self, symbol, data={}):
        """ rename the asset database not supported. """
        raise DataStoreException(
                'This datastore does not support update operation.')
    
    def rename(self, from_sym, to_sym, long_name=None):
        """ rename the asset database not supported. """
        raise DataStoreException(
                'This datastore does not support rename operation.')
    
    def stats(self):
        """ Some basic stats about the database. """
        syms = list(self._db.keys())
        count = len(syms)
        size = sum(len(self._db[asset]) for asset in syms)
        start = min(self._db[asset].index[0] for asset in syms)
        end = max(self._db[asset].index[-1] for asset in syms)
        
        return {'symbols':syms, 'count':count,'size':size,
                'start':start, 'end':end}
        
    def remove(self, symbol):
        if isinstance(symbol, str):
            symbol = self.search(symbol)
            
        symbol = cast(MarketData, symbol)
        self._db.pop(symbol, None)
        
    def truncate(self):
        """ reset the database. """
        self._db = {}
        self._last_timestamp = {}
        
    def delete(self):
        """ delete in-memory database not supported. """
        pass
        
    def ingest_report(self, *args, **kwayrgs):
        """ Ingest reports are not supported. """
        return {}
    
    def backup(self, *args, **kwargs):
        """ Backups are not supported. """
        return
    
    def restore(self, backup_name):
        """ Backups are not supported. """
        return
    
    def list_backups(self, *args, **kwayrgs):
        """ Backups are not supported. """
        return pd.DataFrame({},columns=['backup','time'])
    
    def remove_backup(self, backup_name):
        """ Backups are not supported. """
        return
    
    def add_membership(self, data, name=None):
        """ Membership groups are not supported. """
        pass
    
    def fetch_membership(self, group=None):
        """ Membership groups are not supported. """
        return
    
    def remove_membership(self, group):
        """ Membership groups are not supported. """
        pass
    
    def list_memberships(self):
        """ Membership groups are not supported. """
        return []
    
    def apply_adjustments(self, asset, data, dt=None, precision=2):
        """ adjustments not supported. """
        raise NotImplementedError
    
    def assets_lifetime(self, dts, include_start_date=True, assets=None
                        ) -> tuple[list[MarketData], pd.DatetimeIndex, np.ndarray]:
        """ asset lifetime not supported. """
        raise NotImplementedError
        
    
register_data_store('dataframe', DataFrameStore)
