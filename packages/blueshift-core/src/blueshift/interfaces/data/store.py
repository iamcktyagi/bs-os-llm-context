from __future__ import annotations
from typing import Dict, Type, Callable, Literal, TYPE_CHECKING, Any, cast
from enum import Enum
from abc import ABC, abstractmethod
from pathlib import Path

from blueshift.lib.exceptions import DataStoreException
from blueshift.interfaces.data.adjustments import AdjustmentHandler
from blueshift.interfaces.assets.assetdb import AssetDBDriver
from blueshift.interfaces.assets._assets import MarketData, Asset
from blueshift.calendar.trading_calendar import TradingCalendar
from blueshift.lib.common.functions import listlike

from ..plugin_manager import load_plugins

if TYPE_CHECKING:
    import datetime
    import pandas as pd
    import numpy as np
    
class StoreType(Enum):
    AssetStore = 0              # each asset stored in different files
    ColumnStore = 1             # each columns stored in different files
    DerivativesStore = 2        # specialized for options and futures
    MarketDataStore = 3         # specialized for non-asset market data
    FundamentalStore = 4        # specialized for fundamental data

class DataStore(ABC):
    """ 
        abstract interface for a logical unit of storage with similar 
        information stored alogn with metadata. A store is defined by 
        a namespace within a store. A store has a serialization backend
        (e.g. feather, bcolz or mongo) and a asset database instance (e.g.
        sqlite). A store is specific for a particular set of asset db 
        schema. For example, all foreign exchange related assets (e.g. 
        forex, forex CFDs, forex futures etc.) can be stored in a single 
        store within a single asset database and a price database 
        implementation.
    """
    _TYPE = StoreType.AssetStore
    _metafile = 'store.json'
    _store_type = 'store'
        
    def __str__(self):
        return f"{self.__class__.__name__}({self.name}[{self.frequency}])"
    
    def __repr__(self):
        return self.__str__()
    
    @property
    def type(self) -> str:
        return self._TYPE.name
    
    @property
    def store_type(self) -> str:
        return self._store_type
    
    def __hash__(self):
        raise NotImplementedError
        
    def __eq__(self,y):
        return hash(self) == hash(y)
    
    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError
    
    @property
    @abstractmethod
    def root(self) -> str|None:
        raise NotImplementedError
    
    @property
    @abstractmethod
    def columns(self) -> list:
        raise NotImplementedError
        
    @property
    @abstractmethod
    def dtypes(self) -> dict:
        raise NotImplementedError
        
    @property
    @abstractmethod
    def frequency(self) -> str:
        raise NotImplementedError
        
    @property
    @abstractmethod
    def tz(self) -> str:
        raise NotImplementedError
        
    @property
    @abstractmethod
    def date_range(self) -> tuple:
        raise NotImplementedError
        
    @property
    def exchange(self) -> str|None:
        raise NotImplementedError
        
    @property
    @abstractmethod
    def calendar(self) -> TradingCalendar:
        raise NotImplementedError
        
    @property
    @abstractmethod
    def metadata(self) -> dict:
        raise NotImplementedError
        
    @property
    @abstractmethod
    def db(self) -> AssetDBDriver:
        raise NotImplementedError
        
    @property
    @abstractmethod
    def adjustments_handler(self) -> AdjustmentHandler:
        raise NotImplementedError
    
    @abstractmethod
    def initialize(self, *args, **kwargs) -> None:
        """
            Initialize a store, empty or existing.
            
            Returns:
                None.
        """
        raise NotImplementedError
        
    def refresh(self) -> None:
        pass
    
    def reset(self) -> None:
        pass
        
    @abstractmethod
    def connect(self, *args, **kwargs) -> None:
        """
            Connect to an existing store.
            
            Returns:
                None.
        """
        raise NotImplementedError
        
    @abstractmethod
    def close(self, auto_commit:bool=True) -> None:
        """
            Carry out closing operations on the store. This will 
            automatically invoke a `commit` by default.
            
            Args:
                `auto_commit (bool)`: Commit pending changes.
                
            Returns:
                None.
        """
        raise NotImplementedError
        
    @abstractmethod
    def write(self, *args, **kwargs):
        """ write data to the database. """
        raise NotImplementedError
    
    def add_data(self, *args, **kwargs):
        pass
        
    @abstractmethod
    def commit(self):
        """
            Commit staged write changes to the store. This 
            include committing updates to the symbol (asset) table, 
            and data file chunks. We also refresh the adjustment handler
            in case there are changes in the adjustment tables.
        """
        raise NotImplementedError
        
    @abstractmethod
    def rollback(self):
        """
            Roll back staged write changes to the store. This 
            include discarding updates to the symbol (asset) table, 
            and data file chunks.
            
            Returns:
                None.
        """
        raise NotImplementedError
        
    
    def set_cache_policy(self, policy:str='random'):
        """
            Set the caching policy on this store. If the policy is 
            `sliding` the subsequent reads must be in increasing order 
            of time and reading data for a past date will raise error. 
            Otherwise, random reads will be supported.
            
            Args:
                `policy (str)`: Set the cache policy for reads.
                
            Returns:
                None.
        """
        pass

    def set_roll_policy(self, roll_day:int=0, roll_time:tuple[int,int]|datetime.time=(23,59)):
        pass

    def last_timestamp(self, asset:MarketData) -> pd.Timestamp|None:
        raise NotImplementedError
    
    def size(self, asset:MarketData) -> int:
        raise NotImplementedError
        
    @abstractmethod
    def read(self, asset:MarketData, columns:str|list[str], nbars:int, dt:pd.Timestamp|None=None, 
             adjusted:bool=True, **kwargs) -> pd.DataFrame|pd.Series:
        """
            Read data from store for a given asset and date range.
            
            Args:
                `asset (object)`: Asset to fetch data for.
                
                `columns (list)`: Columns to read data for.
                
                `nbars (int)`: Number of bars to fetch data for.
                
                `frequency (str)`: Frequency of data.
                
                `dt (Timestamp)`: As of timestamp.
                
            Returns:
                Dataframe - indexed by datetime, with requested 
                columns (or all if `columns` is None) for the given 
                symbol in the time range. If the symbol is not in the
                store, or not available in the date range, None
                will be returned.
        """
        raise NotImplementedError
        
    @abstractmethod
    def read_between(self, asset:MarketData, columns:str|list[str], start_dt:pd.Timestamp, 
                     end_dt:pd.Timestamp, adjusted:bool=True, **kwargs) -> pd.Series|pd.DataFrame:
        """
            Read data from store for a given asset and date range.
            If `frequency` is `None`, the native frequency of the 
            data store is returned. Frequency cannot be lower than 
            the native frequency (no upsampling).
            
            Args:
                `asset (object)`: Asset to fetch data for.
                
                `columns (str)`: Column or a list of columns to fetch.
                
                `start_dt (timestamp)`: Start of data to read.
                
                `end_dt (timestamp)`: End of data to read.
                
                `frequency (str)`: Frequency of data.
                
            Returns:
                Series or Dataframe - indexed by datetime, with requested 
                columns for the given asset in the time range. 
                If the asset is not in the store, or not available 
                in the date range, None will be returned.
        """
        raise NotImplementedError
    
    @abstractmethod
    def sweep_bars(self, assets, start_dt, end_dt, columns='close',
                        *args, **kwargs):
        """
            Iterate through the data for all assets between the specified 
            date and column. Returns a tuple of date and array of data, 
            indexed in the order of the input asset list. DO NOT use this 
            for tick data. If `assets` is a callable, it must accept a 
            timestamp and return a list of assets.
            
            Args:
                `assets (list or callable)`: List of assets or universe function.
                
                `start_dt (timestamp)`: Start of data to read.
                
                `end_dt (timestamp)`: End of data to read.
                
                `columns (str)`: column name or list to fetch data.
            
            Returns:
                A generator that returns a tuple of (timestamp, 
                dict of prices).
        """
        raise NotImplementedError
        
    def get_spot_value(self, asset:MarketData, dt:pd.Timestamp, column:str) -> float:
        """
            Read a spot value for a given asset and column at a given 
            timestamp (`dt`). In case there is no exact match, if the 
            column requested is volume, 0 is returned. If the match is 
            before the start index, 0 is returned for volume, and `np.nan` 
            otherwise. If the match is beyond the end index, 0 is returned 
            for volume and the last data for others.
            
            Args:
                `asset (obj)`: Asset object.
                
                `dt (timestamp)`: Timestamp of the value.
                
                `column (str)`: Column name to fetch data for.
                
            Returns:
                Scalar (float or int, depending on the column data type).
        """
        raise NotImplementedError
    
    @abstractmethod
    def apply_adjustments(self, asset, data, dt=None, precision=2):
        """
            Apply adjustments relative to a given date. If `dt` 
            is None, adjustments relative to the last date in the 
            data index are applied. Adjustments are applicable 
            for OHLCV data only.
            
            Args:
                `asset (object)`: Asset for the data.
                
                `data (dataframe)`: Input data to apply adjustments.
                
                `dt (timestamp)`: Relative date of adjustments.
                
                `precision (int)`: Digits after decimal for non-integer data.
            
            Returns:
                Dataframe. Adjusted data.
        """
        raise NotImplementedError
    
    @abstractmethod
    def load_raw_arrays(self, columns, start_date, end_date, sids):
        """
            Load unadjusted data array
            
            Args:
                `columns (list of str)`: Columns to read data.
                
                `start_dt (timestamp)`: Start of data to read.
                
                `end_dt (timestamp)`: End of data to read.
                
                `sids (list of ints)`: asset sids for the date range.
                
                `precision (int)`: Digits after decimal for non-integer data.
            
            Returns:
                Dataframe. Adjusted data.
        """
        raise NotImplementedError
    
    @abstractmethod
    def has_data(self, asset):
        """ Returns true if an asset has data. """
        raise NotImplementedError
        
    @abstractmethod
    def update_metadata(self, *args, **kwargs):
        """ update metadata from keywords. """
        raise NotImplementedError
    
    @abstractmethod
    def list(self) -> pd.DataFrame:
        """
            List available symbols in the store.
                
            Returns:
                Dataframe - indexed by symbols, with details of each 
                entry in the store.
        """
        raise NotImplementedError
    
    @abstractmethod    
    def exists(self, symbols, table=None):
        """
            Check if an asset exists in the store.
            
            Args:
                `symbols (obj or str)`: An asset or value to check (or list).
                
                `table (obj)`: DB Schema object or a table name.
                
            Returns:
                Bool (or list of booleans). True if a match is found.
        """
        raise NotImplementedError
        
    @abstractmethod
    def find(self, assets, key=None):
        """
            Find matching asset(s) in the store.
            
            Args:
                `assets (Asset)`: An asset (or list). Returns all if missing.
                
                `key (str)`: Optional column name to search.
                
            Returns:
                List. A list of asset objects matching query.
        """
        raise NotImplementedError
    
    @abstractmethod
    def search(self, symbols:str|list[str]|None=None, **kwargs) -> list[MarketData]:
        """
            Search the store for assets by symbol name.
            
            Note:
                This function will fetch all asset tables within the db by 
                symbol name by default. If no `symbols` supplied, all 
                available assets will be returned (for the given table, 
                or all table if not `table` is specified).
            
            Args:
                `symbols (str)`: A symbol (or list). Returns all if missing.
                
            Returns:
                List. A list of asset objects.
        """
        raise NotImplementedError
        
    @abstractmethod
    def fuzzy_search(self, symbols:str|list[str], **kwargs) -> list[MarketData]:
        """
            Search the store for assets by symbol name, matching 
            the ones that start with the given value.
            
            Note:
                This function will fetch all asset tables within the db by 
                symbol name by default.
            
            Args:
                `symbols (str)`: A symbol (or list). Returns all if missing.

            Returns:
                List. A list of asset objects or symbols.
        """
        raise NotImplementedError
    
    @abstractmethod
    def sid(self, sec_id:int) -> MarketData:
        """ fetch asset by sid. """
        raise NotImplementedError
    
    @abstractmethod
    def symbol(self, sym:str, dt:pd.Timestamp|None=None, **kwargs) ->MarketData:
        """ fetch asset by symbol. """
        raise NotImplementedError
    
    def sids(self, sids:list[int]|int) -> list[MarketData]:
        if not listlike(sids):
            sids = [sids] # type: ignore
            
        sids = cast(list, sids)
        return [self.sid(sid) for sid in sids]
    
    def symbols(self, syms:list[str]|str) -> list[MarketData]:
        if not listlike(syms):
            syms = [syms] # type: ignore
            
        syms = cast(list, syms)
        return [self.symbol(sym) for sym in syms]
    
    @abstractmethod
    def update(self, symbol, data={}):
        """
            Update an existing asset object details in the store.
            
            Note:
                Supply an asset object with updated data to replace, or
                a symbol with a `data` dictionary with properties to 
                update. If an asset object is supplied, `data` will be
                ignored.
            
            Args:
                `symbol (obj or str)`: An asset object or symbol string, or a list of them.
                
                `data (dict)`: A mapping of fields and values to update.
                
            Returns:
                None.
        """
        raise NotImplementedError    
    
    @abstractmethod
    def rename(self, from_sym, to_sym, long_name=None):
        """
            Rename an existing symbol.
            
            Note:
                This will trigger cascading udpates for all assets pointing
                to this as `underlying`.
            
            Args:
                `from_sym (str)`: The existing symbol to rename.
                
                `to_sym (str)`: New symbol name.
                
                `long_name (str)`: Optional long name of new symbol.
                
            Returns:
                None.
        """
        raise NotImplementedError
        
    @abstractmethod
    def stats(self):
        """
            List a range of statistics about the current database.
            
            Returns:
                Dict. `symbols` is total count of symbols in the 
                store. `chunks` is the total count of data file 
                chunks. `size` is the size on disk in MB. `start` 
                and `end` are the starting and ending dates of 
                available data. `staged` shows if some data are 
                staged and yet to be committed.
        """
        raise NotImplementedError
        
    @abstractmethod
    def remove(self, symbol):
        """
            Drop a symbol from the database and its associated data.
            
            Note:
                This will trigger cascading delete for all assets pointing
                to this as `underlying`.
            
            Args:
                `symbol (str)`: The existing symbol to remove.
                
            Returns:
                None.
        """
        raise NotImplementedError
        
    @abstractmethod
    def truncate(self):
        """
            Delete store data (all data and metadata). If any back-up 
            available, that can be used to restore.
                
            Returns:
                None.
        """
        raise NotImplementedError
        
    @abstractmethod
    def delete(self):
        """
            Delete a store, including all data and metadata. This 
            is a irreversible operation.
                
            Returns:
                None.
        """
        raise NotImplementedError
        
    @abstractmethod
    def ingest_report(self, symlist=None):
        """
            Produce a detailed report on last (un-commited) ingest 
            operation
            
            Args:
                `symlist (list)`: List of symbols expected.
                
            Returns:
                Dict. Key `extra` and `missing` list extra symbols 
                (compared to expected list, if supplied). Key `details`
                list the actual symbols written.
        """
        raise NotImplementedError
        
    @abstractmethod
    def backup(self, *args, **kwargs):
        """
            Create a back-up of the current data and associated metadata.
            
            Returns:
                Str. Name of the backup. Returns None if the store 
                has no data to back up.
        """
        raise NotImplementedError
        
    @abstractmethod
    def restore(self, backup_name):
        """
            Restore a specified backup or the latest backup copy if None.
            
            Args:
                `backup_name (str)`: Name of the backup to restore.
            
            Returns:
                Str. Name of the backup. Returns None if there is no
                back-up available to restore.
        """
        raise NotImplementedError
        
    @abstractmethod
    def list_backups(self):
        """
            Returns a data-frame with list of available back-ups.
            Column names are `backup` and `time` - timestamp of 
            the backup.
        """
        raise NotImplementedError
        
    @abstractmethod
    def remove_backup(self, backup_name):
        """
            Remove a specified backup. If `backup_name` is `all`, all 
            back-ups are removed.
            
            Args:
                `backup_name (str)`: back-up to remove.
                
            Returns:
                Str or list. The back-up(s) that was removed. Returns 
                None if there is no backup or backup name is None.
        """
        raise NotImplementedError
        
    @abstractmethod
    def add_membership(self, data, name=None):
        """
            Add a new membership map for a given group name.
            
            Note:
                Membership data is a dataframe containing three columns - 
                `symbol`, `start_date` and `end_date`, that shows the 
                membership history for a list of symbols for the given 
                group. The `symbol` column need not be unique, a symbol 
                can enter and exit the group multiple times, each time 
                an entry is added.
            
            Args:
                `data (DataFrame)`: Membership details.
                
                `name (str)`: Name of the group.
                
            Returns:
                None.
        """
        raise NotImplementedError
        
    @abstractmethod
    def fetch_membership(self, group=None):
        """
            Fetch membership details for a given group name.
            
            Note:
                Membership data is a dataframe containing three columns - 
                `symbol`, `start_date` and `end_date`, that shows the 
                membership history for a list of symbols for the given 
                group. The `symbol` column need not be unique, a symbol 
                can enter and exit the group multiple times, each time 
                an entry is added.
            
            Args:
                `name (str)`: Name of the group.
                
            Returns:
                DataFrame - membership history for the target group.
        """
        raise NotImplementedError
        
    @abstractmethod
    def remove_membership(self, group):
        """
            Remove membership details for a given group name, if exists.
            
            Args:
                `group (str)`: Name of the group.
                
            Returns:
                None.
        """
        raise NotImplementedError
        
    @abstractmethod
    def list_memberships(self):
        """
            List available membership name. Skip the default (`base`) 
            group.
            
            Returns:
                List. A list of membership groups available.
        """
        raise NotImplementedError
    
    @abstractmethod
    def assets_lifetime(self, dts:pd.DatetimeIndex|list[pd.Timestamp], include_start_date:bool=True, 
                        assets:list[MarketData]|None=None
                        ) -> tuple[list[MarketData], pd.DatetimeIndex, np.ndarray]:
        """
            Get the asset life-time matrix. Returns a tuple of symbols, 
            dates and a boolean array. The array has 1s for dates when 
            the asset is available for trading, 0s otherwise.
            
            Args:
                `dts (list)`: List of dates.
                
                `include_start_date (bool)`: Include the start date.
                
                `assets (list)`: List of assets.
                
            Returns:
                Tuple of assets, dates and boolean array of asset life-time.
        """
        raise NotImplementedError
    
class FundamentalStore(DataStore):
    """ interface for stores for corporate fundamentals. """
    @abstractmethod
    def get_sectors(self, assets:list[Asset], dt:pd.Timestamp|None=None) -> pd.DataFrame:
        raise NotImplemented
    
    @abstractmethod
    def get_available_sectors(self) -> list[str]:
        raise NotImplemented
    
    def read(self, asset:MarketData, columns:str|list[str], nbars:int, dt:pd.Timestamp|None=None, 
             adjusted:bool=True, **kwargs) -> pd.DataFrame|pd.Series:
        raise NotImplementedError
    
    @abstractmethod
    def read_metrics(self, assets:list[Asset], metrics:list[str], nbars:int, 
                     dt:pd.Timestamp|str|None=None, **kwargs) -> dict[Asset, pd.DataFrame]:
        raise NotImplementedError
        

_data_store_registry: Dict[str, Type[DataStore]] = {}
_builtin_data_store_loader: Callable[[Any], None] | None = None
_data_store_cache: Dict[str, DataStore] = {}

def register_data_store(name: str, cls: Type[DataStore]):
    """Register a new data store type."""
    _data_store_registry[name] = cls

def set_builtin_data_store_loader(loader: Callable[[Any], None]):
    """Register a callable that will lazily load built-in data store types."""
    global _builtin_data_store_loader
    _builtin_data_store_loader = loader

def _ensure_builtin_data_store_loaded(store_type:str|None=None):
    if _builtin_data_store_loader:
        _builtin_data_store_loader(store_type)

    if (store_type and store_type not in _data_store_registry) or store_type is None:
        try:
            load_plugins('blueshift.plugins.data')
        except Exception:
            pass

def list_data_stores():
    """ get a list of registered store names. """
    _ensure_builtin_data_store_loaded()
    return list(_data_store_registry)

def store_factory(store_type:str|DataStore|None=None, metadata=None, *args, **kwargs):
    """ factory function to create stores. """
    if isinstance(store_type, DataStore):
        return store_type
    
    import os
    import json
    root = None
    cache = kwargs.pop('cache', False)

    if store_type not in _data_store_registry:
        _ensure_builtin_data_store_loaded()  # lazy load builtins

    if isinstance(store_type, str) and store_type not in _data_store_registry and 'root' not in kwargs:
        kwargs['root'] = store_type # assume the first argument is store `root`
        store_type = None

    if not store_type:
        if 'root' in kwargs:
            root = kwargs.pop('root', None)
        else:
            raise DataStoreException(
                    f'store root not found in the arguments.')
        
        root = os.path.expanduser(root)
        if not os.path.abspath(root):
            root = str(Path(root).resolve())

        root_key = root.replace(os.sep, '/')
        if cache and root_key in _data_store_cache:
            return _data_store_cache[root_key]
        
        if not os.path.exists(root):
            raise DataStoreException(
                    f'root specified does not exist:{root}.')
        
        if not metadata:
            metadata = DataStore._metafile
        metapath = os.path.join(root, metadata)
        
        if os.path.exists(metapath):
            with open(metapath) as fp:
                metadata = json.load(fp)
            if 'store_type' in metadata:
                store_type = metadata['store_type']
            else:
                raise DataStoreException(
                    f'must specify the store type.')
    
    if not store_type:
        raise DataStoreException(f'no store type specified nor any found in the root.')
        
    cls = _data_store_registry.get(store_type) # type: ignore
    if cls is None:
        raise NotImplementedError(f"Unknown data source type: {store_type}")
    
    obj = cls(*args, **kwargs)
    
    if root and cache:
        root_key = root.replace(os.sep, '/')
        _data_store_cache[root_key] = obj
    
    return obj

__all__ = [
    'DataStore',
    'FundamentalStore',
    'StoreType',
    'register_data_store',
    'store_factory',
    'list_data_stores',
    'set_builtin_data_store_loader',
    ]