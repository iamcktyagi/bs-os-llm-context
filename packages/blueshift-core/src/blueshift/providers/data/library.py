from __future__ import annotations
import os
import warnings
import logging
from functools import lru_cache
import datetime
from typing import Literal, TYPE_CHECKING, cast, Iterator, Type
import numpy as np
import math
import json
import copy

from blueshift.calendar import register_calendar, get_calendar
from blueshift.interfaces.assets._assets import (
    MarketData, SpotFX, Asset, Equity, Forex, Crypto,
    EquityFutures, EquityOption)

from blueshift.interfaces.assets.assets import IAssetFinder, convert_asset
from blueshift.interfaces.data.library import ILibrary, register_library
from blueshift.interfaces.assets.utils import get_opt_details
from blueshift.interfaces.data.data_portal import DataPortal
from blueshift.interfaces.data.store import DataStore, FundamentalStore, store_factory
from blueshift.lib.common.types import DeprecationFilter
from blueshift.lib.common.constants import Frequency, CCYPair
from blueshift.lib.common.functions import list_index, listlike
from blueshift.lib.exceptions import (
        SymbolNotFound, DataStoreException, BlueshiftWarning,
        HistoryWindowStartsBeforeData, TerminationError, 
        AssetsDBException)

from .utils import resample, subset

if TYPE_CHECKING:
    import pandas as pd
    from pandas._libs.tslibs.nattype import NaTType
else:
    import blueshift.lib.common.lazy_pandas as pd
    
_CACHE_SIZE = 4096
_MAX_ASSETS = 200
_ASSET_CLASS_PREFERECE = [MarketData, Equity, EquityFutures, 
                          EquityOption, Crypto, SpotFX, Forex, Asset]
_ACTIVE_STORE_ASSET_PREFERECE = [Equity, EquityFutures, EquityOption, Crypto, 
                           SpotFX, Forex, Asset]
_PIPELINE_ASSETS_PREFERENCE = [Equity, Forex, Crypto]
_FNO_ASSETS = [EquityFutures, EquityOption]

def walk_stores(src):
    if not os.path.isdir(src):
        return
    
    artifacts = os.listdir(src)
    if 'store.json' in artifacts:
        yield src
        return
    
    artifacts = [os.path.join(src,e) for e in artifacts]
    dirs = [e for e in artifacts if os.path.isdir(e)]
    
    for d in dirs:
        for s in walk_stores(d):
            yield s
            
def try_store(store):
    if isinstance(store, DataStore):
        return store
    
    if isinstance(store, str):
        try:
            return store_factory(root=store, cache=True)
        except Exception:
            return None
            
def get_relative_path(path, start=None):
    if not path:
        return path
    
    home = os.path.expanduser('~')
    if not start:
        start = home
    
    rel = os.path.relpath(path, start=start)
    if len(rel) < len(path) and not rel.startswith('..'):
        if start == home:
            rel = '~/' + rel.lstrip('/').lstrip('\\')
        return rel
    
    rel = os.path.relpath(path, start=home)
    if len(rel) < len(path):
        return '~/' + rel.lstrip('/').lstrip('\\')
    
    return path

def get_full_path(path, start=None):
    if not path:
        return path
    
    expanded = os.path.expanduser(path)
    if os.path.isabs(expanded):
        return expanded
    
    if start:
        return os.path.join(start, path)
    
    return os.path.expanduser(path)
    

class __Stores__:
    
    def __init__(self, stores, active_store=None, pipeline_store=None, 
                 fx_rate_store=None, futures_store=None, 
                 options_store=None, cache:str='random', 
                 exchange_preference=None):
        # routing tables
        self._exchange_dispatch:dict[str, list[DataStore]] = {}             # non unique mapping
        self._name_dispatch:dict[str, DataStore] = {}                       # unique mapping
        self._asset_dispatch:dict[Type[MarketData], list[DataStore]] = {}   # non unique mapping
        self._freq_dispatch:dict[Frequency,list[DataStore]] = {}            # non unique mapping
        self._asset_freq_dispatch:dict[Type[MarketData], dict[Frequency, dict[str, DataStore]]] = {}      # uniuqe mapping with exchange
        
        self._stores:set[DataStore] = set()
        self._cache_policy:str = cache                                      # caching policy
        self._proxy_stores:list[tuple[Type[MarketData], Frequency, str, DataStore]] = []   # for proxy stores
        self._sorted_stores:list[DataStore] = []                            # sorted stores
        self._exchange_preference:list[str] = exchange_preference or []
        
        for store in stores:
            self._add_store(store)
            
        self._active_store = try_store(active_store)
        self._pipeline_store = try_store(pipeline_store)
        self._fx_rate_store = try_store(fx_rate_store)
        self._futures_store = try_store(futures_store)
        self._options_store = try_store(options_store)
        self._fx_pairs = {}
        self._fx_reverse_pairs = {}
        self._add_fx_rate_store()
            
    @property
    def stores(self) -> list[DataStore]:
        if self._sorted_stores:
            return self._sorted_stores
        
        self._sorted_stores = self.sort_stores(self._stores)
        return self._sorted_stores
    
    def sort_stores(self, stores:list[DataStore]|set[DataStore]) -> list[DataStore]:
        sorting_keys = []
        
        for store in stores:
            assets = store.db.list_assets()
            key1 = list_index(self._exchange_preference, store.exchange, 999)
            key2 = min([list_index(_ASSET_CLASS_PREFERECE, asset,999) for asset in assets])
            key3 = Frequency(store.frequency)
            sorting_keys.append((key1,key2,key3,store))
            
        sorted_vaues = sorted(sorting_keys, key=lambda x: (x[0], x[1], x[2]))
        return [x[3] for x in sorted_vaues]
    
    def _update_proxy_store(self):
        for asset, freq, exchange, store in self._proxy_stores:
            # we do not need to check for asset key, it must be there!!
            if freq not in self._asset_freq_dispatch[asset]:
                mapping = {exchange:store}
                self._asset_freq_dispatch[asset][freq]=mapping
            if exchange not in self._asset_freq_dispatch[asset][freq]:
                self._asset_freq_dispatch[asset][freq][exchange] = store
            if store not in self._stores:
                self._stores.add(store)
                
    def _add_store(self, store:DataStore):
        # reset sorted stores
        self._sorted_stores = []
        
        if 'hidden' in store.metadata and store.metadata['hidden']:
            return
        
        assets = store.db.list_assets()
        for asset in assets:
            self._add_route('asset_dispatch', asset, store)
            exchange = store.exchange
            freq = Frequency(store.frequency)
            daily = Frequency('1d')
            
            if asset in self._asset_freq_dispatch:
                if freq not in self._asset_freq_dispatch[asset]:
                    self._asset_freq_dispatch[asset][freq] = {}
                mapping = self._asset_freq_dispatch[asset][freq]
                if exchange and exchange not in mapping:
                    # do not overwrite
                    mapping[exchange] = store
            elif exchange:
                self._asset_freq_dispatch[asset] = {freq:{exchange:store}}
            
            if exchange and hasattr(store, 'read_daily') and freq < daily:
                # add to proxy store for later processing if we do not 
                # find a daily store for this asset/ exchange
                #dstore = store_factory(root=store.root, cache=False)
                dstore = copy.copy(store) # shallow copy
                dstore._read_between = dstore.read_between          # type: ignore
                dstore.read = dstore.read_daily                     # type: ignore
                if hasattr(dstore, 'read_between_daily'):
                    dstore.read_between = dstore.read_between_daily # type: ignore
                packet = (asset, daily, exchange, dstore)
                self._proxy_stores.append(packet)
                
        if store.exchange:
            if store.exchange not in self._exchange_preference:
                self._exchange_preference.append(store.exchange)
            self._add_route(
                    'exchange_dispatch', store.exchange, store)
            
        if store.frequency:
            self._add_route(
                    'freq_dispatch', Frequency(store.frequency), store)
        
        if store.name:
            self._add_route(
                    'name_dispatch', store.name, store, 
                    unique=True, raise_exception=False)
            
        self._stores.add(store)
        store.set_cache_policy(self._cache_policy)
        
    def _add_fx_rate_store(self, store:DataStore|None=None):
        if store and isinstance(store, DataStore):
            self._fx_rate_store = store
            
        try:
            if not isinstance(self._fx_rate_store, DataStore):
                if self._fx_rate_store:
                    # if not a store, we assume store name
                    fx_stores = [s for s in self.stores if s.name==self._fx_rate_store]
                else:
                    # else we search spotfx and forex stores
                    fx_stores = self._asset_dispatch.get(SpotFX)
                    if fx_stores is None:
                        fx_stores = self._asset_dispatch.get(Forex)
                    
                if fx_stores:
                    fx_stores = list(fx_stores)
                    stores = self._find_store_max_freq(fx_stores)
                    if len(stores) > 1:
                        msg = f'Cannot find unique store for fx rates.'
                        raise DataStoreException(msg)
                    
                    self._fx_rate_store = list(stores)[0]
                else:
                    self._fx_rate_store = None
        except Exception:
            pass
        
        # update the fx pairs if we have a fx store
        if isinstance(self._fx_rate_store, DataStore):
            assets = list(self._fx_rate_store.search())
            pairs = [a.base_ccy+'/'+a.quote_ccy for a in assets] # type: ignore
            reverse_pairs = [a.quote_ccy+'/'+a.base_ccy for a in assets] # type: ignore
            self._fx_pairs = dict(zip(pairs, assets))
            self._fx_reverse_pairs = dict(zip(reverse_pairs, assets))
                
        
    def _find_store_max_freq(self, stores) -> list[DataStore]:
        stores = list(stores)
        
        if not stores:
            return []
        
        periods = [Frequency(store.frequency).minute_bars \
                      for store in stores]
        max_freq = periods[np.argmin(periods)]
        
        return [store for store,period in zip(stores, periods) \
                    if period==max_freq]
    
    def _find_store_min_freq(self, stores) -> list[DataStore]:
        stores = list(stores)
        
        if not stores:
            return []
        
        periods = [Frequency(store.frequency).minute_bars \
                      for store in stores]
        min_freq = periods[np.argmax(periods)]
        
        return [store for store,period in zip(stores, periods) \
                    if period==min_freq]
        
    def _add_route(self, dispatch, key, value, unique=False, 
                   raise_exception=True):
        dispatch = '_' + dispatch
        if not hasattr(self, dispatch):
            setattr(self, dispatch, {})
        
        dispatch = getattr(self, dispatch)
        
        if key in dispatch:
            if unique:
                if raise_exception:
                    msg = f'cannot add store unambiguously, already exists:'
                    msg += f' {key}'
                    raise DataStoreException(msg)
                else:
                    # simply do not add the route
                    pass
            else:
                routes = dispatch[key]
                routes.add(value)
                dispatch[key] = routes
        else:
            if unique:
                dispatch[key] = value
            else:
                dispatch[key] = set([value])
        
    def _set_cache_policy(self, policy:str):
        self._cache_policy = policy
        for store in self._stores:
            store.set_cache_policy(policy)
            
    def _set_roll_policy(self, roll_day:int, roll_time:tuple[int,int]):
        for store in self._stores:
            store.set_roll_policy(roll_day, roll_time)
            
    def find_stores(self, store_name:str|None=None, exchange_name:str|None=None, 
                    asset_class:Type[MarketData]|None=None, frequency:str|Frequency|None=None, 
                    max_freq:bool=True) -> Iterator[DataStore]:
        if store_name:
            # unique mapping
            try:
                yield self._name_dispatch[store_name]
                return
            except Exception as e:
                msg = f'Error resolving store named {store_name} found:{str(e)}'
                raise DataStoreException(msg)
                
        if exchange_name:
            # unique for asset class + freq, else not
            try:
                if asset_class and frequency:
                    f = Frequency(frequency)
                    yield self._asset_freq_dispatch[asset_class][f][exchange_name]
                    return
                
                if max_freq:
                    for store in self.sort_stores(self._find_store_max_freq(
                            self._exchange_dispatch[exchange_name])):
                        yield store
                    return
                else:
                    for store in self.sort_stores(self._exchange_dispatch[exchange_name]):
                        yield store
                    return
            except Exception:
                msg = f'No store for exchange {exchange_name} found.'
                raise DataStoreException(msg) 
                
        if asset_class and frequency:
            # unique if exchange is specified, that case is already handled
            f = Frequency(frequency)
            try:
                stores = self._asset_freq_dispatch[asset_class][f].values()
                
                if max_freq:
                    for store in self.sort_stores(self._find_store_max_freq(stores)):
                        yield store
                    return
                else:
                    for store in self.sort_stores(list(stores)):
                        yield store
                    return
            except Exception:
                msg = f'No store for asset {asset_class} and '
                msg += f'frequency {f.period}.'
                raise DataStoreException(msg)
                
        if asset_class:
            stores = self._asset_dispatch.get(asset_class,[])
            
            if max_freq:
                for store in self.sort_stores(self._find_store_max_freq(stores)):
                    yield store
                return
            else:
                for store in self.sort_stores(stores):
                    yield store
                return
                    
        if frequency:
            f = Frequency(frequency)
            stores = self._freq_dispatch[f]
            if max_freq:
                for store in self.sort_stores(self._find_store_max_freq(stores)):
                    yield store
                return
            else:
                for store in self.sort_stores(stores):
                    yield store
                return
        
        if max_freq:
            for store in self.sort_stores(self._find_store_max_freq(self._stores)):
                yield store
            
            return
                
        for store in self.stores:
                yield store
            
    def guess_active_store(self, name=None) -> DataStore|None:
        if isinstance(self._active_store, DataStore):
            return self._active_store
        
        name = name or self._active_store
        if name:
            stores = [s for s in self.find_stores(name) if s.type in ('AssetStore','ColumnStore')]
            if len(stores) < 1:
                msg = f'No store named {name} found.'
                raise DataStoreException(msg)
            return stores[0]
        
        # find the store with minimum frequency in a defined order of assets
        for asset in _ACTIVE_STORE_ASSET_PREFERECE:
            stores = self.find_stores(asset_class=asset, max_freq=True)
            stores = [s for s in stores if s.type in ('AssetStore','ColumnStore')]
            
            if stores:
                if name:
                    matches = [s for s in stores if s.name==name]
                    if matches:
                        return matches[0]
                if len(stores) > 1:
                    msg = f'No unique store for asset class {asset}, '
                    msg += 'will set the first instance as active.'
                    warnings.warn(msg, BlueshiftWarning)
                    
                return stores[0]
            
    def find_pipeline_store(self,name=None) -> DataStore|None:
        if isinstance(self._pipeline_store, DataStore):
            return self._pipeline_store
        
        # find a store with daily frequency and eligible asset classes
        if not name:
            # we assume store name or None
            name = self._pipeline_store
        
        candidates = []
        freq = Frequency('1d')
        
        for asset in _PIPELINE_ASSETS_PREFERENCE:
            try:
                stores = self.find_stores(asset_class=asset, frequency=freq)
                for store in stores:
                    if store:
                        candidates.append(store)
            except Exception:
                pass
                    
        if name:
            stores = [st for st in candidates if st.name==name]
            if stores:
                return stores[0]
            else:
                msg = f'no eligible store found with name {name}.'
                raise DataStoreException(msg)
        
        if candidates:
            candidates = [c for c in candidates if c.type in ('ColumnStore')]
                
        if len(candidates) > 1:
            msg = 'No unique pipeline store, '
            msg += 'will set the first instance as eligible.'
            warnings.warn(msg, BlueshiftWarning)
            
        if candidates:
            return candidates[0]
        
    def find_futures_store(self) -> DataStore|None:
        if isinstance(self._futures_store, DataStore):
            return self._futures_store
        
        return self.find_fno_store([EquityFutures])
    
    def find_options_store(self) -> DataStore|None:
        if isinstance(self._options_store, DataStore):
            return self._options_store
        
        return self.find_fno_store([EquityOption])
    
    def find_fno_store(self, eligible_assets=None) -> DataStore|None:
        # find the store in eligible asset classes and minimum frequency
        candidates = []
        if not eligible_assets:
            eligible_assets = _FNO_ASSETS
        
        for asset in eligible_assets:
            try:
                stores = self.find_stores(asset_class=asset, max_freq=True)
                for store in stores:
                    if store:
                        candidates.append(store)
            except Exception:
                pass
            
        if candidates:
            candidates = [c for c in candidates if c.type in ('AssetStore','ColumnStore')]
                    
        if len(candidates) > 1:
            msg = 'No unique derivatives store, '
            msg += 'will set the first instance as eligible.'
            warnings.warn(msg, BlueshiftWarning)
        
        if candidates:
            return candidates[0]
        
    def find_fundamental_store(self,freq, exchange=None) -> FundamentalStore|None:
        try:
            store = None
            mapping = self._asset_freq_dispatch[Equity][freq]
            
            if mapping and exchange:
                store = mapping.get(exchange, None)
            elif mapping:
                stores = self.sort_stores(list(mapping.values()))
                stores = [s for s in stores if s.type=='FundamentalStore']
                
                if len(stores) > 0:
                    if len(stores) > 1:
                        msg = 'No unique fundamental store, '
                        msg += 'will choose the first eligible instance.'
                        warnings.warn(msg, BlueshiftWarning)
                    store = stores[0]
                    
            if store:
                store = cast(FundamentalStore, store)
                return store
        except Exception:
            pass

class Library(ILibrary):
    """
        Library is a namespace over lower-level `DataStore`. This is 
        primarily for read and other data query operations. Actual 
        writes should be done using `DataStore` implementation, via an 
        `IngestionEngine` or directly. Therefore no write methods are 
        supported within a library.
        
        To instantiate a library, supply a name and a root location. It 
        will automatically search all stores at the location and include 
        them in the library instance. Also you can supply a list of stores 
        separately.
        
        Library primarily acts as a router for underlying store's 
        reading functions when handling multiple store, potentially 
        covering multiple data frequency, exchanges or asset classes. 
        It also implements the `AssetFinder` and `DataPortal` interfaces 
        to be a complete class to find, fetch and analysis assets and 
        their associated data.
        
        Library interface makes it easy to run exploratory queries over a 
        collection of stores. Also, it is the preferred input for 
        backtests to easily handle dispatching. In general, the cache 
        policy should be 'random'. If used for backtest, set it to 
        'sliding' for an optimized cache for sequential reads.
        
        Specify `lazy_load` as True to load the store readily.
        This is useful as in most cases, a store maynot be needed and 
        store read from the disk and may need a long time to load. 
        This flag allows just-in-time lazy loading. This flag is ignored 
        if a `root` is specified.
        
        Args:
            `root (str)`: Root location of the library.
            
            `name (str)`: Name of the library.
            
            `stores (list)`: List of stores objects.
            
            `active_store (obj)`: The active store or name.
            
            `pipeline_store (obj)`: Pipline store (or name).
            
            `fx_rate_store (obj)`: FX rate store (for fx conversion) or name.
            
            `logger (obj)`: Logger object.
            
            `cache (str)`: Choice of caching policy - sliding or random.
            
            `discover (bool)`: Search for stores.
            
            `lazy_load (bool)`: Load stores lazily (not active store).
            
            `exchange_preference (list)`: Exchange preference for symbol search
    """
    _metafile = 'library.json'
    
    @classmethod
    def list_libraries(cls, path):
        datasets = []
        path = os.path.expanduser(path)
        if not os.path.exists(path):
            return datasets
        
        folders = os.listdir(path)
        for folder in folders:
            if os.path.exists(os.path.join(path,folder,cls._metafile)):
                datasets.append(folder)
                
        return datasets
    
    def __init__(self, root:str|None=None, name:str|None=None, stores:list[DataStore]|None=None, 
                 active_store:DataStore|str|None = None, pipeline_store:DataStore|str|None=None, 
                 fx_rate_store:DataStore|str|None=None, futures_store:DataStore|str|None=None, 
                 options_store:DataStore|str|None=None, logger:logging.Logger|None=None, 
                 cache:str='random', discover:bool=False, metadata:dict|None=None, 
                 max_assets:int=_MAX_ASSETS, lazy_load:bool=True, exchange_preference:list[str]|None=None):
        stores = stores or []
        metadata = metadata or {}
        
        self._root = None
        # for lazy loading
        self._pipeline_store_root = None
        self._fx_rate_store_root = None
        self._futures_store_root = None
        self._options_store_root = None
        
        self._discover = discover
        self._metadata = metadata
        self._max_assets = max_assets
        
        if root:
            root = os.path.abspath(os.path.expanduser(root))
            if os.path.exists(root):
                self._root = root
        
        self._cache_policy = cache
        
        if name:
            self._name = name
        else:
            if self._root:
                self._name = os.path.basename(self._root)
            else:
                self._name = 'library'
        
        if logger:
            self._logger = logger
        else:
            self._logger = logging.getLogger(self.name)
        
        self._logger.addFilter(DeprecationFilter())
        
        self._benchmark = None
        
        self._initialize(
                stores, active_store, pipeline_store, fx_rate_store,
                futures_store, options_store, lazy_load=lazy_load,
                exchange_preference=exchange_preference)
        
    def _initialize(self, stores=[], active_store = None, 
                    pipeline_store=None, fx_rate_store=None,
                    futures_store=None, options_store=None,
                    lazy_load=True, exchange_preference=None):
        # read metadata first
        self._read_metadata()
        
        if exchange_preference:
            self._exchange_preference = exchange_preference
        else:
            self._exchange_preference = self._metadata.get('exchange_preference')
        
        self._exchange_preference = self._exchange_preference or []
        
        if not self._discover:
            if not active_store:
                active_store = self._metadata.get('active_store')
                active_store = get_full_path(active_store, self.root)
            if not pipeline_store:
                pipeline_store = self._metadata.get('pipeline_store')
                pipeline_store = get_full_path(pipeline_store, self.root)
            if not fx_rate_store:
                fx_rate_store = self._metadata.get('fx_rate_store')
                fx_rate_store = get_full_path(fx_rate_store, self.root)
            if not futures_store:
                futures_store = self._metadata.get('futures_store')
                futures_store = get_full_path(futures_store, self.root)
            if not options_store:
                options_store = self._metadata.get('options_store')
                options_store = get_full_path(options_store, self.root)
        
        if self._root:
            all_stores = self._read_stores()
            # stores can be name or Store object, we add to store only 
            # in case it is object. Name means stores are in the root
            if active_store and isinstance(active_store, DataStore) and \
                active_store not in all_stores:
                all_stores.append(active_store)
            if pipeline_store and isinstance(pipeline_store, DataStore) \
                and pipeline_store not in all_stores:
                all_stores.append(pipeline_store)
            if fx_rate_store and isinstance(fx_rate_store, DataStore) \
                and fx_rate_store not in all_stores:
                all_stores.append(fx_rate_store)
            if futures_store and isinstance(futures_store, DataStore) \
                and futures_store not in all_stores:
                all_stores.append(futures_store)
            if options_store and isinstance(options_store, DataStore) \
                and options_store not in all_stores:
                all_stores.append(options_store)
                
            self.__stores = __Stores__(
                    all_stores, 
                    active_store,
                    pipeline_store,
                    fx_rate_store,
                    futures_store,
                    options_store,
                    self._cache_policy,
                    exchange_preference=self._exchange_preference)
        else:
            all_stores = []
            if active_store and isinstance(active_store, DataStore):
                all_stores.append(active_store)
            
            if pipeline_store and isinstance(pipeline_store, DataStore):
                all_stores.append(pipeline_store)
            elif pipeline_store and isinstance(pipeline_store, str) and lazy_load:
                self._pipeline_store_root = pipeline_store
                pipeline_store = None
            
            if fx_rate_store and isinstance(fx_rate_store, DataStore):
                all_stores.append(fx_rate_store)
            elif fx_rate_store and isinstance(fx_rate_store, str) and lazy_load:
                self._fx_rate_store_root = fx_rate_store
                fx_rate_store = None
                
            if futures_store and isinstance(futures_store, DataStore):
                all_stores.append(futures_store)
            elif futures_store and isinstance(futures_store, str) and lazy_load:
                self._futures_store_root = futures_store
                futures_store = None
                
            if options_store and isinstance(options_store, DataStore):
                all_stores.append(options_store)
            elif options_store and isinstance(options_store, str) and lazy_load:
                self._options_store_root = options_store
                options_store = None
                
            self.__stores = __Stores__(
                    all_stores, active_store, pipeline_store, 
                    fx_rate_store, futures_store, 
                    options_store, self._cache_policy,
                    exchange_preference=self._exchange_preference)
        
        if not listlike(stores):
            self.add_store(stores)
        else:
            for store in stores:
                self.add_store(store)
        
        self.__stores._update_proxy_store()
                
        self._active_store = self.__stores.guess_active_store()
        if self._active_store is None:
            msg = f'No store supplied, nor found at root {self._root}.'
            raise DataStoreException(msg)
        
        self._futures_store = self.__stores.find_futures_store()
        self._options_store = self.__stores.find_options_store()
        self._pipeline_store = self.__stores.find_pipeline_store()
        self._quarterly_fundamental_store = \
            self.__stores.find_fundamental_store(Frequency('Q'))
        self._annual_fundamental_store = \
            self.__stores.find_fundamental_store(Frequency('A'))
            
        if self._pipeline_store:
            self._asset_finder_store = self._pipeline_store
        else:
            self._asset_finder_store = self._active_store
            
        for store in self.stores:
            cal = store.calendar
            if cal:
                try:
                    get_calendar(cal.name)
                except Exception:
                    register_calendar(cal.name, cal)
        
        if not self._exchange_preference:
            exchanges = [k for k in self.exchange_dispatch]
            self._exchange_preference = sorted(exchanges)
        
        # delay setting benchmark only when needed
        #self.set_benchmark()
        self._write_metadata()
                
    def __str__(self) -> str:
        return f"Blueshift Library({self.name}@[{self.root}])"
    
    def __repr__(self) -> str:
        return self.__str__()
    
    def __contains__(self, value) -> bool:
        if isinstance(value, DataStore):
            # search for store match
            return value in self.__stores.stores
        
        if isinstance(value, str):
            # search for store name, exchange name and asset symbol
            # in that order
            if value in self.name_dispatch:
                return True
            
            if value in self.exchange_dispatch:
                return True
            
            asset = self._symbol_to_asset(value)
            return asset is not None
        
        if isinstance(value, MarketData):
            # search for asset object
            store = self._asset_to_store(value)
            return store is not None
        
        return False
    
    def __getitem__(self, key:str|MarketData|tuple) -> DataStore:
        # we must resolve to a unique store, else raise exception
        store = None
        
        if isinstance(key, str):
            # can be store name, exchange or frequency
            if key in self.__stores._name_dispatch:
                # case of store name
                return self.__stores._name_dispatch[key]
            elif key in self.__stores._exchange_dispatch:
                # case of exchange name
                stores = self.__stores._find_store_max_freq(
                    self.__stores._exchange_dispatch[key])
                
                if not stores:
                    raise KeyError('No store found for exchange {key}.')
                
                if len(stores) > 1:
                    stores = self.sort_stores(stores)
                    
                return list(stores)[0]
            else:
                # case of frequency
                try:
                    freq = Frequency(key)
                except Exception:
                    raise KeyError(f'Not a valid key {key}.')
                else:
                    stores = self.__stores._freq_dispatch.get(freq, [])
                    if not stores:
                        raise KeyError(f'No store found for frequency {key}.')
                        
                    if len(stores) > 1:
                        stores = self.sort_stores(stores)
                    
                    store = list(stores)[0]
            
            raise KeyError(f'Not a valid key {key}.')
                
        if isinstance(key, Frequency):
            # a frequency object
            stores = self.__stores._freq_dispatch.get(key, [])
            
            if not stores:
                raise KeyError('No store found for frequency {key}.')
            
            if len(stores) > 1:
                stores = self.sort_stores(stores)
                        
            return list(stores)[0]
        
        if type(key) == type and issubclass(key, MarketData):
            # an asset object
            stores = self.__stores._find_store_max_freq(
                    self.__stores._asset_dispatch.get(key, []))
            
            if not stores:
                raise KeyError('No store found for asset {key}.')
                
            if len(stores) > 1:
                stores = self.sort_stores(stores)
            
            return list(store)[0] # type: ignore
        
        if isinstance(key, tuple):
            # asset frequency [exchange] dispatch
            if issubclass(key[0], MarketData) and len(key) > 1 \
                and len(key) < 4:
                try:
                    f = Frequency(key[1])
                    if len(key)==3:
                        store = self.__stores\
                            ._asset_freq_dispatch[key[0]][f][key[2]]
                        if not store:
                            msg =f'No store found for key {key}.'
                            raise KeyError(msg)
                        return store
                    else:
                        # we may be ambiguous on exchange
                        stores = self.__stores._asset_freq_dispatch[key[0]][f].values()
                        if not stores:
                            msg =f'No store found for key {key}.'
                            raise KeyError(msg)
                        if len(stores) > 1:
                            stores = self.sort_stores(list(stores))
                        return list(stores)[0]
                except Exception:
                    pass
        
        raise KeyError(f'Key {key} not found in the library.')      

    def _set_active(self, store:DataStore, suppress_warning:bool=False):
        if store == self.active:
            return
        
        if store not in self.stores:
            warnings.warn(
                    f'Cannot set, the store is not part of the library.', 
                    BlueshiftWarning)
            return
        
        if self._active_store is not None and not suppress_warning:
            warnings.warn(
                    f'The active store is changed to {store.name}.', 
                    BlueshiftWarning)
        
        return store

    @classmethod
    def fetch_metadata(cls, root:str|None=None):
        if not root:
            return {}
        
        root = os.path.expanduser(root)
        meta_path = os.path.join(root, cls._metafile)
        if os.path.exists(meta_path):
            with open(meta_path) as fp:
                metadata = json.load(fp)
                return metadata
            
        return {}
                
    @property
    def name(self) -> str:
        """ name of the libarry. """
        return self._name
    
    @property
    def tz(self) -> str:
        """ timezone of the libarry. """
        if self.active:
            return self.active.tz
        
        timezones  = set([store.tz for store in self.stores])
        if len(timezones) == 1:
            return list(timezones)[0]
        
        raise DataStoreException('Library has stores in mulitple timezones.')
    
    @property
    def root(self) -> str|None:
        """ location of the library. """
        return self._root
    
    @property
    def metadata(self) -> dict:
        """ metadata of the library. """
        return self._metadata
    
    @property
    def stores(self) -> list[DataStore]:
        """ get the incldued stores in the library. """
        return self.__stores.stores
    
    @property
    def active(self) -> DataStore:
        """ get the current active store in the library. """
        if not self._active_store:
            raise DataStoreException(f'no active store defined for the library')
        return self._active_store
    
    @active.setter
    def active(self, value:DataStore):
        """ set the current active store in the library. """
        store = self._set_active(value)
        if store:
            self._active_store = store
    
    @property
    def asset_dispatch(self) -> dict[type[MarketData], list[DataStore]]:
        return self.__stores._asset_dispatch
    
    @property
    def exchange_dispatch(self) -> dict[str, list[DataStore]]:
        return self.__stores._exchange_dispatch
    
    @property
    def frequency_dispatch(self) -> dict[Frequency, list[DataStore]]:
        return self.__stores._freq_dispatch
    
    @property
    def name_dispatch(self) -> dict[str, DataStore]:
        return self.__stores._name_dispatch
    
    @property
    def asset_frequency_dispatch(self) -> dict[type[MarketData], dict[Frequency, dict[str, DataStore]]]:
        return self.__stores._asset_freq_dispatch
    
    @property
    def quarterly_fundamental_store(self) -> FundamentalStore|None:
        return self._quarterly_fundamental_store
    
    @property
    def annual_fundamental_store(self) -> FundamentalStore|None:
        return self._annual_fundamental_store
    
    @property
    def pipeline_store(self) -> DataStore|None:
        if self._pipeline_store:
            return self._pipeline_store
        
        if self._pipeline_store_root:
            store = self._read_store(self._pipeline_store_root)
            if store and isinstance(store, DataStore):
                self.__stores._pipeline_store = store
                self._pipeline_store = store
                self.add_store(store)
                self._asset_finder_store = self._pipeline_store
        
        return self._pipeline_store
    
    @property
    def fx_rate_store(self) -> DataStore|None:
        if self.__stores._fx_rate_store:
            return self.__stores._fx_rate_store
        
        if self._fx_rate_store_root:
            store = self._read_store(self._fx_rate_store_root)
            if store and isinstance(store, DataStore):
                self.__stores._add_fx_rate_store(store)
                
        return self.__stores._fx_rate_store
                
    @property
    def asset_finder_store(self) -> DataStore:
        return self._asset_finder_store
    
    @asset_finder_store.setter
    def asset_finder_store(self, value:DataStore):
        self._asset_finder_store = value
        
    @property
    def futures_store(self) -> DataStore|None:
        if self._futures_store:
            return self._futures_store
        
        if self._futures_store_root:
            store = self._read_store(self._futures_store_root)
            if store and isinstance(store, DataStore):
                self.__stores._futures_store = store
                self._futures_store = store
                self.add_store(store)
                self.__stores._update_proxy_store()
                
        return self._futures_store
    
    @futures_store.setter
    def futures_store(self, value:DataStore):
        self._futures_store = value
        
    @property
    def options_store(self) -> DataStore|None:
        if self._options_store:
            return self._options_store
        
        if self._options_store_root:
            store = self._read_store(self._options_store_root)
            if store and isinstance(store, DataStore):
                self.__stores._options_store = store
                self._options_store = store
                self.add_store(store)
                self.__stores._update_proxy_store()
                
        return self._options_store
    
    @options_store.setter
    def options_store(self, value:DataStore):
        self._options_store = value
        
    @property
    def start_date(self) -> pd.Timestamp|NaTType:
        try:
            return self.active.metadata['start_date']
        except Exception:
            return pd.Timestamp(None) # type: ignore
    
    @property
    def end_date(self) -> pd.Timestamp:
        try:
            return self.active.metadata['end_date']
        except Exception:
            return pd.Timestamp(None) # type: ignore
        
    def _read_metadata(self):
        if not self.root:
            return
        
        meta_path = os.path.join(self.root, self._metafile)
        if os.path.exists(meta_path):
            with open(meta_path) as fp:
                metadata = json.load(fp)
                self._metadata = {**self._metadata, **metadata}
    
    def _write_metadata(self, forced:bool=False):
        if not self.root:
            return
        
        if not self._discover and not forced:
            return
        
        if isinstance(self.active, DataStore):
            self._metadata['active_store'] = get_relative_path(
                    self.active.root, self.root)
        
        if isinstance(self.pipeline_store, DataStore):
            self._metadata['pipeline_store'] = get_relative_path(
                    self.pipeline_store.root, self.root)
            
        if isinstance(self.fx_rate_store, DataStore):
            self._metadata['fx_rate_store'] = get_relative_path(
                    self.fx_rate_store.root, self.root)
            
        if isinstance(self.futures_store, DataStore):
            self._metadata['futures_store'] = get_relative_path(
                    self.futures_store.root, self.root)
            
        if isinstance(self.options_store, DataStore):
            self._metadata['options_store'] = get_relative_path(
                    self.options_store.root, self.root)
            
        if self._exchange_preference:
            self._metadata['exchange_preference'] = self._exchange_preference
        
        metapath = os.path.join(self.root, self._metafile)
        with open(metapath,'w') as fp:
            json.dump(self._metadata, fp)
            
    def _read_store(self, root, ignore_errors=True) -> DataStore|None:
        try:
            store = store_factory(root=get_full_path(root, self.root), cache=True)
        except TerminationError:
            raise
        except Exception as e:
            if ignore_errors:
                return
            else:
                raise DataStoreException(
                        f"error reading store at {root}:{str(e)}")
        else:
            return store
        
    def _read_stores(self, ignore_errors=True) -> list[DataStore]:
        stores = []
        
        if not self._discover and 'stores' in self._metadata:
            for root in self._metadata['stores']:
                try:
                    store = store_factory(
                            root=get_full_path(root, self.root), cache=True)
                except TerminationError:
                    raise
                except Exception as e:
                    if ignore_errors:
                        continue
                    else:
                        raise DataStoreException(
                            f"error reading store at {root}:{str(e)}")
                else:
                    stores.append(store)
                    
            return stores
        
        store_roots = []
        for root in walk_stores(self.root):
            try:
                store = store_factory(root=root, cache=True)
            except TerminationError:
                raise
            except Exception as e:
                if ignore_errors:
                    continue
                else:
                    raise DataStoreException(
                            f"error reading store at {root}:{str(e)}")
            else:
                stores.append(store)
                store_roots.append(get_relative_path(root, self.root))
                
        self._metadata['stores'] = store_roots
        return stores
    
    def _clear_cache(self):
        self._symbol_to_asset.cache_clear() # type: ignore
        self._asset_to_store.cache_clear()
        self.fetch_asset.cache_clear()
        
    #################### library methods ###############################
    
    def add_store(self, store:DataStore):
        """ add a store to the library manually. """
        if not isinstance(store, DataStore):
            raise DataStoreException('not a store.')
        
        self.__stores._add_store(store)
        self._clear_cache()
        
    def find_stores(self, store_name:str|None=None, exchange_name:str|None=None, 
                   asset_class:Type[MarketData]|None=None, frequency:str|Frequency|None=None, 
                   max_freq:bool=True) -> list[DataStore]:
        stores = self.__stores.find_stores(
                store_name=store_name, exchange_name=exchange_name, 
                   asset_class=asset_class, frequency=frequency, 
                   max_freq=max_freq)
        
        return [s for s in stores]
    
    def sort_stores(self, stores:list[DataStore]|set[DataStore]) -> list[DataStore]:
        return self.__stores.sort_stores(stores)
    
    def reset(self):
        for store in self.stores:
            store.reset()
    
    def refresh(self, stores:list[DataStore]|None=None):
        if stores is None:
            stores = []
        self._initialize(stores)
        
    def set_cache_policy(self, policy:str='random'):
        """
            Set the caching policy on this library. If the policy is 
            `sliding` the subsequent reads must be in increasing order 
            of time and reading data for a past date will raise error. 
            Otherwise, random reads will be supported.
            
            Args:
                `policy (str)`: Set the cache policy for reads.
                
            Returns:
                None.
        """
        self._cache_policy = policy
        self.__stores._set_cache_policy(policy)
        
    def set_roll_policy(self, roll_day:int=0, roll_time:tuple|datetime.time=(23,59)):
        """
            Set the roll policy on this library. The parameter ``roll`` is 
            the offset from the next expiry when futures and options are 
            rolled to compute pricing data for continues assets.
            
            Args:
                `roll_day (int)`: Set the roll day offset from the expiry date.
                
                `roll_time (tuple)`: Set (hour, minute) tuple for roll time.
                
            Returns:
                None.
        """
        if isinstance(roll_time, datetime.time):
            roll_time = (roll_time.hour, roll_time.minute)
        self.__stores._set_roll_policy(roll_day, roll_time)
        
    ############ symbol and asset routing methods ####################
        
    @lru_cache(maxsize=_CACHE_SIZE)
    def _symbol_to_asset(self, sym:str, store:DataStore|None=None, dt:pd.Timestamp|None=None, 
                         **kwargs) -> MarketData|None:
        # this does not uniquely identify the store, a given symbol may be 
        # in multiple stores with different frequencies and exchanges. 
        # this method first tries to match the current active store, 
        # followed by the asset_finder store. else this method returns 
        # a first match. It is recommended to add exchange prefix to 
        # symbol whenever there is an ambiguity
        store_types = ('AssetStore','ColumnStore')
        
        if 'use_spot' in kwargs and kwargs['use_spot']:
            try:
                _, _, _, option_type, _, strike_offset, _ = \
                    get_opt_details(sym)
                if option_type is not None and strike_offset is not None:
                    stores = []
                    for asset in (Equity, MarketData, Forex):
                        try:
                            stores.append(self[asset, Frequency('1m')])
                        except Exception:
                            pass
                    
                    if stores:
                        kwargs['spot_stores'] = stores
            except Exception:
                pass
            
        def try_sym_with_dt(sym:str, store:DataStore, dt:pd.Timestamp|None=None, 
                            **kwargs) -> MarketData|None:
            if store:
                try:
                    return store.symbol(sym, dt, **kwargs)
                except AssetsDBException:
                    raise
                except Exception:
                    pass
        
        splits = sym.split(':')
        
        if len(splits) > 1:
            # we have exchange defined!
            exchange, sym = splits[0], splits[1]
            if exchange.upper() == 'FXCM':
                msg = 'Deprecation Warning: '
                msg += 'You do not need to use any prefix in '
                msg += 'forex symbols anymore. This will throw '
                msg += 'error in a future version.'
                self._logger.warning(msg)
                exchange = 'FX'
            
            if store:
                if store.type not in store_types:
                    return
                # if store given check the exchange of the store
                if store.exchange is None or store.exchange == exchange:
                    return try_sym_with_dt(sym, store, dt, **kwargs)
                else:
                    # given store and no exchange match
                    return
            else:
                # check if the current active store match
                if self.active and self.active.exchange == exchange:
                    asset =  try_sym_with_dt(
                            sym, self.active, dt, **kwargs)
                    if asset:
                        return asset
                elif self.active:
                    asset =  try_sym_with_dt(
                            sym, self.active, dt, **kwargs)
                    if asset and asset.exchange_name == exchange:
                        return asset
                    
                # check if the current asset finder store match the exchange
                if self.asset_finder_store and \
                    self.asset_finder_store.exchange == exchange:
                    asset =  try_sym_with_dt(
                            sym, self.asset_finder_store, dt, **kwargs)
                    if asset:
                        return asset
                elif self.asset_finder_store:
                    asset =  try_sym_with_dt(
                            sym, self.asset_finder_store, dt, **kwargs)
                    if asset and asset.exchange_name == exchange:
                        return asset
                
                # else return the first match
                for store in self.sort_stores(self.exchange_dispatch.get(exchange,[])):
                    if store.type not in store_types:
                        continue
                    asset = try_sym_with_dt(sym, store, dt, **kwargs)
                    if asset and asset.exchange_name == exchange:
                        return asset
                    
                # elif exchange is not defined for store
                for store in self.stores:
                    if store.type not in store_types:
                        continue
                    asset = try_sym_with_dt(sym, store, dt, **kwargs)
                    if asset and asset.exchange_name==exchange:
                        return asset
                    
            return
        
        if store:
            if store.type not in store_types:
                return
            return try_sym_with_dt(sym, store, dt, **kwargs)
        else:
            if self.active:
                asset = try_sym_with_dt(sym, self.active, dt, **kwargs)
                if asset:
                    return asset
            
            if self.asset_finder_store:
                asset = try_sym_with_dt(
                        sym, self.asset_finder_store, dt, **kwargs)
                if asset:
                    return asset
            
            for store in self.stores:
                if store.type not in store_types:
                    continue
                asset = try_sym_with_dt(sym, store, dt, **kwargs)
                if asset:
                    return asset
        
    @lru_cache(maxsize=_CACHE_SIZE)
    def _asset_to_store(self, asset:MarketData, freq:str|Frequency|None=None, 
                        store_types:tuple|None=None) -> DataStore|None:
        if not store_types:
            store_types = ('AssetStore','ColumnStore')
        
        if not listlike(store_types):
            store_types = (store_types,)
            
        if not freq:
            # first check the active store
            if self.active.find(asset) and self.active.type in store_types:
                if self.active.exchange and asset.exchange_name:
                    if self.active.exchange == asset.exchange_name:
                        return self.active
                else:
                    return self.active
            # then check the asset finder store
            if self.asset_finder_store and self.asset_finder_store.find(asset) \
                and self.asset_finder_store.type in store_types: # type: ignore
                if self.asset_finder_store.exchange and asset.exchange_name:
                    if self.asset_finder_store.exchange == asset.exchange_name:
                        return self.asset_finder_store
                else:
                    return self.asset_finder_store
        
        if freq:
            store = None
            freq = Frequency(freq)
            
            try:
                # match the asset, freq and exchange triplet
                store = self[asset.__class__, freq, asset.exchange_name]
                if store.type not in store_types:
                    raise ValueError(f'store type does not match')
            except Exception:
                try:
                    # match the asset and freq
                    store = self[asset.__class__, freq]
                    is_valid = store.exchange is None or asset.exchange_name is None
                    is_valid = is_valid or store.exchange==asset.exchange_name
                    is_valid = is_valid and store.type in store_types
                    
                    if not is_valid:
                        raise ValueError(f'no store matches exchange')
                except Exception:
                    # store must match freq and must have no exchange, 
                    # otherwise it should have matched dispatches before
                    store = self.active
                    is_valid = store.exchange is None or asset.exchange_name is None
                    is_valid = is_valid or store.exchange==asset.exchange_name
                    is_valid = is_valid and store.type in store_types # type: ignore
                    
                    if store and store.find(asset) and \
                        Frequency(store.frequency) == freq and is_valid:
                        return store
                    
                    # store must match freq and must have no exchange
                    store = self.asset_finder_store
                    is_valid = store.exchange is None or asset.exchange_name is None
                    is_valid = is_valid or store.exchange==asset.exchange_name
                    is_valid = is_valid and store.type in store_types # type: ignore
                    
                    if store and store.find(asset) and \
                        Frequency(store.frequency) == freq and is_valid:
                        return store
                    else:
                        stores = [s for s in self.stores if s.find(asset) and Frequency(s.frequency)==freq]
                        stores = [s for s in stores if \
                                  s.exchange is None or asset.exchange_name is None or s.exchange==asset.exchange_name]
                        stores = self.sort_stores([s for s in stores if s.type in store_types])
                        
                        if stores:
                            return stores[0]
                        else:
                            store = None
                
            if store and store.find(asset):
                # exchange and type is already matched at this point
                return store
        else:
            # search all stores for the exchange that has this asset
            # return the max frequency match
            stores = self.exchange_dispatch.get(asset.exchange_name,[])
            stores = [s for s in stores if s.find(asset)]
            stores = [s for s in stores if \
                      s.exchange is None or asset.exchange_name is None or s.exchange==asset.exchange_name]
            stores = self.sort_stores([s for s in stores if s.type in store_types])
                
            if stores:
                return stores[0]
            
            # search all stores and return the first match
            stores = [s for s in self.stores if s.find(asset)]
            stores = [s for s in stores if \
                      s.exchange is None or asset.exchange_name is None or s.exchange==asset.exchange_name]
            stores = self.sort_stores([s for s in stores if s.type in store_types])
                
            if stores:
                return stores[0]
        
    def symbol_to_asset(self, sym:str, store:DataStore|None=None, dt:pd.Timestamp|None=None, 
                        **kwargs) -> MarketData:
        """ Fetch asset object given a symbol. """
        if dt is not None:
            try:
                dt = pd.Timestamp(dt)
            except Exception:
                raise SymbolNotFound(f'Illegal timestamp {dt}.')
        try:
            asset = self._symbol_to_asset(sym, store, dt,**kwargs)
        except TerminationError:
            raise
        except AssetsDBException as e:
            raise SymbolNotFound(str(e))
        except Exception as e:
            raise DataStoreException(str(e))
            
        if not asset:
            msg = f'symbol {sym} not found. '
            msg += 'Are you using the correct data-set?'
            raise SymbolNotFound(msg)
            
        return asset
    
    def asset_to_store(self, asset:MarketData, frequency=None, store_types=None) -> DataStore:
        """
            Find the appropriate store given an asset.
            
            Note:
                If frequency is specified, the store that matches the 
                exact frequency and exchange (in ``exchange_name`` field 
                of the asset object) is returned. Else the store with max 
                data frequency which matches the asset is returned. If no 
                match is found, exception is raised.
                
            Args:
                `asset (obj)`: An asset object.
                
                `frequency (str or obj)`: A frequency string or object.
                
            Returns:
                Store, A `store` object.
        """
        store = self._asset_to_store(asset, frequency, store_types)
        if not store:
            if store_types:
                msg = f'no store of type {store_types} found for asset {asset}.'
            else:
                msg = f'no store found for asset {asset}.'
            raise DataStoreException(msg)
            
        return store
        
    ################## AssetFinder interface methods ###############
    
    def refresh_data(self, *args, **kwargs):
        """ refresh data not supported. """
        pass
    
    @lru_cache(maxsize=_CACHE_SIZE)
    def fetch_asset(self, sid:int) -> MarketData: # type: ignore
        """ 
            Fetch asset object given a SID. If `default_none` set to True 
            return None if no asset found, else raise exception.
        """
        asset = None
        try:
            asset = self.asset_finder_store.sid(int(sid))
        except TerminationError:
            raise
        except Exception as e:
            raise DataStoreException(str(e))
            
        if not asset:
            msg = f'Asset ID {sid} not found.'
            raise SymbolNotFound(msg)
            
        return asset
    
    def symbol(self, sym:str, dt:pd.Timestamp|None=None, *args, **kwargs) -> MarketData:
        """ Fetch asset object given a symbol. """ 
        asset = self.symbol_to_asset(sym, dt=dt, **kwargs)
        if 'product_type' in kwargs:
            asset = convert_asset(asset, kwargs['product_type']) # type: ignore
            if not asset:
                msg = f"Asset with symbol {sym} and product type {kwargs['product_type']} not found."
                raise SymbolNotFound(msg)
        return asset
    
    def search(self, symbols:str|list[str]|None=None, **kwargs):
        """
            Search the store for assets by symbol name.
            
            Note:
                This function will fetch all asset tables within the db by 
                symbol name by default. If no `symbols` supplied, all 
                available assets will be returned (for the given table, 
                or all table if not `table` is specified).
            
            Args:
                `symbols (str)`: A symbol (or list). Returns all if None.
                
            Returns:
                List. A list of asset objects.
        """
        if symbols:
            return self.active.search(symbols)
        
        syms:set[MarketData] = set()
        for store in self.stores:
            syms.update(store.search())
            
        assets = sorted(list(syms))
            
        return assets
    
    def fuzzy_search(self, symbol:str|list[str], **kwargs) -> list[MarketData]:
        """
            Search the stores for assets by symbol name, matching 
            the ones that start with the given value. Search the 
            highest frequency stores from the asset dispatch mapping.
            
            Note:
                This function will fetch all asset tables within the db by 
                symbol name by default.
            
            Args:
                `symbol (str)`: A symbol (or list). Returns all if missing.
                
                `table (str)`: Optional table name to search.
                
                `assets(bool)`: Returns assets (True) or symbols (False).

            Returns:
                List. A list of asset objects or symbols.
        """
        out:list[MarketData] = []
        for key in self.asset_dispatch:
            stores = self.asset_dispatch[key]
            stores = self.__stores._find_store_max_freq(stores)
            if len(stores) > 0:
                for store in stores:
                    out.extend(store.fuzzy_search(symbol))
                    
        return list(set(out))
        
    def exists(self, syms):
        """ Check and returns True if the symbols exist in the DB. """
        if not listlike(syms):
            asset = self._symbol_to_asset(syms) # type:ignore
            if asset is None:return False
            return True
            
        assets = [self._symbol_to_asset(sym) for sym in syms]
        return [False if asset is None else True for asset in assets]
    
    def lifetimes(self, dts:pd.DatetimeIndex|list[pd.Timestamp], include_start_date:bool=True, 
                  store:DataStore|None=None, assets:list[MarketData]|None=None,
                  *args, **kwargs) -> pd.DataFrame:
        """ 
            Returns a DataFrame with dates as index and all assets in 
            the DB as columns, filled with zeros (asset not available on 
            the date) or ones. If store is not specified, it searches the 
            currently active store.
            
            Args:
                `dts (list)`: list of dates.
                
                `store (obj)`: Store object to query.
                
                `assets (list)`: list of assets.
                
            Returns:
                Tuple of assets, dates and boolean array of asset life-time. 
                
        """
        if 'country_codes' in kwargs:
            store_name = kwargs['country_codes']
            if listlike(store_name):store_name = list(store_name)[0]
            stores = [s for s in self.find_stores(store_name=store_name)]
            if not stores:
                raise DataStoreException(f'no store found for name {store_name}')
            store = stores[0]
        
        if store is None:
            store = self.asset_finder_store

        if store is None:
            msg = f'no store found to fetch asset lifetimes.'
            raise DataStoreException(msg)
            
        assets, dts, lifetime = store.assets_lifetime( # type: ignore
                dts, include_start_date, assets=assets)
        cols = [asset.sid for asset in assets]
        
        return pd.DataFrame(lifetime, index=dts, columns=cols)
    
    def can_trade(self, asset, dt):
        """ True if the asset was tradeable at the given date-time. """
        if not self.is_alive(asset, dt):
            return False
        
        store = self.asset_to_store(asset)
        px = store.get_spot_value(asset, dt, 'close')
        if math.isnan(px):
            return False
        
        return True
    
    def is_alive(self, asset, dt):
        """ True if the asset was alive at the given date-time. """
        dt = dt.tz_localize(None)
        input_dt = dt
        dt = dt - pd.Timedelta(days=1)
        if not asset.start_date:
            return dt < asset.auto_close_date
        if dt < asset.auto_close_date and input_dt >= asset.start_date:
            return True
        
        return False
    
    ###################### data portal methods ####################
    def read(self, asset:MarketData, columns:str|list[str], nbars:int, frequency:str|Frequency, 
             dt:pd.Timestamp|None=None, adjusted:bool=True, precision:int=2, 
             store:DataStore|None=None, **kwargs):
        """ 
            Returns given number of bars for the assets. If more than 
            one asset or more than one column supplied, returns a 
            dataframe, with assets or fields as column names. If both 
            assets and columns are multiple, returns a multi-index 
            dataframe. For a single asset and a single field, returns 
            a series.
            
            Note:
                if `dt` is None, the values are as of dt, else it is 
                the last available timestamp in the store. Frequency
                must match the frequency of one of the stores available 
                in the library.
            
            Args:
                `asset (obj)`: An asset object.
                
                `columns (str or list)`: A field name or a list.
                
                `nbars (int)`: Number of bars to fetch.
                
                `frequency (str)`: Frequency of the data.
                
                `dt (timestamp)`: As of date-time.
                
                `adjusted (bool)`: If data to be adjusted.
                
                `precision (int)`: Precision in the returned value.
                
                `store (object)`: The store to query.
            
            Returns:
                Series or dataframe.
        """
        store = self.asset_to_store(asset, frequency)
        df = store.read(
                asset, columns, nbars, dt, adjusted, precision=precision)
        
        if isinstance(columns, str) or len(columns)==1:
            if not isinstance(df, pd.Series):
                return df.iloc[:,0]
            
        return df
        
    def read_between(self, asset:MarketData, columns:str|list[str], start_dt:pd.Timestamp, 
                     end_dt:pd.Timestamp, frequency:str|Frequency, adjusted=True, precision:int=2, 
                     store:DataStore|None=None) -> pd.Series|pd.DataFrame:
        """ 
            Returns data between dates for the assets. If more than 
            one asset or more than one column supplied, returns a 
            dataframe, with assets or fields as column names. If both 
            assets and columns are multiple, returns a multi-index 
            dataframe. For a single asset and a single field, returns 
            a series.
            
            Args:
                `asset (obj)`: An asset object.
                
                `columns (str or list)`: A field name or a list.
                
                `start_dt (str or timestamp)`: Start date.
                
                `end_dt (str or timestamp)`: End date.
                
                `frequencty (str)`: Frequency of the data.
                
                `adjusted (bool)`: If data to be adjusted.
                
                `precision (int)`: Precision in the returned value.
                
                `store (object)`: The store to query.
                
            
            Returns:
                Series or dataframe.
        """
        store = self.asset_to_store(asset, frequency)
        df = store.read_between(
                asset, columns, start_dt, end_dt, adjusted, precision=precision)
        if isinstance(columns, str) or len(columns)==1:
            if not isinstance(df, pd.Series):
                return df.iloc[:,0]
            
        return df
        
    def get_spot_value(self, asset:MarketData, dt:pd.Timestamp, column:str, 
                       frequency:str|Frequency, **kwargs) -> float:
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
                
                `frequencty (str)`: Frequency of the data.
                
            Returns:
                Scalar (double or int, depending on the column data type).
        """
        store = self.asset_to_store(asset, frequency)
        return store.get_spot_value(asset, dt, column)
    
    def history(self, assets:MarketData|list[MarketData], columns:str|list[str], nbars:int, 
                frequency:str|Frequency, dt:pd.Timestamp|None=None, adjusted:bool=True, 
                precision:int=2, **kwargs) -> pd.Series|pd.DataFrame:
        """ 
            Returns given number of bars for the assets. If more than 
            one asset or more than one column supplied, returns a 
            dataframe, with assets or fields as column names. If both 
            assets and columns are multiple, returns a multi-index 
            dataframe. For a single asset and a single field, returns 
            a series.
            
            Note:
                if `dt` is not None, the values are as of dt, else it is 
                the last available timestamp in the store. Frequency
                must match the frequency of one of the stores available 
                in the library.
            
            Args:
                `assets (obj or list)`: An asset or a list.
                
                `columns (str or list)`: A field name or a list.
                
                `nbars (int)`: Number of bars to fetch.
                
                `frequencty (str)`: Frequency of the data.
                
                `dt (timestamp)`: As of date-time.
                
                `precision (int)`: Precision in the returned value.
            
            Returns:
                Series or dataframe, depending on the arguments.
        """
        nbars = round(nbars)
        
        try:
            freq = Frequency(frequency)
        except Exception:
            raise DataStoreException('Illegal frequency {frequency}.')
            
        minute = Frequency('1m')
        daily = Frequency('1d')
        colname = None
        
        if freq not in (minute, daily):
            if listlike(assets) and listlike(columns):
                msg = f'For multiple assets and columns, only "1m" or "1d" frequency '
                msg += f'is supported. Query at a frequency higher than the target '
                msg += f'and use the resample method from `blueshift.library.timeseries.transform` '
                msg += 'or Pandas resampling method for each asset instead.'
                raise DataStoreException(msg)
            elif listlike(assets):
                colname = str(columns)
            
        if freq < daily:
            frequency = '1m'
            if freq == minute:
                n = nbars
            else:
                n = nbars*(freq.minute_bars*1.2 + 10)
        else:
            frequency = '1d'
            if freq == Frequency('1d'):
                n = nbars
            elif freq == Frequency('W'):
                n = nbars*5*1.2 + 10
            elif freq == Frequency('M'):
                n = nbars*31*1.2 + 10
            elif freq == Frequency('Q'):
                n = nbars*91*1.2 + 20
            elif freq == Frequency('A'):
                n = nbars*365*1.2 + 30
            
        data = self._history(assets, columns, round(n), frequency, dt=dt, # type: ignore
                adjusted=adjusted, precision=precision)
        
        if freq not in (Frequency('1m'), Frequency('1d')):
            data = resample(data, freq, colname=colname, 
                            calendar=self.active.calendar)
            data = subset(data, nbars)
            
        return data
    
    def _history(self, assets, columns, nbars, frequency, dt=None,
                adjusted=True, precision=2):
        err_msg = 'No data returned, did you try to fetch data '
        err_msg += 'before the start date of the dataset? Please try a '
        err_msg += 'later start date.'
        
        if dt and Frequency(frequency)==Frequency('1d'):
            # history should not have any look-ahead bias
            dt = pd.Timestamp(dt) - pd.Timedelta(days=1)
        
        assets_iter = cols_iter = True
        if not listlike(assets):
            assets_iter = False
            assets = [assets]
        if not listlike(columns):
            cols_iter = False
            columns = [columns]
            
        if self._max_assets and len(assets) > self._max_assets:
            msg = f'Too many assets for data query. Results will be '
            msg += 'truncated.'
            assets = assets[:self._max_assets]
            warnings.warn(msg, BlueshiftWarning)
        
        stores = [self.asset_to_store(asset, frequency) for \
                      asset in assets]
        
        for i, store in enumerate(stores):
            if store is None:
                raise DataStoreException(
                        f'no store found for {assets[i]}.')
        
        if not assets_iter and not cols_iter:
            data = stores[0].read(
                    assets[0], columns[0], nbars, dt=dt, adjusted=adjusted,
                    precision=precision)
            data.name = columns[0]
            if data.empty:
                err_msg += f' Asset data starts from {assets[0].start_date}.'
                raise HistoryWindowStartsBeforeData(err_msg)
            return data
        elif not assets_iter and cols_iter:
            data = stores[0].read(
                    assets[0], columns, nbars, dt=dt, adjusted=adjusted,
                    precision=precision)
            if len(columns)==1:
                if data.empty:
                    err_msg += f' Asset data starts from {assets[0].start_date}.'
                    raise HistoryWindowStartsBeforeData(err_msg)
                df = pd.DataFrame(data)
                df.columns = columns
                return df
            if data.empty:
                err_msg += f' Asset data starts from {assets[0].start_date}.'
                raise HistoryWindowStartsBeforeData(err_msg)
            return data
        elif assets_iter and not cols_iter:
            data = {}
            for i, asset in enumerate(assets):
                px = stores[i].read(
                        asset, columns[0], nbars, dt=dt, adjusted=adjusted,
                        precision=precision)
                if len(px)>0:
                    data[asset] = px
            data = pd.DataFrame(data)
            if data.empty:
                starts = {asset:asset.start_date for asset in assets}
                err_msg += f' Assets data starts {starts}.'
                raise HistoryWindowStartsBeforeData(err_msg)
            return data
        else:
            data = {}
            for i, asset in enumerate(assets):
                data[asset] = stores[i].read(
                        asset, columns, nbars, dt=dt, adjusted=adjusted,
                        precision=precision)
                if len(columns)==1:
                    df = pd.DataFrame(data[asset])
                    df.columns = columns
                    data[asset] = df
                    
            data = pd.concat(data)
            if data.empty:
                starts = {asset:asset.start_date for asset in assets}
                err_msg += f' Assets data starts {starts}.'
                raise HistoryWindowStartsBeforeData(err_msg)
            return data
        
    def current(self, assets:MarketData|list[MarketData], columns:str|list[str]='close', 
                dt:pd.Timestamp|None=None, precision:int=2, last_known:bool=False, 
                frequency:str|Frequency='1m', **kwargs) -> float|pd.Series|pd.DataFrame:
        """
            Return last available price. If either assets and columns 
            are multiple, a series is returned, indexed by assets or 
            fields, respectively. If both are multiple, a dataframe is 
            returned. Otherwise, a scalar is returned.
            
            Note:
                if `dt` is None, the last value is as of dt, else it is 
                the last available value in the store. If `last_known` 
                is False, and there is no matchiing value for dt, NaNs
                will be returned. Else, last known good prices are 
                returned.
            
            Args:
                `assets (obj or list)`: An asset or a list.
                
                `columns (str or list)`: A field name or a list.
                
                `dt (timestamp)`: As of date-time.
                
                `precision (int)`: Precision in the returned value.
                
                `last_known (bool)`: Return last known good values.
                
                `frequency (str)`: Frequency of the data.
                
            Returns:
                Scalar, series or frame.
            
        """
        if not listlike(assets) and \
            not listlike(columns) and not last_known:
            if not dt or not frequency:
                raise DataStoreException('dt and frequency must be specified.')
            return self.get_spot_value(assets, dt, columns, frequency) # type: ignore
        
        assets_iter = cols_iter = True
        if not listlike(assets):
            assets_iter = False
            assets = [assets] # type: ignore

        assets = cast(list, assets)
            
        if self._max_assets and len(assets) > self._max_assets:
            msg = f'Too many assets for data query. Results will be '
            msg += 'truncated.'
            assets = assets[:self._max_assets]
            warnings.warn(msg, BlueshiftWarning)
        
        if not listlike(columns):
            cols_iter = False
            columns = [columns] # type: ignore
        
        columns = cast(list, columns)
        
        if not last_known:
            if not dt or not frequency:
                raise DataStoreException('dt and frequency must be specified.')
            
            if not assets_iter and cols_iter:
                data = [self.get_spot_value(assets[0],dt,c,frequency) for c in columns] # type: ignore
                return pd.Series(data, index=columns) # type: ignore
            elif assets_iter and not cols_iter:
                c = columns[0]
                data = [self.get_spot_value(asset, dt, c, frequency) for \
                        i, asset in enumerate(assets)]
                return pd.Series(data, index=assets)
            else:
                data = {}
                for c in columns:
                    data[c] = [self.get_spot_value(asset,dt,c,frequency) for \
                        i, asset in enumerate(assets)]
                return pd.DataFrame(data, index=assets)
        
        stores = [self.asset_to_store(asset, frequency) \
                                              for asset in assets]
        for i, store in enumerate(stores):
            if store is None:
                raise DataStoreException(
                        f'no store found for {assets[i]}.')
        
        data = {}
        for i, asset in enumerate(assets):
            data[asset] = stores[i].read(
                    asset, columns, 1, dt=dt, adjusted=False, 
                    precision=precision).reset_index(drop=True)
        
        if not assets_iter and not cols_iter:
            if len(data[assets[0]]) > 0:
                return data[assets[0]].iloc[0]
            return np.nan
        elif not assets_iter and cols_iter:
            data = data[assets[0]]
            if len(columns)==1:
                data=pd.DataFrame(data.values,columns=columns) # type: ignore
            return data.iloc[0,:]
        elif assets_iter and not cols_iter:
            data = {asset:value.iloc[0] for asset, value in data.items()}
            return pd.Series(data)
        else:
            if len(columns)==1:
                data = {asset:pd.DataFrame(
                    value.values,columns=columns) for asset, value in data.items()} # type: ignore
            
            data = {asset:value.iloc[0,:] for asset, value in data.items()}
            return pd.DataFrame(data).T
        
    @lru_cache(maxsize=_CACHE_SIZE)
    def _ccy_pair_to_sym(self, ccy_pair:str):
        if isinstance(ccy_pair, CCYPair):
            ccy_pair = CCYPair.pair # type: ignore
            
        ccy_pair = ccy_pair.upper()
        if 'LOCAL' in ccy_pair:
            return True, 'LOCAL', None
        if ccy_pair in self.__stores._fx_pairs:
            return True, self.__stores._fx_pairs[ccy_pair], None
        elif ccy_pair in self.__stores._fx_reverse_pairs:
            return True, None, self.__stores._fx_reverse_pairs[ccy_pair]
        
        asset1 = asset2 = None
        divide = True
        try:
            base, quote = ccy_pair.split('/')
            base_cross, quote_cross = base+'/USD', quote+'/USD'
            
            if base_cross in self.__stores._fx_pairs and \
                quote_cross in self.__stores._fx_pairs:
                asset1 = self.__stores._fx_pairs[base_cross]
                asset2 = self.__stores._fx_pairs[quote_cross]
            elif base_cross in self.__stores._fx_pairs and \
                quote_cross in self.__stores._fx_reverse_pairs:
                asset1 = self.__stores._fx_pairs[base_cross]
                asset2 = self.__stores._fx_reverse_pairs[quote_cross]
                divide = False
            if base_cross in self.__stores._fx_reverse_pairs and \
                quote_cross in self.__stores._fx_pairs:
                asset1 = self.__stores._fx_reverse_pairs[base_cross]
                asset2 = self.__stores._fx_pairs[quote_cross]
                divide = False
            elif base_cross in self.__stores._fx_reverse_pairs and \
                quote_cross in self.__stores._fx_reverse_pairs:
                asset2 = self.__stores._fx_reverse_pairs[base_cross]
                asset1 = self.__stores._fx_reverse_pairs[quote_cross]
        except Exception:
            pass
        
        return divide, asset1, asset2
        
    def getfx(self, ccy_pair, dt) -> float:
        if not self.__stores._fx_rate_store:
            raise DataStoreException('No fx rate store available.')
        
        divide, asset1, asset2 = self._ccy_pair_to_sym(ccy_pair)
        if asset1=='LOCAL' or asset2 is None:
            return 1.0

        if asset1 and not asset2:
            return self.__stores._fx_rate_store.get_spot_value(asset1, dt, 'close')
        elif asset2 and not asset1:
            fx = self.__stores._fx_rate_store.get_spot_value(asset2, dt, 'close')
            return 1.0/fx
        elif asset1 and asset2 and divide:
            fx1 = self.__stores._fx_rate_store.get_spot_value(asset1, dt, 'close')
            fx2 = self.__stores._fx_rate_store.get_spot_value(asset2, dt, 'close')
            return fx1/fx2
        elif asset1 and asset2:
            fx1 = self.__stores._fx_rate_store.get_spot_value(asset1, dt, 'close')
            fx2 = self.__stores._fx_rate_store.get_spot_value(asset2, dt, 'close')
            return fx1*fx2
        
        raise DataStoreException(
                f'cannot resolve pair {ccy_pair} for FX conversion on {dt}.')
        
    def get_sectors(self, assets:list[Asset], frequency:Literal['Q','A'], 
                    dt:pd.Timestamp|None=None) -> pd.DataFrame:
        """
            Get the sectors for the given assets where available. Returns a 
            pandas dataframe with assets as the index and sector names as a 
            column with name 'sector'.
        """
        if frequency not in ('Q','A'):
            raise DataStoreException(
                f'unknown frequency {frequency}, must be either "Q" or "A".')
        
        freq = Frequency(frequency.upper())
        store = self.__stores.find_fundamental_store(freq)
        
        if store:
            return store.get_sectors(assets, dt=dt)
        else:
            raise DataStoreException(f'No sector information available')
            
    def list_sectors(self, frequency:Literal['Q','A']) -> list[str]:
        """
            List the sectors for a given fundamental data source. Returns 
            a list of strings (sector names).
        """
        if frequency not in ('Q','A'):
            raise DataStoreException(
                f'unknown frequency {frequency}, must be either "Q" or "A".')
        
        freq = Frequency(frequency.upper())
        store = self.__stores.find_fundamental_store(freq)
        
        if store:
            return store.get_available_sectors()
        else:
            raise DataStoreException(f'No sector information available')
        
    def fundamentals(self, assets:list[Asset], metrics:list[str], nbars:int, 
                     frequency:Literal['Q','A'], dt:pd.Timestamp|str|None=None, **kwargs) -> dict[Asset, pd.DataFrame]:
        """ 
            Returns given number of bars for the fundamental metrics. This 
            always returns a dict with asset(s) as  the keys, or an empty 
            dict of no data available.
            
            Note:
                If the source data of the store supports point-in-time, the 
                data returned is point-in-time as well.
            
            Args:
                `assets (obj or list)`: An asset or a list.
                
                `metrics (str or list)`: List or a single fundamental metrics.
                
                `nbar (int)`: Number of bars to fetch.
                
                `frequencty (str)`: Frequency of the data, either 'Q' or 'A'.
                
                `dt (timestamp)`: As of date-time.
            
            Returns:
                A dictionary.
        """
        if not assets:
            return {}
        
        if frequency not in ('Q','A'):
            raise DataStoreException(
                f'unknown frequency {frequency}, must be either "Q" or "A".')
        
        freq = Frequency(frequency.upper())
        
        if not listlike(assets):
            assets = [assets] # type: ignore
            
        store = self.asset_to_store(assets[0], frequency=freq, store_types=('FundamentalStore',))
        
        if dt is None:
            dt = pd.Timestamp.now(tz=store.tz)
        else:
            dt = pd.Timestamp(dt)
        
        if store:
            store = cast(FundamentalStore, store)
            return store.read_metrics(assets, metrics, nbars=nbars, dt=dt)
            
        return {}
    
    def load_adjusted_array(self, domain, columns, dates, sids, mask):
        raise NotImplementedError('this method is not implemented.')
        
    ############## utility methods ################################
    
    def set_benchmark(self, store=None, benchmark_file:str|None=None):
        """
            Set the default benchmark for this library. 
            
            Args:
                `store (DataStore)`: Datastore to use for benchmark.
                
                `benchmark_file (str)`: Benchmark file name (csv).
                
            Returns:
                None. Set the benchmark property of this library.
        """
        if not store:
            store = self.active
            
        if not store:
            raise DataStoreException(f'No store in library.')
        
        if not benchmark_file:
            benchmark_file = 'benchmark.csv'
        
        source = os.path.join(store.root,benchmark_file)  # type: ignore
        if not os.path.exists(source):
            raise DataStoreException(f'missing data file {source}.')
            
        try:
            df = pd.read_csv(source)
        except TerminationError:
            raise
        except Exception as e:
            msg = f'failed to read benchmark data: {str(e)}'
            raise DataStoreException(msg)
        
        df.columns = [c.lower() for c in df.columns]
        cols = df.columns
        
        if len(cols) == 2:
            if not 'date' in cols:
                df.columns = ['date','close']
            else:
                idx1 = cols.tolist().index('date')
                idx2 = min(1,max(0,1-idx1))
                df = pd.concat([df.iloc[:,idx1],df.iloc[:,idx2]], axis=1)
                df.columns = ['date','close']
        elif len(cols) > 2:
            if 'date' not in cols or 'close' not in cols:
                msg = f'benchmark input must have "date" and a "close" '
                msg += f'columns, got {cols}'
                raise DataStoreException(msg)
            df = df[['date','close']]
        else:
            msg = 'benchmark data not valid.'
            raise DataStoreException(msg)
            
        try:
            df['date'] = pd.to_datetime(df.date)
            df = df.set_index('date')
        except TerminationError:
            raise
        except Exception as e:
            raise DataStoreException(str(e))
        
        try:
            if not df.index.tz:     # type: ignore
                df.index = df.index.tz_localize(self.tz)    # type: ignore
            else:
                df.index = df.index.tz_convert(self.tz)     # type: ignore
        except Exception:
            # index.tz failed, after to_datetime, which means the 
            # input data had timestamp info
            df.index = pd.to_datetime(df.index, utc=True)
            df.index = df.index.tz_convert(self.tz)         # type: ignore
            
        benchmark = df.close
        benchmark.index.name = None
            
        self._benchmark = benchmark
        
    def get_benchmark(self, start, end) -> pd.Series:
        if self._benchmark is None or len(self._benchmark)==0:
            self.set_benchmark()
            
        benchmark = self._benchmark.copy() # type: ignore
        dts = self.active.calendar.sessions(start, end)
        fill = pd.Series(np.full(len(dts),np.nan),index=dts)
        benchmark = benchmark.combine(fill, func=lambda x,y:x)
        benchmark = benchmark.ffill()
        
        return benchmark[dts] # type: ignore
    
    ########## extra methods for futures and options stores ###########
    
    def get_expiry(self, asset, dt, offset=None, store=None):
        """
            Get expiry date given an asset and offset.
        """
        if store:
            if hasattr(store, 'get_expiry'):
                return store.get_expiry(asset, dt, offset=offset)           # type: ignore
        elif asset.is_opt() and self.options_store:
            return self.options_store.get_expiry(asset, dt, offset=offset)  # type: ignore
        elif asset.is_futures() and self.futures_store:
            return self.futures_store.get_expiry(asset, dt, offset=offset)  # type: ignore
            
        return pd.Timestamp(None) # type: ignore
    
    def get_expiries(self, asset, start_dt, end_dt, offset=None, store=None) -> pd.DatetimeIndex:
        """
            Return the list of expiry days, given an asset.
            
            .. Note::
                Start or end date can be given either as string dates 
                or convertible to Pandas timestamp.
            
            Args:
                `asset (Asset)`: asset for which expiries are required.
                
                `start_dt (str)`: Start date for expiries.
                
                `end_dt (str)`: End date for expiries.
                
            Returns:
                None. Set the benchmark property of this library.
        """
        if store:
            if hasattr(store, 'get_expiries'):
                return store.get_expiries(asset, start_dt, end_dt, offset=offset)           # type: ignore
        elif asset.is_opt() and self.options_store:
            return self.options_store.get_expiries(asset, start_dt, end_dt, offset=offset)  # type: ignore
        elif asset.is_futures() and self.futures_store:
            return self.futures_store.get_expiries(asset, start_dt, end_dt, offset=offset)  # type: ignore
            
        return pd.DatetimeIndex([])
                    
    def option_chain(self, underlying:MarketData, series:str, columns:str|list[str], 
                     strikes:list[float]|None=None, relative:bool=True, dt:pd.Timestamp|None=None, 
                     store:DataStore|None=None, **kwargs) -> tuple[pd.Series|pd.DataFrame, pd.Series|pd.DataFrame]:
        """
            Return option chain snapshot for a given underlying, series and 
            date-time. The `fields` can be any allowed value for 
            :ref:`data.current<Fetching Current Data>` columns.
            
            Args:
                `underlying (str)`: Underlying for the option chain
                
                `series (str)`: Expiry series (e.g. "W0" or "I" or expiry date).
                
                `columns (str or list)`: Data query fields.
                
                `strikes (list)`: List of strikes (absolute or relative)
                
                `relative (bool)`: If strikes should be relative or absolute.
                
                `dt (Timestamp)`: Snapshot time
                
                `store (DataStore)`: Optional store to read from.
                
            Returns:
                Series (if a single column) or DataFrame (for multiple 
                columns), with index as the strike price.
        """
        if not dt:
            raise DataStoreException('dt must be specified.')
                
        put_strikes = call_strikes = []
        
        if not strikes:
            if store:
                if hasattr(store, 'read_strikes'):
                    put_strikes, call_strikes = store.read_strikes(             # type: ignore
                            underlying, series, dt, relative=relative)
            elif self.options_store:
                put_strikes, call_strikes = self.options_store.read_strikes(    # type: ignore
                        underlying, series, dt,relative=relative)
        else:
            put_strikes = call_strikes = strikes
            
        if not put_strikes and not call_strikes:
            msg = f'No data found for underlying/ series '
            msg += f'{underlying}/{series}.'
            raise DataStoreException(msg)
            
        try:
            series = pd.Timestamp(series)  # type: ignore
            root = f'{underlying}{series.strftime("%Y%m%d")}'  # type: ignore
        except Exception:
            root = f'{underlying}-{series}'
            
        put_chain = {}
        for k in put_strikes:
            if relative:
                if k > 0:
                    sym = root + 'PE+' + str(abs(k))
                else:
                    sym = root + 'PE-' + str(abs(k))
            else:
                sym = root + 'PE' + str(abs(k))
            put_chain[k] = self.current(self.symbol(sym), columns, dt=dt)
            
        call_chain = {}
        for k in call_strikes:
            if relative:
                if k < 0:
                    sym = root + 'CE-' + str(abs(k))
                else:
                    sym = root + 'CE+' + str(abs(k))
            else:
                sym = root + 'PE' + str(abs(k))
            call_chain[k] = self.current(self.symbol(sym), columns, dt=dt)
        
        if not listlike(columns):
            call_chain = pd.Series(call_chain)
            put_chain = pd.Series(put_chain)
        else:
            call_chain = pd.DataFrame(call_chain).T
            put_chain = pd.DataFrame(put_chain).T
            
        return put_chain, call_chain
    
register_library('default', Library)

__all__ = ['Library']