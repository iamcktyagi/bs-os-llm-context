from __future__ import annotations
from typing import TYPE_CHECKING, cast

from blueshift.lib.common.functions import  listlike
from blueshift.lib.exceptions import SymbolNotFound, DataStoreException
from blueshift.interfaces.data.store import DataStore, register_data_store
from blueshift.interfaces.trading.broker import IBroker
from blueshift.interfaces.assets._assets import MarketData
from blueshift.lib.common.constants import (
        OHLCV_COLUMNS, OHLCV_DTYPE, Frequency)
from blueshift.providers.data.adjs.noadjustment import NoAdjustments

if TYPE_CHECKING:
    import pandas as pd
    import numpy as np
else:
    import blueshift.lib.common.lazy_pandas as pd

class BrokerStore(DataStore):
    """ 
        An implementation of data store with methods and props sourced from the 
        underlying broker (IBroker) implementation.
    """
    _store_type = 'broker_store'
    _metafile = None
    _cols = OHLCV_COLUMNS
    
    def __init__(self, broker:IBroker, frequency:str|Frequency, store=None):
        self._broker = broker
        self._frequency = Frequency(frequency)
        self._root = None
        self._store = store
        self._metadata = {}
        
    def __hash__(self):
        return hash(self.broker.name)
        
    @property
    def broker(self) -> IBroker:
        return self._broker
    
    @property
    def name(self):
        return self.broker.name
    
    @property
    def root(self):
        pass
    
    @property
    def columns(self):
        return OHLCV_COLUMNS
        
    @property
    def dtypes(self):
        return OHLCV_DTYPE
        
    @property
    def frequency(self):
        return self._frequency.period
        
    @property
    def tz(self):
        return self.broker.tz
        
    @property
    def date_range(self):
        return None, None
        
    @property
    def exchange(self):
        if self._store:
            return self._store.exchange
        
    @property
    def calendar(self):
        return self.broker.calendar
        
    @property
    def metadata(self):
        metadata = {}
        metadata['start_date'] = None
        metadata['end_date'] = pd.Timestamp.now(tz=self.tz).normalize()
        return metadata
        
    @property
    def db(self):
        try:
            return self.broker.store.db
        except Exception:
            raise DataStoreException(
                    f'Missing asset database for store {self}.')
        
    @property
    def adjustments_handler(self):
        return NoAdjustments()
    
    def initialize(self, *args, **kwargs):
        pass
        
    def refresh(self):
        pass
        
    def connect(self, *args, **kwargs):
        pass
        
    def close(self, auto_commit=True):
        pass
        
    def write(self, *args, **kwargs):
        pass
        
    def commit(self):
        pass
        
    def rollback(self):
        pass
        
    def set_cache_policy(self, policy='random'):
        pass
        
    def read(self, asset, columns, nbars, dt=None, adjusted:bool=True, **kwargs):
        return self.broker.history(asset, columns, nbars, self.frequency)
        
    def read_between(self, asset, columns, start_dt, end_dt, adjusted:bool=True, **kwargs):
        raise NotImplementedError
    
    def sweep_bars(self, assets, start_dt, end_dt, columns='close',
                        *args, **kwargs):
        raise NotImplementedError
        
    def get_spot_value(self, asset, dt, column):
        px = self.broker.current(asset, column)
        return cast(float, px)
    
    def apply_adjustments(self, asset, data, dt=None, precision=2):
        raise NotImplementedError
    
    def load_raw_arrays(self, columns, start_date, end_date, sids):
        raise NotImplementedError
    
    def has_data(self, asset):
        try:
            self.broker.store.has_data(asset)
        except Exception:
            return False
        
    def update_metadata(self, *args, **kwargs):
        pass
    
    def list(self):
        raise NotImplementedError
    
    def exists(self, symbols, table=None):
        if not listlike(symbols):
            return self.broker.exists(symbols)
        
        return [self.broker.exists(symbol) for symbol in symbols]
        
    def find(self, assets, key=None):
        if listlike(assets):
            symbols = [obj.symbol for obj in assets]
        else:
            symbols = assets.symbol
            
        return self.search(symbols)
    
    def search(self, symbols=None, **kwargs):
        if symbols is None:
            return self.broker.store.search(symbols)
        
        assets:list[MarketData] = []
        for sym in symbols:
            try:
                assets.append(self.broker.symbol(sym))
            except SymbolNotFound:
                continue
            
        return assets
        
    def fuzzy_search(self, symbols, **kwargs) -> list[MarketData]:
        return self.search(symbols)
    
    def sid(self, sec_id):
        """ fetch asset by sid. """
        raise NotImplementedError
    
    def symbol(self, sym, dt=None, **kwargs):
        """ fetch asset by symbol. """
        return self.broker.symbol(sym, dt=dt, **kwargs)
    
    def update(self, symbol, data={}):
        raise NotImplementedError    
    
    def rename(self, from_sym, to_sym, long_name=None):
        raise NotImplementedError
        
    def stats(self):
        raise NotImplementedError
        
    def remove(self, symbol):
        raise NotImplementedError
        
    def truncate(self):
        raise NotImplementedError
        
    def delete(self):
        raise NotImplementedError
        
    def ingest_report(self, symlist=None):
        raise NotImplementedError
        
    def backup(self, *args, **kwargs):
        raise NotImplementedError
        
    def restore(self, backup_name):
        raise NotImplementedError
        
    def list_backups(self):
        raise NotImplementedError
        
    def remove_backup(self, backup_name):
        raise NotImplementedError
        
    def add_membership(self, data, name=None):
        raise NotImplementedError
        
    def fetch_membership(self, group=None):
        pass
        
    def remove_membership(self, group):
        raise NotImplementedError
        
    def list_memberships(self):
        raise NotImplementedError
    
    def assets_lifetime(self, dts, include_start_date=True, assets=None
                        ) -> tuple[list[MarketData], pd.DatetimeIndex, np.ndarray]:
        raise NotImplementedError
        
register_data_store('broker', BrokerStore)