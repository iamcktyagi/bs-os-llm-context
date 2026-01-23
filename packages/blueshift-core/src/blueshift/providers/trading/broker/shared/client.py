from __future__ import annotations
from typing import TYPE_CHECKING, cast
from functools import wraps

from blueshift.config import MULTI_TENANCY
from blueshift.lib.common.functions import  listlike
from blueshift.interfaces.trading.broker import make_broker_key, ILiveBroker
from blueshift.interfaces.logger import get_logger

if TYPE_CHECKING:
    import pandas as pd
    from blueshift.core.algorithm.algorithm import TradingAlgorithm
    from .shared import SharedBrokerCollection

class SharedBrokerClient(ILiveBroker):
    def __init__(self, collection:SharedBrokerCollection, algo:TradingAlgorithm, algo_user:str, name:str, *args, **kwargs):
        self._algo = algo
        self._algo_user = algo_user
        self._logger = kwargs.pop('logger', None) or get_logger()
        # share broker is keyed just by the name!! This means no 
        # multiple entries with the same broker implementation name, 
        # also different tokens and api keys are mapped to the same 
        # instance, so it should only be used where there is onetoone
        # of entry and implementation as well as uses a single api key
        # and the underlying implementation must handle different 
        # session tokens itself
        if MULTI_TENANCY:
            key = str(name) + '-' + str(self._algo_user)
        else:
            key = str(name)
            
        self._key = make_broker_key(name, key=key)
        
        kwargs['logger'] = kwargs.pop('shared_logger', self._logger)
        collection.init_broker(name, self._key, *args, **kwargs)
        self._broker = collection.get_broker(name, self._key)
        collection.add_algo(algo, self._algo_user, name, self._key)
        self._subscribed_assets = set()
        
    def __str__(self):
        return f'BlueshiftSharedBrokerClient[{str(self._broker)}]'
    
    def __repr__(self):
        return self.__str__()
    
    def initialize(self, *args, **kwargs):
        self._broker.ref_count += 1
    
    def finalize(self, *args, **kwargs):
        self._broker.ref_count -= 1
    
    def __getattr__(self, name):
        return self._broker.get_broker_attr(self._algo, self._algo_user, name)
    
    @property
    def wants_to_quit(self):
        return self._algo.wants_to_quit
                
    @property
    def account_type(self):
        return self._broker.get_broker_attr(self._algo, self._algo_user, 'account_type')
    
    @property
    def is_connected(self):
        return self._broker.get_broker_attr(self._algo, self._algo_user, 'is_connected')
    
    @property
    def calendar(self):
        return self._broker.get_broker_attr(self._algo, self._algo_user, 'calendar')
    
    @property
    def tz(self):
        return self._broker.get_broker_attr(self._algo, self._algo_user, 'tz')
    
    @property
    def store(self):
        return self._broker.get_broker_attr(self._algo, self._algo_user, 'store')
    
    @property
    def assets(self):
        return self._broker.get_broker_attr(self._algo, self._algo_user, 'assets')
    
    @property
    def intraday_cutoff(self):
        return self._broker.get_broker_attr(self._algo, self._algo_user, 'intraday_cutoff')
    
    def set_algo_callback(self, callback, *args, **kwargs):
        if 'algo_user' in kwargs:
            kwargs.pop('algo_user')
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'set_algo_callback')
        return f(callback, *args, **kwargs)
    
    def login(self, *args, **kwargs):
        if 'algo_user' in kwargs:
            kwargs.pop('algo_user')
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'login')
        return f(*args, **kwargs)
            
    def logout(self, *args, **kwargs):
        if 'algo_user' in kwargs:
            kwargs.pop('algo_user')
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'logout')
        return f(*args, **kwargs)
        
    def before_trading_start(self, timestamp):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'before_trading_start')
        return f(timestamp)
        
    def trading_bar(self, timestamp):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'trading_bar')
        return f(timestamp)
        
    def paper_simulate(self, timestamp):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'paper_simulate')
        return f(timestamp)
        
    def paper_reconcile(self, timestamp):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'paper_reconcile')
        return f(timestamp)
    
    def after_trading_hours(self, timestamp):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'after_trading_hours')
        return f(timestamp)
        
    def algo_start(self, timestamp):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'algo_start')
        return f(timestamp)

    def algo_end(self, timestamp):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'algo_end')
        return f(timestamp)

    def heart_beat(self, timestamp):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'heart_beat')
        return f(timestamp)
              
    @property
    def profile(self):
        return self._broker.get_broker_attr(self._algo, self._algo_user, 'profile')
    
    @property
    def account(self):
        return self._broker.get_broker_attr(self._algo, self._algo_user, 'account')
    
    def get_account(self, *args, **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'get_account')
        return f(*args, **kwargs)
    
    @property
    def positions(self):
        return self._broker.get_broker_attr(self._algo, self._algo_user, 'positions')
    
    @property
    def closed_positions(self):
        return self._broker.get_broker_attr(self._algo, self._algo_user, 'closed_positions')
    
    @property
    def open_positions(self):
        return self._broker.get_broker_attr(self._algo, self._algo_user, 'open_positions')
    
    def position_by_asset(self, asset, *args, **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'position_by_asset')
        return f(asset, *args, **kwargs)
    
    def get_positions(self, *args, **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'get_positions')
        return f(*args, **kwargs)
    
    @property
    def orders(self):
        return self._broker.get_broker_attr(self._algo, self._algo_user, 'orders')
    
    @property
    def open_orders(self):
        return self._broker.get_broker_attr(self._algo, self._algo_user, 'open_orders')
    
    def get_order(self, order_id, *args, **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'get_order')
        return f(order_id, *args, **kwargs)
    
    def open_orders_by_asset(self, asset, *args, **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'open_orders_by_asset')
        return f(asset, *args, **kwargs)
    
    def fetch_all_orders(self, *args, **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'fetch_all_orders')
        return f(*args, **kwargs)
    
    def get_all_orders(self, *args, **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'get_all_orders')
        return f(*args, **kwargs)
    
    def place_order(self, order, *args, **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'place_order')
        return f(order, *args, **kwargs)
    
    def update_order(self, order_param, *args, **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'update_order')
        return f(order_param, *args, **kwargs)
        
    def cancel_order(self, order_param, *args, **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'cancel_order')
        return f(order_param, *args, **kwargs)
         
    def get_trading_costs(self, order, *args, **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'get_trading_costs')
        return f(order, *args, **kwargs)
    
    def get_trading_margins(self, orders, positions, *args, **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'get_trading_margins')
        return f(orders, positions, *args, **kwargs)
    
    ######## data portal interface methods #######################
    def has_streaming(self):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'has_streaming')
        return f()
    
    def quote(self, asset, column=None, **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'quote')
        return f(asset, column=column, logger=self._logger)
    
    def getfx(self, ccy_pair, dt):
        try:
            f = self._broker.get_broker_attr(self._algo, self._algo_user, 'getfx')
        except AttributeError:
            return 1.0
        
        return f(ccy_pair, dt)
    
    def history(self, assets, columns, nbars, frequency, **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'history')
        ret = f(assets, columns, nbars, frequency, logger=self._logger, **kwargs)
        
        if not listlike(assets):
            assets = [assets]

        assets = cast(list, assets)
        for asset in assets:
            asset = self.to_base_asset(asset)
            if asset:
                self._subscribed_assets.add(asset)
            
        return ret
    
    def fundamentals(self, assets, metrics, nbars, frequency, dt=None, **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'fundamentals')
        return f(assets, metrics, nbars, frequency, dt=dt)
    
    def get_expiries(self, asset, start_dt, end_dt, offset=None) -> pd.DatetimeIndex:
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'get_expiries')
        return f(asset, start_dt, end_dt, offset=offset)
    
    def current(self, assets, columns:str|list[str]='close', **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'current')
        ret = f(assets, columns=columns, logger=self._logger)
        
        if not listlike(assets):
            assets = [assets]
        assets = cast(list, assets)
        for asset in assets:
            asset = self.to_base_asset(asset)
            self._subscribed_assets.add(asset)
            
        return ret
    
    def subscribe(self, assets, level=1, *args, **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'subscribe')
        f(assets, level, *args, **kwargs)
        
        if not listlike(assets):
            assets = [assets]
        assets = cast(list, assets)
        for asset in assets:
            asset = self.to_base_asset(asset)
            self._subscribed_assets.add(asset)
            
    def unsubscribe(self, assets, level=1, *args, **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'unsubscribe')
        f(assets, level, *args, **kwargs)
        
        if not listlike(assets):
            assets = [assets]
        assets = cast(list, assets)
        for asset in assets:
            asset = self.to_base_asset(asset)
            if asset in self._subscribed_assets:
                self._subscribed_assets.remove(asset)
                
    def pre_trade_details(self, asset, **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'pre_trade_details')
        return f(asset, logger=self._logger, **kwargs)
            
    def option_chain(self, underlying, series, columns, strikes=None, 
                     relative=True, **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'option_chain')
        return f(underlying, series, columns, strikes=strikes, 
                 relative=relative, logger=self._logger)
        
    ######## asset finder interface methods #######################
    def symbol(self, symbol, dt=None, *args, **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'symbol')
        return f(symbol, dt=dt, *args, **kwargs)
    
    def to_base_asset(self, asset):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'to_base_asset')
        return f(asset)
    
    def symbol_to_asset(self, symbol, dt=None, *args, **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'symbol_to_asset')
        return f(symbol, dt=dt, *args, **kwargs)
    
    def exists(self, syms):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'exists')
        return f(syms)
    
    def fetch_asset(self, sid):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'fetch_asset')
        return f(sid)
        
    def sid(self, sid):
        return self.fetch_asset(sid)
        
    def refresh_data(self, *args, **kwargs):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'refresh_data')
        return f(*args, **kwargs)
        
    def retrieve_asset(self, sid):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'retrieve_asset')
        return f(sid)
        
    def retrieve_all(self, sids):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'retrieve_all')
        return f(sids)
        
    def lifetimes(self, dates, assets=None):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'lifetimes')
        return f(dates, assets=assets)
        
    def can_trade(self, asset, dt=None):
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'can_trade')
        return f(asset, dt=dt)
    
    def save(self, path, *args, **kwargs):
        """ Save the object data to disk. """
        if 'algo_user' in kwargs:
            kwargs.pop('algo_user')
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'save')
        return f(path, *args, **kwargs)
    
    def read(self, path, *args, **kwargs):
        """ Read saved object data from disk. """
        if 'algo_user' in kwargs:
            kwargs.pop('algo_user')
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'read')
        return f(path, *args, **kwargs)
    
    def reset(self, *args, **kwargs):
        """ Reset object data. """
        if 'algo_user' in kwargs:
            kwargs.pop('algo_user')
        f = self._broker.get_broker_attr(self._algo, self._algo_user, 'reset')
        return f(*args, **kwargs)
    
__all__ = ['SharedBrokerClient']