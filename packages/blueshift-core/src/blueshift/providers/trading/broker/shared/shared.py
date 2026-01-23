from __future__ import annotations
from typing import TYPE_CHECKING, Callable
import logging
from functools import partial

from blueshift.lib.common.ctx_mgrs import TimeoutRLock
from blueshift.lib.exceptions import ValidationError, BrokerNotFound
from blueshift.config import LOCK_TIMEOUT
from blueshift.interfaces.trading.broker import (
    _broker_factory, ILiveBroker, IBrokerCollection, register_broker_collection)
from blueshift.interfaces.logger import get_logger

if TYPE_CHECKING:
    from blueshift.core.algorithm.algorithm import TradingAlgorithm
    import pandas as pd
else:
    import blueshift.lib.common.lazy_pandas as pd
        
class SharedBroker:
    """
        A shared broker is wrapper for an IBroker interface 
        that is used by multiple algos running in the same process. This 
        should only be used for live trading and not for backtesting.
    """
    def __init__(self, name, *args, **kwargs):
        self._logger = kwargs.get('logger') or get_logger()
        self._last_bts=None
        self._last_ath=None
        self._last_bar=None
        self._callbacks:dict[str, Callable] = {}
        self._algos:set[TradingAlgorithm] = set()
        self._lock = TimeoutRLock(timeout=LOCK_TIMEOUT)
        self._ref_count = 0
        self._overrides = {
                'set_algo_callback':self.set_algo_callback,
                'login':self.login,
                'logout':self.logout,
                'before_trading_start':self.before_trading_start,
                'trading_bar':self.trading_bar,
                'after_trading_hours':self.after_trading_hours,
                'algo_start':self.algo_start,
                'algo_end':self.algo_end,
                'save':self.save,
                'read':self.read,
                'reset':self.reset,
                }
        
        self.init_broker(name, *args, **kwargs)
        
    def _algo_callback_handler(self, event, param=None):
        for algo, cb in self._callbacks.items():
            cb(event, param)
            
    def __str__(self):
        return f'BlueshiftSharedBroker[{str(self._broker)}]'
    
    def __repr__(self):
        return self.__str__()
    
    @property
    def broker(self):
        return self._broker
    
    @property
    def lock(self):
        return self._lock
    
    @property
    def ref_count(self):
        return self._ref_count
    
    @ref_count.setter
    def ref_count(self, value):
        self._ref_count = value
    
    def get_broker_attr(self, algo, algo_user, name):
        if name.startswith('_'):
            raise AttributeError(f'broker has no attribute {name}')
        
        if name in self._overrides:
            # all of these are callables and requires algo
            return partial(self._overrides[name], algo, algo_user)
        
        return getattr(self._broker, name)
        
    def set_broker_attr(self, algo, algo_user, name, value):
        if name.startswith('_'):
            raise AttributeError(f'cannot set attribute {name}, illegal name.')
            
        with self._lock:
            return setattr(self._broker, name, value)
        
    def init_broker(self, name, *args, **kwargs):
        with self._lock:
            if self._broker:
                msg = f'Shared broker already registered with {self._broker.name}.'
                raise ValidationError(msg)
                
            if 'logger' not in kwargs:
                kwargs['logger'] = self._logger
            
            self._broker = _broker_factory(name=name, *args, **kwargs)
            if not isinstance(self._broker, ILiveBroker):
                msg = f'Shared broker support only live brokers, got {type(self._broker)}'
                raise ValidationError(msg)
            
            if hasattr(self._broker, 'set_algo_callback'):
                self._broker.set_algo_callback(self._algo_callback_handler)
            
    def set_algo_callback(self, algo, algo_user, handler, *args, **kwargs):
        with self._lock:
            self.add_algo(algo, algo_user)
            self._callbacks[algo] = handler
        
    def login(self, algo, algo_user, *args, **kwargs):
        if 'forced' in kwargs:
            with self._lock:
                return self._broker.login(*args, **kwargs)
        
        if len(self._algos) <= 1:
            with self._lock:
                return self._broker.login(*args, **kwargs)
            
    def logout(self, algo, algo_user, *args, **kwargs):
        if 'forced' in kwargs:
            with self._lock:
                return self._broker.logout(*args, **kwargs)
        
        if len(self._algos) <= 1:
            with self._lock:
                return self._broker.logout(*args, **kwargs)
        
    def before_trading_start(self, algo, algo_user, timestamp):
        if self._last_bts and \
            timestamp - self._last_bts < pd.Timedelta('12H'):
            return
        
        with self._lock:
            self._last_bts = timestamp
            self._broker.before_trading_start(timestamp)
        
    def trading_bar(self, algo, algo_user, timestamp):
        if self._last_bar and \
            timestamp - self._last_bar < pd.Timedelta(seconds=40):
            return
        
        with self._lock:
            self._last_bar = timestamp
            self._broker.trading_bar(timestamp)
        
    def after_trading_hours(self, algo, algo_user, timestamp):
        if self._last_ath and \
            timestamp - self._last_ath < pd.Timedelta('12H'):
            return
        
        with self._lock:
            self._last_ath = timestamp
            self._broker.after_trading_hours(timestamp)
        
    def algo_start(self, algo, algo_user, timestamp):
        with self._lock:
            self.add_algo(algo, algo_user)
            return self._broker.algo_start(timestamp)

    def algo_end(self, algo, algo_user, timestamp):
        with self._lock:
            self.remove_algo(algo)
            return self._broker.algo_end(timestamp)
    
    def save(self, algo, algo_user, path):
        if len(self._algos) <= 1:
            with self._lock:
                return self._broker.save(path)
    
    def read(self, algo, algo_user, path):
        if len(self._algos) <= 1:
            with self._lock:
                return self._broker.read(path)
    
    def reset(self, algo, algo_user, *args, **kwargs):
        if len(self._algos) <= 1:
            with self._lock:
                return self._broker.reset(*args, **kwargs)
    
    def add_algo(self, algo, algo_user):
        with self._lock:
            self._algos.add(algo)
    
    def remove_algo(self, algo):
        with self._lock:
            if algo in self._algos:
                self._algos.remove(algo)
            self._callbacks.pop(algo, None)

class SharedBrokerCollection(IBrokerCollection):
    """
        A shared broker collections maintains dispatch for multiple 
        shared brokers.
    """
    def __init__(self, *args, **kwargs):
        self._lock = TimeoutRLock(timeout=LOCK_TIMEOUT)
        self.brokers = {}
        self.algos = {}
        
        self._logger = kwargs.get('logger') or get_logger()
        
    def get_broker(self, name, key):
        with self._lock:
            if (name,key) not in self.brokers:
                msg = f'Broker {name} not found, please register it first.'
                raise BrokerNotFound(msg)
            
            broker = self.brokers.get((name, key))
            if not broker or not broker._broker:
                msg = f'Broker {name} not found or not initialized.'
                raise BrokerNotFound(msg)
            
            return broker
    
    def init_broker(self, name, key, *args, **kwargs):
        broker = self.brokers.get((name, key))
        if not broker:
            with self._lock:
                broker = SharedBroker(name, *args, **kwargs)
                self._logger.info(f'Created shared broker {broker}.')
                self.brokers[name,key] = broker
                
    def add_algo(self, algo, algo_user, name, key):
        with self._lock:
            if algo in self.algos:
                self.algos[algo].add((name,key))
            else:
                self.algos[algo] = set([(name,key)])
                
    def remove_algo(self, algo):
        with self._lock:
            if algo in self.algos:
                for name, key in self.algos[algo]:
                    broker = self.brokers.get((name, key))
                    if broker:
                        broker.remove_algo(algo)
                        if broker.ref_count < 1:
                            self._logger.info(
                                    f'Removing shared broker {broker}.')
                            self.brokers.pop((name, key), None)
                            # disconnect any connections
                            try:
                                broker.logout(None, None)
                                del broker
                            except Exception:
                                pass
                            
register_broker_collection(SharedBrokerCollection)