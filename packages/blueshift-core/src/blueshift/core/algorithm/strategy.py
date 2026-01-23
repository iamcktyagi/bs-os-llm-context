from __future__ import annotations
from typing import TYPE_CHECKING

from blueshift.lib.common.enums import AlgoState
from blueshift.lib.exceptions import NotValidStrategy

if TYPE_CHECKING:
    import pandas as pd
    from blueshift.interfaces.context import IContext
    from blueshift.interfaces.data.data_portal import DataPortal
    from blueshift.interfaces.assets._assets import Asset

def protect(protected):
    """
        Returns a metaclass that protects all attributes given as 
        strings
    """
    msg = f'Illegal attempt to override internal method'
    class Protect(type):
        initialized=False
        def __new__(cls, name, bases, attrs):
            for attr in attrs:
                if cls.initialized and attr in protected:
                    raise AttributeError(msg)
            cls.initialized = True
            klass = super().__new__(cls, name, bases, attrs)
            return klass
    return Protect

_protected_methods = (
        '_initialize',
        '_before_trading_start',
        '_handle_data',
        '_after_trading_hours',
        '_analyze',
        '_on_data',
        '_on_trade')

class Strategy(metaclass=protect(_protected_methods)):
    """
        Interface for class based strategy on Blueshift. Subclass this 
        strategy to create a sub-strategy. This class can also be used 
        to create the main strategy (instead of defining the main 
        callback functions directly in the strategy code).
        
        :param str name: Name of the strategy.
        :param float initial_capital: Initial capital allocation.
        
        .. seealso:: 
            :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.add_strategy`.
            :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.cancel_strategy`.
    """
    def __init__(self, name:str, initial_capital:float|None, *args, **kwargs):
        self.__name = name
        self.__init_capital = initial_capital
        self.__state = AlgoState.STARTUP
        
        self.__desc = 'blueshift strategy'
        if 'description' in kwargs:
            self.__desc = str(kwargs['description'])
            
        self.__ldesc = 'blueshift strategy'
        if 'long_description' in kwargs:
            self.__ldesc = str(kwargs['long_description'])
            
        self.__risk_rating = 'high'
        if 'risk_rating' in kwargs:
            self.__risk_rating = str(kwargs['risk_rating'])
            
        self.__asset_class = 'equity'
        if 'asset_class' in kwargs:
            self.__asset_class = str(kwargs['asset_class'])
        
    @property
    def name(self) -> str:
        """ Name (and the context name) of this strategy. """
        return self.__name
    
    @property
    def state(self):
        return self.__state.name
    
    @property
    def initial_capital(self):
        return self.__init_capital
    
    @property
    def starting_cash(self):
        return self.__init_capital
        
    @property
    def description(self):
        return self.__desc
    
    @property
    def long_description(self):
        return self.__ldesc
    
    @property
    def risk_rating(self):
        return self.__risk_rating
    
    @property
    def asset_class(self):
        return self.__asset_class
    
    def __setattr__(self, key, value):
        if hasattr(self, key) and key in _protected_methods:
            raise AttributeError(
                    'protected methods, cannot overwrite.')
            
        return super().__setattr__(key, value)
    
    def _initialize(self, context:IContext):
        if self.__state == AlgoState.STOPPED:
            return
        
        self.__state = AlgoState.INITIALIZED
        if hasattr(self, 'initialize'):
            self.initialize(context)
    
    def _before_trading_start(self, context:IContext, data:DataPortal):
        if self.__state == AlgoState.STOPPED:
            return
        
        if self.__state.value < AlgoState.INITIALIZED.value:
            # we have not called initialize yet
            if hasattr(self, 'initialize'):
                self.__state = AlgoState.INITIALIZED
                self.initialize(context)
        
        self.__state = AlgoState.BEFORE_TRADING_START
        if hasattr(self, 'before_trading_start'):
            self.before_trading_start(context, data)
            
        self.__state = AlgoState.TRADING_BAR
    
    def _handle_data(self, context:IContext, data:DataPortal):
        if self.__state == AlgoState.STOPPED:
            return
        
        if self.__state.value < AlgoState.INITIALIZED.value:
            # we have not called initialize yet
            if hasattr(self, 'initialize'):
                self.__state = AlgoState.INITIALIZED
                self.initialize(context)
        
        if self.__state.value < AlgoState.BEFORE_TRADING_START.value:
            # we have not called bts yet
            if hasattr(self, 'before_trading_start'):
                self.__state = AlgoState.BEFORE_TRADING_START
                self.before_trading_start(context, data)
                
        self.__state = AlgoState.TRADING_BAR
        if hasattr(self, 'handle_data'):
            self.handle_data(context, data)
            
    def _on_data(self, context:IContext, data:DataPortal):
        if self.__state == AlgoState.STOPPED:
            return
        
        if self.__state.value < AlgoState.INITIALIZED.value:
            # we have not called initialize yet
            if hasattr(self, 'initialize'):
                self.__state = AlgoState.INITIALIZED
                self.initialize(context)
        
        if self.__state.value < AlgoState.BEFORE_TRADING_START.value:
            # we have not called bts yet
            if hasattr(self, 'before_trading_start'):
                self.__state = AlgoState.BEFORE_TRADING_START
                self.before_trading_start(context, data)
                
        if hasattr(self, 'on_data'):
            self.__state = AlgoState.TRADING_BAR
            self.on_data(context, data)
            
    def _on_trade(self, context:IContext, data:DataPortal):
        if self.__state == AlgoState.STOPPED:
            return
        
        if self.__state.value < AlgoState.INITIALIZED.value:
            # we have not called initialize yet
            if hasattr(self, 'initialize'):
                self.__state = AlgoState.INITIALIZED
                self.initialize(context)
        
        if self.__state.value < AlgoState.BEFORE_TRADING_START.value:
            # we have not called bts yet
            if hasattr(self, 'before_trading_start'):
                self.__state = AlgoState.BEFORE_TRADING_START
                self.before_trading_start(context, data)
                
        if hasattr(self, 'on_trade'):
            self.__state = AlgoState.TRADING_BAR
            self.on_trade(context, data)
            
    def _on_stoploss(self, context:IContext, asset:Asset|None):
        if self.__state != AlgoState.TRADING_BAR:
            return
                
        if hasattr(self, 'on_stoploss'):
            self.on_stoploss(context, asset)
            
    def _on_takeprofit(self, context:IContext, asset:Asset|None):
        if self.__state != AlgoState.TRADING_BAR:
            return
                
        if hasattr(self, 'on_takeprofit'):
            self.on_takeprofit(context, asset)
    
    def _after_trading_hours(self, context:IContext, data:DataPortal):
        if self.__state == AlgoState.STOPPED:
            return
        
        if self.__state.value < AlgoState.INITIALIZED.value:
            # we have not called initialize yet
            if hasattr(self, 'initialize'):
                self.__state = AlgoState.INITIALIZED
                self.initialize(context)
        
        self.__state = AlgoState.AFTER_TRADING_HOURS
        if hasattr(self, 'after_trading_hours'):
            self.after_trading_hours(context, data)
    
    def _analyze(self, context:IContext, perf:pd.DataFrame):
        if self.__state == AlgoState.STOPPED:
            return
        
        try:
            if self.__state.value < AlgoState.INITIALIZED.value:
                # we have not called initialize yet
                if hasattr(self, 'initialize'):
                    self.__state = AlgoState.INITIALIZED
                    self.initialize(context)
                    
            if hasattr(self, 'analyze'):
                self.__state = AlgoState.INITIALIZED
                self.analyze(context, perf)
        finally:
            self.__state = AlgoState.STOPPED
        
    def _heartbeat(self, context:IContext):
        if self.__state == AlgoState.STOPPED:
            return
        
        if self.__state.value < AlgoState.INITIALIZED.value:
            # we have not called initialize yet
            if hasattr(self, 'initialize'):
                self.__state = AlgoState.INITIALIZED
                self.initialize(context)
                
        if hasattr(self, 'heartbeat'):
            self.__state = AlgoState.INITIALIZED
            self.heartbeat(context)
            
    def _on_error(self, context:IContext, error:Exception|str):
        if self.__state == AlgoState.STOPPED:
            return
        
        try:
            if hasattr(self, 'on_error'):
                self.on_error(context, error)
        except Exception:
            pass
        finally:
            self.__state = AlgoState.STOPPED
        
    def _on_cancel(self, context:IContext):
        if self.__state == AlgoState.STOPPED:
            return
        
        try:
            if hasattr(self, 'on_cancel'):
                self.on_cancel(context)
        except Exception:
            pass
        finally:
            self.__state = AlgoState.STOPPED
            
    def _on_exit(self, context:IContext):
        try:
            if hasattr(self, 'on_exit'):
                self.on_exit(context)
        except Exception:
            pass
        finally:
            self.__state = AlgoState.STOPPED
    
    def initialize(self, context:IContext):
        """ override this function to run at initialization. """
        pass
    
    def handle_data(self, context:IContext, data:DataPortal):
        """ override this function to run at every cycle. """
        pass
    
    def before_trading_start(self, context:IContext, data:DataPortal):
        """ override this function to run at start of the day. """
        pass
    
    def after_trading_hours(self, context:IContext, data:DataPortal):
        """ override this function to run at end of the day. """
        pass
    
    def analyze(self, context:IContext, perf:pd.DataFrame):
        """ override this function to run at end of the run. """
        pass
    
    def on_trade(self, context:IContext, data:DataPortal):
        """ override this function to run at any order fill event. """
        pass
    
    def on_data(self, context:IContext, data:DataPortal):
        """ override this function to run at new data arrival. """
        pass
    
    def on_stoploss(self, context:IContext, asset:Asset|None):
        """ override this function to run after a stoploss is triggered. """
        pass
    
    def on_takeprofit(self, context:IContext, asset:Asset|None):
        """ override this function to run after a takeprofit is triggered. """
        pass
    
    def on_error(self, context:IContext, error:Exception|str):
        """ override this function to run before the strategy exit on error. """
        pass
    
    def on_cancel(self, context:IContext):
        """ override this function to run before the strategy exit on user cancel. """
        pass
    
    def on_exit(self, context:IContext):
        """ override this function to run on exit. """
        pass

    def heartbeat(self, context:IContext):
        """ override this function to run on exit. """
        pass

def strategy_factory(cls, *args, **kwargs) -> Strategy:
    from inspect import getfullargspec, isclass

    if isclass(cls) and issubclass(cls, Strategy) and cls.__name__ != 'Strategy':
        specs = getfullargspec(Strategy.__init__)

        if specs.varkw:
            kw = kwargs.copy()
        else:
            kw = {}
            args_specs = specs.args
            for key in kwargs:
                if key in args_specs:
                    kw[key] = kwargs[key]

        return cls(*args, **kw)
    else:
        msg = f'not a Strategy class, got {cls}, {type(cls)}.'
        raise NotValidStrategy(msg)