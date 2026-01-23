from __future__ import annotations
from typing import cast, TYPE_CHECKING
from blueshift.lib.common.enums import (
        ExecutionMode, BlotterType, BrokerType, AlgoMode)
from blueshift.lib.common.constants import Frequency

from blueshift.interfaces.trading.broker import (
    AccountType, register_broker, ILiveBroker, IBacktestBroker, IPaperBroker)
from blueshift.interfaces.trading._simulation import (
    ABCCostModel, ABCMarginModel, ABCSlippageModel)

from blueshift.providers.data.library import Library
from blueshift.providers.trading.broker.backtester import BacktestBroker
from blueshift.providers.data.store.broker_store import BrokerStore

if TYPE_CHECKING:
    import pandas as pd
    from blueshift.lib.common.constants import CurrencyPair

class PaperBroker(BacktestBroker, IPaperBroker):
    def __init__(self, broker:ILiveBroker, backtester:IBacktestBroker, initial_capital:float|None, 
                 frequency:Frequency|str, *args, **kwargs):
        self._broker = broker
        
        if backtester.library.root:
            # for library with root, we assume the library is a 
            # backtesting library and needs to point to broker data
            # along with pipeline pointing to the backtester library
            broker_store = BrokerStore(broker, frequency)
            pipeline_store = backtester.library.pipeline_store
            
            fx_support = kwargs.pop('fx_support', False)
            if fx_support:
                fx_rate_store = broker_store
            else:
                fx_rate_store = backtester.library.fx_rate_store
            
            library = Library('', active_store=broker_store,
                              pipeline_store=pipeline_store, 
                              fx_rate_store=fx_rate_store,
                              futures_store=broker_store,
                              options_store=broker_store)
        else:
            # else we use the backtester library as is
            library = backtester.library
        
        calendar = broker.calendar
        ccy = broker.ccy
        intraday_cutoff = broker.intraday_cutoff
        logger = broker.logger
        
        cost_model = broker.cost_model or backtester.cost_model
        margin_model = broker.margin_model or backtester.margin_model
        slippage_model = broker.simulator or backtester.simulator

        slippage_model = cast(ABCSlippageModel, slippage_model)
        margin_model = cast(ABCMarginModel, margin_model)
        cost_model = cast(ABCCostModel, cost_model)
        library = cast(Library, library)
        intraday_cutoff = cast(tuple, intraday_cutoff)
        
        super(PaperBroker, self).__init__(
                initial_capital, frequency, library, calendar, ccy, 
                intraday_cutoff, logger, cost_model=cost_model, 
                margin_model=margin_model, slippage_model=slippage_model, 
                is_paper=True, *args, **kwargs)
        
        self._name = f'SimulatedTrading[{broker.name}]'
        self._type = BrokerType.PAPERTRADER
        self._modes_supported = [AlgoMode.PAPER]
        self._exec_modes_supported = [ExecutionMode.AUTO, ExecutionMode.ONECLICK]
        self._blotter_type = BlotterType.EXCLUSIVE
        self._mode = AlgoMode.PAPER
        self._streaming_update = False
        self._account_type = AccountType.SIMULATED
        self._supported_products = backtester.supported_products
        
    @property
    def broker(self):
        return self._broker
    
    @property
    def assets(self):
        return self.broker.assets
    
    @property
    def subscribed_assets(self):
        return self.broker.subscribed_assets
    
    @property
    def rms(self):
        return self.broker.rms
    
    @property
    def is_connected(self):
        return self.broker.is_connected
    
    def initialize(self, *args, **kwargs):
        """ initialize the broker object. """
        self.broker.initialize(*args, **kwargs)
    
    def finalize(self, *args, **kwargs):
        """ finalize the broker object. """
        self.broker.finalize(*args, **kwargs)
    
    ######## Event INTERFACE METHODS ###########################
    
    def login(self, *args, **kwargs):
        if 'timestamp' in kwargs:
            self._timestamp = kwargs['timestamp']
            
        self.broker.login(*args, **kwargs)

    def logout(self, *args, **kwargs):
        if 'timestamp' in kwargs:
            self._timestamp = kwargs['timestamp']
            
        self.broker.logout(*args, **kwargs)
        
    def set_algo_callback(self, *args, **kwargs):
        if hasattr(self.broker, 'set_algo_callback'):
            return self.broker.set_algo_callback(*args, **kwargs)
        
    ######## ASSET FINDER INTERFACE METHODS ##########################
        
    def symbol(self, symbol, dt=None, *args, **kwargs):
        return self.broker.symbol(symbol, dt=dt, *args, **kwargs)
    
    def exists(self, syms):
        """ Check if a symbol exists. """
        return self.broker.exists(syms)
    
    def fetch_asset(self, sid):
        """ SID based asset search not implemented. """
        return self.broker.fetch_asset(sid)
        
    def refresh_data(self, *args, **kwargs):
        if hasattr(self.broker,'refresh_data'):
            self.broker.refresh_data(*args, **kwargs)
    
    def retrieve_asset(self, sid):
        """ Resolve and return asset given an asset object. """
        raise NotImplementedError
        
    def retrieve_all(self, sids):
        """ Resolve and return assets given an iterable of asset objects. """
        raise NotImplementedError
        
    def lifetimes(self, dates, assets=None):
        """ lifetime matrix not implemented. """
        raise NotImplementedError
        
    def can_trade(self, asset, dt):
        return self.broker.can_trade(asset, dt)
    
    ######### Data fetch INTERFACE METHODS ######################
    
    def has_streaming(self) -> bool:
        """ if streaming data supported. """
        return self.broker.has_streaming()
    
    def getfx(self, ccy_pair:str|CurrencyPair, dt:pd.Timestamp):
        if hasattr(self.broker, 'getfx'):
            return self.broker.getfx(ccy_pair, dt)
        raise NotImplementedError
        
    def quote(self, asset, column=None, **kwargs):
        return self.broker.quote(asset, column, **kwargs)
    
    def history(self, assets, columns, nbars, frequency, **kwargs):
        return self.broker.history(assets, columns, nbars, frequency, **kwargs)
    
    def fundamentals(self, assets, metrics, nbars, frequency, **kwargs):
        return self.broker.fundamentals(assets, metrics, nbars, frequency, **kwargs)
    
    def current(self, assets, columns='close', **kwargs):
        return self.broker.current(assets, columns, **kwargs)
    
    def pre_trade_details(self, asset, **kwargs):
        return self.broker.pre_trade_details(asset, **kwargs)
    
    def subscribe(self, assets, level=1, *args, **kwargs):
        return self.broker.subscribe(assets, level, *args, **kwargs)
    
    def unsubscribe(self, assets, level=1, *args, **kwargs):
        return self.broker.unsubscribe(assets, level, *args, **kwargs)
    
    def option_chain(self, underlying, series, columns, strikes=None, 
                     relative=True, **kwargs):
        return self.broker.option_chain(
                underlying, series, columns, strikes=strikes,
                relative=relative, **kwargs)
    
register_broker('paper', PaperBroker, None)