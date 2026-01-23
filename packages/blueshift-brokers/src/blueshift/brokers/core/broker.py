from __future__ import annotations
from typing import TYPE_CHECKING, Callable, cast, Any
import numpy as np
import datetime
from threading import Event
import time

from blueshift.config import LOCK_TIMEOUT
from blueshift.lib.trades._order import Order
from blueshift.lib.trades._position import Position
from blueshift.lib.trades._order_types import OrderSide, ProductType
from blueshift.lib.common.enums import AlgoCallBack, BrokerType
from blueshift.lib.common.platform import  print_msg
from blueshift.lib.common.functions import  listlike
from blueshift.lib.common.types import (
        MaxSizedOrderedDict, MaxSizedList)
from blueshift.lib.common.sentinels import noop
from blueshift.lib.common.decorators import ensure_connected
import blueshift.lib.trades._order_types as ORDER_TYPES
from blueshift.lib.common.ctx_mgrs import TimeoutRLock
from blueshift.lib.common.enums import BrokerTables
from blueshift.lib.common.constants import MAX_COLLECTION_SIZE, MAX_TICKERS_SIZE, LARGE_TIMEOUT
from blueshift.lib.exceptions import (
        ValidationError, InitializationError, TradingBlockError, APIError, SymbolNotFound, ServerError, 
        TerminationError, BrokerError, AuthenticationError, OrderError, OrderAlreadyProcessed, 
        ExceptionHandling, PreTradeCheckError, OrderNotFound, APIException)

from blueshift.interfaces.data.store import store_factory, DataStore
from blueshift.interfaces.data.data_portal import DataPortal, SubscriptionLevel
from blueshift.interfaces.trading.broker import ILiveBroker, AccountType
from blueshift.interfaces.trading.rms import IRMS
from blueshift.interfaces.assets._assets import MarketData, Asset
from blueshift.providers.trading.broker.utils.streaming import BrokerStreamingData
from blueshift.providers.data.ingestor.stream_ingestor import StreamingData

from .config.config import APIBrokerConfig
from .api import BrokerAPIMixin
from .assets import RestAPIBrokerAssetFinder
from .data import RestAPIBrokerDataPortal
from .conversions import create_account, create_order, create_position, create_asset
from .streaming import on_connect, on_disconnect, set_up_streaming

if TYPE_CHECKING:
    from blueshift.interfaces.logger import BlueshiftLogger

class RestAPIBroker(BrokerStreamingData, ILiveBroker):
    """
        Generic live trading rest api broker interface implementation. This 
        accepts a ``APIBrokerConfig`` object and a ``BrokerAPIMixin`` object
        and implements the ``IBroker`` interface for live trading with 
        support for streaming data and order updates.
    """
    DEFAULT_LIBRARY = None
    
    def __init__(self, config:APIBrokerConfig, callback:Callable|None=None, data_portal_mode:bool=False, 
                 data_portal:RestAPIBroker|None=None, logger:BlueshiftLogger|None=None, 
                 disable_streaming:bool=False, **kwargs):
        if not isinstance(config, APIBrokerConfig):
            msg = f'not a valid broker config'
            raise ValidationError(msg)
        
        if logger:
            config.logger = logger

        self._interface = config.broker.options.interafce
        if self._interface == 'data-portal':
            data_portal_mode = True
        elif self._interface == 'trader':
            if not data_portal or not isinstance(data_portal, ILiveBroker):
                raise ValidationError(f'trade interface mode requires a data portal')
        else:
            data_portal = None
            data_portal_mode = False

        self._config = config
        self._variant = config.broker.variant
        self._logger = logger or config.logger
        self._original_logger = self._logger
        self._initialized:bool = False
        self._initialized_event = Event()
        self._api_event = Event()
        self._timeout = min(1, int(config.broker.options.timeout))
        self._calendar = config.broker.calendar
        self._account_type = AccountType.LIVE
        self._data_portal_mode:bool = data_portal_mode # no order streaming if true
        self._data_portal = data_portal # no data streaming if dataportal passed
        self._streaming_data = False
        self._streaming_update = False
        self._ingestor = None
        self._intraday_cutoff = None
        self._intraday_cutoff_dt = None
        self._intraday_reject = False

        ILiveBroker.__init__(
                self, name=config.broker.name,
                broker_type=BrokerType.RESTBROKER,
                supported_modes=config.broker.options.supported_modes,
                execution_modes=config.broker.options.execution_modes,
                blotter_type=config.broker.options.blotter_type, 
                currency=config.broker.options.ccy,
                streaming_update=self._streaming_update)

        try:
            ctx = kwargs.copy()
            ctx['broker'] = self
            self.config.resolve(**ctx)
        except Exception as e:
            raise InitializationError(f'failed to resolve config:{str(e)}') from e
        
        self._rest_api = BrokerAPIMixin(config, self, logger=self._logger, event=self._api_event)
        
        if self._interface == 'trader':
            self._asset_finder = None
            self._data_interface = None
            self._db = None
        else:
            self._asset_finder = RestAPIBrokerAssetFinder(config.broker.name, config, self)
            self._db = store_factory(
                    'dataframe', frequency='1m', calendar=self._calendar, 
                    max_tickers=MAX_TICKERS_SIZE,
                    supported_assets=self._asset_finder.supported_assets,
                    supported_exchanges=self._asset_finder.exchanges,
                    columns=self._asset_finder.columns)
            self._data_interface = RestAPIBrokerDataPortal(config, self)
            
        self._algo_callback:Callable[[AlgoCallBack, list[Any] | None], None] = callback or noop
        self._disable_streaming:bool = disable_streaming or self.config.streaming is None
        
        self._init_data_structures()
        streaming_config = self._init_streaming()
        BrokerStreamingData.__init__(self, streaming_config, logger=self.logger)
        
        if config.broker.options.intraday_cutoff:
            self.set_roll_policy(0,config.broker.options.intraday_cutoff)
            
        products = []
        mapping = self.config.broker.mappings.product_type
        for k,v in mapping.mapping.items():
            if isinstance(v, list):
                products.append(mapping.to_blueshift(v[0]))
            else:
                products.append(mapping.to_blueshift(v))

        self._supported_products = products or [ProductType.DELIVERY]
        self._rms = cast(IRMS, self._config.broker.options.rms)
        self._last_order_fetch = None
        self._implements_update_order = True # optional
        self._implements_get_order = True # optional, fall back to fetch all orders
        
        self._initialize()
        self.subscribe_all()

    def get_context(self) -> dict[str, Any]:
        ctx = {'broker':self, 'credentials':self.config.broker.credentials}
        ctx['logger'] = self.logger
        ctx['api'] = self._rest_api
        ctx['mappings'] = self.config.broker.mappings
        ctx['config'] = self.config

        return ctx
        
    def __str__(self):
        if self._interface == 'data-portal':
            return f"Blueshift DataPortal({self.name}[{self.account_type}])"
        elif self._interface == 'trader':
            return f"Blueshift Trader({self.name}[{self.account_type}])"
        else:
            return f"Blueshift Broker({self.name}[{self.account_type}])"

    def __repr__(self):
        return self.__str__()
    
    def id(self):
        if self._interface == 'data-portal':
            return f"{self.name} DataPortal[{self.account_type.name}|{self.account_id}]"
        elif self._interface == 'trader':
            return f"{self.name} Trader[{self.account_type.name}|{self.account_id}]"
        else:
            return f"{self.name} Broker[{self.account_type.name}|{self.account_id}]"
    
    @property
    def variant(self):
        return self._variant
    
    @property
    def logger(self):
        return self._logger
    
    @logger.setter
    def logger(self, value:BlueshiftLogger):
        self._logger = value
        self._rest_api._logger = value
        
    @property
    def broker_logger(self):
        return self._original_logger
    
    @property
    def store(self):
        if self._data_portal:
            store = self._data_portal._db
        else:
            store = self._db

        return cast(DataStore, store)
    
    @property
    def config(self):
        return self._config
    
    @property
    def asset_finder(self):
        if self._data_portal:
            asset_finder = self._data_portal._asset_finder
        else:
            asset_finder = self._asset_finder

        return cast(RestAPIBrokerAssetFinder, asset_finder)
    
    @property
    def data_interface(self):
        if self._data_portal:
            data_interface = self._data_portal._data_interface
        else:
            data_interface = self._data_interface

        return cast(RestAPIBrokerDataPortal , data_interface)
    
    @property
    def api(self):
        return self._rest_api
    
    @property
    def ingestor(self):
        return self._ingestor
    
    @property
    def assets(self):
        return self.asset_finder.assets
    
    @property
    def account_id(self):
        ids = list(self._accounts.keys())
        if len(ids) > 0:
            return ids[0]
        
    @property
    def account_type(self):
        return self._account_type
    
    @property
    def profile(self):
        return {"account": self.account}
    
    @property
    def tz(self):
        return self._calendar.tz

    @property
    def calendar(self):
        return self._calendar
    
    @property
    def intraday_cutoff(self):
        return self._intraday_cutoff_dt
    
    @property
    def data_portal(self):
        if self._data_portal:
            return self._data_portal
        else:
            return self
        
    def _init_data_structures(self):
        self._accounts = {}
        self._open_orders:dict[str,Order] = {}
        self._closed_orders:dict[str,Order] = MaxSizedOrderedDict(
                max_size=MAX_COLLECTION_SIZE, chunk_size=500)
        self._open_positions:dict[Asset,Position] = {}
        self._open_intraday_positions:dict[Asset,Position] = {}
        self._closed_positions:list[Position] = MaxSizedList(
                max_size=MAX_COLLECTION_SIZE, chunk_size=500)
        self._heartbeat:int = 0
        self._subscribed_assets:set[MarketData] = set()
        self._order_lock = TimeoutRLock(timeout=LOCK_TIMEOUT)
        self._table_locks = {BrokerTables(i):TimeoutRLock(timeout=LOCK_TIMEOUT) for i in range(len(BrokerTables))}
        self._skip_margin_api = False
        self._skip_charges_api = False
    
    def _init_streaming(self):
        streaming_config:dict[str, StreamingData] = {}

        if not self.config.streaming:
            return streaming_config
        
        if self._disable_streaming:
            return streaming_config
        
        channels_seen = set()

        for key in self.config.streaming.connections:
            config = self.config.streaming.connections[key]
            channels = set(config.streams)

            if channels_seen.intersection(channels):
                common = ','.join(list(channels_seen.intersection(channels)))
                raise ValidationError(f'multiple sources not allowed for the same channels {common}')
            
            streamer = set_up_streaming(self, config)

            if 'data' in channels:
                streaming_config['data'] = cast(StreamingData, streamer)
                self._ingestor = streamer
                self._streaming_data = True
            else:
                name = '-'.join(list(channels))
                streaming_config[name] = cast(StreamingData, streamer)

            channels_seen.update(channels)

        return streaming_config
    
    def get_connection_for_channel(self, channel:str):
        if not self.config.streaming:
            return None
        
        for name, conn in self.config.streaming.connections.items():
            if channel in conn.streams:
                return conn
        return None
    
    def get_streaming_subscribe_payload(self, channel):
        conn = self.get_connection_for_channel(channel)
        if not conn or not conn.subscribe or 'subscribe' not in conn.subscribe:
             raise NotImplementedError(f"No connection or subscribe definition for channel {channel}")
        
        ctx = self.get_context()
        msg_defs = conn.subscribe['subscribe']
        if not msg_defs:
            raise NotImplementedError(f"No subscribe definitions for channel {channel}")
        
        # search for subscribe payload specific to the channel
        msg_def = msg_defs.get(channel)
        if not msg_def:
            # else fall back to common payload
            msg_def = msg_defs.get('all')

        if not msg_def:
            raise NotImplementedError(f"No subscribe definition for channel {channel}")
            
        return msg_def.resolve(**ctx)
    
    def subscribe_all(self):
        if not self.config.streaming:
            return
        
        if self._disable_streaming:
            return
        
        ctx = self.get_context()
        
        for k in self._streamers:
            channels = k.split('-')
            for channel in channels:
                if channel in  ('data', 'quote'):
                    # this is handled by the data interface
                    continue

                conn = self.get_connection_for_channel(channel)
                if not conn or not conn.subscribe or 'subscribe' not in conn.subscribe:
                    continue
                
                msg_defs = conn.subscribe['subscribe']
                if not msg_defs:
                    continue

                for channel in msg_defs:
                    msg_def = msg_defs[channel]
                    payload = msg_def.resolve(**ctx)
                    self._streamers[k].subscribe(payload)
                print_msg(f'successfully subscribed to {channel} channel.')

            
    def register_data_callback(self, callback):
        if self.ingestor:
            self.ingestor.register('data',callback)
        elif self._data_portal and self._data_portal.ingestor:
            self._data_portal.ingestor.register('data',callback)
        else:
            msg = f'Cannot set data algo callback, no streaming '
            msg += f'data listening is available.'
            self.logger.warning(msg)
        
    def set_algo_callback(self, callback:Callable[[AlgoCallBack, list[Any] | None], None], *args, **kwargs):
        """ inject callback for allowed events."""
        if self._data_portal_mode:
            # no callback needed for data portal mode
            return
        
        # we assume the data channel can be shared between algo instances
        # for all other channels except data, we can simply overwrite the algo_callback
        self._algo_callback = callback

        # we handle the data channel separately - the ingestor maintains a 
        # list of callback handlers and add to it if the event channel is 'data'
        # the callback is invoked, after the ingestion of the current packet is complete
        def algo_callback(asset):
            if asset in self._subscribed_assets:
                self._algo_callback(AlgoCallBack.DATA, [asset])
            
        # this means, all "on_event" functions must call the callback explicitly
        # except "on_data", where it is delegated to the ingestor. Ideally this should 
        # be the behaviour for all sharable channels - e.g. quote, and news. But they 
        # are not implemented at present.
        self.register_data_callback(algo_callback)
            
    def _wait_for_connection(self, event=None):
        timeout = max(self._timeout, LARGE_TIMEOUT)
        
        if event:
            # early return in case of connection error
            event.wait(timeout=5*self._timeout)
            if not self.is_connected:
                return False
        
        if not self._initialized_event.wait(timeout=timeout):
            return False
        
        if not self._initialized:
            return False
        return True
    
    def _initialize(self):
        print_msg("Connecting to the broker ... ", _type="info", nl=False)
        event = Event() if not self._disable_streaming else None
        self._initialized_event.clear()

        self.connect(event=event, timeout=5*self._timeout)
        
        if self._disable_streaming:
            if self.config.streaming is not None:
                for k in self.config.streaming.connections:
                    conn = self.config.streaming.connections[k]
                    on_connect(self, conn)
            else:
                self._initialized = True
                self._initialized_event.set()
        
        if not self._wait_for_connection(event=event):
            msg = 'Connect to server timed out.'
            self.logger.error(msg)
            raise InitializationError(msg)
        
        print_msg("Done", _type="info")
        
    ######## ASSET FINDER INTERFACE METHODS ##########################
    def symbol(self, symbol, dt=None, product_type=None,
               use_spot=False, *args, **kwargs):
        """
            Get asset object by symbol name

            Args:
                ``symbol (str)``: Symbol name.

            Returns:
                Asset - the asset object matching the symbol.
        """ 
        return self.asset_finder.symbol(
                symbol, dt=dt, product_type=product_type, use_spot=use_spot, *args, **kwargs)
        
    def convert_asset(self, asset, product_type=None, **kwargs):
        return self.asset_finder.convert_asset(
                asset, product_type=product_type, **kwargs)
            
    def to_base_asset(self, asset):
        return self.asset_finder.to_base_asset(asset)
    
    def symbol_to_asset(self, symbol, dt=None, product_type=None, 
                        *args, **kwargs):
        return self.symbol(symbol, dt=dt, product_type=product_type, 
                           *args, **kwargs)
    
    def exists(self, syms:str|list[str]):
        """ Check if a symbol exists. """
        return self.asset_finder.exists(syms)
    
    def fetch_asset(self, sid):
        """ SID based asset search not implemented. """
        return self.asset_finder.fetch_asset(sid)
        
    def sid(self, sid):
        return self.fetch_asset(sid)
        
    def refresh_data(self, *args, **kwargs):
        return self.asset_finder.refresh_data(*args, **kwargs)
        
    def lifetimes(self, dates, assets):
        """ lifetime matrix not implemented. """
        return self.asset_finder.lifetimes(dates, assets=assets)
        
    def can_trade(self, asset, dt=None):
        """ Is the asset tradeable. """
        return self.asset_finder.can_trade(asset, dt=dt)
    
    def get_asset(self, sym, **kwargs):
        return self.symbol(sym, **kwargs)
    
    def get_asset_details(self, asset, **kwargs):
        return self.asset_finder.get_asset_details(asset)
    
    def from_broker_symbol(self, sym, **kwargs):
        return self.asset_finder.from_broker_symbol(sym, **kwargs)
    
    def infer_asset(self, **kwargs):
        return self.asset_finder.infer_asset(**kwargs)
    
    ######## Event INTERFACE METHODS ###########################
    
    def setup_model(self, *args, **kwargs):
        pass
    
    def set_currency(self, *args, **kwargs):
        pass
    
    def set_commissions(self, *args, **kwargs):
        pass
    
    def set_slippage(self, *args, **kwargs):
        pass
    
    def set_margin(self, *args, **kwargs):
        pass
    
    def set_roll_policy(self, roll_day, roll_time):
        if isinstance(roll_time, datetime.time):
            cutoff = roll_time
        else:
            cutoff = datetime.time(*roll_time)
        
        self._intraday_cutoff_time = str(cutoff)
        self._intraday_cutoff_dt = cutoff
        cutoff = cutoff.hour*3600+cutoff.minute*60+cutoff.second
        if self._intraday_cutoff and self._intraday_cutoff < cutoff:
            msg = f"Custom intraday cutoff can only be less than broker"
            msg += " defined cut-off time."
            raise ValidationError()
        self._intraday_cutoff = cutoff
    
    def login(self, *args, **kwargs):
        """ connect to the broker. """
        # this is designed to run once for each day
        forced = kwargs.get('forced', False)
        
        if self.is_connected and self._rest_api.is_valid_session() and not forced:
            return
        
        if self._ingestor:
            self._ingestor.reset()
        
        if kwargs:
            ctx = {**kwargs, **self.get_context()}
            self.config.resolve(**ctx)
        
        try:
            self._rest_api.finalize()
            self.disconnect()
        except Exception:
            pass
        
        self._rest_api.initialize()
        event = Event() if not self._disable_streaming else None
        self._initialized_event.clear()
        self._initialized = False

        if not self._disable_streaming:
            self.connect(event=event, timeout=5*self._timeout)
        
        if self._disable_streaming:
            if self.config.streaming is not None:
                for k in self.config.streaming.connections:
                    conn = self.config.streaming.connections[k]
                    on_connect(self, conn)
            else:
                self._initialized = True
                self._initialized_event.set()
        
        if not self._wait_for_connection(event=event):
            raise ServerError(
                        'failed to connect to server during login.',
                        handling=ExceptionHandling.TERMINATE)

    def logout(self, *args, **kwargs):
        """ disconnect from the broker. """
        try:
            self._rest_api.finalize()
            self.disconnect()
        except Exception:
            pass
        
        self._initialized = False
        self._initialized_event.clear()
        
    def before_trading_start(self, timestamp):
        """ refresh the connection before trading start. """
        # reset intraday flag
        self._intraday_reject = False
        
        # reset data cache of the in-memory DataFrameDB
        if self._ingestor:
            self._ingestor.reset()
            
        self.logger.info('connecting to the broker.')
        self.login()
        self._heartbeat = 0
        
    @ensure_connected(timeout=30, max_retry=2, raise_exception=False)
    def trading_bar(self, timestamp):
        if self._intraday_cutoff is not None:
            elapsed_second = timestamp.hour*3600 + timestamp.minute*60 + timestamp.second
            if not self._intraday_reject and \
                elapsed_second > self._intraday_cutoff:
                self._run_intraday_cutoff(timestamp)
                self._roll_assets(timestamp)
                
        self._heartbeat += 1
        if self._heartbeat % 5 == 0 and self._open_orders:
            # force an order update
            self._fetch_orders()
                
    def _run_intraday_cutoff(self, timestamp):
        self._intraday_reject = True
    
    def _roll_assets(self, timestamp):
        pass
            
    def after_trading_hours(self, timestamp):
        """ close the connection if open """
        self.logger.info('disconnecting with the broker.')
        self.logout()
        
    def algo_start(self, timestamp):
        pass

    def algo_end(self, timestamp):
        pass

    def heart_beat(self, timestamp):
        pass
    
    ######### Data fetch INTERFACE METHODS ######################
    
    def has_streaming(self):
        """ if streaming data supported. """
        if self._data_portal:
            return self._data_portal.has_streaming()
            
        return self._streaming_data
    
    def update_market_quotes(self, asset:Asset, data: dict):
        if self._interface != 'trader':
            self.data_interface.update_market_quotes(asset, data)
    
    def fundamentals(self, assets, metrics, nbars, frequency, dt=None, **kwargs):
        """ underlying fundamental method from library. """
        if not hasattr(self, 'library') or not self.library:
            msg = f'Cannot use research data source, no source defined'
            raise ValidationError(msg)
        return self.library.fundamentals(assets, metrics, nbars, frequency, dt=dt, **kwargs)
    
    def _handle_library_history(self, assets, columns, nbars, frequency, **kwargs):
        if not hasattr(self, 'library') or not self.library:
            msg = f'Cannot use research data source, no source defined'
            raise ValidationError(msg)
        
        try:
            if listlike(assets):
                lib_assets = [self.library.symbol(asset.exchange_ticker) for asset in assets]
            else:
                lib_assets = self.library.symbol(assets.symbol)
        except Exception as e:
            msg = f'historical dataset does not supoprt some or all '
            msg += f'the assets {assets}:{str(e)}'
            raise SymbolNotFound(msg)
            
        return self.library.history(lib_assets, columns, nbars, frequency, **kwargs)
    
    @ensure_connected(timeout=30, max_retry=1, raise_exception=False)
    def history(self, assets, columns, nbars, frequency, logger=None, **kwargs):
        """
            Get historical data.
            
            Note:
                If `assets` is a list, and a single `columns` name is passed,
                or if `assets` is an `Asset` object and a list of `columns`
                is passed, a frame will be returned, indexed by timestamps 
                and column names as assets or columns, respectively. If both 
                arguments are list, a multi-index frame is returned with 
                columns as column names, 0-th level index as the assets and 
                1st level index as timestamps. Otherwise a series with 
                timestamp index is returned.

            Args:
                ``assets (obj or list)``: Asset or a list of asset objects.

                ``columns (str or list)``: name or a list of OHLCV columns

                ``nbars (int)``: Number of bars to return

                ``frequency (str)``: Frequency

            Returns:
                Series, frame or multi-index frame, depending on the 
                arguments.
        """
        if 'research' in kwargs and kwargs['research']:
            kwargs.pop('research', None)
            return self._handle_library_history(assets, columns, nbars, frequency, **kwargs)
        
        self._update_subscribed_list(assets)

        if 'caching' not in kwargs:
            caching = kwargs.pop('cached', True)
            kwargs['caching'] = caching

        api = self._rest_api
        return self.data_interface.history(
                assets, columns, nbars, frequency, logger=logger, api=api, **kwargs)
        
    @ensure_connected(timeout=30, max_retry=1, raise_exception=False)
    def current(self, assets, columns, check_stale=False, logger=None,
                **kwargs):
        """
            Get last available minute candles of prices.
            
            Note:
                If `assets` is a list, and a single `columns` name is passed,
                or if `assets` is an `Asset` object and a list of `columns`
                is passed, a series will be returned, indexed by assets or 
                columns, respectively. If both arguments are list, a frame 
                is returned with columns as column names. Otherwise a 
                scalar float is returned.

            Args:
                ``assets (obj or list)``: Asset or a list.

                ``fields (str or list)``: OHLCV column or list.

            Returns:
                Float, series or Frame, depending on the argument
        """
        self._update_subscribed_list(assets)
        api = self._rest_api
        return self.data_interface.current(
                assets, columns=columns, check_stale=check_stale, logger=logger, api=api, **kwargs)
        
    def pre_trade_details(self, asset, logger=None, **kwargs):
        self._update_subscribed_list(asset)
        api = self._rest_api
        return self.data_interface.pre_trade_details(asset, logger=logger, api=api, **kwargs)
    
    @ensure_connected(timeout=30, max_retry=1, raise_exception=False)
    def quote(self, asset, column=None, logger=None, **kwargs):
        self._update_subscribed_list(asset)
        api = self._rest_api
        return self.data_interface.quote(asset, column, logger=logger, api=api, **kwargs)
    
    def _update_subscribed_list(self, assets:MarketData|list[MarketData], add:bool=True):
        if not listlike(assets):
            subscribe_list = [assets]
        else:
            subscribe_list = assets
            
        subscribe_list = cast(list[MarketData], subscribe_list)
        subscribe_list = [self.to_base_asset(asset) for asset in subscribe_list]
            
        if add:
            for asset in subscribe_list:
                self._subscribed_assets.add(asset)
        else:
            for asset in subscribe_list:
                if asset in self._subscribed_assets:
                    self._subscribed_assets.add(asset)
    
    @ensure_connected(timeout=30, max_retry=1, raise_exception=False)
    def subscribe(self, assets, level=SubscriptionLevel.TRADE, **kwargs):
        if level==1:
            level = SubscriptionLevel.TRADE
        elif level == 2:
            level = SubscriptionLevel.QUOTE
            
        self._update_subscribed_list(assets)

        self.data_interface.subscribe(
                assets, level=level, **kwargs)
        
    @ensure_connected(timeout=30, max_retry=1, raise_exception=False)
    def unsubscribe(self, assets, level=SubscriptionLevel.TRADE, **kwargs):
        if level==1:
            level = SubscriptionLevel.TRADE
        elif level == 2:
            level = SubscriptionLevel.QUOTE
            
        self._update_subscribed_list(assets, add=False)

        self.data_interface.unsubscribe(
                assets, level=level, **kwargs)
    
    ######## Trading INTERFACE METHODS ##########################
    def _fetch_orders(self, logger=None):
        existing_orders = {**self._open_orders, **self._closed_orders}
        open_orders = self._open_orders.copy()
        closed_orders = self._closed_orders.copy()
        self._open_orders = {}
        self._closed_orders = {}
        logger = logger or self.logger
        
        try:
            data = self._rest_api.request_get_orders(logger=logger)
            
            if data:
                with self._table_locks[BrokerTables.ORDERS]:
                    for packet in data:
                        try:
                            asset = self.infer_asset(**packet, logger=logger)
                        except SymbolNotFound:
                            logger.info(f'Could not resolve asset from data {packet}')
                            continue
                        if asset is None:
                            logger.info(f'No asset found from data {packet}')
                            continue
                        
                        asset = cast(Asset, asset)
                        o = create_order(self, packet, asset)
                        if not o:
                            logger.info(f'Could not create order from data {asset}, {packet}')
                            continue
                        
                        # prefill data from existing orders if any
                        if o.oid in existing_orders:
                            oo = existing_orders[o.oid]
                            o.copy_trackers_from_order(oo)
                            if oo.timestamp is not None:
                                o.set_timestamp(oo.timestamp)
                        
                        # put it to correct container
                        if o.is_open():
                            self._open_orders[o.oid] = o
                        else:
                            self._closed_orders[o.oid] = o
        except TerminationError:
            raise
        except AuthenticationError:
            raise
        except Exception as e:
            # restore the origin data
            self._open_orders = open_orders
            self._closed_orders = closed_orders
            raise
    
    def _fetch_accounts(self, logger=None):
        try:
            data = self._rest_api.request_get_account(logger=logger)
        except TerminationError:
            raise
        except AuthenticationError:
            raise
        except Exception as e:
            msg = f'failed to fetch accounts data:{str(e)}'
            raise APIError(msg)
        
        for packet in data:
            try:
                acct = create_account(self, packet)
            except Exception as e:
                msg = f'illegal account data:{str(e)}'
                raise APIError(msg)
            else:
                self._accounts[acct.name] = acct
    
    def _fetch_open_positions(self, logger=None):
        current_pos = self._open_positions.copy()
        current_intraday_pos = self._open_intraday_positions.copy()
        
        try:
            positions, intraday = self._fetch_positions(logger=logger)
            
            # positions and holdings can be duplicated
            # we here potentially mix up margin vs delivery position
            # but from blueshift pov, that should be fine
            self._open_positions = positions
            self._open_intraday_positions = intraday
            
            # update closed intraday positions
            for asset in current_intraday_pos:
                if asset not in self._open_intraday_positions:
                    p = current_intraday_pos[asset]
                    self._closed_positions.append(p)
            
            # update closed positions
            for asset in current_pos:
                if asset not in self._open_positions:
                    p = current_pos[asset]
                    self._closed_positions.append(p)
                    
            # we ideally should be done here, but double check none of 
            # the pos in open_pos is closed
            assets = list(self._open_positions.keys())
            for asset in assets:
                pos = self._open_positions[asset]
                if pos.if_closed():
                    self._open_positions.pop(asset)
                    self._closed_positions.append(pos)
                    
            # do the same for intraday
            assets = list(self._open_intraday_positions.keys())
            for asset in assets:
                pos = self._open_intraday_positions[asset]
                if pos.if_closed():
                    self._open_intraday_positions.pop(asset)
                    self._closed_positions.append(pos)
        except TerminationError:
            raise
        except Exception as e:
            self._open_positions = current_pos
            self._open_intraday_positions = current_intraday_pos
            msg = f'failed to fetch open positions:{str(e)}'
            raise APIError(msg)
            
    def _fetch_positions(self, logger=None):
        positions = {}
        intraday = {}
        logger = logger or self.logger
        
        try:
            reported = self._rest_api.request_get_positions(logger=logger)
            with self._table_locks[BrokerTables.OPEN_POSITIONS]:
                if reported:
                    for packet in reported:
                        try:
                            asset = self.infer_asset(**packet, logger=logger)
                        except SymbolNotFound:
                            logger.info(f'Could not resolve asset from data {packet}')
                            continue
                        
                        if asset is None:
                            logger.info(f'No asset found from data {packet}')
                            continue
                        
                        asset = cast(Asset, asset)
                        pos = create_position(self, packet, asset)
                        if not pos:
                            logger.info(f'Could not create position from data {asset}, {packet}')
                            continue
                        
                        if pos.product_type == ORDER_TYPES.ProductType.INTRADAY:
                            if asset in intraday:
                                intraday[asset].add_to_position(pos)
                            else:
                                intraday[asset] = pos
                        if asset in positions:
                            positions[asset].add_to_position(pos)
                        else:
                            positions[asset] = pos
        except TerminationError:
            raise
        except AuthenticationError:
            raise
        except Exception as e:
            msg = f'failed to fetch current positions:{str(e)}'
            raise APIError(msg)
        
        return positions, intraday
    
    @property
    @ensure_connected(timeout=30, max_retry=1, raise_exception=False)
    def account(self):
        self._fetch_accounts()
        self._fetch_open_positions()
        acc = list(self._accounts.values())[0]
        acc.update_account(acc.cash, acc.margin, self._open_positions)
        return acc.to_dict()
    
    @property
    @ensure_connected(timeout=30, max_retry=1, raise_exception=False)
    def positions(self):
        self._fetch_open_positions()
        return self._open_positions.copy()
    
    @property
    @ensure_connected(max_retry=1, raise_exception=False)
    def intraday_positions(self):
        self._fetch_open_positions()
        return self._open_intraday_positions.copy()
    
    @ensure_connected(timeout=30, max_retry=1, raise_exception=False)
    def position_by_asset(self, asset, intraday=False, *args, **kwargs):
        """
            Fetch positions by an asset.

            Args:
                ``asset (str)``: asset.

            Returns:
                position object - Positions for an particular asset
        """
        try:
            self.symbol(asset.exchange_ticker)
        except SymbolNotFound:
            return
        
        self._fetch_open_positions()
        if intraday:
            if asset in self._open_intraday_positions:
                return self._open_intraday_positions[asset]
        else:
            if asset in self._open_positions:
                return self._open_positions[asset]
    
    @property
    @ensure_connected(timeout=30, max_retry=1, raise_exception=False)
    def open_orders(self):
        return self._open_orders.copy()
    
    @property
    @ensure_connected(timeout=30, max_retry=1, raise_exception=False)
    def orders(self):
        """ all orders. """
        orders = {**self._open_orders, **self._closed_orders}
        if not orders:
            self.fetch_all_orders()

        return {**self._open_orders, **self._closed_orders}
    
    def fetch_all_orders(self, *args, **kwargs):
        if self._data_portal_mode:
            return {}
        
        logger=kwargs.get('logger') or self.logger
        current = time.time()
        if self._last_order_fetch:
            elapsed = current - self._last_order_fetch
            if elapsed < self._config.api.api_delay_protection:
                return {**self._open_orders, **self._closed_orders}
            
        self._last_order_fetch = current
        err = None
        for i in range(3):
            try:
                self._fetch_orders(logger=logger)
                err = None
                break
            except (APIError, ServerError) as e:
                err = e
                time.sleep(1+i*2)
                
        if err:
            raise err
            
        return {**self._open_orders, **self._closed_orders}
    
    @ensure_connected(timeout=30, max_retry=1, raise_exception=False)
    def get_order(self, order_id, *args, **kwargs) -> Order|None:
        """
            Get order by order ID

            Args:
                ``order_id (str)``: Order ID.

            Returns:
                order - Order object by ID
        """
        if order_id in self._open_orders:
            return self._open_orders[order_id]
        elif order_id in self._closed_orders:
            return self._closed_orders[order_id]
        else:
            if not self._implements_get_order:
                self.fetch_all_orders()
                if order_id in self._open_orders:
                    return self._open_orders[order_id]
                elif order_id in self._closed_orders:
                    return self._closed_orders[order_id]
                else:
                    return

            logger = kwargs.pop('logger', None) or self.logger
            data = self._rest_api.request_get_order_by_id(
                    order_id, logger=logger, **kwargs)
            try:
                if data:
                    asset = self.infer_asset(**data)
                    asset = cast(Asset, asset)
                    o = create_order(self, data, asset)
                    if o:
                        if o.is_open and o.oid not in self._open_orders:
                            self._open_orders[o.oid] = o
                        elif o.oid not in self._closed_orders:
                                self._closed_orders[o.oid] = o
                        return o
            except TerminationError:
                raise
            except AuthenticationError:
                raise
            except NotImplementedError:
                self._implements_get_order = False
                self.fetch_all_orders()

                if order_id in self._open_orders:
                    return self._open_orders[order_id]
                elif order_id in self._closed_orders:
                    return self._closed_orders[order_id]
                else:
                    return
            except Exception:
                return
    
    @ensure_connected(timeout=30, max_retry=1, raise_exception=False)
    def open_orders_by_asset(self, asset, *args, **kwargs):
        """
            Get all open orders for a given asset.

            Args:
                ``asset (object)``: Asset object

            Returns:
                List of open orders for the asset.
        """
        return [self._open_orders[oid] for oid in self._open_orders \
                    if self._open_orders[oid].asset == asset]
        
    def is_risk_reducing(self, order):
        if order.asset not in self._open_positions:
            return False
        
        pos = self._open_positions[order.asset]
        side = 1 if order.side == OrderSide.BUY else -1
        exposure = pos.quantity
        order_exposure = order.quantity*side
        
        if abs(exposure) > abs(exposure + order_exposure):
            return True
        
        return False
    
    def get_trading_margins(self, orders, positions, **kwargs):
        if self._data_portal_mode:
            raise BrokerError(f'margin calculation not enabled.')
        
        if self._skip_margin_api:
            return False, np.nan
            
        if isinstance(orders, dict):
            # it is an ID keyed dict, get the values
            orders = list(orders.values())
            
        if not listlike(orders):
            orders = [orders]
            
        if isinstance(positions, dict):
            # it is an ID keyed dict, get the values
            positions = list(positions.values())
            
        if not listlike(positions):
            positions = [positions]
            
        orders = cast(list[Order], orders)
        positions = cast(list[Position], positions)
        try:
            return self._rest_api.request_get_trading_margins(
                    orders, positions, **kwargs)
        except NotImplementedError:
            self._skip_margin_api = True
            msg = f'Failed to fetch trading margins for orders {orders}, '
            msg += f'and positions {positions} - not implemented (will skip next time).'
            self.logger.info(msg)
            return False, np.nan
        except Exception as e:
            msg = f'Failed to fetch trading margins for orders {orders}, '
            msg += f'and positions {positions}: {str(e)}'
            self.logger.info(msg)
            return False, np.nan
    
    def get_trading_costs(self, order, **kwargs):
        if self._data_portal_mode:
            raise BrokerError(f'trading charges calculation not enabled.')
        
        if self._skip_charges_api:
            return 0, 0
            
        try:
            return self._rest_api.request_get_charge(order, **kwargs)
        except NotImplementedError:
            self._skip_charges_api = True
            msg = f'Failed to fetch trading costs for order {order}, '
            msg += f'returning 0 costs instead - not implemented (will skip next time).'
            self.logger.info(msg)
            return 0, 0
        except Exception as e:
            msg = f'Failed to fetch trading costs for order {order}, '
            msg += f'returning 0 costs instead:{str(e)}'
            self.logger.info(msg)
            return 0, 0
        
    @ensure_connected(timeout=30, max_retry=1, raise_exception=False)
    def place_order(self, order, **kwargs):
        """
            Place an order for a particular broker

            Args:
                ``order (object)``: Order object

            Returns:
                Str, the order ID.
        """
        if self._data_portal_mode:
            raise BrokerError(f'order placement is disabled.')
            
        if not isinstance(order, Order):
            raise ValidationError("not a valid order")
            
        if self._intraday_reject:
            if order.product_type == ORDER_TYPES.ProductType.INTRADAY:
                reason = f'intraday orders not accpeted after cut-off time {self._intraday_cutoff_time}.'
                raise BrokerError(reason)
            
            is_rolling = order.asset.is_rolling()
            if is_rolling and not self.is_risk_reducing(order):
                reason = f'Rolling asset orders not accepted after cut-off time {self._intraday_cutoff_time}.'
                raise BrokerError(reason)

        order_type = order.order_type
        # NOTE: no support for stop or stop-limit orders at present
        if order_type not in [ORDER_TYPES.OrderType.LIMIT,ORDER_TYPES.OrderType.MARKET]:
            raise ValidationError(f"not a valid order type {order_type}")

        asset = self.symbol(order.asset.exchange_ticker)
        symbol = asset.symbol
        if not asset:
            raise ValidationError("not a valid asset {symbol}")

        if asset and not asset.can_trade:
            raise TradingBlockError(f"trading blocked for:{symbol}")
        
        order.update_from_dict({'asset':asset}) # capture the correct exchange

        if order_type in [ORDER_TYPES.OrderType.LIMIT] and not order.price:
            raise ValidationError(f"limit price not defined for order.")
        
        try:
            q = order.quantity/order.asset.mult
            if not self._config.broker.options.fractional_trading:
                if q != int(q):
                    msg = f'quantity must be multiple of lot size {order.asset.mult}, '
                    msg += f'got {order.quantity}.'
                    raise ValueError()
        except Exception as e:
            raise ValidationError(str(e))
        else:
            qty = order.quantity
            
            if qty == int(qty) or not self._config.broker.options.fractional_trading:
                qty = int(qty)
        
        if abs(qty) == 0:
            msg = f'quantity must be greater than 0'
            raise ValidationError(msg)
            
        side = 'buy' if order.side==ORDER_TYPES.OrderSide.BUY else 'sell'
            
        if not self._config.broker.options.unwind_before_trade:
            return self._place_order(order, **kwargs)
        
        return self._place_order_with_unwind(order, **kwargs)
        
    def _place_order(self, order:Order, **kwargs):
        logger = kwargs.pop('logger', None) or self.logger
        placed_by = order.placed_by
        symbol = order.asset.symbol
        order_side = ORDER_TYPES.OrderSide(order.side).name.lower()
        qty = order.quantity
        
        order = self.rms.pretrade(order)
        
        try:
            if self._config.broker.options.check_host_before_trade:
                try:
                    logger.info(f'running server reacheability check...')
                    self._rest_api.request_get_latency(logger=logger)
                except Exception as e:
                    raise PreTradeCheckError(
                            f'server reachability check failed:{str(e)}')
            
            oid = None
            with self._order_lock:
                oid = self._rest_api.request_place_trade(
                                order, algo_id=placed_by, logger=logger,
                                **kwargs)
                if oid:
                    order.set_order_id(oid)
                    self._open_orders[oid] = order
        except (TerminationError, AuthenticationError, ValidationError, OrderError) as e:
            cls = e.__class__
            msg = f'{order_side} order for {symbol} for {qty} failed: {str(e)}'
            raise cls(msg, handling=e.handling, url=e.url, keep_default=False)
        except Exception as e:
            msg = f'{order_side} order for {symbol} for {qty} failed: {str(e)}'
            raise OrderError(msg)
        else:
            if not oid:
                msg = f'{order_side} order for {symbol} for {qty} failed: got empty response.'
                raise OrderError(msg)
            
            try:
                self.rms.posttrade(order)
            except Exception as e:
                logger.info(f'order post-trade check failed:{str(e)}')
                
            return oid
        
    def _place_order_with_unwind(self, order:Order, **kwargs):
        logger = kwargs.pop('logger', None) or self.logger
        placed_by = order.placed_by
        symbol = order.asset.symbol
        order_side = ORDER_TYPES.OrderSide(order.side).name.lower()
        qty = order.quantity
        order_type = ORDER_TYPES.OrderType(order.order_type).name.lower()
        
        order = self.rms.pretrade(order)
        
        if self._config.broker.options.check_host_before_trade:
            try:
                logger.info(f'running server reacheability check...')
                self._rest_api.request_get_latency(logger=logger)
            except Exception as e:
                raise PreTradeCheckError(
                        f'server reachability check failed:{str(e)}')
        
        # check exiting position and unwind
        # check_host_before_trade not required as we do this API call
        current_pos = self.positions.get(order.asset,None)
        unwind = None
        
        if current_pos and order_type == 'market':
            current_pos = current_pos.quantity
            pos_side = 'buy' if current_pos > 0 else 'sell'
            if pos_side != order_side and abs(current_pos) < qty:
                qty = qty - abs(current_pos)
                try:
                    unwind = self._rest_api.request_unwind_by_asset(
                            order.asset)
                except (TerminationError, AuthenticationError, ValidationError, OrderError) as e:
                    cls = e.__class__
                    msg = f'unwind beforw {order_side} order for {symbol} for {qty} '
                    msg += f'failed: {str(e)}'
                    raise cls(msg, handling=e.handling, url=e.url, keep_default=False)
                except Exception as e:
                    msg = f'unwind beforw {order_side} order for {symbol} for {qty} '
                    msg += f'failed: {str(e)}'
                    raise OrderError(msg)
                else:
                    if qty == 0:
                        return unwind
        
        try:
            oid = None
            with self._order_lock:
                oid = self._rest_api.request_place_trade(
                        order, algo_id=placed_by, logger=logger)
                if oid:
                    order.set_order_id(oid)
                    self._open_orders[oid] = order
        except (TerminationError, AuthenticationError, ValidationError, OrderError) as e:
            cls = e.__class__
            msg = f'{order_side} order for {symbol} for {qty} failed: {str(e)}'
            raise cls(msg, handling=e.handling, url=e.url, keep_default=False)
        except Exception as e:
            msg = f'{order_side} order for {symbol} for {qty} failed: {str(e)}'
            if unwind:
                logger.error(msg)
                return unwind
            else:
                raise OrderError(msg)
        else:
            if not oid:
                msg = f'{order_side} order for {symbol} for {qty} failed: got empty response.'
                raise OrderError(msg)
                
            try:
                self.rms.posttrade(order)
            except Exception as e:
                logger.info(f'order post-trade check failed:{str(e)}')
                
            if unwind:
                return [unwind, oid]
            else:
                return oid

    @ensure_connected(timeout=30, max_retry=1, raise_exception=False)
    def update_order(self, order_param, *args, **kwargs):
        """
            Update order quantity or limit price.

            Args:
                ``order_param (object)``: Order update parametres or Order object

            Returns:
                Str, order ID.
        """
        if self._data_portal_mode:
            raise BrokerError(f'order update is disabled.')
        
        if not self._implements_update_order:
            raise OrderError(f'order modification is not implemented')
            
        if isinstance(order_param, Order):
            order_id = order_param.oid
        else:
            order_id = order_param
            
        logger = kwargs.pop('logger', None) or self.logger
        o = self.get_order(order_id, logger=logger)

        if o is None:
            raise OrderNotFound(f'no order found for order id {order_id}')
        
        if not o.is_open():
            msg = f'order {order_id} is not open.'
            raise OrderAlreadyProcessed(msg)

        order_id = o.oid
        quantity = price = disclosed = None

        if "quantity" in kwargs:
            quantity = abs(int(kwargs.pop("quantity"))) # no fractional
        
        if "price" in kwargs:
            price = float(kwargs.pop("price"))
            if price == 0:price = None
        
        if "disclosed" in kwargs:
            disclosed = int(kwargs.pop("disclosed"))
            if disclosed == 0:disclosed = None
            
        if "order_type" in kwargs:
            # TODO: support order type change and trigger pretrade
            raise OrderError(f'Cannot change order type in order modification.')
        
        try:
            return self._rest_api.request_update_order(
                    o, quantity=quantity, price=price, 
                    disclosed=disclosed, logger=logger)
        except (TerminationError, OrderAlreadyProcessed, AuthenticationError, OrderError) as e:
            cls = e.__class__
            msg = f'update order failed for {order_id}:{str(e)}.'
            raise cls(msg, handling=e.handling, url=e.url, keep_default=False)
        except NotImplementedError:
            self._implements_update_order = False
            raise OrderError(f'order modification is not implemented')
        except Exception as e:
            msg = f'update order failed for {order_id}:{str(e)}.'
            raise OrderError(msg)

    @ensure_connected(timeout=30, max_retry=1, raise_exception=False)
    def cancel_order(self, order_param, *args, **kwargs):
        """
            Cancel an order.

            Args:
                ``order_param (object)``: Request parametres or Order object

            Returns: 
                None.
        """
        if self._data_portal_mode:
            raise BrokerError(f'order cancel is disabled.')
            
        if isinstance(order_param, Order):
            order_id = order_param.oid
        else:
            order_id = order_param

        logger = kwargs.pop('logger', None) or self.logger
        o = self.get_order(order_id, logger=logger)
        
        if o is None:
            raise OrderNotFound(f'no order found for order id {order_id}')
        
        if not o.is_open():
            msg = f'order {order_id} is not open.'
            raise OrderAlreadyProcessed(msg)
        
        try:
            self._rest_api.request_cancel_order(o, logger=logger)
        except (TerminationError, OrderAlreadyProcessed, AuthenticationError, OrderError) as e:
            cls = e.__class__
            msg = f'cancel order for {order_id} failed:{str(e)}.'
            raise cls(msg, handling=e.handling, url=e.url, keep_default=False)
        except Exception as e:
            msg = f'cancel order for {order_id} failed:{str(e)}.'
            raise OrderError(msg)
            
        return order_id
    
    @ensure_connected(max_retry=1, raise_exception=False)
    def get_account(self, *args, **kwargs):
        """ Returns the underlying ``Account`` object. """
        return list(self._accounts.values())[0]
    