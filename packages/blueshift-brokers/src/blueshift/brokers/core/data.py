from __future__ import annotations
from typing import TYPE_CHECKING, cast, Any
import threading
import time 
import warnings
from functools import lru_cache
import math
import numpy as np

from blueshift.config import LOCK_TIMEOUT
from blueshift.interfaces.data.data_portal import DataPortal, SubscriptionLevel, MarketDepth
from blueshift.interfaces.trading.broker import IBroker
from blueshift.interfaces.assets._assets import Asset, MarketData, Option, Futures
from blueshift.lib.trades._order_types import GREEK
from blueshift.calendar.date_utils import get_aligned_timestamp
from blueshift.calendar.date_utils import combine_date_time
from blueshift.lib.common.functions import listlike
from blueshift.lib.common.constants import (
        Frequency, OHLCVX_COLUMNS, GREEK_COLUMNS, ANNUALIZATION_FACTOR)
from blueshift.lib.models.vols import interpolate_atmfv, bs_implied_volv, bs_greekv
from blueshift.lib.common.ctx_mgrs import TimeoutRLock
from blueshift.lib.exceptions import (
        ValidationError, DataAPIError, PreTradeCheckError, AuthenticationError, 
        TerminationError, ServerError, APIError, BlueshiftWarning, ExceptionHandling, 
        InitializationError)

from .config.config import APIBrokerConfig
from .utils import make_blank_data, make_missing_data
from blueshift.config import BLUESHIFT_DEBUG_MODE

if TYPE_CHECKING:
    import pandas as pd
    from numpy.typing import NDArray
    from blueshift.interfaces.logger import BlueshiftLogger
    from blueshift.interfaces.data.store import DataStore
    from .broker import RestAPIBroker
    from .api import BrokerAPIMixin
    from .assets import RestAPIBrokerAssetFinder
else:
    import blueshift.lib.common.lazy_pandas as pd

class RestAPIBrokerDataPortal(DataPortal):
    '''
        Data portal implemented in memory
    '''
    _DATA_CACHE_INTERVAL='15s'
    
    def __init__(self, config:APIBrokerConfig, broker:RestAPIBroker, *args, **kwargs):
        if not isinstance(broker, IBroker):
            msg = f'expected a broker object, got {broker} of type {type(broker)}'
            raise ValidationError(msg)
            
        if not isinstance(config, APIBrokerConfig):
            msg = f'expected a config object, got {config} of type {type(config)}'
            raise ValidationError(msg)
        
        if broker.asset_finder is None or broker.store is None:
            raise ValidationError(f'a valid asset finder and data store is required.')
            
        self.broker = broker
        self._asset_finder:RestAPIBrokerAssetFinder = broker.asset_finder
        self._store:DataStore = broker.store
        self.config = config
        self._market_depths:dict[MarketData, MarketDepth] = {}
        self._quote_lock = TimeoutRLock(timeout=LOCK_TIMEOUT)
    
    @property
    def tz(self):
        return self.broker.tz
    
    def has_streaming(self):
        """ if streaming data supported. """
        return self.broker.has_streaming()
    
    def getfx(self, ccy_pair, dt):
        """ get currency conversion rate. """
        return self.broker.getfx(ccy_pair, dt)
    
    def current(self, assets:MarketData|list[MarketData], columns:str|list[str], do_subscribe=True, 
                check_stale=False, logger=None, api=None, **kwargs):
        logger = logger or self.broker.logger
        api = api or self.broker._rest_api
        
        cols = [columns] if not listlike(columns) else columns
        securities = [assets] if not listlike(assets) else assets
        cols = cast(list[str], cols)
        securities = cast(list[MarketData], securities)
            
        if len(securities) > self.config.broker.options.max_tickers:
            msg = f'requested number of assets exceeds max allowed {self.config.broker.options.max_tickers}'
            raise ValidationError(msg)
            
        if all([c not in GREEK_COLUMNS for c in cols]):
            # return from the internal method
            return self._current(
                    assets, columns, do_subscribe=do_subscribe, 
                    check_stale=check_stale, logger=logger, api=api, **kwargs)
        
        ohlc_cols = [c.lower() for c in cols if c.lower() in OHLCVX_COLUMNS]
        if ohlc_cols:
            # we cannot have mixed column types, either has to be all 
            # ohlcv or all greeks
            msg = f'OHLCV and GREEK columns cannot be used together. '
            msg += f'Use separate history calls.'
            raise ValidationError(msg)
        
        # we have only greeks columns at this point
        # assets must be all options if we are here
        if any([not s.is_opt() for s in securities]):
            msg = f'asset(s) must be option type for column(s) {columns}.'
            raise ValidationError(msg)
        
        opts = cast(Option|list[Option], assets)

        return self._current_greek(
                opts, columns, do_subscribe=do_subscribe, 
                check_stale=check_stale, logger=logger, api=api, **kwargs)
        
    def history(self, assets:MarketData|list[MarketData], columns:str|list[str], nbars, frequency, check_stale=False, 
                logger=None, api=None, **kwargs):
        logger = logger or self.broker.logger
        api = api or self.broker._rest_api
        
        if nbars > self.config.broker.options.max_nbars:
            msg = f'requested number of bars exceeds max allowed {self.config.broker.options.max_nbars}'
            warnings.warn(msg, BlueshiftWarning)
            nbars = self.config.broker.options.max_nbars
            
        freq = Frequency(frequency)
            
        securities = [assets] if not listlike(assets) else assets
        securities = cast(list[MarketData], securities)
        if len(securities) > self.config.broker.options.max_tickers:
            msg = f'requested number of assets exceeds max allowed {self.config.broker.options.max_tickers}'
            raise ValidationError(msg)
            
        cols = [columns] if not listlike(columns) else columns
        cols = cast(list[str], cols)
        if all([c not in GREEK_COLUMNS for c in cols]):
            # return from the internal method
            return self._history(assets, columns, nbars, freq, 
                                 check_stale=check_stale, logger=logger,
                                 api=api, **kwargs)
        
        ohlc_cols = [c.lower() for c in cols if c.lower() in OHLCVX_COLUMNS]
        if ohlc_cols:
            # we cannot have mixed column types, either has to be all 
            # ohlcv or all greeks
            msg = f'OHLCV and GREEK columns cannot be used together. '
            msg += f'Use separate history calls.'
            raise ValidationError(msg)
        
        # we have only greeks columns at this point
        # assets must be all options if we are here
        if any([not s.is_opt() for s in securities]):
            msg = f'Assets must be option type for column(s) {columns}.'
            raise ValidationError(msg)
        
        opts = cast(Option|list[Option], assets)
        return self._history_greek(opts, columns, nbars, freq, 
                                   check_stale=check_stale, logger=logger,
                                   api=api, **kwargs)
        
    def pre_trade_details(self, asset:Asset, logger:BlueshiftLogger|None=None, 
                          api:BrokerAPIMixin|None=None, **kwargs) -> tuple[float, float, float]:
        logger = logger or self.broker.logger
        api = api or self.broker._rest_api
        
        try:
            # Try to fetch quotes if it supported
            market_depth:MarketDepth = cast(MarketDepth, self.quote(asset))

            if len(market_depth.market_depth.bids) > 0 and len(market_depth.market_depth.asks) > 0:
                mid = 0.5*(
                    market_depth.market_depth.bids[0].price + market_depth.market_depth.asks[0].price)
            else:
                mid = math.nan

            upper_circuit = market_depth.upper_circuit
            lower_circuit = market_depth.lower_circuit
            upper_circuit = upper_circuit if not math.isnan(upper_circuit) else asset.upper_circuit
            lower_circuit = lower_circuit if not math.isnan(lower_circuit) else asset.lower_circuit

            if not math.isnan(mid):
                return mid, upper_circuit, lower_circuit
            else:
                px = self.current(asset, 'close', **kwargs)
                return cast(float, px), math.nan, math.nan
        except NotImplementedError:
            px = self.current(asset, 'close', **kwargs)
            return cast(float, px), math.nan, math.nan
        except Exception as e:
            msg = f'failed to fetch pre-trade details for {asset}:{str(e)}'
            raise PreTradeCheckError(msg)
        
    def update_market_quotes(self, asset:Asset, data:dict[str, Any]):
        with self._quote_lock:
            if asset in self._market_depths:
                self._market_depths[asset].update_from_dict(data)
            else:
                self._market_depths[asset] = MarketDepth.from_dict(data)
        
    def quote(self, asset:Asset, column:str|None=None, logger:BlueshiftLogger|None=None, 
              api:BrokerAPIMixin|None=None, **kwargs):
        logger = logger or self.broker.logger
        
        if not isinstance(asset, Asset):
            msg = f'Trade quotation not available for {asset}.'
            raise ValidationError(msg)
            
        if column and column not in ('bid','ask','market_depth'):
            msg = f'Column, if specified, must be either bid or ask.'
            raise ValidationError(msg)
            
        asset = cast(Asset, self.broker.to_base_asset(asset))
        market_depth:MarketDepth|None = None
        
        # shortcut for best bid/ best offer
        if asset in self._market_depths:
            market_depth = self._market_depths[asset]
            t = market_depth.timestamp
            now = pd.Timestamp.now(tz=self.broker.tz)
            
            stale:bool = False
            if pd.isna(t):
                stale = True
            else:
                tolerance = self.config.broker.options.stale_data_tolerance*60
                stale = (now-t).total_seconds() > tolerance

            if stale:
                market_depth = None

        if market_depth is None:
            api = api or self.broker._rest_api
            try:
                quotes = api.request_get_quote(asset, logger=logger,** kwargs)
                self.update_market_quotes(asset, quotes)
                market_depth = self._market_depths.get(asset)
                if market_depth is None:
                    raise ValueError(f'missing quotes data for asset {asset}')
            except Exception as e:
                raise DataAPIError(f'failed to fetch quotes for {asset}:{str(e)}')
            
        try:
            with self._quote_lock:
                self._market_depths[asset] = market_depth
        except Exception as e:
            msg = f'failed to process quote for asset {asset}:{str(e)}'
            DataAPIError(msg)
        else:
            try:
                self.broker.subscribe(asset, level=SubscriptionLevel.QUOTE)
            except Exception as e:
                msg = f'failed to subscirbe to quote for asset {asset}:{str(e)}'
                self.broker.logger.error(msg)

        if column:
            if column == 'market_depth':
                return market_depth.market_depth
            elif column == 'bid':
                return market_depth.market_depth.bids
            else:
                return market_depth.market_depth.asks
        else:
            return market_depth
    
    def get_streaming_subscribe_payload(self, broker:RestAPIBroker, channel:str, assets, **kwargs):
        conn = broker.get_connection_for_channel(channel)
        if not conn or not conn.subscribe or 'subscribe' not in conn.subscribe:
             raise NotImplementedError(f"No subscribe payload definitions for channel {channel}")
        
        msg_defs = conn.subscribe['subscribe']
        if not msg_defs:
            raise NotImplementedError(f"No subscribe payload definitions found for channel {channel}")
        
        msg_def = msg_defs.get(channel)
        if not msg_def:
            msg_def = msg_defs.get('all')
        if not msg_def:
            raise NotImplementedError(f"No subscribe payload definition for channel {channel}")
            
        return msg_def.resolve(subscribe_assets=assets, **kwargs)
    
    def get_streaming_unsubscribe_payload(self, broker, channel, assets, level=None, **kwargs):
        conn = broker.get_connection_for_channel(channel)
        if not conn or not conn.subscribe or 'unsubscribe' not in conn.subscribe:
             raise NotImplementedError(f"No unsubscribe payload definitions for channel {channel}")
        
        msg_defs = conn.subscribe['unsubscribe']
        if not msg_defs:
            raise NotImplementedError(f"No unsubscribe payload definitions found for channel {channel}")
        
        msg_def = msg_defs.get(channel)
        if not msg_def:
            msg_def = msg_defs.get('all')
        if not msg_def:
            raise NotImplementedError(f"No subscribe payload definition for channel {channel}")
            
        return msg_def.resolve(subscribe_assets=assets, **kwargs)
    
    def subscribe(self, assets:MarketData|list[MarketData], level=SubscriptionLevel.TRADE, 
                  logger:BlueshiftLogger|None=None, **kwargs):
        logger = logger or self.broker.logger
        channel = 'quote' if level==SubscriptionLevel.QUOTE else 'data'
        
        if not self.broker.ingestor:
            msg = f'cannot subscribe, no ingestor defined'
            logger.warning(msg)
            return
        
        if not listlike(assets):
            assets = [assets] # type: ignore
            
        assets = cast(list[MarketData], assets)
        if not self.config.broker.options.multi_assets_data_subscribe:
            for asset in assets:
                try:
                    payload = self.get_streaming_subscribe_payload(
                            self.broker, channel, asset, **kwargs)
                    
                    if level==SubscriptionLevel.TRADE:
                        # track asset only in channel 1
                        self.broker.ingestor.subscribe(payload, asset)
                    else:
                        self.broker.ingestor.subscribe(payload)
                except Exception as e:
                    msg = f'Failed to subscribe to data feed for asset {asset}:{str(e)}.'
                    logger.warning(msg)
        else:
            payload = self.get_streaming_subscribe_payload(
                    self.broker, 'data', assets, level=level, **kwargs)
            
            if level==SubscriptionLevel.TRADE:
                self.broker.ingestor.subscribe(payload, assets)
            else:
                self.broker.ingestor.subscribe(payload)
    
    def unsubscribe(self, assets:MarketData|list[MarketData], level=SubscriptionLevel.TRADE, 
                    logger:BlueshiftLogger|None=None, **kwargs):
        logger = logger or self.broker.logger
        
        if not self.broker.ingestor:
            msg = f'cannot unsubscribe, no ingestor defined'
            logger.warning(msg)
            return
        
        if not listlike(assets):
            assets = [assets] # type: ignore

        channel = 'quote' if level==SubscriptionLevel.QUOTE else 'data'
            
        assets = cast(list[MarketData], assets)
        for asset in assets:
            try:
                # ingestor remove trigger unsubscribe command to 
                # the streaming source automatically
                self.broker.ingestor.remove(asset)
                
                if level !=SubscriptionLevel.TRADE:
                    payload = self.get_streaming_unsubscribe_payload(
                        self.broker, channel, asset, **kwargs)
                    self.broker.ingestor.subscribe(payload)
            except Exception as e:
                msg = f'Failed to unsubscribe from data feed for asset {asset}:{str(e)}.'
                logger.warning(msg)
    
    def _current(self, assets:MarketData|list[MarketData], fields:str|list[str], do_subscribe:bool, 
                 check_stale:bool, logger:BlueshiftLogger, api:BrokerAPIMixin, **kwargs):
        """ internal current method for non-computable columns """
        freq = Frequency("1m")
        cols = [fields] if not listlike(fields) else fields
        securities = [assets] if not listlike(assets) else assets
        cols = cast(list[str], cols)
        securities = cast(list[MarketData], securities)
        data = self.get_bars(securities, cols, 1, freq, logger=logger, api=api, **kwargs)
        
        # now subscribe for streaming updates, only for minute-freq
        if do_subscribe and self.broker.has_streaming() and self.broker.ingestor:
            assets_to_subscribe = [asset for asset in securities if \
                                 not self.broker.ingestor.is_subscribed(asset)]
        else:
            assets_to_subscribe=[]

        if assets_to_subscribe:
            self.broker.subscribe(assets_to_subscribe, level=SubscriptionLevel.TRADE)
                
        if check_stale and all([self.check_stale_data(v, logger=logger) for k,v in data.items()]):
            error = APIError(f'Got stale data from data query:{data}')
            error.handling = ExceptionHandling.WARN
            raise error
        
        try:
            if not listlike(assets):
                assets = cast(Asset, assets)
                if not listlike(fields):
                    return cast(float,data[assets].iloc[-1,0])
                return data[assets].iloc[-1]
            else:
                df = pd.concat(data)
                if isinstance(df.index, pd.MultiIndex):
                    return df.groupby(level=0).last()
                else:
                    return df.iloc[-1]
        except Exception as e:
            raise DataAPIError(
                    f'something went wrong with data query:{str(e)}')
        
    def _current_greek(self, assets:Option|list[Option], fields:str|list[str], do_subscribe:bool, 
                 check_stale:bool, logger:BlueshiftLogger, api:BrokerAPIMixin, **kwargs):
        freq = Frequency("1m")
        cols = [fields] if not listlike(fields) else fields
        securities = [assets] if not listlike(assets) else assets
        cols = cast(list[str], cols)
        securities = cast(list[Option], securities)
        
        data = self.get_greeks(securities, cols, 1, freq, logger=logger, api=api, **kwargs)

        if do_subscribe and self.broker.has_streaming() and self.broker.ingestor:
            assets_to_subscribe = [asset for asset in securities if \
                                 not self.broker.ingestor.is_subscribed(asset)]
        else:
            assets_to_subscribe=[]

        if assets_to_subscribe:
            self.broker.subscribe(assets_to_subscribe, level=SubscriptionLevel.TRADE)
        
        if check_stale and all([self.check_stale_data(v, logger=logger) for k,v in data.items()]):
            error = APIError(f'Got stale data from data query:{data}')
            error.handling = ExceptionHandling.WARN
            raise error
            
        try:
            if not listlike(assets):
                assets = cast(Option, assets)
                if not listlike(fields):
                    return cast(float,data[assets].iloc[-1,0])
                return data[assets].iloc[-1]
            else:
                df = pd.concat(data)
                if isinstance(df.index, pd.MultiIndex):
                    return df.groupby(level=0).last()
                else:
                    return df.iloc[-1]
        except Exception as e:
            if not listlike(assets) and not listlike(fields):
                return np.nan
            raise DataAPIError(f'something went wrong with greeks computation:{str(e)}')

    def _history(self, assets:MarketData|list[MarketData], fields:str|list[str], nbars:int, 
                 frequency:Frequency, check_stale:bool, logger:BlueshiftLogger, api:BrokerAPIMixin, 
                 **kwargs):
        securities = [assets] if not listlike(assets) else assets
        cols = [fields] if not listlike(fields) else fields
        securities = cast(list[MarketData], securities)
        cols = cast(list[str], cols)
        securities = [self.broker.to_base_asset(sec) for sec in securities]
            
        # subscribe for intraday requests
        if_subscribe = True if frequency < Frequency('1d') else False
        
        # first call the data, if we need to hit the API
        data = self.get_bars(
                securities, cols, nbars, frequency, logger=logger, 
                api=api, **kwargs)
        
        # now subscribe for streaming updates
        if if_subscribe and self.broker.has_streaming() and self.broker.ingestor:
            assets_to_subscribe = [asset for asset in securities if \
                                    not self.broker.ingestor.is_subscribed(asset)]
        else:
            assets_to_subscribe = []
            
        if assets_to_subscribe:
            for asset in assets_to_subscribe:
                self.subscribe(asset, level=SubscriptionLevel.TRADE)
        
        try:
            if not listlike(assets):
                if not listlike(fields):
                    return data[cast(MarketData, assets)].iloc[:,0]
                return data[cast(MarketData, assets)]
            else:
                if not listlike(fields):
                    data = {k:v.iloc[:,0] for k,v in data.items()}
                    return pd.concat(data, axis=1)
                return pd.concat(data)
        except Exception as e:
            raise DataAPIError(
                    f'something went wrong with data query:{str(e)}')
        
    def _history_greek(self, assets:Option|list[Option], fields:str|list[str], nbars:int, 
                       frequency:Frequency, check_stale:bool, logger:BlueshiftLogger, 
                       api:BrokerAPIMixin, **kwargs):
        securities = [assets] if not listlike(assets) else assets
        cols = [fields] if not listlike(fields) else fields
        securities = cast(list[Option], securities)
        cols = cast(list[str], cols)
        securities = [self.broker.to_base_asset(sec) for sec in securities]
        securities = cast(list[Option], securities)
        
        data = self.get_greeks(
                securities, cols, nbars, frequency, logger=logger, 
                api=api, **kwargs)
        
        try:
            if not listlike(assets):
                assets = cast(Option, assets)
                if not listlike(fields):
                    return data[assets].iloc[:,0]
                return data[assets]
            else:
                if not listlike(fields):
                    data = {k:v.iloc[:,0] for k,v in data.items()}
                    return pd.concat(data, axis=1)
                return pd.concat(data)
        except Exception as e:
            raise DataAPIError(
                    f'something went wrong with greeks computation:{str(e)}')
        
    def check_stale_data(self, data:pd.DataFrame, logger:BlueshiftLogger) -> bool:
        if data is None:
            return True
        if data.empty:
            return True
        
        try:
            if not isinstance(data.index, pd.DatetimeIndex):
                return True
            
            last_dt = data.index[-1]
            dt = pd.Timestamp.now(tz=self.broker.tz)
            tolerance = max(1, int(self.config.broker.options.stale_data_tolerance))
            expected_idx = self.broker.calendar.last_n_minutes(dt, tolerance)[0]
            if expected_idx > last_dt:
                return True
            return False
        except Exception as e:
            logger.error(f'Failed checking data sanity for data:{data}')
            logger.error(f'Sanity check failied with error:{str(e)}')
            return True

    def get_greeks(self, assets:list[Option], fields:list[str], nbars:int, frequency:Frequency, 
                   logger:BlueshiftLogger, api:BrokerAPIMixin, **kwargs):
        for f in fields:
            if f.lower() not in GREEK_COLUMNS:
                raise ValidationError(f"illegal column {f}.")
        
        cache:dict[Option,pd.DataFrame] = {}
        for asset in assets:
            try:
                cache[asset] = self._get_greeks(
                        asset, fields, nbars, frequency, logger=logger,
                        api=api, **kwargs)
            except AuthenticationError:
                raise
            except Exception as e:
                logger.error(f'error fetching data for asset {asset}: {str(e)}')
                cache[asset] = make_blank_data(fields, self.broker.tz)
            
        return cache
        
    def get_bars(self, assets:list[MarketData], fields:list[str], nbars:int, frequency:Frequency, 
                 logger:BlueshiftLogger, api:BrokerAPIMixin, **kwargs):
        for f in fields:
            if f.lower() not in OHLCVX_COLUMNS:
                raise ValidationError(f"illegal column {f}.")
                
        if self.config.broker.options.multi_assets_data_query:
            try:
                return self._get_bars_multi(
                        assets, fields, nbars, frequency, logger=logger,
                        api=api, **kwargs)
            except (TerminationError, AuthenticationError):
                raise
            except Exception as e:
                msg = f'error fetching data for {assets}: {str(e)}'
                raise DataAPIError(msg)
    
        try:
            cache:dict[MarketData, pd.DataFrame] = {}
            for asset in assets:
                cache[asset] = self._get_bars_single(
                        asset, fields, nbars, frequency, logger=logger,
                        api=api, **kwargs)
                
            return cache
        except (TerminationError, AuthenticationError):
            raise
        except Exception as e:
            msg = f'error fetching data for {assets}: {str(e)}'
            raise DataAPIError(msg)
        
    def _get_greeks(self, asset:Option, fields:list[str], nbar:int, frequency:Frequency, 
                    logger:BlueshiftLogger, api:BrokerAPIMixin, **kwargs) -> pd.DataFrame:
        def _ensure_same_size(target:pd.Series, origin:pd.Series) -> pd.Series:
            idx = pd.DatetimeIndex(origin.index.union(target.index))
            target = target.reindex(idx, method='bfill').ffill()
            target = target.reindex(origin.index)
            return target.ffill()
        
        valid = asset.is_opt() and asset.can_trade
        
        if not valid:
            return make_blank_data(fields, self.broker.tz)
            
        underlying, futures, expiry, option_type, _ = \
            self._asset_finder.get_occ_option_details(asset)
        
        if 'check_stale' not in kwargs:
            kwargs['check_stale'] = True
        
        underlying = self._asset_finder.symbol(underlying)
        opt_price = self._history(asset,'close', nbar, frequency, logger=logger, 
                                  api=api, **kwargs)
        opt_price = cast(pd.Series, opt_price)
        opt_price.index = pd.DatetimeIndex(opt_price.index) # just in case
        fut_price = self._history(futures,'close', nbar, frequency, logger=logger,
                                  api=api, **kwargs)
        fut_price = _ensure_same_size(cast(pd.Series,fut_price), cast(pd.Series,opt_price))
        fut_price = fut_price.ffill()
        opt_expiry = asset.expiry_date.tz_localize(self.broker.tz).normalize()
        fut_expiry = futures.expiry_date.tz_localize(self.broker.tz).normalize()
        opt_expiry = combine_date_time(opt_expiry, self.broker.calendar.close_time)
        fut_expiry = combine_date_time(fut_expiry, self.broker.calendar.close_time)
        
        if futures.expiry_date != asset.expiry_date:
            # interpolate atmf
            spot_price = self._history(underlying,'close', nbar, frequency, logger=logger, api=api, **kwargs)
            spot_price = cast(pd.Series,spot_price)
            spot_price = _ensure_same_size(spot_price, opt_price)
            T = (fut_expiry - opt_price.index).total_seconds().values/86400 # type: ignore
            t = (opt_expiry - opt_price.index).total_seconds().values/86400 # type: ignore
            atmfs = interpolate_atmfv(
                    fut_price.values, spot_price.values, # type: ignore
                    t.astype('float'), T.astype('float'), 0.05, 2) # type: ignore
            F = pd.Series(atmfs, index=opt_price.index)
        else:
            # atmf is the futures pries
            F = fut_price
        
        K = pd.Series(asset.strike, index=opt_price.index)
        T:pd.Timedelta = opt_expiry - opt_price.index   # type: ignore
        T = cast(pd.Timedelta, T)
        # expiry as fraction of a trading day
        try:
            T = T.total_seconds().values/86400/ANNUALIZATION_FACTOR # type: ignore
            T = T.astype('float') # type: ignore
        except Exception as e:
            msg = f'Error converting expiries to day fraction for {T}: {str(e)}.'
            logger.warning(msg)
            try:
                T = np.array([t.value/1E9/86400/ANNUALIZATION_FACTOR for t in T]) # type: ignore
                T = T.astype('float') # type: ignore
            except Exception as e:
                msg = f'Error converting expiries to day fraction for {T}: {str(e)}.'
                logger.error(msg)
                raise
        
        cols = {}
        for f in fields:
            if f == 'atmf':
                cols[f] = F
            else:
                v = bs_implied_volv(F.values, K.values, opt_price.values, T, option_type) # type: ignore
                if f == 'implied_vol':
                    cols[f] = pd.Series(v, index=opt_price.index)
                else:
                    greek = GREEK[f.upper()]
                    px = bs_greekv(F.values, K.values, v, T, option_type, greek) # type: ignore
                    cols[f] = pd.Series(px, index=opt_price.index)
                    
        return pd.DataFrame(cols)

    def _get_bars_single(self, asset:MarketData, fields:list[str], nbar:int, frequency:Frequency|str, 
                         logger:BlueshiftLogger, api:BrokerAPIMixin, **kwargs):
        if isinstance(asset, Asset) and not asset.can_trade:
            return make_blank_data(fields, self.broker.tz)
        
        add_data = False
        try:
            freq = Frequency(frequency)
        except Exception:
            raise ValidationError(f"illegal frequency {frequency}.")
            
        caching:bool = kwargs.get('caching', True)
        timestamp = get_aligned_timestamp(pd.Timestamp.now(tz=self.broker.tz), freq)
        if caching and self._store.has_data(asset) and freq == Frequency("minute"):
            required_bars = freq.minute_bars*nbar
            available_bars = self._store.size(asset)
            enough_data = required_bars <= available_bars
            last_timestamp = self._store.last_timestamp(asset)

            if last_timestamp:
                last_timestamp = last_timestamp + pd.Timedelta(minutes=1)
            else:
                last_timestamp = pd.NaT
            
            enough_data = enough_data and (timestamp <= last_timestamp)
            if enough_data:
                candles = self._store.read(asset, fields, nbar, frequency=freq)
                return cast(pd.DataFrame,candles)
            else:
                add_data = True
        
        # fetch from API with an interval caching
        signature = pd.Timestamp.now(tz=self.broker.tz).round(self._DATA_CACHE_INTERVAL)
        signature = signature.strftime('%Y-%m-%d %H:%M:%S%Z')
        candles = self._fetch_bars_single(asset, nbar, frequency, signature, 
                                   logger, api, **kwargs)
        
        if len(candles) < 1:
            return make_blank_data(fields, self.broker.tz)
        
        if freq == Frequency('minute'):
            # add data only if missing
            if not self._store.has_data(asset) or add_data:
                if self.broker._ingestor:
                    self.broker._ingestor.add_chunk(asset, candles)
        
        return candles[fields]
    
    def _get_bars_multi(self, assets:list[MarketData], fields:list[str], nbar:int, 
                        frequency:Frequency|str, logger:BlueshiftLogger, api:BrokerAPIMixin, 
                        **kwargs) -> dict[MarketData, pd.DataFrame]:
        try:
            freq = Frequency(frequency)
        except Exception:
            raise ValidationError(f"illegal frequency {frequency}.")
            
        if not listlike(assets):
            assets = [assets] # type: ignore
        
        caching:bool = kwargs.get('caching', True)
        assets = cast(list[MarketData], assets)
        timestamp = get_aligned_timestamp(pd.Timestamp.now(tz=self.broker.tz), freq)
        cache:dict[MarketData, pd.DataFrame] = {}
        fetch_data:set[MarketData] = set()
        
        for asset in assets:
            if caching and self._store.has_data(asset) and freq == Frequency("minute"):
                required_bars = freq.minute_bars*nbar
                available_bars = self._store.size(asset)
                enough_data = required_bars <= available_bars
                last_timestamp = self._store.last_timestamp(asset)
                
                if last_timestamp is not None:
                    last_timestamp = last_timestamp + pd.Timedelta(minutes=1)
                else:
                    last_timestamp = pd.NaT

                enough_data = enough_data and (timestamp <= last_timestamp)
                
                if enough_data:
                    candles = self._store.read(asset, fields, nbar, frequency=freq)
                    cache[asset] = cast(pd.DataFrame,candles)
                else:
                    fetch_data.add(asset)
            else:
                fetch_data.add(asset)
                    
        if not fetch_data:
            # this means add_data is false as well, so we can return from here
            return cache
        
        signature = pd.Timestamp.now(tz=self.broker.tz).round(self._DATA_CACHE_INTERVAL)
        signature = signature.strftime('%Y-%m-%d %H:%M:%S%Z')
        
        missing_data = self._fetch_bars_multi(
                frozenset(fetch_data), nbar, frequency, signature, logger, api, **kwargs)
        
        for asset in fetch_data:
            candles = missing_data.get(asset, make_blank_data(fields, self.broker.tz))
            if freq == Frequency('minute'):
                if not self._store.has_data(asset) or asset in fetch_data:
                    if self.broker.ingestor:
                        self.broker.ingestor.add_chunk(asset, candles)
            
            cache[asset] = candles[fields]
        
        return cache
    
    def _validate_data_from_api(self, df:dict[MarketData, pd.DataFrame]):
        if not isinstance(df, dict) or not [isinstance(df[k], pd.DataFrame) for k in df]:
            raise DataAPIError('failed to fetch data from API: expected a multi-index dataframe')
            
        if any(['close' in df[k] and df[k].close.iloc[-1]==0 for k in df]):
            missing = {k:v.tail() for k,v in df.items()}
            msg = f'Got 0 values for close prices: {missing}'
            raise DataAPIError(msg)
        elif all([len(df[k]) == 0 for k in df]):
            msg = f'No data received from API'
            raise DataAPIError(msg)
        
    def _ensure_index_and_size(self, df:pd.DataFrame, nbars:int, freq:Frequency):
        try:
            df.index = pd.DatetimeIndex(pd.to_datetime(df.index)) # type: ignore
        except Exception as e:
            msg = f'expected datetime index in the returned data.'
            raise DataAPIError(msg)
        
        if df.index.tz: # type: ignore
            df.index = df.index.tz_convert(self.broker.tz) # type: ignore
        else:
            df.index = df.index.tz_localize(self.broker.tz) # type: ignore
        
        if freq >= Frequency('day'):
            df.index = pd.DatetimeIndex(pd.to_datetime(df.index.normalize())) # type: ignore
        else:
            df.index = pd.DatetimeIndex(pd.to_datetime(df.index)) # type: ignore

        return df.iloc[-nbars:]
        
    @lru_cache(maxsize=128)
    def _fetch_bars_single(self, asset:MarketData, nbars:int, frequency:Frequency|str, signature:str, 
                           logger:BlueshiftLogger, api:BrokerAPIMixin, **kwargs) -> pd.DataFrame:
        size = nbars
        last_timestamp = self._store.last_timestamp(asset)
        logger.info(f'{asset}:stale or low data in db ({last_timestamp}), fetching from API.')
        
        freq = Frequency(frequency)
        now = get_aligned_timestamp(pd.Timestamp.now(tz=self.broker.tz),Frequency('1m'))
        to_date = now
        
        if freq < Frequency('1d'):
            if freq < Frequency('10m'):
                nbars = max(nbars, 30)
            elif freq <= Frequency('30m'):
                nbars = max(nbars, 10)

            nbars = int(nbars*freq.minute_bars)
            nbars = max(1, nbars)
            ndays = round(nbars/(self.broker.calendar.minutes_per_day/nbars))
            ndays =  ndays + max(5, round(0.2*ndays))
            ndays = min(self.config.broker.options.max_data_fetch, ndays)
            to_date = self.broker.calendar.last_trading_minute(to_date) + pd.Timedelta(minutes=1)
            from_date = self.broker.calendar.last_n_minutes(to_date, nbars)[0]
        elif freq == Frequency('1d'):
            nbars = nbars + max(5, int(0.2*nbars))
            ndays = int(nbars)
            ndays = ndays + max(5, round(0.2*ndays))
            ndays = min(self.config.broker.options.max_data_fetch, ndays)
            to_date = self.broker.calendar.last_trading_minute(to_date).normalize() + pd.Timedelta(days=1)
            sessions = self.broker.calendar.last_n_sessions(
                    pd.Timestamp.now(tz=self.broker.tz).normalize(), ndays)
            from_date = sessions[0]
        else:
            raise ValidationError(f'unsupported frequency {frequency}.')
                        
        nbars = nbars + max(5, round(0.2*nbars))
        df = err = None
        is_main = threading.current_thread() is threading.main_thread()

        for i in range(self.config.api.max_retries):
            try:
                df = api.request_get_history(
                        asset, freq, from_date, to_date, nbars, logger=logger)
                err = None
                break
            except (APIError, ServerError) as e:
                err = e
                if is_main:
                    msg = f'failed to fetch data from API, will not retry '
                    msg += f'from the main thread'
                    logger.error(msg)
                    break
                else:
                    s = 1+i*2
                    msg = f'failed to fetch data from API, will retry '
                    msg += f'after {s}s'
                    logger.info(msg)
                    time.sleep(s)
            
        if err:
            msg = f'failed to fetch data from API:{str(err)}'
            raise DataAPIError(msg)
        elif df is None:
            msg = f'No data received from API.'
            raise DataAPIError(msg)
            
        df= self._ensure_index_and_size(df, size, freq)
        self._validate_data_from_api({asset:df})
        return df
    
    @lru_cache(maxsize=128)
    def _fetch_bars_multi(self, assets:list[MarketData], nbars:int, frequency:Frequency|str, 
                          signature:str, logger:BlueshiftLogger, api:BrokerAPIMixin, 
                          **kwargs) -> dict[MarketData,pd.DataFrame]:
        size = nbars
        logger.info(f'stale or low data in db, fetching from API.')
        
        freq = Frequency(frequency)
        now = get_aligned_timestamp(pd.Timestamp.now(tz=self.broker.tz),Frequency('1m'))
        to_date = now
        
        if freq <= Frequency('1d'):
            if freq < Frequency('10m'):
                nbars = max(nbars, 30)
            elif freq <= Frequency('30m'):
                nbars = max(nbars, 10)
            
            nbars = int(nbars*freq.minute_bars)
            nbars = max(1, nbars)
            ndays = round(nbars/(self.broker.calendar.minutes_per_day/nbars))
            ndays =  ndays + max(5, round(0.2*ndays))
            ndays = min(self.config.broker.options.max_data_fetch, ndays)
            to_date = self.broker.calendar.last_trading_minute(to_date) + pd.Timedelta(minutes=1)
            from_date = self.broker.calendar.last_n_minutes(to_date, nbars)[0]
        elif freq == Frequency('1d'):
            nbars = nbars + max(5, int(0.2*nbars))
            ndays = int(nbars)
            ndays = ndays + max(5, round(0.2*ndays))
            ndays = min(self.config.broker.options.max_data_fetch, ndays)
            to_date = self.broker.calendar.last_trading_minute(to_date).normalize() + pd.Timedelta(days=1)
            sessions = self.broker.calendar.last_n_sessions(
                    pd.Timestamp.now(tz=self.broker.tz).normalize(), ndays)
            from_date = sessions[0]
        else:
            raise ValidationError(f'unsupported frequency {frequency}.')
                        
        err = None
        is_main = threading.current_thread() is threading.main_thread()
        
        df = {}
        for i in range(self.config.api.max_retries):
            try:
                df = api.request_get_history_multi(
                        assets, freq, from_date, to_date, nbars, logger=logger)
                err = None
                break
            except (APIError, ServerError) as e:
                err = e
                if is_main:
                    msg = f'failed to fetch data from API, will not retry '
                    msg += f'from the main thread:{str(e)}.'
                    logger.error(msg)
                    break
                else:
                    s = 1+i*2
                    msg = f'failed to fetch data from API, will retry '
                    msg += f'after {s}s.'
                    logger.info(msg)
                    time.sleep(s)
            
        if err:
            msg = f'failed to fetch data from API:{str(err)}'
            raise DataAPIError(msg)
            
        df = {self.broker.symbol(k):self._ensure_index_and_size(v, size, freq) for k,v in df.items()}
        self._validate_data_from_api(df)
        return df
    
