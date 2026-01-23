from __future__ import annotations
from typing import TYPE_CHECKING, cast
import logging
from collections import OrderedDict
import threading

from blueshift.lib.common.types import ListLike
from blueshift.lib.common.constants import CONNECTION_TIMEOUT
from blueshift.interfaces.assets._assets import MarketData
from blueshift.lib.common.ctx_mgrs import TimeoutRLock
from blueshift.lib.common.sentinels import noops
from blueshift.interfaces.data.ingestor import StreamingIngestor, IngestionMode, register_ingestor
from blueshift.interfaces.data.source import DataSource
from blueshift.interfaces.data.store import DataStore
from blueshift.interfaces.logger import get_logger
from blueshift.providers.data.store.dataframe_store import DataFrameStore
from blueshift.providers.assets.dictdb import DictDBDriver
from blueshift.lib.exceptions import IngestionException
from blueshift.providers.streaming.streamhandler import StreamHandler


class ContinuousIngestor(StreamingIngestor):
    """
        An ingestion engine/ streaming data handler class that takes 
        in a streaming source, a stream handler and a store, manages data 
        updates, auto-reconnect, channel subscriptions etc. Due to a 
        potential high rate of writing, asset db is NOT updated during 
        the ingestion, but rather is expected to be passed on as an 
        argument (which is referred to during the write process to 
        identify the asset). Consequently there is no support for injecting 
        metadata during the write process as well. Also, it only ingest 
        data that are requested (`'subscribed`), and hence no universe 
        filtering is supported either.
    """
    def __init__(self, source:DataSource, store:DataStore|None=None,
                 assetdb=None, callback=None, timeout=2, logger=None,
                 max_tickers=0, unsubscribe_func=None):
        """
            Takes a source and optionally a store objects. If `handler` is 
            not passed, a new one will be created. The `source` iteration 
            is controlled by the `handler`, and if a `store` is passed, 
            the data handler `on_data` will write to the `store` object.
            The data handler `on_data` expects a metadata (dict) and 
            data object in the arguments and calls the `write` method of 
            the `store`. The metadata argument at minumum must contain a 
            `symbol` key and a `timestamp` key required for the `write` 
            method. If an `assetdb` is passed, it will be queried to map 
            the `symbol` to an `asset` object. The argument `callback` is 
            an optional function that will be called for a data event.
        """
        super(ContinuousIngestor, self).__init__(source, store)
        self._mode = IngestionMode.CONTINUOUS
        
        if source is not None:
            if not hasattr(self._source, 'connected'):
                raise IngestionException(
                        'must supply a streaming source object.')
            self._handler = StreamHandler(source, logger=logger)
        else:
            self._handler = None
            
        if logger:
            self._logger = logger
        else:
            if source is not None:
                self._logger = source.logger
            else:
                self._logger = get_logger()
            
        if timeout or source is None:
            self._timeout = timeout
        else:
            self._timeout = source.timeout
        
        self._DBLock = TimeoutRLock(self._timeout)
        
        if isinstance(assetdb, dict):
            self._assetdb = DictDBDriver(assetdb)
        else:
            self._assetdb = assetdb
            
        self._callbacks = []
        if callback:
            if not isinstance(callback, ListLike):
                callback = [callback]
            callback = cast(list, callback)
            self._callbacks = list(callback)
        
        self._validate_asset = False
        self._watchlist = OrderedDict()
        self._max_tickers = max_tickers            
        self._unsubscribe_func = unsubscribe_func or noops
        
    @StreamingIngestor.source.setter
    def source(self, value):
        if isinstance(value, DataSource):
            self._source = value
        else:
            raise IngestionException('must supply a valid source object.')
        
        if not hasattr(self._source, 'connected'):
            raise IngestionException('must supply a streaming source object.')
        self._handler = StreamHandler(self._source)
        
    @property
    def added_symbols(self):
        if not self.store:
            return set()
        
        assets = self.store.db.search()
        if assets:
            if not isinstance(assets, ListLike):
                assets = [assets]
            assets = cast(list, assets)
            return set([asset.exchange_ticker for asset in assets])
        return set()
    
    @property
    def is_connected(self):
        if self._source is None:
            return True
        
        return self._source.connected
    
    @property
    def error(self):
        # propagate source error
        if self._handler:
            return self._handler.error
        
    @property
    def logger(self):
        return self._logger
    
    @property
    def max_tickers(self):
        return self._max_tickers

    def run(self):
        if not self._source or not self._handler:
            return
        
        self._validate_before_run()
        self._handler.run(self.on_data)
        
    def stop(self):
        self.disconnect()
        
    def register(self, channel, handler, **kwargs):
        """ 
            Register a handler for a given channel.
            data channel callback update handled at this instance. 
            Else, it is passed on to the underlying source.
        """
        if not self._source:
            return
        
        if channel == self._source.data_channel:
            self._callbacks.append(handler)
        else:
            self._source.register(channel, handler, **kwargs)

    def subscribe(self, payload, assets=None):
        """ 
            subscribe/ unsubscribe to a specific channel. Do not pass 
            assets parameters for unsubscribe.
        """
        if not self._source:
            return
        
        if assets:
            self._check_watchlist(assets)
        
        self._source.subscribe(payload)
    
    def ensure_connected(self, timeout=None):
        # the reconnect function will not wait for the event, we need 
        # to explicitly wait for the event here.
        if not self._source:
            return
        
        if self.is_connected:
            return
        
        timeout = timeout or CONNECTION_TIMEOUT
        evt = threading.Event()
        evt.clear()
        
        self.reconnect(event=evt)
        self._logger.info(f'waiting for reconnect@{self._source}')
        evt.wait(timeout=timeout)
        self._logger.info(f'waiting for reconnect@{self._source} done')
        
        if not self.is_connected:
            msg = f'Failed to ensure connection in streaming.'
            if self.error:
                msg += f':{str(self.error)}'
            raise ConnectionError(msg)

    def reconnect(self, forced=False, event=None):
        if not self._source or not self._handler:
            return
        
        if not forced and self.is_connected:
            return
        
        if not self.is_connected:
            self.connect(event=event)
        else:
            self._handler.reset(event=event)

    def disconnect(self):
        # no need to wait for actual disconnect, hence no event
        if not self._source or not self._handler:
            return
        
        self._handler.stop()
        
    def connect(self, forced=False, event=None):
        if not self._source or not self._handler:
            return
        
        if not forced and self.is_connected:
            return
        
        self._handler.run(self.on_data, event=event)
        
    def add_chunk(self, asset, data):
        """ add data chunk. """
        if not self._store or not self._source:
            return
        
        if data.empty:
            self._logger.debug(f"empty data chunk for {asset}.")
            return
        
        sym = asset.exchange_ticker if isinstance(asset, MarketData) else asset
        if self._assetdb and not self._assetdb.exists(sym):
            if isinstance(asset, MarketData):
                self._logger.info(f'Asset not in asset db, adding {asset}.')
                self._assetdb.add(asset)
            else:
                # cannot add data without the asset
                msg = f'Cannot add data for symbol {sym}, expected asset.'
                self._logger.error(msg)
                return
        
        try:
            with self._DBLock:
                self._store.add_data(asset, data)
        except TimeoutError:
            self._logger.info(f"failed adding chunk at ingestor:timeout.")
        except Exception as e:
            self._logger.error(f"failed adding chunk at ingestor:{str(e)}.")
            raise e
        else:        
            self._logger.debug(f"added data chunk for {asset}.")
            
    def remove(self, asset):
        if not self._store or not self._source:
            return
        
        try:
            with self._DBLock:
                if asset:
                    exists = self._watchlist.pop(asset, None)
                    self._store.remove(asset)
                    if exists:
                        self._unsubscribe_func(asset)
        except TimeoutError:
            self._logger.info(f"failed removing asset from ingestor:timeout.")
        except Exception as e:
            self._logger.error(
                    f"failed removing asset {asset} from ingestor:{str(e)}")
            raise e
            
        self._logger.debug(f"reset data for {asset}.")
        
    def on_data(self, metadata, data):
        """ 
            Main data handler. This handler supports no metadata, 
            and expect and assetdb to be supplied separately if the 
            underlying store requires asset object to write data. 
            Otherwise, only the symbol is passed, alongwith a `timestamp`
            to the data store. This is in contrast to other types of 
            ingestors where elaborate metadata handlings are supported.
            This is because, this particular mode of ingestion can be 
            forced to handle numerous writes per second and hence 
            writing or updating an asset database at the same time may 
            have major performance impact.
        """
        if not self._store or not self._source:
            return
        
        if not metadata or not data:
            return
        
        try:
            with self._DBLock:
                asset = metadata['symbol']
                timestamp = metadata['timestamp']
                
                if isinstance(asset, MarketData):
                    if self._assetdb and self._validate_asset and \
                        not self._assetdb.exists(asset.exchange_ticker):
                        self._logger.info(f"asset not found {asset}, will add.")
                        self._assetdb.add(asset)
                else:
                    if not self._assetdb:
                        self._logger.error(f"must provide an asset db.")
                        return
                    asset = self._assetdb.search(asset)
                    if not asset:
                        self._logger.error(f"symbol not found {asset}")
                        return
                
                if self._max_tickers and asset not in self._watchlist:
                    # watchlist is active but this asset is not subscribed
                    return
                else:
                    self._watchlist[asset] = True
                    
                self._store.write(asset, timestamp, **data)
        except TimeoutError:
            self._logger.info(f"failed adding data update at ingestor:timeout.")
        except Exception as e:
            self._logger.error(
                    f"failed updating at ingestor:{str(e)}")
        else:
            if self._callbacks:
                for cb in self._callbacks:
                    cb(asset)
            
    def reset(self, asset=None, assetdb=None, **kwargs):
        """ reset data for a given asset or all (if asset is None). """
        if not self._store or not self._source:
            return
        
        try:
            with self._DBLock:
                if assetdb:
                    if isinstance(assetdb, dict):
                        self._assetdb = DictDBDriver(assetdb)
                    else:
                        self._assetdb = assetdb
                
                if asset:
                    exists = self._watchlist.pop(asset, None)
                    self._store.remove(asset)
                    if exists:
                        self._unsubscribe_func(asset)
                elif asset is None:
                    # we skip unsubscribe here to avoid sending a large
                    # number of socket calls, the subscription remains 
                    # unchaged and should be automatically handled via 
                    # subsequent watchlist check
                    self._store.truncate()
        except TimeoutError:
            self._logger.info(f"failed in reset at ingestor:timeout.")
        except Exception as e:
            self._logger.error(
                    f"failed resetting database at ingestor:{str(e)}")
            raise e
            
        self._logger.debug(f"reset data for {asset}.")
        
    def _validate_before_run(self):
        if self.store and not isinstance(self.store, DataFrameStore):
            raise IngestionException(
                        'only dataframe store is supported.')
            
        if not isinstance(self._handler, StreamHandler):
            raise IngestionException(
                        'must add a valid source object.')
            
        if self._max_tickers and self.store:
            self._max_tickers = self._max_tickers or self.store.max_tickers
            self._max_tickers = min(self._max_tickers, self.store.max_tickers)
        else:
            self._max_tickers = 0
            
        if self._max_tickers and not callable(self._unsubscribe_func):
            msg = f'unsubscribe_func must be a callable with '
            msg += f'signature func(asset), '
            msg += f'got {type(self._unsubscribe_func)}.'
            raise IngestionException()
            
        if not callable(self._unsubscribe_func):
            self._unsubscribe_func = noops
            
    def add_membership(self, data=None, drop_syms=[]):
        """ membership data and extended metadata not supported. """
        pass
    
    def update_assets(self, assetdb=None):
        if not assetdb:
            return
        if isinstance(assetdb, dict):
            self._assetdb = DictDBDriver(assetdb)
        else:
            self._assetdb = assetdb
            
    def update_asset(self, asset):
        if not self._assetdb:
            return
        self._assetdb.add(asset)
        
    def _check_watchlist(self, assets):
        if assets is None or not self._max_tickers:
            return
        
        if not isinstance(assets, ListLike):
            assets = [assets]
        assets = cast(list, assets)
        unsubscribe_list = []
        for asset in assets:
            if len(self._watchlist) >= self._max_tickers:
                old, _ = self._watchlist.popitem(last=False)
                unsubscribe_list.append(old)
            
            if asset in self._watchlist:
                self._watchlist.move_to_end(asset)
            else:
                self._watchlist[asset] = True
                
        if unsubscribe_list:
            # to unsubscibe assets, first remove the data and then
            # send unsubscribe request (which may fail)
            for asset in unsubscribe_list:
                if self._store:
                    self._store.remove(asset)
                self._unsubscribe_func(asset)
                
    def is_subscribed(self, asset):
        if asset not in self._watchlist:
            return False
        
        self._watchlist.move_to_end(asset)
        return True
        
class StreamingData(ContinuousIngestor):
    """ An alias for `ContinuousIngestor` class. """
    pass

register_ingestor('continuous', ContinuousIngestor)
register_ingestor('streaming', StreamingData)