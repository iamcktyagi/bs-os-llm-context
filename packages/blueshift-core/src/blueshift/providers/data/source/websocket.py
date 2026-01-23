from __future__ import annotations
from typing import TYPE_CHECKING, Callable, Any
import logging
import re
import websockets  # type: ignore -> optional dependency

try:
    from websockets.protocol import State as SocketState # type: ignore -> optional dependency
except ImportError:
    from websockets.connection import State as SocketState # type: ignore -> optional dependency
    
import json
import asyncio
from urllib.parse import urlencode
import types
from threading import Event
import ssl
from urllib.request import getproxies

from blueshift.lib.common.sentinels import NOTHING
from blueshift.lib.exceptions import DataSourceException, AuthenticationError
from blueshift.lib.common.decorators import set_event_on_finish_async
from blueshift.lib.common.ctx_mgrs import TimeoutRLock
from blueshift.lib.common.constants import CONNECTION_TIMEOUT
from blueshift.lib.common.enums import ConnectionStatus
from blueshift.interfaces.data.source import DataSource, DataSourceType, register_data_source
from blueshift.interfaces.logger import get_logger

if TYPE_CHECKING:
    import pandas as pd
else:
    import blueshift.lib.common.lazy_pandas as pd

_permanent_errors = (AuthenticationError)

class WSSource(DataSource):
    """
        Class defining the implementation of data source from websocket
        streams.
        
        Although this class is implemented primarily to handle data streams
        from websocket, it can potentially handle other types of updates 
        as well. The argument `on_data` expect a function that takes in a 
        packet of data from websocket, and generates a tuple of 
        `(symbol, timestamp, data)`. The default `parser` takes in 
        the raw value returned from the websocket and publishes it under 
        the `data` channel. This automatically calls `on_data` function.
        
        To handle other updates apart from data, pass a dict of functions 
        as `on_data`. Specify the data handling function with the 
        key `data`. For other channels, if any, specify the channel name 
        as key and handler as the corresponding function. Then define a 
        custom parse function that takes in the raw websocket packet and 
        returns a `(channel, data)` tuple. Each such data packet, once 
        received, will then be forwarded to the handler function(s) that 
        matches (using `regex`) the channel name. Note: returns values from 
        the channels, other than `data` will be ignored. You can also use 
        the `on` decorator to add handlers (instead of passing a dictionary
        of them at the intialization).
        
        Note:
            Any `handler` functions (passed as dictionary during init or 
            declared with the `on` decorator) must return a tuple of 
            `(symbol, timestamp, values)`. The `values` can be any user 
            defined type (usually a dictionary of column name and 
            corresponding value). Also the `parser` function, if supplied, 
            must produce a tuple of `(channel, data)`. The `channel` is 
            used for selecting the `handler`. The `data` part is passed on 
            to the `handler` as argument. The `parser` function can either 
            return this `(channel, data)` tuple, or it can also yield them 
            as generator (useful if a single socket read produces many 
            data packets, e.g. a packet list is sent).
        
        Example:
            Instantiate an object of this class with required params. 
            This will produce an async generator that emits a tuple of 
            (metadata, data) that can be consumed in a async loop. `metadata` 
            is an arbitrary dictionary with key and value, but must 
            contain keys `symbol`. The `data` part can be anything, usualy 
            a dictionary with column names and values, for further processing.
            
            >> source = WSSource(root, auth, ..)
            >>
            >> @source.on('data')
            >> def on_data(data):
            ..    return data['symbol'], data['timestamp'], data['data']
            >>
            >> @source.on('accounts')
            >> def on_accounts(data):
            ..    # do some account book-keeping
            >>
            >> @source.on('orders')
            >> def on_orders(data):
            ..    # do some order updates
            >>
            >> # or we could have passed a dict like {'data':on_data,...}
            >> # during initialization of the source object for on_data.
            >>
            >> # define the parsing func to convert the raw packets 
            >> # from the websocket to (channel, data) tuple format for 
            >> # proper dispatch to appropriate handler.
            >>
            >> def custom_parser(rawdata):
            ..    data = convert_func(rawdata) # json or binary parsing here!
            ..    if 'data' in data:
            ..        return 'data', data # match / call the data handler
            ..    elif 'orders' in data:
            ..        return `order`, data # match/ call order handler
            ..    elif 'accounts' in data:
            ..        return `accounts`, data # match/ call account handler
            >>
            >> # If you get multiple data packets at 
            >> # once from single receive on the websocket, you can also
            >> # define the parser as a generator emitting the tuple. This
            >> # is shown in the below function
            >>
            >> def custom_parser2(rawdata):
            ..    data = convert_func(rawdata)
            ..    for packet in data:
            ..        channel, values = some_func(packet)
            ..        yield channel, values
            >>
            >> # update the parse in the object
            >> source.add_parser(parser)
            >>
            >> # run the loop inside a co-routine...
            >> async def run(source):
            ..     async for mdata, data in source:
            ..         do_something(mdata, data)
            >>
            >> asyncio.ensure_future(run(source))
    """
    
    _blank = {}, None
    _DATA_CHANNEL = 'data'
    _MAX_RETRIES = 10
    
    def __init__(self, root:str, auth:dict|None=None, url_auth:dict|None=None, frequency:str|None=None, 
                 master_data:pd.DataFrame=pd.DataFrame(), validate:Callable[[Any], bool]|None=None, 
                 parser:Callable[[Any],tuple[str, Any]]|None=None, on_data:dict={}, timeout:int=5, 
                 reply:bool=True, reply_on_connect:bool=False, logger:logging.Logger|None=None, 
                 verify:bool=True, proxies:dict={}, loop:asyncio.AbstractEventLoop|None=None, 
                 ping_timeout:int=20):
        """
            Specify the source to read from a websocket.
            
            Args:
                ``root (str)``: The url of the server.
                
                ``auth (dict)``; The formatted credentials payload
                    for auth. Only one of `auth` or `url_auth` should 
                    be used.
                    
                ``url_auth (dict)``; The formatted credentials for encoding
                    as url parameters. Only one of `auth` or `url_auth` 
                    should be used.
                
                ``frequency (str)``; Frequency of data, assumed continuous if 
                    `None`.
                
                ``master_data (Dataframe)``: A dataframe with master 
                    data (metadata) information about each symbol. If 
                    found, the matching row will be passed on as part of 
                    metadata for the symbol during iteration. It must have a 
                    column named `symbol` to match the row.
                
                ``validate (func)``: A function to check if the connection 
                    was successful. Must return `True` on successful 
                    connection.
                    
                ``parser (func)``: A function that takes in a message 
                    received from the websocket, parses and generate an 
                    tuple of (`channel`, `data`). This tuple will be dispatched 
                    to registered handlers for further processing.
                    
                ``on_data (func or dict)``: A function or a dict. If a dict, 
                    the channel from `parser` (see above) is matched and 
                    passed to the function under that channel. If a func, it is 
                    invoked for the default channel `data`.
                    
                ``timeout (int)``: Timeout for connectiosn, packet receive 
                    calls and pings.
                    
                ``reply (bool)``: If true, expect a reply on sending any 
                    message to the server. The client will wait for this 
                    reply and apply the `validate` function (see above) 
                    to check if the operation was a success or not.
                    
                ``reply_on_connect (bool)``: Expect reply on ws connect.
                    
                ``logger (obj)``: A logger object. If `None` a default one
                    will be used.
                    
                ``verify (bool)``: Ignore SSL certicate check if False.
        """
        self._loop = loop
        self._type = DataSourceType.WEBSTREAM
        self._root = root
        self._frequency = frequency
        self._metadata = {'frequency':self.frequency}
        self._ping_timeout = ping_timeout
        
        if auth and url_auth:
            raise DataSourceException('only one method of auth can be usd.') 
        
        self._auth = auth
        self._url_auth = url_auth
        self._verify_cert = verify
        
        self._validate = validate
        self._parser = parser
        
        if callable(on_data):
            self._handlers = {self._DATA_CHANNEL:on_data}
        elif isinstance(on_data, dict):
            self._handlers = {}
            for channel, func in on_data.items():
                self.register(channel, func)
        else:
            self._logger.error('illegal handler type, will be ignored.')
            self._handlers = {}

        self._timeout = timeout
        self._connection_status = ConnectionStatus.UNSET
        
        self._socket = None
        
        if logger:
            self._logger = logger
        else:
            self._logger = get_logger()
            
        self._master_data = master_data
            
        self._reply = reply
        self._reply_on_connect = reply_on_connect
        self._subscriptions = set()
        self._wants_to_close = False
        self._reconnect_flag = False
        self._socket_lock = TimeoutRLock(timeout=2*CONNECTION_TIMEOUT)
        self._timeout_counter = 0
        
        system_proxies = getproxies()
        self._proxies = {**proxies, **system_proxies}
    
    def __str__(self):
        return f"Blueshift Data Source[{self.type}]@[{self.root}]"
    
    def __repr__(self):
        return self.__str__()
    
    @property
    def type(self):
        """ Returns the data source type. """
        return self._type.name
    
    @property
    def timeout(self) -> int:
        return self._timeout
    
    @property
    def logger(self) -> logging.Logger:
        return self._logger
        
    @property
    def frequency(self):
        """ Data frequency in Pandas format. """
        return self._frequency
        
    @property
    def root(self):
        """ Returns root of the connection url. """
        return self._root
    
    @property
    def connection_status(self):
        return self._connection_status
        
    @property
    def connected(self):
        """ Check if connected to the source. """
        is_socket = self._socket is not None and \
            self._socket.state == SocketState.OPEN and \
            self._connection_status == ConnectionStatus.ESTABLISHED
        
        return is_socket
    
    @property
    def data_channel(self) -> str|None:
        return self._DATA_CHANNEL
        
    @property
    def metadata(self):
        """ Returns metadata about the source. """
        return self._metadata
    
    @property
    def loop(self):
        """ Returns the event loop for this source. """
        return self._loop
    
    @loop.setter
    def loop(self, value:asyncio.AbstractEventLoop|None):
        self._loop = value
    
    def connect(self, event=None):
        """ Connect to the source. """
        # TODO: use nest_async.apply() to make synchronous easy.
        if self.error and isinstance(self.error, _permanent_errors):
            raise self.error
            
        event = event or Event()
            
        if self.loop:
            asyncio.ensure_future(self._connect(event=event), loop=self.loop)
        else:
            event.set()
            msg = 'failed to connect, no running event loop'
            raise DataSourceException(msg)
            
        status = event.wait(timeout=self._timeout*self._MAX_RETRIES)
            
        if self._connection_status == ConnectionStatus.PENDING:
            self._connection_status = ConnectionStatus.ABORTED
            
        if not status:
            msg = f'Failed to connected to websocket@{self.root} - timed out'
            raise DataSourceException(msg)
            
        if self._connection_status != ConnectionStatus.ESTABLISHED:
            msg = f'Failed to connected to websocket@{self.root}.'
            if self.error:
                e = self.error
                if isinstance(e, _permanent_errors):
                    raise e
                else:
                    self.error = None
                    msg = f'Failed to connected to websocket@{self.root}:{str(e)}'
                raise DataSourceException(msg)
        
    def close(self, event=None):
        """ Close connection to the source. """
        event = event or Event()
        if self.loop:
            asyncio.ensure_future(self._close(event=event), loop=self.loop)
        else:
            event.set()
            msg = 'failed to close, no running event loop'
            raise DataSourceException(msg)
            
        event.wait(timeout=self._timeout)
        
        if self._connection_status == ConnectionStatus.PENDING:
            self._connection_status = ConnectionStatus.ABORTED
            self._socket = None
        
    def disconnect(self):
        """ alias for close. """
        if self.error and isinstance(self.error, _permanent_errors):
            return
        self.close()
            
    def reconnect(self, event=None):
        """ Re-connect to the source. """
        if self.error and isinstance(self.error, _permanent_errors):
            raise self.error
            
        event = event or Event()
        self._reconnect_flag = True
        if self.loop:
            asyncio.ensure_future(self._reconnect(event=event), loop=self.loop)
        else:
            event.set()
            msg = 'failed to reconnect, no running event loop'
            raise DataSourceException(msg)
            
        status = event.wait(timeout=self._timeout*self._MAX_RETRIES)
        
        if self._connection_status == ConnectionStatus.PENDING:
            self._connection_status = ConnectionStatus.ABORTED
            self._reconnect_flag = False
            
        if not status:
            msg = f'Failed to reconnected to websocket@{self.root} - timed out'
            raise DataSourceException(msg)
            
        if self._connection_status != ConnectionStatus.ESTABLISHED:
            msg = f'Failed to connected to websocket@{self.root}.'
            if self.error:
                e = self.error
                if isinstance(e, _permanent_errors):
                    raise e
                else:
                    self.error = None
                    msg = f'Failed to connected to websocket@{self.root}:{str(e)}'
                raise DataSourceException(msg)
        
    def reset(self):
        """ Close current connection and remove all subscriptions. """
        self._subscriptions = set()
        if self.loop:
            asyncio.ensure_future(self._close(), loop=self.loop)
            
    def subscribe(self, payload):
        """ subscribe to channel(s)."""
        if self.loop:
            asyncio.ensure_future(self._subscribe(payload), loop=self.loop)
            
    def send(self, payload):
        """ subscribe to channel(s). """            
        if self.loop:
            asyncio.ensure_future(self._send(payload), loop=self.loop)
        
    def add_masterdata(self, master_data:pd.DataFrame):
        """
            Add metadata symbol-wise. Add a master data dataframe, with 
            a `symbol` column that provides additional fields for 
            enriching metadata for a given symbol.
        """
        if len(master_data) >0 and 'symbol' not in master_data.columns:
            raise DataSourceException('master data missing symbol column.')
        self._master_data = master_data
        
    def add_metadata(self, **kwargs):
        """ Add extra metadata to be applied for **all** symbols. """
        self._metadata = {**self._metadata, **kwargs}
        
        for key in list(self._metadata.keys()):
            if self._metadata[key] == NOTHING:
                self._metadata.pop(key)
                
    def set_parser(self, func):
        """ Update the parse function. """
        self._parser = func
        
    def on(self, channel):
        """ Register a channel handler as through decoration. """
        def decorator(f):
            self.register(channel, f)
            return f
        return decorator
    
    def register(self, channel, handler, **kwargs):
        """ Register a channel handler. """
        if channel != self._DATA_CHANNEL:
            channel = re.compile(channel)
        self._handlers[channel] = handler # type: ignore
        
    def remove_handler(self, channel, handler):
        """ Remove a channel handler. """
        channel = re.compile(channel)
        if channel in self._handlers:
            self._handlers.pop(channel) # type: ignore
        
    def __iter__(self):
        raise NotImplementedError
    
    def __aiter__(self):
        return self._generator()
        
    async def _generator(self):
        while self.connected and not self._wants_to_close:
            async for data in self._consume():
                yield data
        
    def _apply_metadata(self, sym):
        mdata = {}
        if len(self._master_data)>0 and sym in self._master_data.symbol:
            mdata = self._master_data[\
                    self._master_data.symbol==sym].iloc[0].to_dict()
            
        return mdata
    
    async def _release_resources(self, close=True):
        await asyncio.sleep(0)
    
    async def _handle_error(self, msg, error=None, log=True, close=True, 
                            exception=False):
        if error:
            msg = msg+f':{str(error)}'
        
        if log:
            self._logger.error(msg)
        
        if close:
            await self._release_resources()
            
        if exception:
            if '401' in msg:
                raise AuthenticationError(msg)
            raise DataSourceException(msg)
            
    async def _send(self, payload, wait_for_reply=False):
        if not self._socket:
            msg = 'websocket not connected, call connect first.'
            raise DataSourceException(msg)

        if isinstance(payload, dict):
            try:
                payload = json.dumps(payload)
            except Exception:
                msg = 'subscription payload must be json serializable.'
                raise DataSourceException(msg)
                
        try:
            await asyncio.wait_for(
                self._socket.send(payload),timeout=5*self._timeout)
        
            if wait_for_reply and self._validate:
                r = await asyncio.wait_for(
                        self._socket.recv(), timeout=5*self._timeout)
                status = self._validate(r)
                if not status:
                    await self._handle_error(
                            f'failed to send payload {payload}, error {r}',
                            close=False)
                    return False
            return True
        except RuntimeError:
            pass
        except asyncio.CancelledError:
            return False
        except asyncio.TimeoutError:
            await self._handle_error("send attempt timedout.", close=False)
            return False
        except Exception as e:
            await self._handle_error(
                    f"failed in send {payload}", error=e, close=False)
            return False
        
    async def aconnect(self, *args, **kwargs):
        return await self._connect(*args, **kwargs)
    
    @set_event_on_finish_async()
    async def _connect(self, event=None):
        try:
            with self._socket_lock:
                self.error = None
                
                if self.connected:
                    return
                
                if self.loop is None:
                    await self._handle_error(
                            'event loop not initialized.', close=False)
                
                if self._connection_status == ConnectionStatus.PENDING:
                    try:
                        await asyncio.sleep(self._timeout)
                    except asyncio.CancelledError:
                        self._connection_status = ConnectionStatus.ABORTED
                        return
                    
                    if self.connected:
                        return
                
                self._wants_to_close = False
                self._connection_status = ConnectionStatus.PENDING
                
                
                if self._socket:
                    try:
                        self._logger.info(
                                f'disconnecting before new connect.')
                        await asyncio.wait_for(
                                self._socket.close(), timeout=5*self._timeout)
                        await asyncio.sleep(self._timeout)
                    except asyncio.CancelledError:
                        self._connection_status = ConnectionStatus.ABORTED
                        return
                    except Exception as e:
                        self._logger.error(
                                f'failed disconnecting before new connect:{str(e)}.')
                        self._connection_status = ConnectionStatus.ABORTED
                        return
                self._socket = None
                
                try:
                    ws_url = self._root
                    if self._url_auth:
                        ws_url = self._root+'?' + urlencode(self._url_auth)
                    
                    if self._verify_cert:
                        self._logger.info('using websocket with tls.')
                        ws = await asyncio.wait_for(
                            websockets.connect(
                                    ws_url, ping_timeout=self._ping_timeout), timeout=5*self._timeout)
                    else:
                        self._logger.info('using websocket without tls.')
                        ctx = ssl.create_default_context()
                        ctx.check_hostname = False
                        ctx.verify_mode = ssl.CERT_NONE
                    
                        ws = await asyncio.wait_for(
                                websockets.connect(
                                        ws_url, ping_timeout=self._ping_timeout, ssl=ctx), timeout=5*self._timeout)
                    
                    if self._reply_on_connect and self._validate:
                        r = await asyncio.wait_for(
                                    ws.recv(), timeout=5*self._timeout)
                        self._logger.info(f'Got reply from ws on connect:{r}.')
                        status = self._validate(r)
                        if not status:
                            self._connection_status = ConnectionStatus.ABORTED
                            await self._handle_error(f'could not connect. {r}')
                    
                    if self._auth:
                        await asyncio.wait_for(
                                ws.send(json.dumps(self._auth)),
                                timeout=5*self._timeout)
                        if self._reply and self._validate:
                            r = await asyncio.wait_for(
                                    ws.recv(), timeout=5*self._timeout)
                            self._logger.info(f'Got reply from ws on auth:{r}')
                            status = self._validate(r)
                            if not status:
                                self._connection_status = ConnectionStatus.ABORTED
                                await self._handle_error(
                                        f'failed to connect:unauthorized. {r}')
                    self._socket = ws
                except asyncio.TimeoutError as e:
                    self.error = e
                    self._connection_status = ConnectionStatus.ABORTED
                    await self._handle_error(
                            f"connect attempt timed out for {ws_url}.", # type: ignore
                            exception=True)
                except asyncio.CancelledError as e:
                    self.error = e
                    await self._release_resources()
                except Exception as e:
                    self.error = e
                    self._connection_status = ConnectionStatus.ABORTED
                    if 'getaddrinfo failed' in str(e):
                        await self._handle_error(
                            "failed to establish websocket connection - seems internet connection is down.",
                            exception=True)
                    else:
                        await self._handle_error(
                                "failed to establish websocket connection", error=e,
                                exception=True)
                else:
                    self.error = None
                    self._logger.info(f'successfully connected to {self._root}')
                    self._connection_status = ConnectionStatus.ESTABLISHED
                    # set the event early to avoid long running on_connect
                    if event:
                        event.set()
                        
                    if self._subscriptions:
                        payloads = self._subscriptions.copy()
                        self._subscriptions = set()
                        for payload in payloads:
                            self._logger.info(
                                    f'sending subscription request {payload}')
                            try:
                                await self._subscribe(payload, validate=False)
                            except Exception as e:
                                self._logger.info(
                                        f'Failed sending subscription request: {str(e)}')
                    
                    for pattern, callback in self._handlers.copy().items():
                        if pattern == self._DATA_CHANNEL:
                            continue
                        if pattern.match('on_connect'): # type: ignore
                            callback()
        except TimeoutError as e:
            self.error = e
            self._connection_status = ConnectionStatus.ABORTED
            await self._handle_error(
                    f"connect attempt @{ws_url} timed out waiting for lock.",  # type: ignore
                    exception=True)
            
    async def aclose(self, *args, **kwargs):
        return await self._close(*args, **kwargs)
    
    @set_event_on_finish_async()
    async def _close(self, event=None):
        try:
            with self._socket_lock:
                if not self._socket:
                    return
                
                if self._socket.state not in [SocketState.OPEN, SocketState.CONNECTING]:
                    # the socket is not connected, set as none
                    self._socket = None
                    return
                
                if self._connection_status == ConnectionStatus.PENDING:
                    try:
                        await asyncio.sleep(self._timeout)
                    except asyncio.CancelledError:
                        return
                    if not self._socket:
                        return
                
                self._wants_to_close = True
                last_status = self._connection_status
                self._connection_status = ConnectionStatus.PENDING
                
                
                try:
                    if self._socket is not None:
                        await asyncio.wait_for(self._socket.close(), self._timeout)
                        self._socket = None
                except asyncio.TimeoutError as e:
                    self.error = e
                    self._connection_status = ConnectionStatus.ABORTED
                    await self._handle_error(
                            "websocket close timed out", error=e, close=False)
                except websockets.exceptions.ConnectionClosedOK:
                    # do nothing, we closed the socket normally (disconnect)
                    self._connection_status = ConnectionStatus.DISCONNECTED
                except asyncio.CancelledError:
                    self._connection_status = last_status
                    await self._release_resources(close=False)
                except Exception as e:
                    self._connection_status = ConnectionStatus.ABORTED
                    await self._handle_error(
                            "websocket close failed", error=e, close=False)
                else:
                    self._logger.info(f'successfully disconnected from {self._root}')
                    self._connection_status = ConnectionStatus.DISCONNECTED
                    self._on_disconnect()
        except TimeoutError as e:
            self.error = e
            self._connection_status = ConnectionStatus.ABORTED
            await self._handle_error("websocket close timed out", error=e, close=False)
            
    async def _reconnect(self, event=None):
        try:
            self._reconnect_flag = True
            await self._close()
            await self._connect(event=event)
            
            for pattern, callback in self._handlers.copy().items():
                if pattern == self._DATA_CHANNEL:
                    continue
                if pattern.match('on_reconnect'): # type: ignore
                    callback()
            self._reconnect_flag = False
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._logger.error(f'Failed to re-reconnect:{str(e)}')
            
    async def _ping(self):
        if not self.connected:
            self._logger.info('cannot ping, not connected.')
            return
        try:
            await asyncio.wait_for(self._socket.ping(),timeout=5*self._timeout) # type: ignore
        except asyncio.TimeoutError as e:
            await self._handle_error("ping timed out", error=e)
        except websockets.exceptions.ConnectionClosedOK:
            # do nothing, we closed the socket normally (disconnect)
            pass
        except asyncio.CancelledError:
            await self._handle_error('websocket cancelled')
        except Exception as e:
            await self._handle_error("ping failed", error=e)
            
    async def _subscribe(self, payload, validate=True):
        if validate and not self.connected:
            return await self._handle_error(
                    'cannot subscribe, not connected', close=False)
        
        if isinstance(payload, dict):
            try:
                if payload:
                    payload = json.dumps(payload)
            except Exception:
                msg = 'subscription payload must be json serializable.'
                raise DataSourceException(msg)
        try:
            if payload:
                await asyncio.wait_for(
                        self._socket.send(payload), # type: ignore
                        timeout=5*self._timeout)
                
                if payload and payload not in self._subscriptions:
                    self._subscriptions.add(payload)
                if self._reply and self._validate:
                    self._logger.debug(f'waiting for subscription reply')
                    r = await asyncio.wait_for(
                            self._socket.recv(), timeout=5*self._timeout) # type: ignore
                    status = self._validate(r)
                    if not status:
                        await self._handle_error(
                                f'failed to subscribe with {payload}',
                                close=False)
                    else:
                        self._logger.info(
                                f'successfully subscribed with {payload}')
                else:
                    self._logger.info(f'sent subscribe request with {payload}')
        except RuntimeError:
            pass
        except asyncio.TimeoutError:
            if payload not in self._subscriptions:
                # timed out in send the first time, fail at this point
                await self._handle_error(
                        "subscribe attempt timed out.", 
                        exception=True,
                        close=False)
            else:
                await self._handle_error(
                        "subscribe acknowledgement timed out", 
                        log=False,
                        close=False)
        except websockets.exceptions.ConnectionClosedOK:
            # do nothing, we closed the socket normally (disconnect)
            pass
        except asyncio.CancelledError:
            await self._handle_error('websocket cancelled')
        except Exception as e:
            await self._handle_error(
                    f"failed in subscription {payload}", error=e, close=False)
            
    def _dispatch(self, channel, data):
        if not channel:
            return self._blank
        
        res = self._blank
        if channel == self._DATA_CHANNEL and self._DATA_CHANNEL in self._handlers:
            try:
                res = self._handlers[self._DATA_CHANNEL](data)
                sym, timestamp, values = res # type: ignore
            except (ValueError, TypeError):
                msg = 'data handler function must return a tuple'
                msg = msg + f' of (asset, timestamp, value), got {type(res)}, {res}'
                raise DataSourceException(msg)
                
            if not sym:
                # something went wrong with data packet processing.
                return self._blank
            
            # datapoint specific metadata
            metadata = {'symbol':sym, 'timestamp': timestamp}
            # symbol specific metadata
            mdata = self._apply_metadata(sym)
            # squash metadata including the general part
            metadata = {**self.metadata, **mdata, **metadata}
            
            return metadata, values
        
        for pattern, callback in self._handlers.copy().items():
            if pattern == self._DATA_CHANNEL:
                continue
            if pattern.match(channel): # type: ignore
                callback(data)
                
        return self._blank
                
    async def _consume(self):
        """ for data push from server. """
        if not self.connected:
            await self._handle_error(
                    'cannot consume, not connected', close=False)
            await asyncio.sleep(1)
            yield self._blank
        
        try:
            r = await asyncio.wait_for(
                    self._socket.recv(), timeout=1) # type: ignore
            self._timeout_counter = 0
            if self._parser:
                packets = self._parser(r)
            else:
                packets = self._DATA_CHANNEL, r
            
            try:
                if isinstance(packets, types.GeneratorType):
                    for channel, data in packets:
                        yield self._dispatch(channel, data)
                else:
                    channel, data = packets
                    yield self._dispatch(channel, data)
            except (TypeError, ValueError):
                msg = 'parser must return a tuple of (channel, data)'
                msg = msg + ', or a generator that can produce them.'
                raise DataSourceException(msg)
        except RuntimeError:
            pass
        except asyncio.TimeoutError:
            if self._timeout_counter>10:
                try:
                    self._timeout_counter = 0
                    await self._ping()
                    yield self._blank
                except Exception as e:
                    await self._handle_error(
                            'ping failed in ws on timeout.', error=e)
                    self._on_disconnect()
                    yield self._blank
            else:
                self._timeout_counter += 1
                yield self._blank
        except websockets.exceptions.ConnectionClosedOK:
            # do nothing, we closed the socket normally (disconnect)
            pass
        except websockets.exceptions.ConnectionClosedError:
            msg = 'error receiving ws packet - seems internet connection '
            msg += 'is down, or the upstream (broker) server disconnected actively.'
            await self._handle_error(msg)
            self._on_disconnect()
        except websockets.exceptions.WebSocketException as e:
            await self._handle_error(
                    "error receiving ws packet", error=e, close=False)
            self._on_disconnect()
        except DataSourceException as e:
            raise e
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._handle_error(
                    "error while receiving packet", error=e, close=False)
            self._on_disconnect()
                        
    def _on_disconnect(self):
        for pattern, callback in self._handlers.copy().items():
            if pattern == self._DATA_CHANNEL:
                continue
            if pattern.match('on_disconnect'): # type: ignore
                try:
                    callback()
                except Exception as e:
                    self._logger.error(
                            f'failed to run callback on disconnect:{str(e)}')
                        
            
register_data_source('websocket', WSSource)
