from __future__ import annotations
from typing import TYPE_CHECKING
import logging
import time
import threading
from threading import RLock
from urllib.parse import urlencode
from functools import wraps
from urllib.request import getproxies

import requests # type: ignore -> optional dependency
from engineio.payload import Payload # type: ignore -> optional dependency
from socketio.client import Client as SocketIO # type: ignore -> optional dependency
from socketio.exceptions import ConnectionError, BadNamespaceError # type: ignore -> optional dependency
import engineio.client # type: ignore -> optional dependency

from blueshift.lib.common.sentinels import NOTHING, noops
from blueshift.lib.exceptions import DataSourceException, AuthenticationError
from blueshift.lib.common.decorators import set_event_on_finish
from blueshift.lib.common.ctx_mgrs import TimeoutRLock
from blueshift.lib.common.constants import CONNECTION_TIMEOUT
from blueshift.lib.common.enums import ConnectionStatus
from blueshift.interfaces.data.source import DataSource, DataSourceType, register_data_source
from blueshift.interfaces.logger import get_logger

if TYPE_CHECKING:
    import pandas as pd
else:
    import blueshift.lib.common.lazy_pandas as pd

Payload.max_decode_packets = 500

# overwrite engineio background task to run as daemon
def start_background_task(self, target, *args, **kwargs):
    th = threading.Thread(target=target, name='SocketIOBgTask', args=args, kwargs=kwargs)
    th.daemon = True
    th.start()
    return th

engineio.client.Client.start_background_task = start_background_task

_permanent_errors = (AuthenticationError)

class SocketIOSource(DataSource):
    """
        Class defining the implementation of data source from socketIO
        streams.
        
        This class can be used to add callbacks to handle any sort of 
        updates from the upstream server, including data. The argument 
        `on_data` expect a function that takes in a frame from the server, 
        and generates a tuple of `(symbol, timestamp, data)`.
        
        To handle other updates apart from data, pass a dict of functions 
        as `on_data`. You can also use the `on` decorator to add handlers 
        (instead of passing a dictionary of them at the intialization).
        
        Note:
            Any data `handler` functions (passed as dictionary during init or 
            declared with the `on` decorator) must return a tuple of 
            `(symbol, timestamp, values)`. The `values` can be any user 
            defined type (usually a dictionary of column name and 
            corresponding value).
        
        Example:
            Instantiate an object of this class with required params. 
            This will automatically start the engineIO loop in the 
            background. Any 'on_data' function is automatically enriched
            with metadata - a common metadata part and a symbol specific 
            part. The user supplied on_data handler must return a tuple of 
            symbol, timestamp and data. This will be automatically converted 
            to a tuple of metadata and data (with metadata enriching). This 
            output can be consumed by a `finalizer` method - typically 
            supplied by an external ingestor.
            
            >> source = SocketIOSource(root, auth, ..)
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
            >> # set the finalizer to process the data output
            >> def data_writer(metadata, data):
            >>     # write data to a database
            >>     pass
            >>
            >> source.finalizer = data_writer
    """
    
    _blank = {}, None
    _DATA_CHANNEL = 'data'
    
    def __init__(self, root:str, auth:dict|None=None, url_auth:dict|None=None, frequency:str|None=None, 
                 master_data:pd.DataFrame=pd.DataFrame(), on_data:dict={}, timeout:int=5, 
                 logger:logging.Logger|None=None, transports:str|None=None, headers:dict={}, 
                 namespaces:dict|None=None, auth_in_header:bool=True, socketio_path:str='socket.io',
                 verify:bool=True, proxies:dict={}):
        """
            Specify the source to read from a socketIO connection.
            
            Args:
                ``root (str)``: The url of the server.
                
                ``auth (dict)``; The formatted credentials payload
                    for auth. Only one of `auth` or `url_auth` should 
                    be used. If `auth` is specified, it will be send as 
                    headers during a `connect`.
                    
                ``url_auth (dict)``; The formatted credentials for encoding
                    as url parameters. Only one of `auth` or `url_auth` 
                    should be used. If `url_auth` is given, it will be 
                    added to the root url, and the `auth` parameter will 
                    be ignored.
                
                ``frequency (str)``; Frequency of data, assumed continuous if 
                    `None`.
                
                ``master_data (Dataframe)``: A dataframe with master 
                    data (metadata) information about each symbol. If 
                    found, the matching row will be passed on as part of 
                    metadata for the symbol during iteration. It must have a 
                    column named `symbol` to match the row.
                    
                ``on_data (func or dict)``: A function or a dict. If a dict, 
                    the channel from `parser` (see above) is matched and 
                    passed to the function under that channel. If a func, it is 
                    invoked for the default channel `data`.
                    
                ``timeout (int)``: Timeout for connectiosn, packet receive 
                    calls and pings.
                    
                ``logger (obj)``: A logger object. If `None` a default one
                    will be used.
                    
                ``headers(dict)``: Headers for connect.
                
                ``namespaces(list)`: Namespaces for connect.
                    
                ``auth_in_header(bool)``: If auth is in header or connect 
                    payload (requires socketio>=5.0.0).
        """
        
        self._type = DataSourceType.SOCKETIO
        self._root = root
        self._frequency = frequency
        self._metadata = {'frequency':self.frequency}
        self._transports = transports
        self._socketio_path = socketio_path
        self._auth_in_header = auth_in_header
        self._headers = headers
        self._namespaces = namespaces
        self._verify = verify
        
        system_proxies = getproxies()
        self._proxies = {**proxies, **system_proxies}
        
        if auth and url_auth:
            raise DataSourceException('only one method of auth can be used.') 
        
        self._auth = auth
        self._url_auth = url_auth
        self._timeout = timeout
        self._connection_status = ConnectionStatus.UNSET
        self._socket = None
        
        if logger:
            self._logger = logger
        else:
            self._logger = get_logger()
        
        if callable(on_data):
            key = (self._DATA_CHANNEL, None)
            self._handlers = {key:on_data}
        elif isinstance(on_data, dict):
            self._handlers = {}
            for key, func in on_data.items():
                if isinstance(key, tuple):
                    event, namespace = key
                else:
                    event = key
                    namespace = None
                self.register(event, func, namespace)
        else:
            self._logger.error('illegal handler type, will be ignored.')
            self._handlers = {}
            
        self._master_data = master_data
            
        self._reply = False
        self._reply_on_connect = False
        self._subscriptions = set()
        self._wants_to_close = False
        self._socket_lock = TimeoutRLock(timeout=2*CONNECTION_TIMEOUT)
        self._finalizer = noops
    
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
            self._socket.connected and \
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
    def finalizer(self):
        """ Returns data finalizer method. """
        return self._finalizer
    
    @finalizer.setter
    def finalizer(self, func):
        """ Returns data finalizer method. """
        if callable(func):
            self._finalizer = func
        
    def reset(self):
        """ Close current connection and remove all subscriptions. """
        self._subscriptions = set()
        self.close()
        
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
        
    def on(self, event, handler=None, namespace=None):
        """ Register a channel handler as through decoration. """
        if handler:
            return self.register(event, handler, namespace)
        
        def decorator(f):
            self.register(event, f, namespace)
            return f
        return decorator
    
    def register(self, event, handler, namespace=None, **kwargs):
        """ Register a channel handler. """
        self._handlers[(event, namespace)] = handler
        
        if event in ('connect','disconnect'):
            # no overrides for these two.
            return
        
        if self._socket:
            # do not catch timeout error, raise it to fail
            with self._socket_lock:
                self._socket.on(event, handler, namespace)
        
    def remove_handler(self, event, handler=None, namespace=None):
        """ Remove a channel handler. """
        self._handlers.pop((event, namespace), None)
        
        namespace = namespace or '/'
        if self._socket:
            try:
                with self._socket_lock:
                    self._socket.handlers.get(namespace, {}).pop(event, None)
            except TimeoutError:
                self._logger.error(
                        f'Failed to remove handler from SocketIO - timed out.')
                
    def off(self, event, handler=None, namespace=None):
        return self.remove_handler(event, handler, namespace)
        
    def __iter__(self):
        raise NotImplementedError
        
    def _apply_metadata(self, sym):
        mdata = {}
        if len(self._master_data)>0 and sym in self._master_data.symbol:
            mdata = self._master_data[\
                    self._master_data.symbol==sym].iloc[0].to_dict()
            
        return mdata
    
    def _data_func_wrapper(self, callback):
        @wraps(callback)
        def decorated(*args, **kwargs):
            try:
                sym, timestamp, values = callback(*args, **kwargs)
            except (ValueError, TypeError):
                msg = 'data handler function must return a tuple'
                msg = msg + ' of (asset, timestamp, value)'
                raise DataSourceException(msg)
            else:
                # datapoint specific metadata
                metadata = {'symbol':sym, 'timestamp': timestamp}
                # symbol specific metadata
                mdata = self._apply_metadata(sym)
                # squash metadata including the general part
                metadata = {**self.metadata, **mdata, **metadata}
                
                # process the metadata and data inputs
                return self._finalizer(metadata, values)
        return decorated
    
    def _release_resources(self, close=True):
        if close:
            self.close()
    
    def _handle_error(self, msg, error=None, log=True, close=True, 
                            exception=False):
        if error:
            err_msg = str(error)
            if not err_msg:
                try:
                    err_msg = type(error).__name__
                except Exception:
                    pass
            msg = msg+f':{err_msg}'
        
        if log:
            self._logger.error(msg)
        
        if close:
            self._release_resources()
            
        if exception:
            raise DataSourceException(msg)
            
    def emit(self, event, payload, namespace=None, callback=None):
        """ emit some data to the server with optonal namespace and callback."""
        try:
            with self._socket_lock:
                self._socket.emit(event, payload, namespace, callback) # type: ignore
            return True
        except BadNamespaceError as e:
            # this means the socket is probably not connected anymore
            self._handle_error(
                    f"failed in send {payload}", error=e, close=True)
            return False
        except Exception as e:
            self._handle_error(
                    f"failed in send {payload}", error=e, close=False)
            return False
        
    def send(self, event, payload, namespace=None, callback=None):
        """ equivalent to emit. """
        self.emit(event, payload, namespace, callback)
    
    @set_event_on_finish()
    def connect(self, transports=None, namespaces={}, 
                socketio_path='socket.io', event=None):
        """ connect to a server, optionally with namespace(s). """
        
        if self.connected:
            self._logger.info(f'already connected to {self._root}')
            return
        
        if self._connection_status == ConnectionStatus.PENDING:
            self._logger.info(f'waiting for connection to {self._root}')
            time.sleep(self._timeout)
            if self.connected:
                self._logger.info(f'pending connection complete to {self._root}')
                return
        
        try:
            with self._socket_lock:
                self._logger.info(f'starting connection to {self._root}')
                self.error = None
                
                self._wants_to_close = False
                self._connection_status = ConnectionStatus.PENDING
                
                namespaces = namespaces or self._namespaces
                transports = transports or self._transports
                socketio_path = socketio_path if socketio_path != 'socket.io' \
                                    else self._socketio_path
            
            
                try:
                    session = requests.session()
                    session.proxies = self._proxies
                    
                    socket = SocketIO(
                            request_timeout=5*self._timeout,
                            http_session=session,
                            ssl_verify=self._verify)
                    ws_url = self._root
                    
                    # setup the callback handlers before connect, if any.
                    self._socket = socket
                    self._socket.on('connect', self.on_connect)
                    self._socket.on('disconnect', self.on_disconnect)
                    for key, callback in self._handlers.items():
                        event, namespace = key
                        if event in ('connect','disconnect'):
                            # they will be called within the handlers above.
                            continue
                        self._socket.on(event, callback, namespace)
                    
                    if self._url_auth:
                        ws_url = self._root+'?' + urlencode(self._url_auth)
                        socket.connect(ws_url, namespaces = namespaces,
                                       transports = transports,
                                       wait_timeout=5*self._timeout,
                                       socketio_path = socketio_path)
                    elif self._auth:
                        if self._auth_in_header:
                            headers = {**self._headers, **self._auth}
                            socket.connect(ws_url, headers=headers, 
                                           namespaces=namespaces, 
                                           transports = transports,
                                           wait_timeout=5*self._timeout,
                                           socketio_path = socketio_path)
                        else:
                            socket.connect(ws_url, headers=self._headers, 
                                           auth=self._auth,
                                           namespaces=namespaces, 
                                           transports = transports,
                                           wait_timeout=5*self._timeout,
                                           socketio_path = socketio_path)
                except ConnectionError as e:
                    self.error = e
                    self._socket = None
                    self._connection_status = ConnectionStatus.ABORTED
                    self._handle_error(
                            "failed to establish socketIO connection", 
                            error=e, exception=True)
                except Exception as e:
                    self.error = e
                    self._socket = None
                    self._connection_status = ConnectionStatus.ABORTED
                    self._handle_error(
                            "error during socketIO connect", error=e,
                            exception=True)
                else:
                    self.error = None
                    self._connection_status = ConnectionStatus.ESTABLISHED
                    self._logger.info(f'successfully started connection to {self._root}')
        except TimeoutError as e:
            self.error = e
            self._socket = None
            self._connection_status = ConnectionStatus.ABORTED
            self._handle_error(
                    "socketIO connection timed out", error=e, exception=True)
                    
    def on_connect(self):
        """ callback handler for `connect` event. """
        for key, callback in self._handlers.items():
            event, namespace = key
            if event == 'connect':
                callback()
                break
            
        # re-instate subscriptions, if any
        if self._subscriptions:
            payloads = self._subscriptions.copy()
            # reset sub list and add back fresh
            self._subscriptions = set()
            for payload in payloads:
                self._logger.info(
                        f'sending subscription request {payload}')
                try:
                    self.subscribe(dict(payload), validate=False)
                except Exception as e:
                    self._logger.info(
                        f'Failed sending subscription request: {str(e)}')
            
    def on_disconnect(self, msg=''):
        """ callback handler for `disconnect` event. """
        self._connection_status = ConnectionStatus.DISCONNECTED
        
        for key, callback in self._handlers.items():
            event, namespace = key
            if event == 'disconnect':
                callback()
                break
    
    def close(self):
        """ request a disconnect of the socketIO connection. """
        if not self._socket:
            return
        
        if not self._socket.connected:
            return
        
        if self._connection_status == ConnectionStatus.PENDING:
            time.sleep(self._timeout)
            if not self.connected:
                return
            
        last_status = self._connection_status
        try:
            with self._socket_lock:
                self._wants_to_close = True
                self._connection_status = ConnectionStatus.PENDING
            
                try:
                    if self._socket.connected:
                        self._socket.disconnect()
                        self._socket = None
                except Exception as e:
                    self._connection_status = last_status
                    self._handle_error(
                            "socketIO close failed", error=e, close=False)
                else:
                    self._logger.info(f'successfully disconnected from {self._root}')
                    self._connection_status = ConnectionStatus.DISCONNECTED
        except TimeoutError as e:
            self._connection_status = last_status
            self._handle_error("socketIO close timed out", error=e, close=False)
            
    def disconnect(self):
        """ equivalent to the method close. """
        self.close()
            
    def reconnect(self, event=None):
        try:
            with self._socket_lock:
                self.close()
                self.connect(event=event)
        except TimeoutError as e:
            self.error = e
            self._handle_error(
                    "socketIO reconnect timed out", error=e, exception=True)
            
    def subscribe(self, payload={}, validate=True):
        """
            Subscribe to a particular event stream. Subscription 
            can be either initiatied by a `emit` call from this 
            socket, or less usually, a separate API call to the 
            server. For the latter case, the argument `func` must 
            be a callable that accepts the current instance, the 
            channel name and the payload variables, and returns 
            either True or None on success. In case of subscription 
            through `emit`, the `command` variable will be used as 
            the first argument of the `emit` call. The second 
            argument will be `payload` or the channel if the payload 
            is None.
        """
        if validate and not self.connected:
            return self._handle_error(
                    'cannot subscribe, not connected', close=False)
            
        if not payload:
            self._logger.info('empty payload, nothing to subscribe.')
            return
        
        if 'event' not in payload:
            self._logger.info('missing event in payload, cannot subscribe.')
            return
        
        try:
            # event is a required key and assumed to be unique
            payload_save = frozenset(payload.items())
            event = payload.pop('event')
            channel = payload.pop('channel', self._DATA_CHANNEL)
            namespace = payload.pop('namespace', None)
            command = payload.pop('command','subscribe')
            callback = payload.pop('callback', None)
            
            if callback is None:
                callback = self._handlers.get((channel, namespace), noops)
            
            if channel == self._DATA_CHANNEL:
                # inject metadata for data callback.
                callback = self._data_func_wrapper(callback)
            
            if 'func' not in payload:
                payload = payload or event
                success = self.emit(command, payload)
            else:
                func = payload.pop('func')
                payload = payload or event
                success = func(self, command, payload)
            
            if success is True or success is None:
                self.on(event, callback, namespace)
        except Exception as e:
            self._handle_error(
                    f"failed in subscription {payload}", error=e, close=False)
        else:
            #existing_events = [dict(p)['event'] for p in self._subscriptions]
            #if dict(payload_save)['event'] not in existing_events:
            if payload_save not in self._subscriptions:
                    self._subscriptions.add(payload_save)
            self._logger.debug(f'subscribed successfully for payload {payload}.')

register_data_source('socketio', SocketIOSource)
