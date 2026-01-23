import logging
import pandas as pd
import json
import types
import zmq
import time
import re

from blueshift.providers.streaming.zmq_stream import ZeroMQSubscriber
from blueshift.lib.common.sentinels import NOTHING, noops
from blueshift.lib.common.decorators import set_event_on_finish
from blueshift.lib.exceptions import DataSourceException
from blueshift.lib.common.ctx_mgrs import TimeoutRLock
from blueshift.lib.common.constants import CONNECTION_TIMEOUT
from blueshift.lib.common.enums import ConnectionStatus
from blueshift.interfaces.logger import get_logger
from blueshift.interfaces.data.source import DataSource, DataSourceType, register_data_source

class ZMQSource(DataSource):
    """
        Class defining the implementation of data source from ZMQ
        subscriber.
        
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
            Any 'on_data' function is automatically enriched
            with metadata - a common metadata part and a symbol specific 
            part. The user supplied on_data handler must return a tuple of 
            symbol, timestamp and data. This will be automatically converted 
            to a tuple of metadata and data (with metadata enriching). This 
            output can be consumed by a `finalizer` method - typically 
            supplied by an external ingestor.
            
            >> source = ZMQSource(root, auth, ..)
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
    
    def __init__(self, root, topic, port=None, frequency=None, 
                 master_data=pd.DataFrame(), parser = None, 
                 on_data= {}, timeout=5, logger=None):
        self._type = DataSourceType.ZMQ
        self._root = root
        self._frequency = frequency
        self._metadata = {'frequency':self.frequency}
        self._socket_lock = TimeoutRLock(timeout=CONNECTION_TIMEOUT)
        
        roots = root.split(':')
        try:
            if len(roots) == 2:
                root = roots[0]
                port = int(roots[1])
            elif len(roots) > 2:
                root = roots[1]
                port = int(roots[2])
        except Exception:
            raise ValueError(f'Illegal root {root}, must be host:port')
        
        self._host = root
        self._port = port
        
        self._topic = topic
        self._timeout = timeout
        self._connection_status = ConnectionStatus.UNSET
        self._subscriber = None
        self._parser = parser
        self._handlers = {}
        
        if logger:
            self._logger = logger
        else:
            self._logger = get_logger()
        
        if callable(on_data):
            handlers = {self._DATA_CHANNEL:on_data}
            self._handlers[self._topic] = handlers
        elif isinstance(on_data, dict):
            for channel, func in on_data.items():
                self.register(channel, func)
        else:
            self._logger.error('illegal handler type, will be ignored.')
            
        self._master_data = master_data
            
        self._reply = False
        self._reply_on_connect = False
        self._wants_to_close = False
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
        flag = self._subscriber is not None and \
            self._connection_status == ConnectionStatus.ESTABLISHED
        
        return flag
    
    @property
    def data_channel(self) -> str|None:
        return self._DATA_CHANNEL
        
    @property
    def metadata(self):
        """ Returns metadata about the source. """
        return self._metadata
        
    def reset(self):
        """ Close current connection and remove all subscriptions. """
        self.close()
        
    def add_masterdata(self, master_data):
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
        
    def __iter__(self):
        return self._generator()
    
    def _generator(self):
        while self.connected and not self._wants_to_close:
            for data in self._consume():
                yield data
        
    def _apply_metadata(self, sym):
        mdata = {}
        if len(self._master_data)>0 and sym in self._master_data.symbol:
            mdata = self._master_data[\
                    self._master_data.symbol==sym].iloc[0].to_dict()
            
        return mdata
    
    def on(self, channel):
        """ Register a channel handler as through decoration. """
        def decorator(f):
            self.register(channel, f)
            return f
        return decorator
    
    def register(self, channel, handler, topic=None, **kwargs):
        """ Register a channel handler. """
        if channel != self._DATA_CHANNEL:
            channel = re.compile(channel)
            
        topic = topic or self._topic
        if topic not in self._handlers:
            self._handlers[topic] = {}
        self._handlers[topic][channel] = handler
        
    def remove_handler(self, channel, handler, topic=None):
        """ Remove a channel handler. """
        channel = re.compile(channel)
        
        topic = topic or self._topic
        if topic not in self._handlers:
            return
        
        if channel in self._handlers[topic]:
            self._handlers[topic].pop(channel)
    
    def _release_resources(self, close=True):
        if close:
            self.close()
    
    def _handle_error(self, msg, error=None, log=True, close=True, 
                            exception=False):
        if error:
            msg = msg+f':{str(error)} ({type(error)}).'
        
        if log:
            self._logger.error(msg)
        
        if close:
            self._release_resources()
            
        if exception:
            raise DataSourceException(msg)
        
    def send(self, event, payload, namespace=None, callback=None):
        """ ZMQ sub is a one-way protocol. """
        pass
    
    @set_event_on_finish()
    def _connect(self, event=None):
        """ connect to a pub server with the topic. """
        self.error = None
        if self.connected:
            return
        
        if self._connection_status == ConnectionStatus.PENDING:
            time.sleep(self._timeout)
            if self.connected:
                return
        
        self._wants_to_close = False
        self._connection_status = ConnectionStatus.PENDING
        
        try:
            self._subscriber = ZeroMQSubscriber(
                    self._host, self._port, self._topic)
            self._subscriber.connect()
        except zmq.ZMQError as e:
            self.error = e
            self._subscriber = None
            self._connection_status = ConnectionStatus.ABORTED
            self._handle_error(
                    "failed to establish ZMQ sub connection", error=e,
                    exception=True)
        except Exception as e:
            self.error = e
            self._subscriber = None
            self._connection_status = ConnectionStatus.ABORTED
            self._handle_error(
                    "error during zmq sub connect", error=e,
                    exception=True)
        else:
            self.error = None
            self._connection_status = ConnectionStatus.ESTABLISHED
            self._logger.info(f'successfully started connection to {self._root}')
                            
    def connect(self, event=None):
        try:
            with self._socket_lock:
                self._connect(event=event)
                for topic in self._handlers:
                    handlers = self._handlers[topic]
                    for pattern, callback in handlers.items():
                        if pattern == self._DATA_CHANNEL:
                            continue
                        if pattern.match('on_connect'):
                            try:
                                callback()
                            except Exception as e:
                                self._logger.error(
                                    f'failed to run callback on connect:{str(e)}')
        except TimeoutError as e:
            self._handle_error(
                    "error during zmq sub connect - timed out", error=e,
                    exception=True)
    
    def _close(self):
        """ request a disconnect of the ZMQ sub connection. """
        if not self._subscriber:
            return
        
        if self._connection_status == ConnectionStatus.PENDING:
            time.sleep(self._timeout)
            if not self.connected:
                return
            
        self._wants_to_close = True
        last_status = self._connection_status
        self._connection_status = ConnectionStatus.PENDING
        
        try:
            self._subscriber.close()
            self._subscriber = None
        except Exception as e:
            self._connection_status = last_status
            self._handle_error(
                    "ZMQ close failed", error=e, close=False)
        else:
            self._connection_status = ConnectionStatus.DISCONNECTED
            self._logger.info(f'successfully disconnected from {self._root}')
                        
    def close(self):
        try:
            with self._socket_lock:
                self._close()
                for topic in self._handlers:
                    handlers = self._handlers[topic]
                    for pattern, callback in handlers.items():
                        if pattern == self._DATA_CHANNEL:
                            continue
                        if pattern.match('on_disconnect'):
                            try:
                                callback()
                            except Exception as e:
                                self._logger.error(
                                        f'failed to run callback on disconnect:{str(e)}')
        except TimeoutError:
            self._logger.error(f'failed to disconnect zmq: timed out')
            
    def disconnect(self):
        """ equivalent to the method close. """
        self.close()
            
    def reconnect(self, event=None):
        self._close()
        self._connect(event=event)
        
        for topic in self._handlers:
            handlers = self._handlers[topic]
            for pattern, callback in handlers.items():
                if pattern == self._DATA_CHANNEL:
                    continue
                if pattern.match('on_reconnect'):
                    try:
                        callback()
                    except Exception as e:
                        self._logger.error(
                                f'failed to run callback on disconnect:{str(e)}')
            
    def subscribe(self, topic):
        """ ZMQ subscriber source does not support subscription. """
        if not self._subscriber:
            raise DataSourceException(f'subscriber not connected.')
        
        if not isinstance(topic, str):
            raise DataSourceException(f'Expected a string (topic).')
        
        self._subscriber.subscribe(topic)
        
    def unsubscribe(self, topic):
        if not self._subscriber:
            raise DataSourceException(f'subscriber not connected.')
        
        if not isinstance(topic, str):
            raise DataSourceException(f'Expected a string (topic).')
            
        self._subscriber.unsubscribe(topic)
        self._handlers.pop(topic, None)
    
    def _dispatch(self, topic, channel, data):
        if not channel:
            return self._blank
            
        if topic not in self._handlers:
            return self._blank
        
        handlers = self._handlers[topic]
        if channel == self._DATA_CHANNEL and self._DATA_CHANNEL in handlers:
            try:
                sym, timestamp, values = handlers[self._DATA_CHANNEL](data)
            except (ValueError, TypeError):
                msg = 'data handler function must return a tuple'
                msg = msg + ' of (asset, timestamp, value)'
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
        
        for pattern, callback in handlers.items():
            if pattern == self._DATA_CHANNEL:
                continue
            if pattern.match(channel):
                try:
                    callback(data)
                except Exception as e:
                    msg = f'Failed running callback at zmq stream:{str(e)}'
                    self._logger.error(msg)
                
        return self._blank
    
    def _default_parser(self, msg):
        if not isinstance(msg, dict):
            return self._DATA_CHANNEL, msg
        
        if 'event' in msg:
            # structure is {'event':'name','payload':{...}}
            event = msg.pop('event')
            if 'payload' in msg:
                msg = msg['payload']
            if event in ('data', 'DATA'):
                event = self._DATA_CHANNEL
            return event, msg
        
        return self._DATA_CHANNEL, msg
    
    def _consume(self):
        """ for data push from server. """
        if not self.connected:
            self.connect()

        if not self._subscriber:
            raise DataSourceException(f'subscriber not connected.')
            
        try:
            topic, r = self._subscriber.recv()
            if not isinstance(r, dict):
                try:
                    r = json.loads(r)  # type: ignore
                except Exception:
                    pass
            
            if self._parser:
                packets = self._parser(r)
            else:
                packets = self._default_parser(r)
            
            try:
                if isinstance(packets, types.GeneratorType):
                    for channel, data in packets:
                        yield self._dispatch(topic, channel, data)
                else:
                    channel, data = packets
                    yield self._dispatch(topic, channel, data)
            except (TypeError, ValueError):
                msg = 'parser must return a tuple of (channel, data)'
                msg = msg + ', or a generator that can produce them.'
                raise DataSourceException(msg)
        except zmq.ZMQError as e:
            self._handle_error(
                    "error receiving packet", error=e, close=False)
        except DataSourceException as e:
            raise e
        except Exception as e:
            self._handle_error(
                    "error while receiving packet", error=e, close=False)

register_data_source('zmq', ZMQSource)
