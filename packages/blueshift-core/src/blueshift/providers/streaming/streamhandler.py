from typing import Callable, Any
import threading
import asyncio
import sys
from functools import partial
import logging

from blueshift.lib.common.ctx_mgrs import AsyncCtxMgrWithHooks
from blueshift.lib.common.sentinels import noops
from blueshift.lib.common.platform import get_exception
from blueshift.lib.exceptions import AuthenticationError
from blueshift.lib.common.ctx_mgrs import TimeoutRLock
from blueshift.lib.common.enums import ConnectionStatus
from blueshift.config import LOCK_TIMEOUT
from blueshift.interfaces.data.source import DataSource, DataSourceType

_exc_raise = (asyncio.TimeoutError, AuthenticationError)

class StreamHandler:
    """
        A class to handle streaming source data. This requries a streaming 
        source as input. You can invoke a run (start reading the source 
        output) by calling the `run` method, and stop calling the `stop` 
        method. This will handle required re-start/ re-connect behaviour 
        and run the source generator in a separate thread.
        
        Example:
            >> source = WSSource(root, auth, ..)
            >> handler = StreamHandler(source)
            >> # the function arguments must match the generator
            >> # output of the source stream
            >> def f(metadata, data):
            ..    sym = metadata.pop('symbol')
            ..    timestamp = metadata.pop('timestamp')
            ..    print(f'{sym}:{timestamp}:{data}')
            >> handler.run(f)
            >> # to stop the source generator, invoke stop
            >> handler.stop()
            
    """
    
    def __init__(self, source:DataSource, timeout:int|None=None, max_retries:int|None=None, logger:logging.Logger|None=None):
        self._thread = None
        self._wants_to_close = False
        self._max_retries = max_retries
        self._exception = None
        self._connection_event = threading.Event()
        self._lock = TimeoutRLock(timeout=LOCK_TIMEOUT)
        
        if not isinstance(source, DataSource):
            raise TypeError(f'source must be of type DataSource, got {type(source)} {source}.')
        if source._type not in (
                DataSourceType.WEBSTREAM, DataSourceType.SOCKETIO,
                DataSourceType.ZMQ, DataSourceType.UNDEFINED):
            raise TypeError(f'source must be of type streaming DataSource, got {type(source)} {source}.')
        self._source = source
        
        if timeout:
            self._timeout = timeout
        else:
            self._timeout = source.timeout
        
        if logger:
            self._logger = logger
        else:
            self._logger = source.logger
            
    def __str__(self):
        return f"Blueshift StreamingHandler({str(self.type)})"
    
    def __repr__(self):
        return self.__str__()
            
    @property
    def type(self):
        if self._source is not None:
            return self._source.type
        
    @property
    def error(self):
        return self._source.error
            
    @property
    def is_running(self):
        if self._source.type == DataSourceType.UNDEFINED:
            return True
        
        if self._source.type == DataSourceType.SOCKETIO:
            return self._source.connected
        
        socket_running = self._source.connection_status in [
                ConnectionStatus.PENDING, ConnectionStatus.ESTABLISHED]
        thread_running = self._thread and self._thread.is_alive()
        
        return socket_running and thread_running
        
    def run(self, func:Callable[..., None]|None=None, event=None):
        """ run the streaming loop. """
        if self._source.type == DataSourceType.UNDEFINED:
            return
        
        self._logger.info(f'starting streaming loop@{self._source} connect routine.')
        with self._lock:
            if self._source._type == DataSourceType.SOCKETIO:
                # socketIO starts the engineIO loop on connect automatically.
                self._wants_to_close = False
                if self._source.connected:
                    if event:event.set()
                    self._logger.info(f'socketio loop@{self._source} already running, stop to restart.')
                    return
                else:
                    self._logger.info(f'connecting to socketio loop@{self._source}.')
                    if func:
                        self._source.finalizer = func
                    return self._source.connect(event=event)
            
            if self.is_running:
                if event:event.set()
                self._logger.info(f'streaming loop@{self._source} already running, stop to restart.')
                return
            
            self._logger.info(f'preparing to start stream@{self._source} iteration in the background.')
            
            if func is None:
                func = noops
            if not callable(func):
                if event:event.set()
                raise TypeError(f'function argument must be a callable.')
            self._func = func
            self._exception = None
            self._connection_event.clear()
            
            if hasattr(self._source, '__aiter__'):
                self._thread = threading.Thread(
                        target=self._run_async, name='AsyncStreamingSourceHandler', 
                        kwargs={'event':event})
            else:
                self._thread = threading.Thread(
                        target=self._run, name='StreamingSourceHandler', 
                        kwargs={'event':event})
            
            self._thread.daemon = True
            self._thread.start()
            self._logger.info(f'waiting for connection event @{self._source}')
            self._connection_event.wait(timeout=self._timeout)
            self._logger.info(f'connection event for {self._source} done')
            if self._exception:
                raise self._exception
            self._logger.info(f'started stream iteration@{self._source} in the background.')
        
    def stop(self):
        """ stop the streaming loop and disconnect. """
        if self._source.type == DataSourceType.UNDEFINED:
            return
        
        self._logger.info(f'preparing to stop stream iteration@{self._source}.')
        
        with self._lock:
            self._wants_to_close = True
            
            if self._source._type == DataSourceType.SOCKETIO:
                return self._source.close()
            
            self._logger.info(f'initiated stream iteration stop@{self._source}.')
        
    def reset(self, event=None):
        """ stop and start the streaming loop. """
        if self._source.type == DataSourceType.UNDEFINED:
            return
        
        self._logger.info(f'preparing to reset stream connection@{self._source}.')
        
        with self._lock:
            self._logger.info(f'stopping stream loop@{self._source} before reconnect.')
            self.stop()
            
            if self._thread and self._thread.is_alive():
                self._wants_to_close = True # it should already be True
                self._thread.join(timeout=2*self._timeout)
                if self._thread.is_alive():
                    self._logger.error(f'streaming loop@{self._source} disconnect timed out.')
            
            self.run()
            self._logger.info(f'successfully reconnected in streaming loop@{self._source}.')
        
    def _run(self, event=None):
        self._wants_to_close = False
        retries_left = 0 if self._max_retries is None else self._max_retries
        while True:
            if self._wants_to_close:
                self._connection_event.set()
                if event:event.set()
                break
            try:
                self._loop_fn(self._func, event=event)
            except Exception as e:
                self._connection_event.set()
                msg = f'failed in streaming thread@{self._source}:{str(e)}'
                self._logger.error(msg)
                if isinstance(e, _exc_raise):
                    self._exception = e
            finally:      
                self._connection_event.set()
                if retries_left <= 0 or self._exception:
                    # exceeded max retries
                    msg = f'retry failed in streaming loop@{self._source}, try connecting later:'
                    if self._exception:
                        msg += f' {str(self._exception)}'
                    else:
                        msg += f' Retry exhausted.'
                    self._logger.info(msg)
                    break
                else:
                    # try to restart 
                    msg = f'retrying in streaming loop@{self._source} (retries left {retries_left})'
                    self._logger.info(msg)
                    retries_left = retries_left - 1
                    
        self._logger.info(f'exited streaming loop@{self._source}.')
    
    def _run_async(self, event=None):
        """ run async generator with restart. """
        retries_left = 0 if self._max_retries is None else self._max_retries
        self._wants_to_close = False
            
        while True:
            if self._wants_to_close:
                # we want to quit, cancel task and wait a while
                self._connection_event.set()
                if event:event.set()
                break
            try:
                asyncio.run(self._async_loop_fn(self._func, event=event))
            except Exception as e:
                msg = f'failed in websocket thread@{self._source}:{str(e)}'
                self._logger.error(msg)
                self._connection_event.set()
                if isinstance(e, _exc_raise):
                    self._exception = e
            finally: 
                self._connection_event.set()
                if self._wants_to_close:
                    break
                elif retries_left <= 0 or self._exception:
                    # exceeded max retries
                    msg = f'retry failed in streaming loop @{self._source}, try connecting later:'
                    if self._exception:
                        msg += f' {str(self._exception)}'
                    else:
                        msg += f' Retry exhausted.'
                    self._logger.error(msg)
                    break
                else:
                    # try to restart 
                    msg = f'retrying in streaming loop@{self._source} (retries left {retries_left})'
                    self._logger.info(msg)
                    retries_left = retries_left - 1
                    
        self._logger.info(f'exited streaming loop@{self._source}.')
        
    def _loop_fn(self, func, event=None):
        """ function to handle a regular generator source. """
        if self._wants_to_close:
            self._connection_event.set()
            if event:event.set()
            return
        
        if not self._source.connected:
            self._source.connect(event=event)
            self._connection_event.set()
            
        if event:event.set()
        self._connection_event.set()
        try:
            for metadata, data in self._source:
                if self._wants_to_close:
                    break
                func(metadata, data)
        finally:
            try:
                self._source.close()
            except Exception:
                pass
    
    async def _async_loop_fn(self, func, event=None):
        """ function to handle a async generator source. """
        if self._wants_to_close:
            self._connection_event.set()
            if event:event.set()
            return
        
        preop = partial(self._source.aconnect, event=event)
        postop=self._source.aclose
        
        try:
            loop = asyncio.get_running_loop()
            loop.set_exception_handler(self.handle_loop_exception)
            self._source.loop = loop
            
            async with AsyncCtxMgrWithHooks(preop=preop, postop=postop):
                self._connection_event.set()
                if event:event.set()
                await asyncio.sleep(1)
                async for metadata, data in self._source: # type: ignore
                    if self._wants_to_close:
                        break
                    func(metadata, data)
        except asyncio.CancelledError:
            self._wants_to_close = True
        except RuntimeError as e:
            self._logger.error(f'runtime error in streaming data@{self._source}:{str(e)}')
            exc_type, exc_value, exc_traceback = sys.exc_info()
            err_msg = get_exception(exc_type, exc_value, exc_traceback)
            self._logger.info(err_msg)
            # for runtime error, retrying does not make sense
            self._wants_to_close = True
        except Exception as e:
            self._logger.error(f'error in streaming data@{self._source}:{str(e)}')
            exc_type, exc_value, exc_traceback = sys.exc_info()
            err_msg = get_exception(exc_type, exc_value, exc_traceback)
            self._logger.info(err_msg)
            raise e
        finally:
            self._source.loop = None
            
    def handle_loop_exception(self, loop, context):
        exp = context.get('exception')
        if not exp:
            return
        
        exc_type, exc_value, exc_traceback = (type(exp), exp, exp.__traceback__)
        msg = f'Streaming loop @{self._source} encountered an error: {str(exp)}.'
        err_msg = get_exception(exc_type, exc_value, exc_traceback)
        msg = msg + ' \n' + err_msg
        self._logger.info(msg)