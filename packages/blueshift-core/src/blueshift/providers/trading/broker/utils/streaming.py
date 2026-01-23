from threading import Event
import logging

from blueshift.lib.common.constants import CONNECTION_TIMEOUT, LARGE_TIMEOUT
from blueshift.providers.data.ingestor.stream_ingestor import StreamingData
from blueshift.lib.exceptions import ValidationError
from blueshift.lib.common.decorators import sliding_rate_limit
from blueshift.interfaces.logger import get_logger

class BrokerStreamingData:
    """
        A wrapper class that handles multiple StreamingData objects and 
        provides a consistent interface to them (`connect`, `reconnect`, 
        `disconnect` and `subscribe`). For all these methods, passing a 
        specific name affects that StreamingData objects. Else all are 
        affected.
    """
    def __init__(self, streamers:dict[str, StreamingData], logger:logging.Logger|None=None):
        for name in streamers:
            if not isinstance(streamers[name], StreamingData):
                msg = 'object in not of valid type.'
                msg = msg + ' Expected StreamingData.'
                raise ValidationError(msg)
        
        self._streamers = streamers
        self._disconnected_since = 0
        self._abort_reconnect = False
        
        if logger:
            self._logger = logger
        else:
            self._logger = get_logger()

    @property
    def is_connected(self, name=None):
        if name:
            streamer = self._streamers.get(name, None)
            if streamer:
                return streamer.is_connected
            else:
                return False
            
        status = [self._streamers[k].is_connected for k in self._streamers]
        return all(status)
    
    @property
    def logger(self):
        return self._logger
    
    @property
    def disconnected_since(self):
        return self._disconnected_since
    
    @disconnected_since.setter
    def disconnected_since(self, value):
        self._disconnected_since = value
        
    @property
    def abort_reconnect(self):
        return self._abort_reconnect
    
    @abort_reconnect.setter
    def abort_reconnect(self, value):
        self._abort_reconnect = value
        
    def reset_connection_abort(self):
        self._disconnected_since = 0
        self._abort_reconnect = False
    
    @sliding_rate_limit(1, CONNECTION_TIMEOUT, raise_exception=False, 
                        err_msg='Skipping re-connect.')
    def ensure_connected(self, timeout=LARGE_TIMEOUT):
        # the reconnect function already waits for the event, no need 
        # to wait here as well.
        if self.is_connected:
            return
        
        timeout = timeout or CONNECTION_TIMEOUT
        evt = Event()
        evt.clear()
        self.reconnect(event=evt, timeout=timeout)
        if not self.is_connected:
            msg = f'Failed to ensure connection in streaming.'
            
            for name in self._streamers:
                if self._streamers[name].error:
                    msg += f':{str(self._streamers[name].error)}'
                    break
            
            raise ConnectionError(msg)
    
    def connect(self, name=None, event=None, timeout=LARGE_TIMEOUT):
        if self.is_connected:
            if event:
                event.set()
            return
        
        if hasattr(self,'_initialized'):
            # broker will try to initialize data on connect
            self._initialized = False
        
        if name:
            streamer = self._streamers.get(name, None)
            if streamer:
                return streamer.connect(event=event)
            if event:
                event.set()
            return
        
        try:
            if event:
                evts = {name:Event() for name in self._streamers}
            else:
                evts = {name:None for name in self._streamers}
            
            for name in self._streamers:
                streamer = self._streamers[name]
                streamer.connect(event=evts[name])
                
            if event:
                for name, evt in evts.items():
                    if evt is None:
                        continue
                    self.logger.info(f'waiting for connect for {name}')
                    evt.wait(timeout=timeout)
                    self.logger.info(f'waiting for connect for {name} done')
                
        finally:
            if event:
                event.set()

    def reconnect(self, name=None, forced=False, event=None, 
                  timeout=LARGE_TIMEOUT):
        if self.is_connected:
            if event:
                event.set()
            return
        
        if hasattr(self,'_initialized'):
            # broker will try to initialize data on connect
            self._initialized = False
            
        if name:
            streamer = self._streamers.get(name, None)
            if streamer:
                return streamer.reconnect(forced=forced, event=event)
            
            if event:
                event.set()
            return
        
        try:
            if event:
                evts = {name:Event() for name in self._streamers}
            else:
                evts = {name:None for name in self._streamers}
                
            for name in self._streamers:
                streamer = self._streamers[name]
                streamer.reconnect(forced=forced, event=evts[name])
                
            if event:
                for name, evt in evts.items():
                    if evt is None:
                        continue
                    self.logger.info(f'waiting for reconnect for {name}')
                    evt.wait(timeout=timeout)
                    self.logger.info(f'waiting for reconnect for {name} done')
        finally:
            if event:
                event.set()

    def disconnect(self, name=None):
        if name:
            streamer = self._streamers.get(name, None)
            if streamer:
                return streamer.disconnect()
            return
        
        for name in self._streamers:
            streamer = self._streamers[name]
            streamer.disconnect()
            
        if hasattr(self,'_initialized'):
            # broker will try to initialize data on connect
            self._initialized = False


