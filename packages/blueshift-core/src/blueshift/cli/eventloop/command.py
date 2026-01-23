from __future__ import annotations
from typing import Callable, cast
import zmq
import logging
import threading


from blueshift.interfaces.streaming import get_cmd_client, get_cmd_server, CmdRepServer
from blueshift.interfaces.logger import get_logger, BlueshiftLogger
from blueshift.lib.common.ctx_mgrs import MessageBrokerCtxManager
from blueshift.lib.exceptions import ValidationError

class CommandChannel():
    """ ZMQ loop for req-rep command processing. """
    def __init__(self, host:str, port:int, handler:Callable|None=None, timeout:int=10, 
                 blocking:bool=True, EOM:str='break', logger:BlueshiftLogger|None=None):
        self._wants_to_close:bool = False
        self._EOM = EOM
        
        if host == 'localhost':
            # for some reason it seems zmq cannot bind to localhost
            host = '127.0.0.1'
            
        if handler and not callable(handler):
            msg = f'Expected a callable for command handler.'
            raise ValidationError(msg)
        
        self._rep = get_cmd_server(
            host=host, port=port, handler=handler, no_block=not blocking, timeout=timeout, 
            EOM=self._EOM)
        
        self._req = get_cmd_client(host=host, port=port)
        self._req.connect(timeout = 1000*int(timeout))
        
        if not logger:
            logger = get_logger()
        self._logger = logger
        
    def stop(self):
        self._wants_to_close = True
        self._req.send_command(self._EOM)
        
    def listen(self):
        self._wants_to_close = False
        with MessageBrokerCtxManager(self._rep, enabled=True) as cmd:
            cmd = cast(CmdRepServer, self._rep)
            while not self._wants_to_close:
                try:
                    msg = cmd.process_command()
                    if msg == self._EOM:
                        self._wants_to_close = True
                        break
                except zmq.error.Again:
                    continue
                except Exception as e:
                    msg = f'Error in receiving zmq message:{str(e)}.'
                    self._logger.warning(msg)
                    
class CommandProcessor():
    """ A thread-based command channel handler. """
    def __init__(self, host:str='127.0.0.1', port:int=9000, 
                 handler:Callable|None=None, timeout:int=5, blocking:bool=True, EOM:str='break',
                 max_retries:int=5, logger:BlueshiftLogger|None=None):
        if not logger:
            logger = get_logger()
        self._logger = logger
        
        self._timeout = timeout
        self._max_retries = max_retries
        self._host = host
        self._port = port
        self._cmd = CommandChannel(
                host, port, handler=handler, timeout=timeout, 
                blocking=blocking, EOM=EOM, logger=logger)
        
        self._zmq_thread = None
        
    def __str__(self):
        return f'CommandProcessor@{self.host}:{self.port}]'
    
    def __repr__(self):
        return self.__str__()
        
    @property
    def host(self):
        return self._host
    
    @property
    def port(self):
        return self._port
    
    def _run(self):
        if self._cmd._wants_to_close:
            self._logger.error(f'Cannot start zmq thread - already shutting down.')
        
        try:
            self._cmd.listen()
        except Exception as e:
            self._logger.error(f'Failed in zmq thread:{str(e)}.')
    
    def start(self, logger:BlueshiftLogger|None=None):
        if logger:
            self._logger = logger
            self._cmd._logger = logger
            
        try:
            th = threading.Thread(target=self._run, name='CommandChannelListener')
            th.daemon = True
            self._zmq_thread = th
            self._zmq_thread.start()
        except Exception as e:
            msg = f'Failed to start {self}:{str(e)}.'
            self._logger.error(msg)
        else:
            msg = f'Started the command processer {self}.'
            self._logger.info(msg)
        
    def stop(self, *args, **kwargs):
        self._logger.warning(f'Shutting down command channel...')
        self._cmd.stop()
        # wait for the thread to join
        
        if not self._zmq_thread:
            # nothing to stop, no thread started
            return
        
        if self._zmq_thread.ident != threading.get_ident():
            if self._zmq_thread and self._zmq_thread.is_alive():
                self._zmq_thread.join(self._timeout)
                
            for i in range(self._max_retries+1):
                if self._zmq_thread and self._zmq_thread.is_alive():
                    self._zmq_thread.join(self._timeout)
                
            if self._zmq_thread and self._zmq_thread.is_alive():
                msg = f'Failed to stop the service {self}.'
                self._logger.error(msg)
            
            msg = f'Successfully stopped the service {self}.'
        else:
            msg = f'Successfully initiated stop for the service {self}.'
        
        self._logger.info(msg)
        
    def poll(self) -> bool:
        if self._zmq_thread and self._zmq_thread.is_alive():
            return True
        return False