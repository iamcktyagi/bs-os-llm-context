from __future__ import annotations
from typing import Type, Callable, Any
from abc import ABC, abstractmethod
from io import TextIOBase
import logging
import threading

from .plugin_manager import load_plugins

class StreamingPublisher(ABC):
    """ interface for streaming info from a running algo. """
    def isatty(self) -> bool:
        return False

    @property
    @abstractmethod
    def socket(self):
        raise NotImplementedError
    
    @property
    @abstractmethod
    def is_connected(self) -> bool:
        raise NotImplementedError
    
    @abstractmethod
    def connect(self, *args, **kwargs):
        """ open streaming connection. """
        raise NotImplementedError
    
    @abstractmethod
    def close(self, *args, **kwargs):
        """ close streaming connection. """
        raise NotImplementedError

    @abstractmethod
    def send(self, msg:str|dict, **kwargs):
        """ publish the message. """
        raise NotImplementedError
    
    @abstractmethod
    def send_to_topic(self, topic:str, msg:str|dict, **kwargs):
        """ publish the message to a specific topic. """
        raise NotImplementedError
    
class StreamingSubscriber:
    """ interface for streaming receiver for algo. """
    @property
    @abstractmethod
    def socket(self):
        raise NotImplementedError
    
    @property
    @abstractmethod
    def is_connected(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def connect(self):
        """ connect to specified socket. """
        raise NotImplementedError
            
    @abstractmethod
    def subscribe(self, topic):
        raise NotImplementedError
            
    @abstractmethod
    def unsubscribe(self, topic):
        raise NotImplementedError

    @abstractmethod
    def recv(self, *args, **kwargs) -> tuple[str, Any]|tuple[None, None]:
        raise NotImplementedError

    @abstractmethod
    def recv_all(self, *args, **kwargs):
        """ receive messages from the specified socket in a loop. """
        raise NotImplementedError

    @abstractmethod
    def close(self):
        """ close and clean up the connection. """
        raise NotImplementedError
    
class CmdReqClient:
    """Interface for dispatching commands to running algos. """
    @abstractmethod
    def connect(self, timeout=None):
        raise NotImplementedError

    @abstractmethod
    def send_command(self, strcmd=None) -> Any:
        """ send a command. current reads from the input device (stdin).
            command name is a name of a function that the algorithm can
            run (part of command API). Args a comma-separated list of
            arguments. Kwargs is a list of comma-separated keyword=value
            pairs.
        """
        raise NotImplementedError

    @abstractmethod
    def dispatch(self, msg:str):
        raise NotImplementedError

    @abstractmethod
    def close(self):
        """ close and clean up the connection. """
        raise NotImplementedError
    
class CmdRepServer:
    """Interface for receiving commands to running algos. """
    @abstractmethod
    def connect(self, timeout=None):
        raise NotImplementedError

    @abstractmethod
    def process_command(self) -> Any:
        """ waits on the socket and receive the next command. this is non-blocking. """
        raise NotImplementedError

    @abstractmethod
    def close(self):
        """ close and clean up the connection. """
        raise NotImplementedError
    
class IStreamHandler(ABC):
    """ interface for streaming source handler. """
    @property
    @abstractmethod
    def error(self) -> Exception|None:
        raise NotImplementedError
    
    @property
    @abstractmethod
    def is_running(self) -> bool:
        raise NotImplementedError
    
    @abstractmethod
    def run(self, func:Callable[[dict, dict],None]|None=None, event:threading.Event|None=None):
        raise NotImplementedError
    
    @abstractmethod
    def stop(self):
        raise NotImplementedError
    
    @abstractmethod
    def reset(self, event=None):
        raise NotImplementedError

_publisher_cls:Type[StreamingPublisher]|None = None
_subscriber_cls:Type[StreamingSubscriber]|None = None
_cmd_client_cls:Type[CmdReqClient]|None = None
_cmd_server_cls:Type[CmdRepServer]|None = None

def register_publisher(cls:Type[StreamingPublisher]):
    global _publisher_cls
    _publisher_cls = cls

def register_subscriber(cls:Type[StreamingSubscriber]):
    global _subscriber_cls
    _subscriber_cls = cls

def register_cmd_client(cls:Type[CmdReqClient]):
    global _cmd_client_cls
    _cmd_client_cls = cls

def register_cmd_server(cls:Type[CmdRepServer]):
    global _cmd_server_cls
    _cmd_server_cls = cls

def get_publisher(*args, **kwargs) -> StreamingPublisher:
    from inspect import getfullargspec

    if not _publisher_cls:
        try:
            load_plugins('blueshift.plugins.streaming')
        except Exception:
            pass

    if not _publisher_cls:
        raise NotImplementedError('publisher is not implemented')
    
    specs = getfullargspec(_publisher_cls.__init__)

    if specs.varkw:
        kw = kwargs.copy()
    else:
        kw = {}
        args_specs = specs.args
        for key in kwargs:
            if key in args_specs:
                kw[key] = kwargs[key]

    return _publisher_cls(*args, **kw)

def get_subscriber(*args, **kwargs) -> StreamingSubscriber:
    from inspect import getfullargspec

    if not _subscriber_cls:
        try:
            load_plugins('blueshift.plugins.streaming')
        except Exception:
            pass

    if not _subscriber_cls:
        raise NotImplementedError('subscriber is not implemented')
    
    specs = getfullargspec(_subscriber_cls.__init__)

    if specs.varkw:
        kw = kwargs.copy()
    else:
        kw = {}
        args_specs = specs.args
        for key in kwargs:
            if key in args_specs:
                kw[key] = kwargs[key]

    return _subscriber_cls(*args, **kw)

def get_cmd_client(*args, **kwargs) -> CmdReqClient:
    from inspect import getfullargspec

    if not _cmd_client_cls:
        try:
            load_plugins('blueshift.plugins.streaming')
        except Exception:
            pass

    if not _cmd_client_cls:
        raise NotImplementedError('req/rep client is not implemented')
    
    specs = getfullargspec(_cmd_client_cls.__init__)
    
    if specs.varkw:
        kw = kwargs.copy()
    else:
        kw = {}
        args_specs = specs.args
        for key in kwargs:
            if key in args_specs:
                kw[key] = kwargs[key]

    return _cmd_client_cls(*args, **kw)

def get_cmd_server(*args, **kwargs) -> CmdRepServer:
    from inspect import getfullargspec

    if not _cmd_server_cls:
        try:
            load_plugins('blueshift.plugins.streaming')
        except Exception:
            pass

    if not _cmd_server_cls:
        raise NotImplementedError('req/rep server is not implemented')
    
    specs = getfullargspec(_cmd_server_cls.__init__)

    if specs.varkw:
        kw = kwargs.copy()
    else:
        kw = {}
        args_specs = specs.args
        for key in kwargs:
            if key in args_specs:
                kw[key] = kwargs[key]

    return _cmd_server_cls(*args, **kw)

__all__ = [
    'StreamingPublisher',
    'StreamingSubscriber',
    'CmdReqClient',
    'CmdRepServer',
    'IStreamHandler',
    'register_publisher',
    'register_subscriber',
    'register_cmd_client',
    'register_cmd_server',
    'get_publisher',
    'get_subscriber',
    'get_cmd_client',
    'get_cmd_server',
    ]