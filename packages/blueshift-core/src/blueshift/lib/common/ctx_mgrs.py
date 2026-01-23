from __future__ import annotations
from typing import Callable, TYPE_CHECKING
from sys import path as sys_path
from os import path as os_path
from threading import Lock, RLock
from contextlib import AbstractContextManager

if TYPE_CHECKING:
    from logging import Logger
    from io import TextIOBase
    from blueshift.interfaces.streaming import (
        StreamingPublisher, StreamingSubscriber, CmdReqClient, CmdRepServer)

class nullcontext(AbstractContextManager):
    """
        Python 3.7+ null context implementation from contextlib.
    """

    def __init__(self, enter_result=None):
        self._enter_result = enter_result

    def __enter__(self):
        return self._enter_result

    def __exit__(self, *excinfo):
        pass

class AddPythonPath():
    '''
        A context manager to modify sys path with a clean up after
        we are done with the work.
    '''
    def __init__(self, path:str):
        self._path = None
        if path:
            self._path = os_path.expanduser(path)
        
    def __enter__(self):
        '''
            Add the path to SYS PATH at the end.
        '''
        if self._path:
            sys_path.append(self._path)
        return self
    
    def __exit__(self, *args):
        '''
            Remove the last entry from SYS PATH, if any.
        '''
        if self._path and self._path in sys_path:
            idx = [i for i in range(len(sys_path)) \
                   if sys_path[i]==self._path][-1]
            sys_path.pop(idx)
                    

class CtxMgrWithHooks():
    '''
        A context manager with entry and exit hooks.
    '''
    def __init__(self, preop:Callable|None=None, postop:Callable|None=None, *args, **kwargs):
        from blueshift.lib.common.sentinels import noop

        self.preop = preop if preop else noop
        self.postop = postop if postop else noop
        self.args = args
        self.kwargs=kwargs
    
    def __enter__(self):
        return self.preop(*(self.args), **(self.kwargs))
    
    def __exit__(self, *args):
        return self.postop(*(self.args), **(self.kwargs))
    
class AsyncCtxMgrWithHooks():
    '''
        Async context manager with entry and exit hooks.
    '''
    def __init__(self, preop:Callable|None=None, postop:Callable|None=None, *args, **kwargs):
        from blueshift.lib.common.sentinels import async_noop

        self.preop = preop if preop else async_noop
        self.postop = postop if postop else async_noop
        self.args = args
        self.kwargs=kwargs
    
    async def __aenter__(self):
        return await self.preop(*(self.args), **(self.kwargs))
    
    async def __aexit__(self, *args):
        return await self.postop(*(self.args), **(self.kwargs))
    
    
class MessageBrokerCtxManager():
    '''
        A context manager for handling ZMQ sockets creation and 
        clean-up.
    '''
    
    def __init__(self, message_broker:StreamingPublisher|StreamingSubscriber|CmdRepServer|CmdReqClient, 
                 enabled:bool=False):
        self._message_broker = message_broker
        self._enabled = enabled
        if not message_broker:
            self._enabled = False
    
    def __enter__(self):
        if not self._enabled:
            return None
        
        self._message_broker.connect()
        return self._message_broker
    
    def __exit__(self, *args):
        if self._enabled:
            self._message_broker.close()

class StdioRedirect:
    '''
        A context manager with stdio and stderr redirect.
    '''
    def __init__(self, io_stream:TextIOBase|str|None=None, err_stream:TextIOBase|str|None=None, 
                 logger:Logger|None=None, close=True):
        msg = "input must be a file like object or stream."
        self.enabled = False
        self._open_stdout_file = False
        self._open_stderr_file = False
        self._logger = logger
        self._close = close
        
        if io_stream is not None:
            if isinstance(io_stream, str):
                self._open_stdout_file = True
            elif not self.is_stream(io_stream):
                raise IOError(msg)
            self._io_stream = io_stream
            self.enabled = True
            
            if isinstance(err_stream, str):
                self._open_stderr_file = True
            elif err_stream is not None:
                if not self.is_stream(err_stream):
                    raise IOError(msg)
            self._err_stream = err_stream
        
        self._stdout = None
        self._stderr = None
    
    @classmethod
    def is_stream(cls, stream):
        if hasattr(stream,"writable"):
            return stream.writable()
        return False
    
    def __enter__(self):
        if self.enabled:
            import sys
            
            if self._io_stream:
                if self._open_stdout_file:
                    self._io_stream = open(self._io_stream, 'a') # type: ignore
                self._stdout = sys.stdout
                sys.stdout = self._io_stream
                
            if self._err_stream:
                if self._open_stderr_file:
                    self._err_stream = open(self._err_stream, 'a') # type: ignore
                self._stderr = sys.stderr
                sys.stderr = self._err_stream
            elif self._io_stream:
                self._stderr = sys.stderr
                sys.stderr = self._err_stream
        
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        if self.enabled:
            import sys
            import traceback as tb
            import io
            from contextlib import closing
            from blueshift.lib.exceptions import BlueshiftException
            
            exc_type, exc_value, exc_traceback = sys.exc_info()
            if exc_type and not issubclass(exc_type, BlueshiftException):
                try:
                    with closing(io.StringIO()) as error_string:
                        tb.print_exception(
                                exc_type, exc_value, exc_traceback,
                                file=error_string)
                        err_msg = error_string.getvalue()
                        if self._logger:
                            self._logger.error(err_msg)
                        else:
                            print(err_msg, file=sys.stderr)
                except Exception:
                    pass
            
            if self._io_stream and self._open_stdout_file and self._close:
                self._io_stream.close() # type: ignore
            if self._err_stream and self._open_stderr_file and self._close:
                self._err_stream.close() # type: ignore
            
            if self._stdout:
                sys.stdout = self._stdout
            if self._stderr:
                sys.stderr = self._stderr
        
class TimeoutLock:
    """
        A context manager with entry and exit for Locks with 
        defined timeout.
    """
    def __init__(self, timeout:int|None=None, lock:RLock|None=None):
        if lock is None:
            self._lock = Lock()
        else:
            self._lock = lock
            
        self._timeout = timeout
        self._acquired = False
    
    def __enter__(self):
        if self._timeout:
            status = self._lock.acquire(timeout=self._timeout)
        else:
            status = self._lock.acquire()
        
        if status:
            self._acquired = True
            return self._lock
        else:
            raise TimeoutError('failed to acquire lock')
            
    @property
    def locked(self):
        return self._acquired
    
    def __exit__(self, *args):
        try:
            self._lock.release()
            self._acquired = False
        except Exception:
            pass
            
        return
    
class TimeoutRLock:
    """
        A context manager with entry and exit for Locks with 
        defined timeout.
    """
    def __init__(self, timeout:int|None=None, lock:RLock|None=None):
        if lock is None:
            self._lock = RLock()
        else:
            self._lock = lock
            
        self._timeout = timeout
        self._acquired = False
    
    def __enter__(self):
        if self._timeout:
            status = self._lock.acquire(timeout=self._timeout)
        else:
            status = self._lock.acquire()
        
        if status:
            self._acquired = True
            return self._lock
        else:
            raise TimeoutError('failed to acquire lock')
            
    @property
    def locked(self):
        return self._acquired
    
    def __exit__(self, *args):
        try:
            self._lock.release()
            self._acquired = False
        except Exception:
            pass

class StringStack():
    """ a context manager for manageing a string stack """
    def __init__(self, string_list:list[str], current:str, target):
        self._stack = string_list
        self._str = current
        self._target = target

    def __enter__(self):
        '''
            Add the string to stack.
        '''
        self._stack.append(self._str)
        self._target._current_frame = self._str
        return self

    def __exit__(self, *args):
        """ we are done, pop the stack """
        _str = self._stack.pop()
        #assert _str == self._str, "Assert failed in stack management."
        #assert len(self._stack) > 0, "Stack length is zero"
        if _str != self._str:
            raise ValueError("stack is corrupted.")
        if len(self._stack) <= 0:
            raise ValueError("stack length is zero")
        self._target._current_frame = self._stack[-1]

    def top(self):
        """ return the top most item from the stak """
        #assert len(self._stack) > 0, "Stack length is zero"
        if len(self._stack) <= 0:
            raise ValueError("stack length is zero")
        return self._stack[-1]
