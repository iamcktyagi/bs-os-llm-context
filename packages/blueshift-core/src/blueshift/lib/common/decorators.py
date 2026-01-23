from __future__ import annotations
from typing import Callable, Literal, Type, Any, TYPE_CHECKING
import os
from functools import wraps
import time
import asyncio
import threading
from concurrent.futures import Executor
from logging import Logger

if TYPE_CHECKING:
    import pandas as pd
    from io import TextIOBase
else:
    import blueshift.lib.common.lazy_pandas as pd


def wrap_with_kwarg(f:Callable, name:str) ->Callable:
    """
        wrap a callable to handle an extra keyword argument. If the callable 
        `f` declares the parameter `name` in its signature, the `name` param 
        is passed while calling, else it is skipped.
    """
    from inspect import signature
    keyword = False
    
    try:
        sig = signature(f)
        for param in sig.parameters.values():
            if param.kind in (param.POSITIONAL_OR_KEYWORD, param.KEYWORD_ONLY) \
                and param.name==name:
                keyword = True
                break
            elif param.kind == param.VAR_KEYWORD:
                keyword = True
                break
    except Exception:
        # may fail for built-ins or ctypes
        pass
    
    @wraps(f)
    def wrapped(*args, **kwargs):
        if keyword and name in kwargs:
            param = {name:kwargs.get(name)}
            return f(*args, **param)
        return f(*args)
    
    return wrapped

def diskcache(cached:bool=True, cachedir:str='~/.blueshift/__cache__', 
              ttl:int|Literal['day']='day') -> Callable:
    """ 
        Disk-based caching decorator with auto-expiry. The argument 
        `ttl` can be an integer for time-to-live in seconds, else 'day' 
        for daily expiry. Set ttl=0 for no expiry.
        
        Args:
            `cached (bool)`: Set caching to True.
            `cachedir (str)`: Path to cache directory.
            `ttl (str or int)`: Time in seconds or 'day' for daily expiry.
    """
    import hashlib
    import bz2
    import _pickle as cPickle

    xtn = '.pbz2'
    path = os.path.abspath(os.path.expanduser(cachedir))
    path_maps = {}
    
    def key_func(f, *args, **kwargs):
        # we assume none of the objects can fail on __str__ call
        key = f.__name__
        for arg in args:
            key = key + str(arg)
        for kw in kwargs:
            key = key + str(kw) + str(kwargs[kw])
            
        digest = hashlib.sha256(key.encode('utf-8')).hexdigest()
        digest = digest[:32] + '__'
            
        if ttl:
            if ttl=='day':
                key2 = pd.Timestamp.now().normalize().strftime('%Y-%m-%d')
            else:
                key2 = str(int(time.time()//ttl)) # type: ignore
            key = digest + key2
        else:
            key = digest
            
        return digest, key
    
    def get_fname(key, full=True):
        if full:
            return os.path.join(path, key+xtn)
        else:
            return key+xtn
        
    def remove_expired_keys(f, *args, **kwargs):
        if not ttl:
            return
            
        digest, current_key = key_func(f, *args, **kwargs)
        targets = [k for k in os.listdir(path) if k.startswith(digest)]
        expired_keys = [k for k in targets if k!=get_fname(
                current_key,full=False)]
        
        for key in expired_keys:
            try:
                os.remove(os.path.join(path, key))
            except Exception:
                pass
    
    def decorator(f) -> Callable:
        @wraps(f)
        def decorated(*args, **kwargs):
            if not os.path.exists(path):
                os.makedirs(path)
            digest, key = key_func(f, *args, **kwargs)
            fname = os.path.join(path, key+xtn)
            
            if f.__name__ in path_maps:
                path_maps[f.__name__].add(fname)
            else:
                path_maps[f.__name__] = set([fname])
            
            if not os.path.exists(fname) or not cached:
                remove_expired_keys(f, *args, **kwargs)
                data = f(*args, **kwargs)
                with bz2.BZ2File(fname, 'w') as fp:
                    cPickle.dump(data, fp)
                return data
            else:
                with bz2.BZ2File(fname, 'rb') as fp:
                    data = cPickle.load(fp)
                return data
            
        def cache_clear(*args, **kwargs):
            all_keys = path_maps.get(f.__name__, set([]))
            if args or kwargs:
                # if we have args or kwargs, match the key to delete
                digest, _ = key_func(f, *args, **kwargs)
                targets = [k for k in os.listdir(path) if k.startswith(
                        digest)]
                targets = set([os.path.join(path,k) for k in targets])
            else:
                # else delete all keys
                targets = path_maps.get(f.__name__, set([]))
                
            removed = set([])
            for fname in targets:
                try:
                    os.remove(fname)
                    removed.add(fname)
                except Exception:
                    pass
            
            remains = all_keys - removed
            if remains:
                path_maps[f.__name__] = remains
            else:
                path_maps.pop(f.__name__, None)
        decorated.cache_clear = cache_clear # type: ignore
        return decorated
    return decorator

def asyncify(loop:asyncio.AbstractEventLoop|None=None, executor:Executor|None=None) -> Callable:
    """
        A decorator to convert a regular function compatible to be
        run in an async mode within an event loop.
        
        Note:
            If the loop is not already running, an exception will 
            be raised.
        
        Args:
            ``loop (obj)``: An event loop.
            
            ``executor (obj)``: An asyncio executor.
    """
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            nonlocal loop
            
            async def async_wrap():
                def fff():
                    return f(*args, **kwargs)
                #ff = partial(f, *args, *kwargs)
                #res = await loop.run_in_executor(executor, ff)
                res = await loop.run_in_executor(executor, fff) # type: ignore
                return res
            
            if loop is None:
                try:
                    loop = asyncio.get_running_loop()
                except Exception:
                    raise RuntimeError(
                        "No running event loop found!")
            
            if loop.is_running():
                #return asyncio.gather(async_wrap(),loop=loop)
                return asyncio.ensure_future(async_wrap(),loop=loop)
            else:
                raise RuntimeError(
                        "loop must already be running!")
        
        if asyncio.iscoroutinefunction(f):
            return f
        return decorated
    return decorator

def run_with_redirect(stdout:str|TextIOBase|None, stderr:str|TextIOBase|None=None, logger:Logger|None=None) -> Callable:
    """
        Run a function with stdout redirection.
    """
    from blueshift.lib.common.ctx_mgrs import StdioRedirect
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            nonlocal logger
            logger = kwargs.get('logger') or logger

            with StdioRedirect(stdout, err_stream=stderr, logger=logger):
                return f(*args, **kwargs)
        return decorated
    return decorator

def asyncify_with_redirect(loop:asyncio.AbstractEventLoop|None=None, executor:Executor|None=None, 
                           stdout:str|TextIOBase|None=None, stderr:str|TextIOBase|None=None, 
                           logger:Logger|None=None) -> Callable:
    """ asyncify with redirect. """
    from blueshift.lib.common.ctx_mgrs import StdioRedirect
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            nonlocal loop
            nonlocal logger
            logger = kwargs.get('logger') or logger

            async def async_wrap():
                def fff():
                    if stdout:
                        with StdioRedirect(stdout, err_stream=stderr, logger=logger):
                            return f(*args, **kwargs)
                    else:
                        return f(*args, **kwargs)
                res = await loop.run_in_executor(executor, fff) # type: ignore
                return res
            
            if loop is None:
                try:
                    loop = asyncio.get_running_loop()
                except Exception:
                    raise RuntimeError(
                        "No running event loop found!")
                    
            if loop.is_running():
                #return asyncio.gather(async_wrap(),loop=loop)
                return asyncio.ensure_future(async_wrap(),loop=loop)
            else:
                raise RuntimeError(
                        "loop must already be running!")
        if asyncio.iscoroutinefunction(f):
            return f
        return decorated
    return decorator

def set_event_on_finish() -> Callable:
    """
        Run any function and set a threading event after the run.
    """
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            e = kwargs.get('event')
            try:
                return f(*args, **kwargs)
            finally:
                if e and isinstance(e, threading.Event):
                    e.set()
        return decorated
    return decorator

def set_event_on_finish_async() -> Callable:
    """
        Run any function and set a threading event after the run.
    """
    def decorator(f):
        @wraps(f)
        async def decorated(*args, **kwargs):
            e = kwargs.get('event')
            try:
                return await f(*args, **kwargs)
            finally:
                if e and isinstance(e, threading.Event):
                    e.set()
        return decorated
    return decorator

def handle_async_cancel(handler:Callable|None=None, **kws) -> Callable:
    """
        Handle async task cancel automatically. If `handler` is supplied, 
        on cancel, handler is called with `kwds` as arguments and 
        awaited.
    """
    def decorator(f):
        @wraps(f)
        async def decorated(*args, **kwargs):
            try:
                return await f(*args, **kwargs)
            except asyncio.CancelledError:
                if handler:
                    return await handler(**kws)
                else:
                    return
        return decorated
    return decorator

def ensure_state(states:str|list[str]=[], err_msg:str="", raise_exception:bool=True):
    """
        A decorator to ensure a desired state of the algo machine.
        This is valid only for api function, but we do not do a 
        runtime check here as this can be statically checked.
        
        Args:
            ``states (list)``: A list of allowed states.
    """
    from blueshift.lib.exceptions import StateError
    def decorator(f):
        @wraps(f)
        def decorated(self, *args, **kwargs):
            if not states:
                return f(self, *args, **kwargs)
            algo_state = self.get_current_state()
            if algo_state is None:
                meth_name = f.__name__
                msg = f"{meth_name} called in wrong state, "
                msg = msg + err_msg
                raise StateError(msg=msg)
            if algo_state not in states:
                if not raise_exception:
                    return
                meth_name = f.__name__
                msg = f"{meth_name} called in wrong state {algo_state}, "
                msg = msg + err_msg
                raise StateError(msg=msg)
            return f(self, *args, **kwargs)
        return decorated
    return decorator

def retry_on_error(retry:int=2, sleep:float=0, exception:Type[Exception]|None=None, inc:float=0, 
                   logger:Logger|None=None) -> Callable:
    """
        A decorator to make a function retry n times on error. Should
        work on either functions or bound methods.

        Args:
            ``retry (int)``: Number of tries.
            ``sleep (float)``: Number of second to sleep.
            ``exception (Exception)``: To catch selectively.
            ``inc (float)``: Increments to sleep on each retry.
    """
    from blueshift.lib.exceptions import RetryError, TerminationError

    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            nonlocal logger
            logger = kwargs.get('logger') or logger

            obj = args[0] if args else None
            if obj:
                logger = getattr(obj, 'logger', None) or logger
                
            is_main = threading.get_ident()==threading.main_thread().ident
            if not isinstance(sleep, list):
                wait = [sleep+i*inc for i in range(retry)]
            elif len(sleep) >= retry:
                wait = sleep[:retry]
            else:
                stub = [sleep[-1] for i in range((retry-len(sleep)))]
                wait = sleep.copy()
                wait.extend(stub)
            for count in range(retry):
                ret = None
                if exception:
                    try:
                        ret = f(*args, **kwargs)
                        return ret
                    except exception as e:
                        if count ==(retry-1):
                            raise RetryError(str(e))
                        elif obj and hasattr(obj, 'wants_to_quit') and obj.wants_to_quit:
                            raise RetryError(str(e))
                        elif is_main:
                            if logger:
                                logger.info(f"cannot retry {f.__name__} in main thread")
                            raise

                        if logger:
                            logger.info(f"retrying {f.__name__} for {count} time")
                        time.sleep(wait[count])
                else:
                    try:
                        ret = f(*args, **kwargs)
                        return ret
                    except Exception as e:
                        if isinstance(e, TerminationError):
                            raise
                        elif is_main:
                            if logger:
                                logger.info(f"cannot retry {f.__name__} in main thread")
                            raise
                        if count ==(retry-1):
                            raise RetryError(str(e))
                        elif obj and hasattr(obj, 'wants_to_quit') and obj.wants_to_quit:
                            raise RetryError(str(e))
                        
                        if logger:
                            logger.info(f"retrying {f.__name__} for {count} time")
                        time.sleep(wait[count])
        return decorated
    return decorator

def async_retry_on_error(retry:int=2, sleep:float=0, exception:Type[Exception]|None=None, 
                         inc:float=0, logger:Logger|None=None) -> Callable:
    """
        An async decorator to make a function retry n times on error. Should
        work on either functions or bound methods.

        Args:
            ``retry (int)``: Number of tries.
            ``sleep (float)``: Number of second to sleep.
            ``exception (Exception)``: To catch selectively.
            ``inc (float)``: Increments to sleep on each retry.
    """
    from blueshift.lib.exceptions import RetryError, TerminationError

    def decorator(f):
        @wraps(f)
        async def decorated(*args, **kwargs):
            nonlocal logger
            logger = kwargs.get('logger') or logger

            obj = args[0] if args else None
            if obj:
                logger = getattr(obj, 'logger', None) or logger
                
            if not isinstance(sleep, list):
                wait = [sleep+i*inc for i in range(retry)]
            elif len(sleep) >= retry:
                wait = sleep[:retry]
            else:
                stub = [sleep[-1] for i in range((retry-len(sleep)))]
                wait = sleep.copy()
                wait.extend(stub)
            
            for count in range(retry):
                ret = None
                if exception:
                    try:
                        return await f(*args, **kwargs)
                    except exception as e:
                        if count ==(retry-1):
                            raise RetryError(str(e))
                        elif obj and hasattr(obj, 'wants_to_quit') and obj.wants_to_quit:
                            raise RetryError(str(e))
                        
                        if logger:
                            logger.info(f"retrying {f.__name__} for {count} time")
                        await asyncio.sleep(wait[count])
                else:
                    try:
                        ret = f(*args, **kwargs)
                        return ret
                    except Exception as e:
                        if isinstance(e, TerminationError):
                            raise
                        if count ==(retry-1):
                            raise RetryError(str(e))
                        elif obj and hasattr(obj, 'wants_to_quit') and obj.wants_to_quit:
                            raise RetryError(str(e))
                        
                        if logger:
                            logger.info(f"retrying {f.__name__} for {count} time")
                        await asyncio.sleep(wait[count])
        return decorated
    return decorator

def with_lock() -> Callable:
    def decorator(f):
        @wraps(f)
        def decorated(self, *args, **kwargs):
            with self._lock:
                return f(self, *args, **kwargs)
        return decorated
    return decorator

def ensure_connected(timeout:float|None=None, max_retry:int=5, 
                     abort_retry:float=0, min_wait:float=5, 
                     max_wait:float=120, raise_exception:bool=True, logger:Logger|None=None) -> Callable:
    """
        A decorator to ensure the underlying connection is up. The object 
        must have a property `is_connected` to check the connection, and 
        a method `ensure_connected` to re-establish the connection. The 
        method must accept a `timeout` parameter that specify time to wait 
        for a background connection to establish.
    """
    from blueshift.lib.common.constants import CONNECTION_TIMEOUT
    from blueshift.lib.exceptions import TerminationError, AuthenticationError, ServerError
    _exc_raise = (TerminationError, AuthenticationError)

    timeout = timeout or CONNECTION_TIMEOUT

    timeout = min(timeout, CONNECTION_TIMEOUT)
    
    if not abort_retry:
        from blueshift.config.config import STREAMING_TIMEOUT
        abort_retry = STREAMING_TIMEOUT or 0
        exit_threshold = STREAMING_TIMEOUT or 600
    
    def decorator(f):
        @wraps(f)
        def decorated(self, *args, **kwargs):
            nonlocal logger
            logger = kwargs.get('logger') or getattr(self, 'logger', None) or logger

            is_main = threading.get_ident()==threading.main_thread().ident
            error = None
            abort = False
            current_abort_retry = abort_retry
            
            try:
                now = pd.Timestamp.now(tz=self.tz)
                close = self.calendar.to_close(now)
                if now < close and (close-now).total_seconds() < exit_threshold: # type: ignore
                    # if we are close to closing time, quit reconnect attempts
                    current_abort_retry = 1
            except Exception:
                pass
            
            if not hasattr(self, 'disconnected_since'):
                self.disconnected_since = 0
            if not hasattr(self, 'abort_reconnect'):
                self.abort_reconnect = False
                
            if self.is_connected:
                # shortcut 1: return quick if connected
                self.disconnected_since = 0
                self.abort_reconnect = False
                return f(self, *args, **kwargs)
            elif current_abort_retry > 0 and self.disconnected_since > 0 and not self.abort_reconnect:
                # shortcut 2: check and return quick if timed out
                if  time.time() - self.disconnected_since > current_abort_retry:
                    msg = 'Permanently failed to connect, timed out with attempts.'

                    if logger:
                        logger.warning(msg)
                    self.abort_reconnect = True
                    if raise_exception:
                        raise ConnectionError(msg)
                
            if self.abort_reconnect:
                # shortcut 3: return quick if already timed out
                return f(self, *args, **kwargs)
            
            for attempt in range(max_retry):
                if not self.is_connected:
                    if hasattr(self,'wants_to_quit') and self.wants_to_quit:
                        if logger:
                            msg = f'Qutting reconnect attempt on exit.'
                            logger.warning(msg)
                        return f(self, *args, **kwargs)
                    
                    if logger:
                        logger.info(f"retrying to connect {self} before executing {f.__name__}")
                    try:
                        self.ensure_connected(timeout=timeout)
                    except Exception as e:
                        error = e
                        # do not retry for auth errors
                        if isinstance(e, _exc_raise):
                            abort = True
                            break
                        elif is_main:
                            # do not sleep in main thread
                            if raise_exception:
                                abort = True
                            break
                        
                        if attempt+1 < max_retry:
                            # sleep for 5s, 11s, 17s, 23s etc. only if we 
                            # are retrying again
                            frac = round(2*attempt*attempt + min_wait)
                            frac = min(frac, max_wait) # do not wait > 2 mins
                            
                            if logger:
                                msg = f'Failed to ensure server connection '
                                msg += f'before executing {f.__name__}, '
                                msg += f'will try after {frac} seconds.'
                                logger.info(msg)
                            time.sleep(frac)
                        continue
                    else:
                        break
            
            if not self.is_connected:
                if not self.disconnected_since:
                    self.disconnected_since = time.time()
                msg = f'Failed to ensure server connection '
                msg += f'before executing {f.__name__}'
                msg += f':{str(error)}.' if error else '.'
                
                if current_abort_retry and time.time() - self.disconnected_since > current_abort_retry:
                    msg += ' Permanently failed to connect, timed out with attempts.'
                    self.abort_reconnect = True
                    if raise_exception:
                        raise ConnectionError(msg)
                elif abort:
                    self.abort_reconnect = True
                    msg += ' Permanently failed to connect, will abort attempts.'
                    raise ConnectionError(msg)
                elif not raise_exception:
                    if logger:
                        msg += f' Will ignore the error.'
                        logger.info(msg)
                else:
                    raise ServerError(msg)
            else:
                self.abort_reconnect = False
                self.disconnected_since = 0
            
            return f(self, *args, **kwargs)
        return decorated
    return decorator

def api_rate_limit(max_count:int|None=None, time_period:float|None=None, tracker:Any=None, 
                   sleep:bool=True, logger:Logger|None=None) -> Callable:
    """
        decorator to enforce rate limits on API calls. This assumes a member
        variable `rate_limit_count` to keep track of limit consumption and
        a variable `rate_limit_since` as function/ method attributes.

        Args:
            ``max_count (int)``: max calls per time period.

            ``time_period (int)``: period in seconds for API limits.

            ``tracker (object)``: entity book-keeping the rate breach.
            
            ``sleep (bool)``: Cool-off or raise exception.

        Note:
            The book-keeping tracker is the one responsible for persisting
            the records. This defaults to the decorated function. This is
            useful for a function level rate breach tracking - where the
            API provider has method-wise rate limits. Otherwise it
            can be instance of a class, in case we want a common tracking
            for all function calls for a session - useful where the API
            provider has a combined rate limit over any call.
    """
    from blueshift.lib.exceptions import TooManyRequests

    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            nonlocal logger
            logger = kwargs.get('logger') or logger

            is_main = threading.get_ident()==threading.main_thread().ident
            if max_count is None or time_period is None or is_main:
                return f(*args, **kwargs)
            
            if tracker is None:
                entity = f
            else:
                entity = tracker

            if not hasattr(entity, "rate_limit_count") or\
                    not hasattr(entity, "rate_limit_since"):
                setattr(entity,'rate_limit_count', 1)
                setattr(entity,'rate_limit_since', pd.Timestamp.now())
                return f(*args, **kwargs)

            t = pd.Timestamp.now()
            sec_elapsed = (t - entity.rate_limit_since).total_seconds()
            if sec_elapsed < time_period:
                if entity.rate_limit_count >= max_count:
                    if not sleep:
                        raise TooManyRequests(
                                f"in {f.__name__}: too many requests.")
                    else:
                        s = time_period - sec_elapsed + 1
                        if logger:
                            logger.error(
                                    f'{f.__name__}:too many requests, sleep for {s} seconds.')
                        time.sleep(s)
                else:
                    entity.rate_limit_count += 1
            else:
                entity.rate_limit_count = 1
                entity.rate_limit_since = t
            
            return f(*args, **kwargs)
        return decorated
    return decorator

def sliding_rate_limit(max_calls:int, time_window:float, raise_exception:bool=True,
                       exc:Exception|None=None, err_msg:str|None=None, 
                       logger:Logger|None=None) -> Callable:
    """
    Sliding window rate limiter for object methods. `None` should be an allowed 
    return type from the method - as it returns `None` in case of `raise_exception` 
    is False. Pass an exception object as `exc` to raise it when limit is 
    breached and `raise_exception` is True. Use `err_msg` to set custom error 
    message (raised either as an exception or logged as warning).
    
    Args:
        max_calls: Maximum number of allowed calls
        time_window: Time window in seconds
    """
    from collections import deque
    import bisect
    from blueshift.lib.exceptions import TooManyRequests

    def decorator(func):
        call_times = deque()
        
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            nonlocal logger
            logger = kwargs.get('logger') or getattr(self, 'logger', None) or logger
            
            now = time.time()
            cutoff = now - time_window
            
            # Find the index of the first timestamp to keep
            if call_times:
                idx = bisect.bisect_right(call_times, cutoff)
                
                # Remove all timestamps before the cutoff
                try:
                    for _ in range(idx):
                        call_times.popleft()
                except Exception:
                    pass
            
            # Check if we've hit the limit
            if len(call_times) >= max_calls:
                oldest_call = call_times[0]
                wait_time = time_window - (now - oldest_call)
                msg = f"Limit exceeded for {func.__name__}: {max_calls} calls per {time_window}s. "
                
                if raise_exception:
                    if not err_msg:
                        msg += f"Try again in {round(wait_time,2)} seconds."
                    else:
                        msg += err_msg
                        
                    e = TooManyRequests(msg)
                    if exc:
                        raise exc from e
                    else:
                        raise e
                else:
                    if logger:
                        if not err_msg:
                            msg += f" Will ignore the error."
                        else:
                            msg += err_msg
                            
                        logger.info(msg)
                    return
            
            # Record this call and execute
            call_times.append(now)
            return func(self, *args, **kwargs)
        
        return wrapper
    return decorator

def wrap_signature(f:Callable, required:int) -> Callable:
    from inspect import signature, Parameter

    sig = signature(f)
    n_args = len(sig.parameters)
    if n_args >= required:
        return f
    for name in sig.parameters:
        p = sig.parameters[name]
        if p.kind == Parameter.VAR_POSITIONAL:
            return f
    
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            args = args[:n_args]
            return f(*args, **kwargs)
        return decorated
    
    return decorator(f)
