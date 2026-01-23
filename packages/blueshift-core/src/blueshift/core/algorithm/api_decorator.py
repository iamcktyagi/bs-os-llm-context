from typing import Callable, ParamSpec, TypeVar
import blueshift.api

from blueshift.lib.common.ctx_mgrs import TimeoutRLock

P = ParamSpec("P")
R = TypeVar("R")
lock = TimeoutRLock(timeout=60)

def api_method(f:Callable[P,R]) -> Callable[P, R]:
    """
        decorator for API binding, using a defined attribute.
    """
    with lock:
        setattr(blueshift.api, f.__name__, f)
        blueshift.api.__all__.append(f.__name__) # type: ignore
        f.is_api = True # type: ignore
        return f

def command_method(f:Callable):
    """
        decorator to flag a method as a command method in the algorithm.
    """
    f.is_command = True # type: ignore
    return f