from __future__ import annotations
from typing import Any, Callable, Dict, TYPE_CHECKING, Type

from ..plugin_manager import load_plugins
from ._exit_handler import IExitHandler, ExitMethod

if TYPE_CHECKING:
    from blueshift.core.algorithm.algorithm import TradingAlgorithm

_exit_handler_registry: Dict[str, Type[IExitHandler]] = {}
_builtin_exit_handler_loader: Callable[[Any], None] | None = None

def register_exit_handler(name: str, cls: Type[IExitHandler]):
    """Register a new exit handler type."""
    _exit_handler_registry[name] = cls

def set_builtin_exit_handler_loader(loader: Callable[[Any], None]):
    """Register a callable that will lazily load built-in exit handler types."""
    global _builtin_exit_handler_loader
    _builtin_exit_handler_loader = loader

def _ensure_builtin_exit_loaded(handler_type:str|None=None):
    if _builtin_exit_handler_loader:
        _builtin_exit_handler_loader(handler_type)

    if (handler_type and handler_type not in _exit_handler_registry) or handler_type is None:
        try:
            load_plugins('blueshift.plugins.exit_handler')
        except Exception:
            pass

def exit_handler_factory(handler_type, algo:TradingAlgorithm, *args, **kwargs) -> IExitHandler:
    """ factory function to create exit handler. """
    from inspect import getfullargspec

    if handler_type not in _exit_handler_registry:
        _ensure_builtin_exit_loaded(handler_type)  # lazy load builtins
    cls = _exit_handler_registry.get(handler_type)
    if cls is None:
        raise NotImplementedError(f"Unknown exit handler type: {handler_type}")
    
    specs = getfullargspec(cls.__init__)

    if specs.varkw:
        kw = kwargs.copy()
    else:
        args_specs = specs.args
        kw = {}
        for key in kwargs:
            if key in args_specs:
                kw[key] = kwargs[key]

    return cls(algo, *args, **kw)

def get_exit_handler(algo:TradingAlgorithm,**kwargs) -> IExitHandler:
    return exit_handler_factory('default', algo, **kwargs)

__all__ = [
    'IExitHandler',
    'ExitMethod',
    'register_exit_handler',
    'exit_handler_factory',
    'set_builtin_exit_handler_loader',
    'get_exit_handler',
    ]