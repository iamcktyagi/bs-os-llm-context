from __future__ import annotations
from typing import Type, Callable, Any
from abc import ABC, abstractmethod

from blueshift.lib.common.enums import CallBackResult
from .plugin_manager import load_plugins

class BlueshiftCallback(ABC):
    def __init__(self, url, auth=None, **kwargs):
        pass
    
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def close(self):
        pass
    
    @abstractmethod
    def callback(self, name, data) -> tuple[CallBackResult, str]:
        return CallBackResult.SUCCESS, 'success'
    
_callback_registry:dict[str, Type[BlueshiftCallback]] = {}
_builtin_callback_loader: Callable[[], None] | None = None
_builtins_callback_loaded = False

def register_server_callback(callback_type:str, cls:Type[BlueshiftCallback]):
    _callback_registry[callback_type] = cls

def set_builtin_callback_loader(loader: Callable[[], None]):
    """Register a callable that will lazily load built-in blueshift callback types."""
    global _builtin_callback_loader
    _builtin_callback_loader = loader

def _ensure_builtin_callback_loaded():
    global _builtins_callback_loaded
    if not _builtins_callback_loaded and _builtin_callback_loader:
        _builtin_callback_loader()
        _builtins_callback_loaded = True

def server_callback_factory(callback_type, *args, **kwargs) -> BlueshiftCallback:
    from inspect import getfullargspec

    if callback_type not in _callback_registry:
        _ensure_builtin_callback_loaded()  # lazy load builtins
    cls = _callback_registry.get(callback_type)

    if not cls:
        try:
            load_plugins('blueshift.plugins.callback')
        except Exception:
            pass

    cls = _callback_registry.get(callback_type)
    if not cls:
        raise NotImplementedError(f'callback {callback_type} is not implemented')
    
    specs = getfullargspec(cls.__init__)

    if specs.varkw:
        kw = kwargs.copy()
    else:
        args_specs = specs.args
        kw = {}
        for key in kwargs:
            if key in args_specs:
                kw[key] = kwargs[key]

    return cls(*args, **kw)

__all__ = [
    'BlueshiftCallback',
    'register_server_callback',
    'server_callback_factory',
    ]