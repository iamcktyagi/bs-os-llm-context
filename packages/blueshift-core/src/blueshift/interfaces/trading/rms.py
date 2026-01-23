from __future__ import annotations
from typing import TYPE_CHECKING, Type, Callable, Dict, Any
from abc import ABC, abstractmethod

from blueshift.lib.trades._order import Order
from blueshift.lib.trades._position import Position
from ..plugin_manager import load_plugins

if TYPE_CHECKING:
    from .broker import IBroker

class IRMS(ABC):
    """
        Abstract interface for Risk Management System implemented by a 
        broker. This is separate from built-in RMS in core blueshift and 
        is meant to handle asset-class/geography/exchange or other specific 
        risk management requirements.
    """
    def __init__(self, broker:IBroker):
        self._broker = broker
        
    @property
    @abstractmethod
    def name(self):
        """
            The name of this particular RMS.
        """
        raise NotImplementedError
        
    @abstractmethod
    def pretrade(self, order:Order, *args, **kwargs) -> Order:
        """
            pre-trade risk check implemented by the RMS. It must return 
            an order object, modifying the input order if required, else 
            raise exception, if the pre-trade check fails. This shall be 
            called at the very start of the `place_order` function of 
            a broker implemented.
        """
        raise NotImplementedError
        
    @abstractmethod
    def posttrade(self, order:Order, *args, **kwargs):
        """
            updates after an order is successfully submitted. This is a 
            slight misnomer, as this is triggered after successful order 
            placement, not after trade execution.
        """
        raise NotImplementedError
        
    @abstractmethod
    def monitor(self, position:Position):
        """
            Monitor a position for RMS action (e.g. auto-squareoff). Not 
            used at present.
        """
        raise NotImplementedError
        
class NoRMS(IRMS):
    """ A dummy RMS implementation. """
    @property
    def name(self):
        return "no-rms"
    
    def pretrade(self, order, *args, **kwargs):
        return order
    
    def posttrade(self, order, *args, **kwargs):
        pass
    
    def monitor(self, position):
        pass
    
_rms_registry:Dict[str, Type[IRMS]] = {}
_builtin_rms_loader: Callable[[Any], None] | None = None

def register_rms(name:str, rms:Type[IRMS]) -> None:
    """ register an RMS type. """
    _rms_registry[name] = rms

def set_builtin_rms_loader(loader: Callable[[Any], None]):
    """Register a callable that will lazily load built-in rms types."""
    global _builtin_rms_loader
    _builtin_rms_loader = loader

def _ensure_builtin_rms_loaded(rms_type:str|None=None):
    if _builtin_rms_loader:
        _builtin_rms_loader(rms_type)

    if (rms_type and rms_type not in _rms_registry) or rms_type is None:
        try:
            load_plugins('blueshift.plugins.rms')
        except Exception:
            pass

def rms_factory(rms_type: str, *args, **kwargs) -> IRMS:
    """ create an RMS instance by type name. """
    from inspect import getfullargspec
    
    if rms_type not in _rms_registry:
        _ensure_builtin_rms_loaded(rms_type)  # lazy load builtins

    cls = _rms_registry.get(rms_type)
    if cls is None:
        raise NotImplementedError(f"Unknown RMS type: {rms_type}")
    
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

def list_rms() -> list[str]:
    """ get a list of registered RMS names. """
    return list(_rms_registry)


register_rms('no-rms', NoRMS)

__all__ = [
    'IRMS', 
    'NoRMS',
    'register_rms',
    'rms_factory',
    'list_rms',
    'set_builtin_rms_loader',
    ]
