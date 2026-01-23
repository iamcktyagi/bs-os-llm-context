from typing import Type, Any, Dict, Callable

from ..plugin_manager import load_plugins
from ._simulation import ABCChargesModel, ABCCostModel, ABCMarginModel, ABCSlippageModel

_slippage_model_registry: Dict[str, Type[ABCSlippageModel]] = {}
_margin_model_registry: Dict[str, Type[ABCMarginModel]] = {}
_cost_model_registry: Dict[str, Type[ABCCostModel]] = {}
_charge_model_registry: Dict[str, Type[ABCChargesModel]] = {}

_builtin_slippage_model_loader: Callable[[], None] | None = None
_builtin_margin_model_loader: Callable[[], None] | None = None
_builtin_cost_model_loader: Callable[[], None] | None = None
_builtin_charge_model_loader: Callable[[], None] | None = None

def register_slippage_model(name: str, cls: Type[ABCSlippageModel]):
    """Register a new slippage model type."""
    _slippage_model_registry[name] = cls

def register_margin_model(name: str, cls: Type[ABCMarginModel]):
    """Register a new margin model type."""
    _margin_model_registry[name] = cls

def register_cost_model(name: str, cls: Type[ABCCostModel]):
    """Register a new cost model type."""
    _cost_model_registry[name] = cls

def register_charge_model(name: str, cls: Type[ABCChargesModel]):
    """Register a new charges model type."""
    _charge_model_registry[name] = cls

def set_builtin_slippage_model_loader(loader: Callable[[], None]):
    """Register a callable that will lazily load built-in slippage model types."""
    global _builtin_slippage_model_loader
    _builtin_slippage_model_loader = loader

def set_builtin_margin_model_loader(loader: Callable[[], None]):
    """Register a callable that will lazily load built-in margin model types."""
    global _builtin_margin_model_loader
    _builtin_margin_model_loader = loader

def set_builtin_cost_model_loader(loader: Callable[[], None]):
    """Register a callable that will lazily load built-in cost model types."""
    global _builtin_cost_model_loader
    _builtin_cost_model_loader = loader

def set_builtin_charge_model_loader(loader: Callable[[], None]):
    """Register a callable that will lazily load built-in charges model types."""
    global _builtin_charge_model_loader
    _builtin_charge_model_loader = loader

def _ensure_builtin_slippage_loaded(model_type:str|None=None):
    if _builtin_slippage_model_loader:
        _builtin_slippage_model_loader()

    if (model_type and model_type not in _slippage_model_registry) or model_type is None:
        try:
            load_plugins('blueshift.plugins.finance')
        except Exception:
            pass

def _ensure_builtin_margin_loaded(model_type:str|None=None):
    if _builtin_margin_model_loader:
        _builtin_margin_model_loader()

    if (model_type and model_type not in _margin_model_registry) or model_type is None:
        try:
            load_plugins('blueshift.plugins.finance')
        except Exception:
            pass

def _ensure_builtin_cost_loaded(model_type:str|None=None):
    if _builtin_cost_model_loader:
        _builtin_cost_model_loader()

    if (model_type and model_type not in _cost_model_registry) or model_type is None:
        try:
            load_plugins('blueshift.plugins.finance')
        except Exception:
            pass

def _ensure_builtin_charge_loaded(model_type:str|None=None):
    if _builtin_charge_model_loader:
        _builtin_charge_model_loader()

    if (model_type and model_type not in _charge_model_registry) or model_type is None:
        try:
            load_plugins('blueshift.plugins.finance')
        except Exception:
            pass

def slippage_model_factory(model_type: str, *args, **kwargs) -> ABCSlippageModel:
    """ create slippage model instance by type name. """
    from inspect import getfullargspec

    if model_type not in _slippage_model_registry:
        _ensure_builtin_slippage_loaded(model_type)  # lazy load builtins
    cls = _slippage_model_registry.get(model_type)
    if cls is None:
        raise NotImplementedError(f"Unknown slippage model type: {model_type}")
    
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

def margin_model_factory(model_type: str, *args, **kwargs) -> ABCMarginModel:
    """ create margin model instance by type name. """
    from inspect import getfullargspec

    if model_type not in _margin_model_registry:
        _ensure_builtin_margin_loaded(model_type)  # lazy load builtins
    cls = _margin_model_registry.get(model_type)
    if cls is None:
        raise NotImplementedError(f"Unknown margin model type: {model_type}")
    
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

def cost_model_factory(model_type: str, *args, **kwargs) -> ABCCostModel:
    """ create cost model instance by type name. """
    from inspect import getfullargspec

    if model_type not in _cost_model_registry:
        _ensure_builtin_cost_loaded(model_type)  # lazy load builtins
    cls = _cost_model_registry.get(model_type)
    if cls is None:
        raise NotImplementedError(f"Unknown cost model type: {model_type}")
    
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

def charge_model_factory(model_type: str, *args, **kwargs) -> ABCChargesModel:
    """ create cost model instance by type name. """
    from inspect import getfullargspec

    if model_type not in _charge_model_registry:
        _ensure_builtin_charge_loaded(model_type)  # lazy load builtins
    cls = _charge_model_registry.get(model_type)
    if cls is None:
        raise NotImplementedError(f"Unknown charge model type: {model_type}")
    
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

def list_slippage_models() -> list[str]:
    """ registered margin model class names. """
    _ensure_builtin_slippage_loaded()
    return list(_slippage_model_registry)

def list_margin_models() -> list[str]:
    """ registered margin model class names. """
    _ensure_builtin_margin_loaded()
    return list(_margin_model_registry)

def list_cost_models() -> list[str]:
    """ registered cost model class names. """
    _ensure_builtin_cost_loaded()
    return list(_cost_model_registry)

def list_charge_models() -> list[str]:
    """ registered margin model class names. """
    _ensure_builtin_charge_loaded()
    return list(_charge_model_registry)

__all__ = [
    'ABCChargesModel',
    'ABCCostModel',
    'ABCMarginModel',
    'ABCSlippageModel',
    'register_slippage_model',
    'register_margin_model',
    'register_cost_model',
    'register_charge_model',
    'slippage_model_factory',
    'margin_model_factory',
    'cost_model_factory',
    'charge_model_factory',
    'list_slippage_models',
    'list_margin_models',
    'list_cost_models',
    'list_charge_models',
    'set_builtin_slippage_model_loader',
    'set_builtin_margin_model_loader',
    'set_builtin_cost_model_loader',
    'set_builtin_charge_model_loader',
    ]