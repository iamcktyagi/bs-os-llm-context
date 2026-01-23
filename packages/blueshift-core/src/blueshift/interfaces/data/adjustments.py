from __future__ import annotations
from typing import TYPE_CHECKING, Literal, Callable, Dict, Any, Type
from abc import ABC, abstractmethod

from blueshift.interfaces.assets.assetdb import AssetDBBackend
from ..plugin_manager import load_plugins

if TYPE_CHECKING:
    import pandas as pd

class AdjustmentHandler(ABC):
    """interface for adjustment handler. """
    
    def __init__(self):
        self._backend=AssetDBBackend.UNDEFINED
    
    def __str__(self) -> str:
        return f"Blueshift Adjustment Handler [{self._backend}]"
    
    def __repr__(self):
        return self.__str__()
    
    @abstractmethod
    def initialize(self, *args, **kwargs):
        """ intitialization of the db, if any. """
        raise NotImplementedError
    
    @abstractmethod
    def write(self, *args, **kwargs):
        """ Write adjustment data to underlying database. """
        raise NotImplementedError
        
    @abstractmethod
    def read(self, *args, **kwargs):
        """ Read adjustment data from underlying database. """
        raise NotImplementedError
        
    @abstractmethod
    def rename(self, *args, **kwargs):
        """ Rename symbols in the adjustments tables. """
        raise NotImplementedError
        
    @abstractmethod
    def refresh(self):
        """ Refresh and reflect new data after a recent write. """
        raise NotImplementedError
        
    @abstractmethod
    def dividend_for_assets(self, dt:pd.Timestamp, assets:list[str]|None=None) -> dict[str,float]:
        """
            Fetch the dividend ratio for given assets for a given date.
            
            Note:
                If `assets` is None, adjustment ratios for all assets 
                in the database will be returned.
            
            Args:
                `dt (Timestamp)`: A date to fetch the adjustment for.
                
                `assets (list)`: List of assets to fetch for.
                
            Returns:
                Dict. A dictionary of ratios, keyed by asset symbols.
        """
        raise NotImplementedError
        
    @abstractmethod
    def split_for_assets(self, dt:pd.Timestamp, assets:list[str]|None=None) -> dict[str,float]:
        """
            Fetch the split ratio for given assets for a given date.
            
            Note:
                If `assets` is None, adjustment ratios for all assets 
                in the database will be returned.
            
            Args:
                `dt (Timestamp)`: A date to fetch the adjustment for.
                
                `assets (list)`: List of assets to fetch for.
                
            Returns:
                Dict. A dictionary of ratios, keyed by asset symbols.
        """
        raise NotImplementedError
        
    @abstractmethod
    def merger_for_assets(self, ddt:pd.Timestamp, assets:list[str]|None=None) -> dict[str,float]:
        """
            Fetch the mergers ratio for given assets for a given date.
            
            Note:
                If `assets` is None, adjustment ratios for all assets 
                in the database will be returned.
            
            Args:
                `dt (Timestamp)`: A date to fetch the adjustment for.
                
                `assets (list)`: List of assets to fetch for.
                
            Returns:
                Dict. A dictionary of ratios, keyed by asset symbols.
        """
        raise NotImplementedError
        
    @abstractmethod
    def adjs_for_assets(self, start_dt:pd.Timestamp, end_dt:pd.Timestamp, assets:list[str], 
                        adj_type:str|None=None) -> dict[str,tuple[pd.DatetimeIndex,list[float]]]:
        """
            Computes the adjustments available for given assets between
            (and including) the date range.
            
            Note:
                if `assets` is None (default), adjustments for **all** 
                assets in the database is returned. If `adj_type` is 
                None, all adjustment types (mergers, splits and dividends)
                are returned.
            
            Args:
                `start_dt (Timestamp)`: Start of the date range.
                
                `end_dt (Timestamp)`: End of the date range.
                
                `assets (list)`: A list of asset symbols.
                
                `adj_type (str)`: One of `mergers`, `splits`, or `dividends`.
                
            Returns:
                Dict. A dictionary keyed by asset symbol with tuples of 
                date index and corresponding ratios to apply.
        """
        raise NotImplementedError
    
    @abstractmethod
    def load_adjustments(self,
                         dates:pd.DatetimeIndex|list[pd.Timestamp],
                         assets:list[int],
                         should_include_splits:bool,
                         should_include_mergers:bool,
                         should_include_dividends:bool,
                         adjustment_type:Literal['price','vol','all']) -> dict[str,dict]:
        """
            Return adjustment ratios for the dates and assets (sids). Pass 
            `adjustment_type` as 'price' or 'volume' for only price or volume 
            adjustments, or pass `all` for both.
            
            Args:
                `dates (pd.DatetimeIndex)`: Dates for adjustments
                
                `assets (pd.Index)`: SIDs of assets.
                
                `should_include_splits (bool)`: If splits to be included.
                
                `should_include_mergers (bool)`: If mergers to be included.
                
                `should_include_dividends (bool)`: If dividends to be included.
                
                `adjustment_type (str)`: Price or volume adjusments (or both).
                
            Returns:
                Dict (dict[str -> dict[int -> Adjustment]]).
        """
        raise NotImplementedError
    
    def load_pricing_adjustments(self, columns:list[str], dates:pd.DatetimeIndex|list[pd.Timestamp], 
                                 assets:list[int]):
        if 'volume' not in set(columns):
            adjustment_type = 'price'
        elif len(set(columns)) == 1:
            adjustment_type = 'volume'
        else:
            adjustment_type = 'all'

        adjustments = self.load_adjustments(
            dates,
            assets,
            should_include_splits=True,
            should_include_mergers=True,
            should_include_dividends=True,
            adjustment_type=adjustment_type, # type: ignore
        )
        price_adjustments = adjustments.get('price')
        volume_adjustments = adjustments.get('volume')

        return [
            volume_adjustments if column == 'volume'
            else price_adjustments
            for column in columns
        ]

_adj_handler_registry: Dict[str, Type[AdjustmentHandler]] = {}
_builtin_adj_handler_loader: Callable[[Any], None] | None = None

def register_adj_handler(name: str, cls: Type[AdjustmentHandler]):
    """Register a new adjustment handler type."""
    _adj_handler_registry[name] = cls

def set_builtin_adj_handler_loader(loader: Callable[[Any], None]):
    """Register a callable that will lazily load built-in adjustment handler types."""
    global _builtin_adj_handler_loader
    _builtin_adj_handler_loader = loader

def _ensure_builtin_adj_handler_loaded(adj_handler_type:str|None=None):
    if _builtin_adj_handler_loader:
        _builtin_adj_handler_loader(adj_handler_type)

    if (adj_handler_type and adj_handler_type not in _adj_handler_registry) or adj_handler_type is None:
        try:
            load_plugins('blueshift.plugins.data')
        except Exception:
            pass

def adj_handler_factory(adj_handler_type: str, *args, **kwargs) -> AdjustmentHandler:
    """ create adjustment handler instance by type name. """
    from inspect import getfullargspec

    if adj_handler_type not in _adj_handler_registry:
        _ensure_builtin_adj_handler_loaded(adj_handler_type)  # lazy load builtins
    cls = _adj_handler_registry.get(adj_handler_type)
    if cls is None:
        raise NotImplementedError(f"Unknown adjustment handler type: {adj_handler_type}")
    
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

def list_adj_handlers() -> list[str]:
    """ registered adjustment handlers class names. """
    _ensure_builtin_adj_handler_loaded()
    return list(_adj_handler_registry)

__all__ = [
    'AdjustmentHandler',
    'register_adj_handler',
    'adj_handler_factory',
    'list_adj_handlers',
    'set_builtin_adj_handler_loader',
    ]