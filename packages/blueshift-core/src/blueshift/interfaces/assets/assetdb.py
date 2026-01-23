from __future__ import annotations
from typing import Type, Any, cast, Callable
from abc import ABC, abstractmethod
from enum import Enum

from blueshift.interfaces.assets._assets import MarketData
from ..plugin_manager import load_plugins

class AssetDBBackend(Enum):
    """ backend types of a metadata interfacing class. """
    UNDEFINED = 0
    SQLITE = 1
    DICT = 2
    DUCKDB = 3
    OTHERS = 4

class AssetDBDriver(ABC):
    """ interface for asset database. """
    __metatable = 'metadata'
    
    def __init__(self, *args, **kwargs):
        self._url = None
        self._backend = AssetDBBackend.UNDEFINED
        self._metadata = {}
        
    def __str__(self):
        return f"Blueshift AssetDB Driver([{self.backend}]@[{self.url}])"
    
    def __repr__(self):
        return self.__str__()
    
    @property
    def url(self) -> str|None:
        """ Returns the databse url. """
        return self._url
        
    @property
    def backend(self) -> str:
        """ Returns the databse type. """
        return self._backend.name
    
    @property
    def metadata(self) -> dict[str, Any]:
        """ Fetches metadata for this database."""
        return self._metadata
        
    @metadata.setter
    def metadata(self, data):
        """ Sets metadata for this database."""
        pass
    
    @abstractmethod
    def list_assets(self) -> list[Type[MarketData]]:
        """ List supported assets."""
        raise NotImplementedError
    
    @abstractmethod
    def connect(self, *args, **kwargs):
        """ Connect to the underlying database. """
        raise NotImplementedError
        
    @abstractmethod
    def initialize(self, *args, **kwargs):
        """ Initialize the object. """
        raise NotImplementedError
        
    @abstractmethod
    def list(self, *args, **kwayrgs) -> dict:
        """
            List the tables in the database and count of assets thereoff.
            
            Returns:
                Dict. A list of names and table objects.
        """
        raise NotImplementedError
    
    @abstractmethod    
    def exists(self, symbols:str|list[str], table, key) -> bool|list[bool]:
        """
            Check if an asset exists in the database.
            
            Args:
                `symbols (str or list)`: Symbol (or list of symbols) to check.
                
                `table (obj)`: DB Schema object or a table name.
                
                `key (str)`: The column name to match in the table.
                
            Returns:
                Bool or list of bool. True if a match is found.
        """
        raise NotImplementedError
        
    @abstractmethod
    def find(self, assets:MarketData|list[MarketData], key=None, 
             convert=True) ->  MarketData|list[MarketData]|list[MarketData|None]|None:
        """ 
            Find objects if they exists in the database.
            
            Args:
                `assets (Asset or list)`: An asset (or a db schema) object, or a list of them.
                
                `key (str)`: The column name to match in the table.
                
                `convert (bool)`: Converts to underlying asset objects.
                
            Returns:
                List. A list of asset objects.
        """
        raise NotImplementedError
    
    @abstractmethod
    def search(self, symbols:str|list[str]|None=None, **kwargs) -> list[MarketData]:
        """ 
            Fetch assets from database based on specified key-value 
            matches. Fetches all if `values` is None.
            
            Args:
                `symbols (list)`: A list of symbols to match from table.
                
            Returns:
                List. A list of (converted asset) objects.
        """
        raise NotImplementedError
        
    def fuzzy_search(self, symbols:str|list[str], **kwargs) ->  list[MarketData]:
        """ 
            Fetch assets from database based on specified value. 
            Returns all assets that has symbol starting with value.
            
            Args:
                `symbols (str)`: A string or list of strings to match symbols.
                
            Returns:
                List. A list of (converted asset) objects.
        """
        raise NotImplementedError
        
    @abstractmethod
    def add(self, asset:MarketData):
        """
            Add an asset object to database.
            
            Args:                
                `asset (Asset or list)`: An asset (or a db schema) object.
                
            Returns:
                None.
        """
        raise NotImplementedError
        
    @abstractmethod
    def update(self, assets:MarketData|list[MarketData], data:dict={}, key=None):
        """
            Update an existing asset object to database.
            
            Note:
                Supply an asset object with updated data to replace, or
                a symbol with a `data` dictionary with properties to 
                update. If an asset object is supplied, `data` will be
                ignored.
            
            Args:
                `assets (Asset or str)`: An asset object or symbol string, or a list of them.
                
                `data (dict)`: A mapping of fields and values to update.
                
                `key (str)`: The column name to match in the table.
                
            Returns:
                None.
        """
        raise NotImplementedError
        
    @abstractmethod
    def upsert(self, assets:MarketData|list[MarketData], key=None):
        """
            Update an existing asset object to database or add a new.
            
            Args:
                `obj (obj or list)`: An asset (or a db schema) object, or a list of them.
                
                `key (str)`: The column name to match in the table.
                
            Returns:
                None.
        """ 
        raise NotImplementedError
        
    @abstractmethod
    def rename(self, from_:str, to:str, long_name:str|None=None, table:str|None=None,
               dependency_field:str='underlying') -> list[str]:
        """
            Rename an existing symbol in the database.
            
            Note: 
                This will also trigger a cascading updates of all assets
                in the schema tables if their field 'underlying' points 
                to this asset. The cascading update will not affect the
                symbol of those assets, allowing this function to be 
                used in a recursive manner.
            
            Args:
                `from_ (str)`: Existing symbol name.
                
                `to (str)`: New symbol name.
                
                `long_name (str)`: Long description of the new symbol.
                
                `table (str)`: Optional table name in which to look.
                
                `dependency_field (str)`: Field to use for cascading updates.
            
            Returns:
                List. List of symbols for the dependents renamed.
        """
        raise NotImplementedError
        
    @abstractmethod
    def remove(self, symbol:str, table:str|None=None, dependency_field:str='underlying') -> list[str]:
        """
            Remove a symbol from the database.
            
            Note:
                This will trigger cascading delete of dependent 
                symbols from the database.
            
            Args:
                `symbol (str)`: A symbol to remove
                
                `table (str)`: Optional table name in which to look.
                
                `dependency_field (str)`: Field to use for cascading delete.
                
            Returns:
                List. List of symbols for the dependents deleted.
        """
        raise NotImplementedError

        
_assetdb_registry = {}
_builtin_finder_loader: Callable[[Any], None] | None = None

def register_assetdb_driver(assetdb_type, driver):
    """ register a db driver type to the global mapping. """
    _assetdb_registry[assetdb_type] = driver

def set_builtin_assetdb_loader(loader: Callable[[Any], None]):
    """Register a callable that will lazily load built-in assetdb types."""
    global _builtin_finder_loader
    _builtin_finder_loader = loader

def _ensure_builtin_assetdb_loaded(assetdb_type:str|None=None):
    if _builtin_finder_loader:
        _builtin_finder_loader(assetdb_type)

    if (assetdb_type and assetdb_type not in _assetdb_registry) or not assetdb_type is None:
        try:
            load_plugins('blueshift.plugins.assets')
        except Exception:
            pass
    
def assetdb_factory(assetdb_type: str, *args, **kwargs) -> AssetDBDriver:
    """ create assetdb instance by type name. """
    from inspect import getfullargspec

    if assetdb_type not in _assetdb_registry:
        _ensure_builtin_assetdb_loaded(assetdb_type)  # lazy load builtins
    cls = _assetdb_registry.get(assetdb_type)
    if cls is None:
        raise NotImplementedError(f"Unknown asset db type: {assetdb_type}")
    
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

def list_assetdbs() -> list[str]:
    """ registered asset db class names. """
    _ensure_builtin_assetdb_loaded()
    return list(_assetdb_registry)

__all__ = [
    'AssetDBDriver',
    'AssetDBBackend',
    'register_assetdb_driver',
    'assetdb_factory',
    'list_assetdbs',
    'set_builtin_assetdb_loader',
    ]