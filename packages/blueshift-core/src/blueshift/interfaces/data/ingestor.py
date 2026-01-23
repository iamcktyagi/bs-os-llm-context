from __future__ import annotations
from typing import Dict, Type, Callable, TYPE_CHECKING, Any
from enum import Enum
from abc import ABC, abstractmethod

from .store import DataStore
from .source import DataSource
from ..plugin_manager import load_plugins

from blueshift.lib.exceptions import IngestionException
from blueshift.interfaces.assets._assets import MarketData

if TYPE_CHECKING:
    import pandas as pd

class IngestionMode(Enum):
    UNDEFINED = 0
    ONESHOT = 1
    INCREMENTAL = 2
    CONTINUOUS = 3

class IngestionEngine(ABC):
    """ The interface for ingestion engine. """
    def __init__(self, source:DataSource, store:DataStore|None, *args, **kwargs):
        self._mode = IngestionMode.UNDEFINED
        self._store = store
        self._source = source
        
    def __str__(self) -> str:
        return f'BlueshiftIngestor[{self.mode}())])'
        
    def __repr__(self) -> str:
        return self.__str__()
        
    @property
    def mode(self) -> str:
        """ the store object for this ingestor. """
        return self._mode.name
        
    @property
    def store(self) -> DataStore|None:
        """ the store object for this ingestor. """
        return self._store
    
    @store.setter
    def store(self, value):
        if value is None:
            self._store = None
        elif isinstance(value, DataStore):
            self._store = value
        else:
            raise IngestionException(f'invalid type, expected a data store type, got {type(value)}')
    
    @property
    def source(self) -> DataSource:
        """ the source object for this ingestor. """
        return self._source
    
    @source.setter
    def source(self, value):
        if isinstance(value, DataSource):
            self._source = value
        else:
            raise IngestionException(f'invalid type, expected a data source type, got {type(value)}')
        
    @property
    def added_symbols(self):
        return set()
    
    @property
    def logger(self):
        raise NotImplementedError
        
    @abstractmethod
    def run(self, *args, **kwargs):
        """ starts the main ingestion process/ loop. """
        raise NotImplementedError
        
    @abstractmethod
    def stop(self, *args, **kwargs):
        """ stops the main ingestion process/ loop. """
        raise NotImplementedError
    
class StreamingIngestor(IngestionEngine):
    """ interface for an ingestor with a streaming source. """
    @property
    @abstractmethod
    def is_connected(self) -> bool:
        return False

    @abstractmethod
    def connect(self, *args, **kwargs):
        raise NotImplementedError
    
    @abstractmethod
    def reconnect(self, *args, **kwargs):
        raise NotImplementedError
    
    @abstractmethod
    def disconnect(self):
        raise NotImplementedError
    
    @abstractmethod
    def ensure_connected(self, *args, **kwargs):
        raise NotImplementedError
    
    @abstractmethod
    def register(self, channel, handler, **kwargs):
        raise NotImplementedError
    
    @abstractmethod
    def subscribe(self, payload, assets=None):
        raise NotImplementedError
    
    @abstractmethod
    def is_subscribed(self, asset:MarketData) -> bool:
        raise NotImplementedError
    
    @abstractmethod
    def remove(self, asset:MarketData):
        raise NotImplementedError
    
    @abstractmethod
    def add_chunk(self, asset:MarketData, data):
        raise NotImplementedError
    
    def reset(self, *args, **kwargs):
        pass
        
_ingestor_registry: Dict[str, Type[IngestionEngine]] = {}
_builtin_ingestor_loader: Callable[[Any], None] | None = None

def register_ingestor(name: str, cls: Type[IngestionEngine]):
    """Register a new ingestor type."""
    _ingestor_registry[name] = cls

def set_builtin_ingestor_loader(loader: Callable[[Any], None]):
    """Register a callable that will lazily load built-in ingestor types."""
    global _builtin_ingestor_loader
    _builtin_ingestor_loader = loader

def _ensure_builtin_ingestor_loaded(ingestor_type:str|None=None):
    if _builtin_ingestor_loader:
        _builtin_ingestor_loader(ingestor_type)

    if (ingestor_type and ingestor_type not in _ingestor_registry) or ingestor_type is None:
        try:
            load_plugins('blueshift.plugins.data')
        except Exception:
            pass

def ingestor_factory(ingestor_type: str, *args, **kwargs) -> IngestionEngine:
    """ create ingestor instance by type name. """
    from inspect import getfullargspec

    if ingestor_type not in _ingestor_registry:
        _ensure_builtin_ingestor_loaded(ingestor_type)  # lazy load builtins
    cls = _ingestor_registry.get(ingestor_type)
    if cls is None:
        raise NotImplementedError(f"Unknown data source type: {ingestor_type}")
    
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

def list_ingestors() -> list[str]:
    """ registered ingestor class names. """
    _ensure_builtin_ingestor_loaded()
    return list(_ingestor_registry)

class IngestionBundle(ABC):
    @abstractmethod
    def ingest(self, src, dest, *args, **kwargs):
        raise NotImplementedError
    
_ingest_bundle_registry: Dict[str, IngestionBundle] = {}
_builtin_ingest_bundle_loader: Callable[[Any], None] | None = None

def register_ingestion_bundle(name: str, cls: IngestionBundle):
    """Register a new ingestion bundle type."""
    _ingest_bundle_registry[name] = cls

def set_builtin_ingestion_bundle_loader(loader: Callable[[Any], None]):
    """Register a callable that will lazily load built-in ingestion bundle types."""
    global _builtin_ingest_bundle_loader
    _builtin_ingest_bundle_loader = loader

def _ensure_builtin_ingestion_bundle_loaded(bundle_type:str|None=None):
    if _builtin_ingest_bundle_loader:
        _builtin_ingest_bundle_loader(bundle_type)

    if (bundle_type and bundle_type not in _ingest_bundle_registry) or bundle_type is None:
        try:
            load_plugins('blueshift.plugins.data')
        except Exception:
            pass

def get_ingestion_bundle(bundle: str) -> IngestionBundle:
    """ create ingestion bundle instance by type name. """
    if bundle not in _ingest_bundle_registry:
        _ensure_builtin_ingestion_bundle_loaded(bundle)  # lazy load builtins
    obj = _ingest_bundle_registry.get(bundle)
    if obj is None:
        raise NotImplementedError(f"Unknown ingestion bundle: {bundle}")
    return obj

def list_ingestion_bundles() -> list[str]:
    """ registered ingestion bundles class names. """
    _ensure_builtin_ingestor_loaded()
    return list(_ingest_bundle_registry)

__all__ = [
    'IngestionEngine',
    'StreamingIngestor',
    'IngestionMode',
    'IngestionBundle',
    'register_ingestor',
    'ingestor_factory',
    'list_ingestors',
    'set_builtin_ingestor_loader',
    'register_ingestion_bundle',
    'get_ingestion_bundle',
    'list_ingestion_bundles',
    'set_builtin_ingestion_bundle_loader',
    ]