from typing import Dict, Type, Callable, Generator, Any, AsyncGenerator
from enum import Enum
from abc import ABC, abstractmethod
import logging
import asyncio
from collections.abc import Iterable, AsyncIterable

from blueshift.lib.common.enums import ConnectionStatus
from blueshift.lib.common.sentinels import noop, async_noop

from ..plugin_manager import load_plugins

class DataSourceType(Enum):
    """ Data source types. """
    UNDEFINED = 0
    MEMORY = 1          # in-memory source like a dataframe.
    FILE = 2            # file source like csv files or a directory.
    HTTP = 3             # URL source like a http server.
    WEBSTREAM = 4       # streaming source like a websocket.
    SOCKETIO = 5       # streaming source like a socetIO connection.
    MSGSTREAM = 6       # message stream source like a kafka stream.
    ZMQ = 7            # message stream source from zmq subscriber.

class DataSource(ABC):
    """
        Abstract base class defining the interface of source objects.
        The source object is usually an iterator, which is iterated 
        during an ingestion process. At each iterator, a tuple of 
        `metadata` and `data` is generated. The `data` part is the 
        actual pricing data (or other types of data to be persisted).
        The `metadata` identifies the entity (a stock, a forex pair 
        etc.) to which the data pertains to. The `metada` should have
        enough information to create the corresponding `asset` object 
        for the entity during the ingestion. There are two ways to 
        support adding `metadata` to the source output tuple. One is 
        to use the method `add_metadata`, that injects global information
        that are applicable for all assets from this source. The method 
        `add_masterdata` on the other hand can be used to inject 
        symbol/ asset specific information. The `masterdata` is defined 
        before the ingestion iteration begins and definse symbol-specific 
        metadata.
    """
    def __init__(self, *args, **kwargs):
        self._type = DataSourceType.UNDEFINED
        self._root = None           # the root location or url of source
        self._frequency = None
        self._metadata = None
        self._exception = None

    def __init_subclass__(cls, *args, **kwargs):
        super(cls).__init_subclass__(*args, **kwargs)

        if cls is DataSource:
            return

        has_sync = issubclass(cls, Iterable)
        has_async = issubclass(cls, AsyncIterable)

        if not (has_sync or has_async):
            raise TypeError(
                f"{cls.__name__} must implement either the sync "
                f"iterator protocol (__iter__) or the async iterator "
                f"protocol (__aiter__)."
            )
    
    def __str__(self):
        return f"Blueshift Data Source([{self.type}]@[{self.root}])" 
    
    def __repr__(self):
        return self.__str__()
    
    @property
    def type(self) -> str:
        """ Returns the data source type. """
        return self._type.name
    
    @property
    def error(self) -> Exception|None:
        return self._exception
    
    @error.setter
    def error(self, value):
        self._exception = value

    @property
    def data_channel(self) -> str|None:
        pass
    
    @property
    @abstractmethod
    def timeout(self) -> int:
        raise NotImplementedError
    
    @property
    @abstractmethod
    def logger(self) -> logging.Logger:
        raise NotImplementedError

    @property
    def finalizer(self) -> Callable:
        """ Returns data finalizer method. """
        return noop

    @finalizer.setter
    def finalizer(self, func:Callable[..., None]):
        pass
        
    @property
    @abstractmethod
    def frequency(self):
        """ Data frequency in Pandas format. """
        raise NotImplementedError
        
    @property
    @abstractmethod
    def root(self) -> str|None:
        """ Returns root of the connection url. """
        return self._root
        
    @property
    @abstractmethod
    def connected(self) -> bool:
        """ Check if connected to the source. """
        raise NotImplementedError
    
    @property
    @abstractmethod
    def connection_status(self) -> ConnectionStatus:
        raise NotImplementedError
        
    @property
    @abstractmethod
    def metadata(self) -> dict:
        """ Returns metadata about the source. """
        raise NotImplementedError
    
    @abstractmethod
    def connect(self, *args, **kwargs) -> None:
        """ Connect to the source. """
        raise NotImplementedError
    
    async def aconnect(self, *args, **kwargs):
        return await async_noop(*args, **kwargs)
        
    @abstractmethod
    def close(self, *args, **kwargs) -> None:
        """ Close connection to the source. """
        raise NotImplementedError
    
    async def aclose(self, *args, **kwargs):
        return await async_noop(*args, **kwargs)
    
    @property
    def loop(self) -> asyncio.AbstractEventLoop|None:
        """ Returns the event loop for this source. """
        pass
    
    @loop.setter
    def loop(self, value:asyncio.AbstractEventLoop|None):
        pass
        
    @abstractmethod
    def add_masterdata(self, *args, **kwargs) -> None:
        """
            Add metadata symbol-wise. Add a master data dataframe, with 
            a `symbol` column that provides additional fields for 
            enriching metadata for a given symbol.
        """
        raise NotImplementedError
        
    @abstractmethod
    def add_metadata(self, *args, **kwargs) -> None:
        """ Add extra metadata to be applied for **all** symbols. """
        raise NotImplementedError
        
    def __iter__(self) -> Generator:
        raise NotImplementedError
    
    def __aiiter__(self) -> AsyncGenerator:
        raise NotImplementedError
    
    def update(self, *args, **kwargs):
        pass

    def register(self, *args, **kwargs):
        pass

    def subscribe(self, *args, **kwargs):
        pass

    def unsubscribe(self, *args, **kwargs):
        pass

_data_source_registry: Dict[str, Type[DataSource]] = {}
_builtin_data_source_loader: Callable[[Any], None] | None = None

def register_data_source(name: str, cls: Type[DataSource]):
    """Register a new data source type."""
    _data_source_registry[name] = cls

def set_builtin_data_source_loader(loader: Callable[[Any], None]):
    """Register a callable that will lazily load built-in data store types."""
    global _builtin_data_source_loader
    _builtin_data_source_loader = loader

def _ensure_builtin_data_source_loaded(source_type:str|None=None):
    if _builtin_data_source_loader:
        _builtin_data_source_loader(source_type)

    if (source_type and source_type not in _data_source_registry) or source_type is None:
        try:
            load_plugins('blueshift.plugins.data')
        except Exception:
            pass

def data_source_factory(source_type: str, *args, **kwargs) -> DataSource:
    """ create data source instance by type name. """
    from inspect import getfullargspec

    if source_type not in _data_source_registry:
        _ensure_builtin_data_source_loaded(source_type)  # lazy load builtins
    cls = _data_source_registry.get(source_type)
    if cls is None:
        raise NotImplementedError(f"Unknown data source type: {source_type}")
    
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

def list_data_sources() -> list[str]:
    """ registered data sources class names. """
    _ensure_builtin_data_source_loaded()
    return list(_data_source_registry)

__all__ = [
    'DataSource',
    'DataSourceType',
    'register_data_source',
    'data_source_factory',
    'list_data_sources',
    'set_builtin_data_source_loader',
    ]