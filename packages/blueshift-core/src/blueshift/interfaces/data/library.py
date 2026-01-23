from __future__ import annotations
from typing import TYPE_CHECKING, Literal, Type, Dict, Callable, Any
import os
import json
import datetime

from abc import abstractmethod
from .data_portal import DataPortal
from .store import DataStore
from ..assets.assets import IAssetFinder
from ..plugin_manager import load_plugins
from ..assets._assets import MarketData, Asset

if TYPE_CHECKING:
    import pandas as pd
    from pandas._libs.tslibs.nattype import NaTType
    from blueshift.lib.common.constants import Frequency

class ILibrary(IAssetFinder, DataPortal):
    """
        Library is a namespace over a collections lower-level `DataStore`. 
        This is primarily for read and other data query operations. Writes 
        should be done using `DataStore` implementation. Therefore this 
        interface should support no write methods.
        
        To instantiate a library, supplying a root location should be enough. 
        It should automatically search all valid stores at the location and 
        include them in the library instance. Alternative implementation option 
        is supplying a list of stores explicitly.
        
        Library primarily acts as a router for underlying store's 
        reading functions when handling multiple store, potentially 
        covering multiple data frequency, exchanges or asset classes. 
        It must also implements the `IAssetFinder` and `DataPortal` interfaces 
        to be a complete class to find, fetch and analysis assets and 
        their associated data.
        
    """
    _metafile = 'library.json'
    
    @classmethod
    def list_libraries(cls, path) -> list[str]:
        """ list all librarys (directory with library.json file). """
        datasets = []
        path = os.path.expanduser(path)
        if not os.path.exists(path):
            return datasets
        
        folders = os.listdir(path)
        for folder in folders:
            if os.path.exists(os.path.join(path,folder,cls._metafile)):
                datasets.append(folder)
                
        return datasets
    
    @classmethod
    def fetch_metadata(cls, root:str|None=None) -> dict:
        """ find the metadata from the location. """
        if not root:
            return {}
        
        root = os.path.expanduser(root)
        meta_path = os.path.join(root, cls._metafile)
        if os.path.exists(meta_path):
            with open(meta_path) as fp:
                metadata = json.load(fp)
                return metadata
            
        return {}
    
    @abstractmethod
    def __contains__(self, value:str|MarketData|tuple) -> bool:
        """ if the library contains a store. """
        raise NotImplementedError
    
    @abstractmethod
    def __getitem__(self, key:str|MarketData|tuple) -> DataStore:
        """ 
            resolve to an unique store, given the key, which can be either a 
            string or a tuple. Must raise exception if no unique store is found.
        """
        raise NotImplementedError
                
    @property
    @abstractmethod
    def name(self) -> str:
        """ name of the libarry. """
        raise NotImplementedError
    
    @property
    @abstractmethod
    def tz(self) -> str:
        """ timezone of the libarry. """
        raise NotImplementedError
    
    @property
    @abstractmethod
    def root(self) -> str|None:
        """ location of the library. """
        raise NotImplementedError
    
    @property
    @abstractmethod
    def metadata(self) -> dict:
        """ metadata of the library. """
        raise NotImplementedError
    
    @property
    @abstractmethod
    def stores(self) -> list[DataStore]:
        """ get the incldued stores in the library. """
        raise NotImplementedError
    
    @property
    @abstractmethod
    def active(self) -> DataStore|None:
        """ get the current active store in the library. """
        raise NotImplementedError
    
    @property
    @abstractmethod
    def quarterly_fundamental_store(self) -> DataStore|None:
        raise NotImplementedError
    
    @property
    @abstractmethod
    def annual_fundamental_store(self) -> DataStore|None:
        raise NotImplementedError
    
    @property
    @abstractmethod
    def pipeline_store(self) -> DataStore|None:
        raise NotImplementedError
    
    @property
    @abstractmethod
    def fx_rate_store(self) -> DataStore|None:
        raise NotImplementedError
                
    @property
    @abstractmethod
    def asset_finder_store(self) -> DataStore|None:
        raise NotImplementedError
        
    @property
    @abstractmethod
    def futures_store(self) -> DataStore|None:
        raise NotImplementedError
        
    @property
    @abstractmethod
    def options_store(self) -> DataStore|None:
        raise NotImplementedError
        
    @property
    @abstractmethod
    def start_date(self) -> pd.Timestamp|NaTType:
        raise NotImplementedError
    
    @property
    @abstractmethod
    def end_date(self) -> pd.Timestamp|NaTType:
        raise NotImplementedError
    

    #################### library methods ###############################
    @abstractmethod
    def add_store(self, store:DataStore) -> None:
        """ add a store to the library manually. """
        raise NotImplementedError
        
    @abstractmethod
    def find_stores(self, *args, **kwargs) -> list[DataStore]:
        raise NotImplementedError
    
    @abstractmethod
    def sort_stores(self, stores) -> list[DataStore]:
        raise NotImplementedError
    
    @abstractmethod
    def reset(self) -> None:
        raise NotImplementedError
    
    @abstractmethod
    def refresh(self, stores:list[DataStore]|None=None) -> None:
        raise NotImplementedError
        
    @abstractmethod
    def set_cache_policy(self, policy:str='random') -> None:
        """
            Set the caching policy on this library. If the policy is 
            `sliding` the subsequent reads must be in increasing order 
            of time and reading data for a past date will raise error. 
            Otherwise, random reads will be supported.
            
            Args:
                `policy (str)`: Set the cache policy for reads.
                
            Returns:
                None.
        """
        raise NotImplementedError
        
    @abstractmethod
    def set_roll_policy(self, roll_day:int=0, roll_time:tuple|datetime.time=(23,59)) -> None:
        """
            Set the roll policy on this library. The parameter ``roll`` is 
            the offset from the next expiry when futures and options are 
            rolled to compute pricing data for continues assets.
            
            Args:
                `roll_day (int)`: Set the roll day offset from the expiry date.
                
                `roll_time (tuple)`: Set (hour, minute) tuple for roll time.
                
            Returns:
                None.
        """
        raise NotImplementedError

    @abstractmethod
    def asset_to_store(self, asset:MarketData, *args, **kwargs) -> DataStore:
        """ Find the appropriate store given an asset. """
        raise NotImplementedError

    @abstractmethod
    def read(self, asset:MarketData, columns:str|list[str], nbars:int, frequency:str|Frequency, 
             dt:pd.Timestamp|None=None, adjusted:bool=True, precision:int=2, 
             store:DataStore|None=None) -> pd.Series|pd.DataFrame:
        """ 
            Returns given number of bars for the assets. If more than 
            one asset or more than one column supplied, returns a 
            dataframe, with assets or fields as column names. If both 
            assets and columns are multiple, returns a multi-index 
            dataframe. For a single asset and a single field, returns 
            a series.
            
            Note:
                if `dt` is None, the values are as of dt, else it is 
                the last available timestamp in the store. Frequency
                must match the frequency of one of the stores available 
                in the library.
            
            Args:
                `asset (obj)`: An asset object.
                
                `columns (str or list)`: A field name or a list.
                
                `nbars (int)`: Number of bars to fetch.
                
                `frequency (str)`: Frequency of the data.
                
                `dt (timestamp)`: As of date-time.
                
                `adjusted (bool)`: If data to be adjusted.
                
                `precision (int)`: Precision in the returned value.
                
                `store (object)`: The store to query.
            
            Returns:
                Series or dataframe.
        """
        raise NotImplementedError
        
    @abstractmethod
    def read_between(self, asset:MarketData, columns:str|list[str], start_dt:pd.Timestamp, 
                     end_dt:pd.Timestamp, frequency:str|Frequency, adjusted:bool=True, precision:int=2, 
                     store:DataStore|None=None) -> pd.Series|pd.DataFrame:
        """ 
            Returns data between dates for the assets. If more than 
            one asset or more than one column supplied, returns a 
            dataframe, with assets or fields as column names. If both 
            assets and columns are multiple, returns a multi-index 
            dataframe. For a single asset and a single field, returns 
            a series.
            
            Args:
                `asset (obj)`: An asset object.
                
                `columns (str or list)`: A field name or a list.
                
                `start_dt (str or timestamp)`: Start date.
                
                `end_dt (str or timestamp)`: End date.
                
                `frequencty (str)`: Frequency of the data.
                
                `adjusted (bool)`: If data to be adjusted.
                
                `precision (int)`: Precision in the returned value.
                
                `store (object)`: The store to query.
                
            
            Returns:
                Series or dataframe.
        """
        raise NotImplementedError
        
    @abstractmethod
    def get_spot_value(self, asset:MarketData, dt:pd.Timestamp, column:str, 
                       frequency:str|Frequency, **kwargs) -> float|int:
        """
            Read a spot value for a given asset and column at a given 
            timestamp (`dt`). In case there is no exact match, if the 
            column requested is volume, 0 is returned. If the match is 
            before the start index, 0 is returned for volume, and `np.nan` 
            otherwise. If the match is beyond the end index, 0 is returned 
            for volume and the last data for others.
            
            Args:
                `asset (obj)`: Asset object.
                
                `dt (timestamp)`: Timestamp of the value.
                
                `column (str)`: Column name to fetch data for.
                
                `frequencty (str)`: Frequency of the data.
                
            Returns:
                Scalar (double or int, depending on the column data type).
        """
        raise NotImplementedError

    @abstractmethod
    def get_expiry(self, asset:Asset, dt:str|pd.Timestamp, offset:int|None=None, 
                   store:DataStore|None=None) -> pd.Timestamp:
        """
            Get expiry date given an asset and offset.
        """
        raise NotImplementedError
    
    @abstractmethod
    def get_expiries(self, asset:MarketData, start_dt:str|pd.Timestamp, end_dt:str|pd.Timestamp, 
                     offset:int|None=None, store:DataStore|None=None) -> pd.DatetimeIndex:
        """
            Return the list of expiry days, given an asset.
            
            .. Note::
                Start or end date can be given either as string dates 
                or convertible to Pandas timestamp.
            
            Args:
                `asset (Asset)`: asset for which expiries are required.
                
                `start_dt (str)`: Start date for expiries.
                
                `end_dt (str)`: End date for expiries.
                
            Returns:
                None. Set the benchmark property of this library.
        """
        raise NotImplementedError
    
    @abstractmethod
    def get_benchmark(self, start, end) -> str|pd.Series:
        """ returns benchmark file (.csv) or a dataframe. """
        raise NotImplementedError
    
    def get_sectors(self, assets:list[Asset], frequency:Literal['Q','A'], dt:pd.Timestamp|None=None):
        """
        get sectors for the given assets
        
        :param assets: list of Assets
        :type assets: list[Asset]
        :param frequency: Reporting frequency
        :type frequency: Literal['Q', 'A']
        :param dt: as of date
        :type dt: pd.Timestamp | None
        """
        raise NotImplementedError
    
    def list_sectors(self, frequency:Literal['Q','A']):
        """
        get all available industry sectors.
        
        :param frequency: Reporting frequency
        :type frequency: Literal['Q', 'A']
        """
        raise NotImplementedError
    
_library_registry:Dict[str, Type[ILibrary]] = {}
_builtin_library_loader: Callable[[Any], None] | None = None

def register_library(name: str, cls: Type[ILibrary]):
    """Register a new library type."""
    _library_registry[name] = cls

def set_builtin_library_loader(loader: Callable[[Any], None]):
    """Register a callable that will lazily load built-in library types."""
    global _builtin_library_loader
    _builtin_library_loader = loader

def _ensure_builtin_library_loaded(library_type:str|None=None):
    if _builtin_library_loader:
        _builtin_library_loader(library_type)

    if (library_type and library_type not in _library_registry) or library_type is None:
        try:
            load_plugins('blueshift.plugins.data')
        except Exception:
            pass

def library_factory(library_type, root:str|None=None, *args, **kwargs) -> ILibrary:
    """ factory function to create library. """
    from inspect import getfullargspec

    if library_type not in _library_registry:
        _ensure_builtin_library_loaded(library_type)  # lazy load builtins
    cls = _library_registry.get(library_type)
    if cls is None:
        raise NotImplementedError(f"Unknown library type: {library_type}")
    
    specs = getfullargspec(cls.__init__)

    if specs.varkw:
        kw = kwargs.copy()
    else:
        args_specs = specs.args
        kw = {}
        for key in kwargs:
            if key in args_specs:
                kw[key] = kwargs[key]

    return cls(root, *args, **kw)

def get_library(root:str|None=None, **kwargs) -> ILibrary:
    return library_factory('default', root=root, **kwargs)

__all__ = [
    'ILibrary',
    'register_library',
    'library_factory',
    'get_library',
    'set_builtin_library_loader',
    ]