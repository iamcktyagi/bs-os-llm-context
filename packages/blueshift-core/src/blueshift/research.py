from __future__ import annotations
from typing import Any, TYPE_CHECKING, Literal
from os.path import expanduser as _os_expanduser
from os.path import join as _os_join
from functools import lru_cache

from blueshift.lib.exceptions import BlueshiftException, SymbolNotFound
from blueshift.config import get_config
from blueshift.calendar.date_utils import make_consistent_tz
from blueshift.interfaces.data.library import ILibrary, get_library as get_default_library

if TYPE_CHECKING:
    import pandas as pd
    from blueshift.interfaces.assets._assets import MarketData, Asset
else:
    import blueshift.lib.common.lazy_pandas as pd

try:
    from blueshift.pipelines import setup_pipeline_engine # type: ignore -> optional dependency
    PIPELINE_SUPPORT = True
except ImportError:
    PIPELINE_SUPPORT = False

_cache:dict[str, Any] = {'current':None, 'engine': None, 'calendar': None}

@lru_cache()
def list_datasets():
    """ List available dataset names. """
    try:
        _root = get_config().datasets.get('library','')
        return ILibrary.list_libraries(_root)
    except Exception:
        raise BlueshiftException(f'failed to list datasets.')

@lru_cache()
def use_dataset(name:str):
    """ Set the current dataset by name. """
    try:
        _root = _os_expanduser(get_config().datasets.get('library',''))
        path = _os_join(_root, name)
        lib = get_default_library(root=path)
        _cache['current'] = lib
        _cache['engine'] = None
        _cache['calendar'] = None
    except Exception:
        raise BlueshiftException(f'failed to set dataset to {name}.')

def _get_lib() -> ILibrary:
    _lib = _cache.get('current')
    if not _lib:
        msg = f'No dataset selected, use `blueshift.research.use_dataset` to select the '
        msg += f'dataset first. Use `blueshift.research.list_dataset` to list available datasets.'
        raise BlueshiftException(msg)
    return _lib

def get_library():
    return get_data_portal()

def get_data_portal():
    """ get the current data portal object for selected dataset. """
    return _get_lib()

def symbol(sym, dt=None, *args, **kwargs) -> MarketData:
    """ Get the asset for a given instrument symbol. """
    _lib = _get_lib()
    
    try:
        return _lib.symbol(sym, dt=dt, *args, **kwargs)
    except Exception as e:
        raise SymbolNotFound(f'Asset not found for symbol {sym}:{str(e)}') from e

@lru_cache(maxsize=4096)
def sid(sec_id) -> MarketData:
    """ Get the asset for a given security ID from the pipeline store. """
    _lib = _get_lib()
    try:
        if not _lib.pipeline_store:
            raise BlueshiftException(f'current dataset does not support pipeline APIs.')
        return _lib.pipeline_store.sid(sec_id)
    except Exception as e:
        raise SymbolNotFound(f'Asset not found for SID {sec_id}:{str(e)}') from e
    

def current(assets:MarketData|list[MarketData], columns:str|list[str]='close', 
            dt:pd.Timestamp|str|None=None, last_known:bool=True, **kwargs) -> float|pd.Series|pd.DataFrame:
    """
        Return last available price. If either assets or columns 
        is a list, a series is returned, indexed by assets or 
        fields, respectively. If both are lists, a dataframe is 
        returned. Otherwise, a scalar is returned. Only OHLCV column 
        names are supported in general. However, for futures and options,
        ``open_interest``, ``implied_vol`` and greeks are supported as well.
        
        :param assets: An asset or a list for which to fetch data.
        :type assets: :ref:`asset<Asset>` object or a ``list`` of assets.
        :param columns: A field name or a list of OHLCV columns.
        :type columns: ``str`` or a ``list``.
        :param dt: The timestamp at which to fetch the data.
        :type dt: pd.Timestamp or string that can be convereted to Timestamp.
        :param bool last_known: If missing, last known good value (instead of NaN).
        :return: current price of the asset(s).
        :rtype: ``float`` (``int`` in case of volume), ``pandas.Series`` or ``pandas.DataFrame``.
        
    """
    _lib = _get_lib()
    if not dt:
        dt = pd.Timestamp.now(tz=_lib.tz)

    dt = make_consistent_tz(dt, _lib.tz)
    return _lib.current(
            assets, columns=columns, dt=dt, last_known=last_known, 
            frequency='1m')

def history(assets:MarketData|list[MarketData], columns:str|list[str], nbars:int, 
            frequency:str, dt:pd.Timestamp|None=None, adjusted:bool=True, 
            **kwargs) -> pd.Series|pd.DataFrame:
    """ 
        Returns given number of bars for the assets. If more than 
        one asset or more than one column supplied, returns a 
        dataframe, with assets or fields as column names. If both 
        assets and columns are multiple, returns a multi-index 
        dataframe with columns as the column names and asset as the 
        second index level. For a single asset and a single field, 
        returns a series. Only OHLCV column names are supported. However,
        for futures and options, ``open_interest``, ``implied_vol`` and greeks 
        are also supported.
            
        :param assets: An asset or a list for which to fetch data.
        :type assets: :ref:`asset<Asset>` object or a ``list`` of assets.
        :param columns: A field name or a list of OHLCV columns.
        :type columns: ``str`` or a ``list``.
        :param int nbars: Number of bars to fetch.
        :param str frequency: Frequency of bars (either '1m' or '1d').
        :param dt: The timestamp at which to fetch the data.
        :type dt: pd.Timestamp or string that can be convereted to Timestamp.
        :param bool adjusted: Whether to apply adjustments.
        :return: historical bars for the asset(s).
        :rtype: ``pandas.Series`` or ``pandas.DataFrame``.
        
        .. code-block:: python
            
            # this assumes we have already selected the dataset
            from blueshift.research import symbol, current, history
            
            # fetch an asset by symbol "ABC"
            asset = symbol('ABC')
            
            # fetch historical data as of a given date
            df = history(asset, ['close','high'], 10, '1m', dt="2023-05-05 14:30:00")
            df.close.plot()
        
    """
    _lib = _get_lib()

    if not dt:
        dt = pd.Timestamp.now(tz=_lib.tz)
    dt = make_consistent_tz(dt, _lib.tz)

    return _lib.history(
            assets, columns=columns, nbars=nbars, frequency=frequency, 
            dt=dt, adjusted=adjusted, **kwargs)
    
def get_expiries(asset:Asset, start_dt:str|pd.Timestamp, end_dt:str|pd.Timestamp, 
                 offset:int|None=None) -> pd.DatetimeIndex:
    """
        Returns expiry dates for the given asset between the start and 
        the end dates. If `offset` is specified, the asset offset is 
        ignored and the specified offset is used instead. The offset 
        is the instrument offset e.g. 0 for near month, 1 for next etc.
        
        :param asset: An asset object.
        :type asset: :ref:`asset<Asset>` object.
        :param pd.Timestamp start_dt: start date for expiries
        :param pd.Timestamp end_dt: start date for expiries
        :param int offset: The instrument offset.
        :returns: pandas datetime index
    """
    _lib = _get_lib()
    return _lib.get_expiries(asset, start_dt, end_dt, offset=offset)
    
def fundamentals(assets:list[Asset], metrics:list[str], nbars:int, frequency:Literal['Q','A'], 
                 dt:pd.Timestamp|str|None=None) -> dict[Asset, pd.DataFrame]:
    """ 
        Returns given number of bars for the fundamental metrics. This 
        always returns a dict with asset(s) as the key(s), or an empty 
        dict of no data available.
        
        .. note::
            Available only if fundamental data source is supported. If the 
            source data of the store supports point-in-time, the data 
            returned is point-in-time as well.
        
        :param assets: An asset or a list for which to fetch data.
        :type assets: :ref:`asset<Asset>` object or a ``list`` of assets.
        :param metrics: A field name or a list of Fundamental metrics.
        :type metrics: ``str`` or a ``list``.
        :param int nbars: Number of records to fetch.
        :param str frequency: Frequency of bars (either 'Q' or 'A').
        :param dt: The timestamp at which to fetch the data.
        :type dt: pd.Timestamp or string that can be convereted to Timestamp.
        :return: historical fundamental metrics for the asset(s).
        :rtype: dict.
            
        .. code-block:: python
            
            # this assumes we have already selected the dataset
            from blueshift.research import symbol, fundamentals
            from blueshift.protocol import FundamentalColumns
            
            # see available columns
            print(FundamentalColumns.Quarterly)
            print(FundamentalColumns.Annual)
            print(FundamentalColumns.Ratios)
            
            # fetch an asset by symbol "ABC"
            asset = symbol('ABC')
            
            # fetch historical data for profit before tax and profit after 
            # tax as of a given date
            metrics = [FundamentalColumns.Quarterly.pbt, FundamentalColumns.Quarterly.pat]
            df = fundamentals(asset, metrics, 10, 'Q', dt="2023-05-05 14:30:00")
            df[asset].pbt.plot()
    """
    _lib = _get_lib()
    
    return _lib.fundamentals(
            assets, metrics=metrics, nbars=nbars, frequency=frequency, 
            dt=dt)
    
def get_sectors(assets:list[Asset], frequency:Literal['Q','A'], dt:pd.Timestamp|None=None):
    """
        Get the sectors for the given assets where available. Returns a 
        pandas dataframe with assets as the index and sector names as a 
        column with name 'sector'.
        
        .. note::
            Available only if fundamental data source is supported.
    """
    _lib = _get_lib()
    
    return _lib.get_sectors(assets, frequency, dt=dt)

def list_sectors(frequency:Literal['Q','A']):
    """
        List the sectors for a given fundamental data source. Returns 
        a list of strings (sector names).
        
        .. note::
            Available only if fundamental data source is supported.
        
        :param str frequency: Frequency of data (either 'Q' or 'A').
        :return: available sectors.
        :rtype: list.
        
    """
    _lib = _get_lib()
    
    return _lib.list_sectors(frequency)
    
def _create_pipeline_engine():
    library = _get_lib()
        
    try:
        if not PIPELINE_SUPPORT:
            raise ImportError(f'Blueshift pipeline package not installed.')
        daily_store = library.pipeline_store
        if not daily_store:
            raise BlueshiftException(f'pipeline is not supported.')
    except Exception as e:
        raise BlueshiftException(f'The library has no support for pipeline:{str(e)}')
        
    adj_handler = daily_store.adjustments_handler
    quarterly_fundamental_store = library.quarterly_fundamental_store
    annual_fundamental_store = library.annual_fundamental_store
            
    engine = setup_pipeline_engine( # type: ignore
        library, daily_store, adj_handler, quarterly_fundamental_store, 
        annual_fundamental_store)
    _cache['engine'] = engine
    _cache['calendar'] = daily_store.calendar

def run_pipeline(pipeline, start_date, end_date):
    """ 
        Run a pipeline between given dates. This will return a pandas 
        multi-index dataframe with timestamp as the first index, the 
        selected assets the second index and the pipeline factor(s) as 
        the column(s).
        
        .. important::
            This function will work only in the research environment, 
            using this in a strategy will throw error.
        
        .. code-block:: python
            
            # import Pipeline constructor and built-in factors/ filters
            # this assumes we have already selected the dataset
            
            from blueshift.research import run_pipeline
            from blueshift.pipeline import Pipeline
            from blueshift.library.pipelines import select_universe, period_returns
            
            # create the pipeline
            def create_pipeline():
                pipe = Pipeline()
                liquidity = select_universe(200, 200) # a built-in liquidy screener
                mom = period_returns(20) # return over period factor
                mom_filter = mom > 0 # positive momentum
                
                pipe.add(mom,'momentum')
                pipe.set_screen(liquidity & mom_filter)
                
                return pipe
                
            # run the pipeline
            pipe = create_pipeline()
            results = run_pipeline(pipe, '2022-05-04', '2023-05-05')
            
            # returns multi-index df with dates as the first level and 
            # filtered assets as the second level indices, with factors 
            # added using pipe.add() as columns
            print(results.describe())
                
    """
    _ = _get_lib()
    
    if not _cache['engine']:
        _create_pipeline_engine()
    
    engine, cal = _cache['engine'], _cache['calendar']
    
    start_date = make_consistent_tz(start_date, cal.tz)
    end_date = make_consistent_tz(end_date, cal.tz)
    sessions = cal.sessions(start_date, end_date)
    
    return engine.run_pipeline(pipeline, sessions[0], sessions[-1])


__all__ = [
        'list_datasets',
        'use_dataset',
        'symbol',
        'sid',
        'current',
        'history',
        'run_pipeline'
        ]

