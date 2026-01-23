from __future__ import annotations
from typing import TypedDict, TYPE_CHECKING, Literal, Any
from collections import namedtuple
from enum import Enum
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import math


from blueshift.lib.common.types import ListLike
from blueshift.lib.common.constants import CurrencyPair
from ..assets._assets import MarketData, Asset

if TYPE_CHECKING:
    import pandas as pd
else:
    import blueshift.lib.common.lazy_pandas as pd

@dataclass(slots=True)
class Quote:
    price:float=math.nan
    volume:int=0

@dataclass(slots=True)
class BidAskQuote:
    """ data type for market quote (L1). """
    bids:list[Quote] = field(default_factory=list)
    asks:list[Quote] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data:dict[str, Any]):
        obj = BidAskQuote()
        obj.update_from_dict(data)
        return obj

    @classmethod
    def extract_quotes(cls, data:list, side:str):
        quotes:list[Quote] = []

        for item in data:
            if not isinstance(item, tuple) or len(item) < 2:
                continue
            quotes.append(Quote(price=float(item[0]), volume=int(item[1])))

        if side in ('buy', 'bid'):
            return sorted(quotes, key=lambda x:x.price, reverse=True)
        else:
            return sorted(quotes, key=lambda x:x.price)

    def update_from_dict(self, data):
        if not isinstance(data, dict):
            return

        bids = data['bids'] if 'bids' in data else data.get('buy',[])
        if isinstance(bids, list):
            self.bids = self.extract_quotes(bids, 'bid')
        
        asks = data['asks'] if 'asks' in data else data.get('sell',[])
        if isinstance(asks, list):
            self.asks = self.extract_quotes(asks, 'ask')

@dataclass(slots=True)
class MarketDepth:
    """ data type for market depth (L2 and higher) quotes. """
    timestamp:pd.Timestamp = pd.NaT # type: ignore
    last:float = math.nan
    close:float = math.nan
    open:float = math.nan
    high:float = math.nan
    low:float = math.nan
    open_interest:int = 0
    upper_circuit:float = math.nan
    lower_circuit:float = math.nan
    market_depth:BidAskQuote = field(default_factory=BidAskQuote)

    @classmethod
    def from_dict(cls, data:dict[str, Any]):
        obj = MarketDepth()
        obj.update_from_dict(data)
        return obj

    def update_from_dict(self, data:dict[str, Any]):
        if not isinstance(data, dict):
            return
        
        if 'timestamp' in data:
            self.timestamp = pd.Timestamp(data['timestamp'])

        for attr in ('last','close','open','high','low','upper_circuit','lower_circuit'):
            if attr in data:
                setattr(self, attr, float(data[attr]))

        for attr in ('open_interest'):
            if attr in data:
                setattr(self, attr, int(data[attr]))

        if 'market_depth' in data:
            self.market_depth.update_from_dict(data['market_depth'])
        elif 'bid' in data or 'ask' in data:
            bid, bid_volume = float(data['bid']), int(data.get('bid_volume',0))
            ask, ask_volume = float(data['ask']), int(data.get('ask_volume',0))
            bids = [Quote(bid, bid_volume)]
            asks = [Quote(ask, ask_volume)]
            self.market_depth = BidAskQuote(bids=bids, asks=asks)
    
class SubscriptionLevel(Enum):
    TRADE='TRADE'
    QUOTE='QUOTE'
    CANDLES='CANDLES'
    TBT='TBT'
    CUSTOM='CUSTOM'

class DataPortal(ABC):
    """
        DataPortal class defines the interface for the ``data`` object 
        in the callback functions. It defines two basic methods - ``current`` 
        and ``history``. User strategy should use these methods to 
        query and fetch data from within a running algo.
    """
    @property
    @abstractmethod
    def tz(self) -> str:
        raise NotImplementedError
    
    def has_streaming(self) -> bool:
        """ if streaming data supported. """
        return False
    
    def getfx(self, ccy_pair:str|CurrencyPair, dt:pd.Timestamp) -> float:
        """ get currency conversion rate. """
        raise NotImplementedError
        
    def update_market_quotes(self, asset:Asset, data:dict):
        """ implement to update market quotes. """
        pass
    
    @abstractmethod
    def current(self, assets:MarketData|list[MarketData], columns:str|list[str], 
                **kwargs) -> float|pd.Series|pd.DataFrame:
        """
            Return last available price. If either assets and columns 
            are multiple, a series is returned, indexed by assets or 
            fields, respectively. If both are multiple, a dataframe is 
            returned. Otherwise, a scalar is returned. Only OHLCV column 
            names are supported in general. However, for futures and options,
            ``open_interest`` is supported as well.
            
            :param assets: An asset or a list for which to fetch data.
            :type assets: :ref:`asset<Asset>` object or a ``list`` of assets.
            :param columns: A field name or a list of OHLCV columns.
            :type columns: ``str`` or a ``list``.
            :return: current price of the asset(s).
            :rtype: ``float`` (``int`` in case of volume), ``pandas.Series`` or ``pandas.DataFrame``.
            
            .. warning::
                The data returned can be a ``NaN`` value, an empty series or
                an empty DataFrame, if there are missing data for the 
                asset(s) or column(s). Also, the returned series or frame 
                may not contain all the asset(s) or column(s) if such 
                asset or column has missing data. User strategy must 
                always check the returned data before further processing.
            
        """
        raise NotImplementedError
        
    @abstractmethod
    def history(self, assets:MarketData|list[MarketData], columns:str|list[str], nbars:int, 
                frequency:str, **kwargs) -> pd.Series|pd.DataFrame:
        """ 
            Returns given number of bars for the assets. If more than 
            one asset or more than one column supplied, returns a 
            dataframe, with assets or fields as column names. If both 
            assets and columns are multiple, returns a multi-index 
            dataframe with columns as the column names and asset as the 
            second index level. For a single asset and a single field, 
            returns a series. Only OHLCV column names are supported. However,
            for futures and options, ``open_interest`` is also supported.
                
            :param assets: An asset or a list for which to fetch data.
            :type assets: :ref:`asset<Asset>` object or a ``list`` of assets.
            :param columns: A field name or a list of OHLCV columns.
            :type columns: ``str`` or a ``list``.
            :param int nbars: Number of bars to fetch.
            :param str frequency: Frequency of bars (either '1m' or '1d').
            :return: historical bars for the asset(s).
            :rtype: ``pandas.Series`` or ``pandas.DataFrame``.
            
            .. important::
                By passing a keyword argument `dt`, you can set the timestamp 
                from which the the data as seen is returned. Be careful to use 
                this in backtest, as passing a timestamp greater than the 
                current backtest timestamp (returned by `get_datetime` API 
                method) can lead to look-ahead bias in your strategy. In 
                case of live/ paper trading passing  timestamp has no impact 
                and will be ignored.
                
            .. important::
                In live trading, passing a keyword argument `research=True` 
                will trigger a query to the saved research data, instead of 
                realtime data. In case the live broker implementation does 
                not support it, it will raise an error. Separately, by passing 
                keyword `cached=False` will force an API data fetch (bypassing 
                data from streaming, if available). Be careful to use it, as it 
                may slow down the algo performance as well as may lead to API 
                rate limit related issues.
            
            .. warning::
                The data returned can be an empty series or
                an empty DataFrame, if there are missing data for the 
                asset(s) or column(s). Also, the returned series or frame 
                may not contain all the asset(s) or column(s) if such 
                asset or column has missing data. In addition, for 
                multi-indexed DataFrame, user strategy code must not 
                assume aligned data with same timestamps for different assets 
                (however, columns will always be aligned for a given
                asset). User strategy must always check the returned 
                data before further processing.
        """
        raise NotImplementedError
        
    def pre_trade_details(self, asset:Asset, **kwargs) -> tuple[float,float|None,float|None]:
        """
            RMS function for pre-trade checks. Returns current mid price 
            (defaults to ltp in case not available), as well as the upper 
            and lower values of the allowed price band if applicable (
            else None).
        """
        px = self.current(asset, 'close', **kwargs)
        return px, None, None # type: ignore
        
    def quote(self, asset:Asset, column:str|None=None, **kwargs) -> MarketDepth|BidAskQuote|list[Quote]:
        """ 
            Returns current bid/ask or market depth information.
                     
            In case the column is specified, it must be one of 'bid', 
            'ask' or 'market_depth'. For 'bid' and 'ask' a list of best bid
            or ask (`list[Quote]`). For 'market_depth', return type is `BidAskQuote`
            and if no column specified a `MarketDepth` object is returned.
                     
            The `timestamp` field is a pandas Timestamp object (tz-aware). 
            The `market_depth` data might be empty (default values). If 
            not empty, the depth depends on the available data from the 
            broker and strategies should not assume it will not be empty or 
            bid and ask will have certain or even equal depth.
                
            :param assets: An asset or a list for which to fetch data.
            :type assets: :ref:`asset<Asset>` object or a ``list`` of assets.
            
            .. warning::
                This method will return empty `dict` if the data portal 
                does not support quotes or market depth data.
        """
        return MarketDepth()
    
    def reset(self, *args, **kwargs) -> None:
        """ 
            Function to reset the data portal, including any caching.
        """
        pass
    
    def subscribe(self, assets:MarketData|list[MarketData], 
                  level:SubscriptionLevel=SubscriptionLevel.TRADE, *args, **kwargs) -> None:
        """
            Subscribe to price data feed. The`level` parameter determines 
            the subscription type. 
            
            .. important::
                Typically level=1 means trade price and level=2 means full 
                quotes subscriptions. However, this is implementation 
                dependent and level greater 1 may not be supported and may 
                mean different things. A value of 1 for level is supported 
                always (when the underlying source supports streaming 
                data in the first place of course).
            
            :param assets: An asset or a list for which to subscribe.
            :type assets: :ref:`asset<Asset>` object or a ``list`` of assets.
            :param int level: The level of data subscription.
            
        """
        pass
    
    def unsubscribe(self, assets:MarketData|list[MarketData], 
                    level:SubscriptionLevel=SubscriptionLevel.TRADE, *args, **kwargs) -> None:
        """
            Unsubscribe from price data feed. This will also remove 
            any temporary data stored for the asset(s).
            
            .. important::
                See the note from ``subscribe`` method above.
            
            :param assets: An asset or a list for which to subscribe.
            :type assets: :ref:`asset<Asset>` object or a ``list`` of assets.
            :param int level: The level of data subscription.
        """
        pass
    
    def get_expiries(self, asset:MarketData, start_dt:pd.Timestamp, end_dt:pd.Timestamp, 
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
        if not asset.is_rolling() and (asset.is_opt() or asset.is_futures()):
            return pd.to_datetime([asset.expiry_date]).tz_localize(self.tz) # type: ignore
        
        return pd.DatetimeIndex([])
    
    def option_chain(self, underlying:MarketData, series:str, columns:str|list[str], 
                     strikes:list[float]|None=None, relative:bool=True, 
                     **kwargs) -> tuple[pd.Series|pd.DataFrame, pd.Series|pd.DataFrame]:
        """
            Return option chain snapshot for a given underlying, series and 
            date-time. The `fields` can be any allowed value for 
            :ref:`data.current<Fetching Current Data>` columns.
            
            Args:
                `underlying (str)`: Underlying for the option chain
                
                `series (str)`: Expiry series (e.g. "W0" or "I" or expiry date).
                
                `columns (str or list)`: Data query fields.
                
                `strikes (list)`: List of strikes (absolute or relative)
                
                `relative (bool)`: If strikes should be relative or absolute.
                
            Returns:
                Series (if a single column) or DataFrame (for multiple 
                columns), with index as the strike price.
        """
        if not isinstance(columns, ListLike):
            return pd.Series(), pd.Series()
        
        if not isinstance(columns, ListLike):
            return pd.Series(columns=columns), pd.Series(columns=columns)
        else:
            return pd.DataFrame(columns=columns), pd.DataFrame(columns=columns) # type: ignore
    
    def fundamentals(self, assets:list[Asset], metrics:list[str], nbars:int, 
                     frequency:Literal['Q','A'], **kwargs) -> dict[Asset, pd.DataFrame]:
        """ 
            Returns given number of bars for the fundamental metrics. This 
            always returns a dict with asset(s) as  the keys, or an empty 
            dict of no data available. For available columns, see the 
            code snippet below.
            
            .. code-block:: python
            
                # this assumes we have already selected the dataset
                from blueshift.protocol import FundamentalColumns
                
                def before_trading_start(context):
                    print(FundamentalColumns.Quarterly)
                    print(FundamentalColumns.Annual)
                    print(FundamentalColumns.Ratios)
                
                    asset = symbol('ABC')
                    metrics = [FundamentalColumns.Quarterly.pbt, FundamentalColumns.Quarterly.pat]
                    df = fundamentals(asset, metrics, 10, 'Q')
            
            Note:
                This will work only in deployments where the fundamental 
                data is available, else it will throw an error. Where 
                available, ff the source data of the store supports 
                point-in-time, the data returned is point-in-time as well.
            
            Args:
                `assets (obj or list)`: An asset or a list.
                
                `metrics (str or list)`: List or a single fundamental metrics.
                
                `nbars (int)`: Number of bars to fetch.
                
                `frequencty (str)`: Frequency of the data, either 'Q' or 'A'.
                
                `dt (timestamp)`: As of date-time.
            
            Returns:
                A dictionary.
        """
        return {}
    
__all__ = [
    'Quote',
    'BidAskQuote',
    'MarketDepth',
    'SubscriptionLevel',
    'DataPortal',
    ]