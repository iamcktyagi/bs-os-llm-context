from __future__ import annotations
from typing import Type, Dict, Callable, Any, TYPE_CHECKING
from abc import ABC, abstractmethod

from blueshift.lib.common.sentinels import noops
from blueshift.lib.exceptions import AssetsException, SymbolNotFound
from blueshift.lib.trades._order_types import ProductType

from ..plugin_manager import load_plugins

from ._assets import (
        MarketData, Asset, Equity, EquityMargin, EquityIntraday, 
        EquityFutures, CFDAsset, Forex, Crypto, FXFutures, SpotFX, 
        EquityOption, AssetClass, InstrumentType, Futures, Option)

from .utils import (
        get_name_from_sym, get_exchange_from_sym, get_base_ccy_from_sym, 
        get_quote_ccy_from_sym, get_ccy_pair_from_sym, get_clean_sym, 
        get_calendar_from_sym, get_opt_type_from_sym)

if TYPE_CHECKING:
    import pandas as pd

_ASSETCLASS_MAP = {'mktdata':MarketData,
                  'marketdata':MarketData,
                  'market_data':MarketData,

                  'asset': Asset,
                  'assets': Asset,
                  'equity':Equity,
                  'equitiies':Equity,
                  'eqty':Equity,
                  'eq':Equity,
                  
                  'equitymargin':EquityMargin,
                  'equity-margin':EquityMargin,
                  'eqty-margin':EquityMargin,
                  'eq-margin':EquityMargin,
                  'equity_margin':EquityMargin,
                  'eqty_margin':EquityMargin,
                  'eq_margin':EquityMargin,
                  
                  'equityintraday':EquityIntraday,
                  'equity-intraday':EquityIntraday,
                  'eqty-intraday':EquityIntraday,
                  'eq-intraday':EquityIntraday,
                  'equity_intraday':EquityIntraday,
                  'eqty_intraday':EquityIntraday,
                  'eq_intraday':EquityIntraday,

                  'equity_futures':EquityFutures,
                  'equity_future':EquityFutures,
                  'equity-futures':EquityFutures,
                  'equity-future':EquityFutures,
                  'eqty_futures':EquityFutures,
                  'eqty_future':EquityFutures,
                  'eqty_fut':EquityFutures,
                  'eq_fut':EquityFutures,
                  'equityfutures':EquityFutures,

                  'equity_options':EquityOption,
                  'equity_option':EquityOption,
                  'equity-options':EquityOption,
                  'equity-option':EquityOption,
                  'eq_options':EquityOption,
                  'eqty_option':EquityOption,
                  'eqty_option':EquityOption,
                  'equity_opt':EquityOption,
                  'eqty_opt':EquityOption,
                  'eq_opt':EquityOption,
                  'equityoption':EquityOption,

                  'fx':Forex,
                  'forex':Forex,
                  'fx_cfd':Forex,
                  'cfd_fx':Forex,
                  
                  'crypto':Crypto,

                  'spotfx':SpotFX,
                  'spot_fx':SpotFX,
                  'fxspot':SpotFX,
                  'fx_spot':SpotFX,

                  'fx_futures':FXFutures,
                  'fx_future':FXFutures,
                  'fx-futures':FXFutures,
                  'fx-future':FXFutures,
                  'fx_fut':FXFutures,
                  'fxfutures':FXFutures,
                  }

_INTERNAL_ATTRIBUTES = ['asset_class', 
                        'instrument_type', 
                        'mktdata_type',
                        'hashed_id',
                        ]

_instrument_type_map = {
    'SPOT':InstrumentType.SPOT,
    'FUTURES':InstrumentType.FUTURES,
    'OPT':InstrumentType.OPT,
    'OPTION':InstrumentType.OPT,
    'MARGIN':InstrumentType.MARGIN,
    'FUNDS':InstrumentType.FUNDS,
    'FUND':InstrumentType.FUNDS,
    'CFD':InstrumentType.CFD,
    'STRATEGY':InstrumentType.STRATEGY,
    }

def _get_class_attribs(obj):
    attrs = [f for f in dir(obj) if not callable(getattr(obj,f))
        and not str(f).endswith('_') and not str(f).startswith('_')]
    return attrs

def _infer_asset_cls(asset_cls, instrument_type):
    try:
        instrument_type = InstrumentType(instrument_type)
    except Exception:
        raise AssetsException(f'illegal instrument type {instrument_type}')
    
    if asset_cls == Equity:
        if instrument_type==InstrumentType.SPOT:
            return Equity
        elif instrument_type==InstrumentType.CFD:
            return CFDAsset
        elif instrument_type==InstrumentType.MARGIN:
            return EquityMargin
        elif instrument_type==InstrumentType.FUNDS:
            return Equity
    elif asset_cls == Forex:
        if instrument_type==InstrumentType.MARGIN:
            return Forex
        elif instrument_type==InstrumentType.SPOT:
            return SpotFX
    elif asset_cls == Crypto:
        if instrument_type==InstrumentType.MARGIN:
            return Crypto
        
    return asset_cls

def _parse_symbol(sym,
                 target,
                 sym_func=None,
                 name_func=None,
                 exchange_func=None,
                 calendar_func=None,
                 mult_func=None,
                 tick_size_func=None,
                 qoute_ccy_func=None,
                 base_ccy_func=None,
                 ccy_pair_func=None,
                 root_func=noops,
                 expiry_func=noops,
                 strike_func=noops,
                 opt_type_func=noops,
                 *args,
                 **kwargs):

    attribs = _get_class_attribs(target)

    if sym_func is None:
        sym_func = get_clean_sym
    if name_func is None:
        name_func = get_name_from_sym
    if exchange_func is None:
        exchange_func = get_exchange_from_sym
    if calendar_func is None:
        calendar_func = get_calendar_from_sym
    if mult_func is None:
        mult_func = lambda s:1
    if tick_size_func is None:
        tick_size_func = lambda s:100
    if qoute_ccy_func is None:
        qoute_ccy_func = get_quote_ccy_from_sym
    if base_ccy_func is None:
        base_ccy_func = get_base_ccy_from_sym
    if ccy_pair_func is None:
        ccy_pair_func = get_ccy_pair_from_sym
    if root_func is None:
        root_func = noops
    if expiry_func is None:
        expiry_func = noops
    if strike_func is None:
        strike_func = noops
    if opt_type_func is None:
        opt_type_func = get_opt_type_from_sym

    data = {}
    data['symbol'] = sym_func(sym, *args, **kwargs)
    data['name'] = name_func(sym, *args, **kwargs)

    if 'exchange_name' in attribs:
        data['exchange_name'] = exchange_func(sym, *args, **kwargs)
    if 'calendar_name' in attribs:
        data['calendar_name'] = calendar_func(sym, *args, **kwargs)
    if 'quote_ccy' in attribs:
        data['quote_ccy'] = qoute_ccy_func(sym, *args, **kwargs)
    if 'base_ccy' in attribs:
        data['base_ccy'] = base_ccy_func(sym, *args, **kwargs)
    if 'ccy_pair' in attribs:
        data['ccy_pair'] = ccy_pair_func(sym, *args, **kwargs)
    if 'root' in attribs:
        data['root'] = root_func(sym, *args, **kwargs)
    if 'expiry_date' in attribs:
        data['expiry_date'] = expiry_func(sym, *args, **kwargs)
    if 'strike' in attribs:
        data['strike'] = strike_func(sym, *args, **kwargs)
    if 'option_type' in attribs:
        data['option_type'] = opt_type_func(sym, *args, **kwargs)

    return data

def convert_asset(asset:Asset, product_type:str|ProductType|None=None, **kwargs) -> Asset|None:
    """
        Convert asset based on product type
    """
    if not product_type:
        return asset
    
    if isinstance(product_type, str):
        product_type = product_type.lower()
        if product_type == 'intraday':
            product_type = ProductType.INTRADAY         # type: ignore
        elif product_type == 'margin':
            product_type = ProductType.MARGIN           # type: ignore
        elif product_type == 'delivery':
            product_type = ProductType.DELIVERY         # type: ignore
        elif not product_type:
            product_type = ProductType.DELIVERY         # type: ignore
        else:
            return
        
    if asset.get_product_type() == product_type:
        return asset
    
    if isinstance(asset, (EquityFutures, EquityOption)):
        # no intraday for equity futures and options
        return asset
    
    _ASSET_MAP = {
            (Equity, ProductType.MARGIN):'equitymargin',
            (Equity, ProductType.INTRADAY):'equityintraday',
            (EquityMargin, ProductType.DELIVERY):'equity',
            (EquityMargin, ProductType.INTRADAY):'equityintraday',
            (EquityIntraday, ProductType.DELIVERY):'equity',
            (SpotFX, ProductType.MARGIN):'forex',
            (Forex, ProductType.DELIVERY):'spotfx',
            }
    
    key = (type(asset), product_type)
    if key in _ASSET_MAP:
        asset_type = _ASSET_MAP[key]
        return asset_factory(asset_type=asset_type,**asset.to_dict()) # type: ignore
    
_asset_registry: Dict[str, Type[MarketData]] = {
    'marketdata':MarketData,
    'asset': Asset,
    'equity':Equity,
    'opt':Option,
    'options':Option,
    'futures':Futures,
    'equity-margin':EquityMargin,
    'equity-intraday':EquityIntraday,
    'equity-futures':EquityFutures,
    'equity-option':EquityOption,
    'forex':Forex,
    'crypto':Crypto,
    'spotfx':SpotFX,
    'fx-futures':FXFutures,
    }

def register_asset(name: str, cls: Type[MarketData]):
    """Register a new Asset type."""
    _asset_registry[name] = cls

def get_asset_type(name:str) -> Type[MarketData]:
    if name in _asset_registry:
        return _asset_registry[name]
    
    raise AssetsException('no asset class registerd for name {name}')

def asset_factory(asset_type=None, **data) -> MarketData:
    """ create asset instance by type name. """
    if 'symbol' in data:
        symbol = data.pop('symbol')
    elif 'sym' in data:
        symbol = data.pop('sym')
    else:
        raise AssetsException('missing symbol')
    
    sym_func=data.pop('sym_func', None)
    name_func=data.pop('name_func', None)
    exchange_func=data.pop('exchange_func', None)
    calendar_func=data.pop('calendar_func', None)
    mult_func=data.pop('mult_func', None)
    tick_size_func=data.pop('tick_size_func', None)
    qoute_ccy_func=data.pop('qoute_ccy_func', None)
    base_ccy_func=data.pop('base_ccy_func', None)
    ccy_pair_func=data.pop('ccy_pair_func', None)
    root_func=data.pop('root_func', None)
    expiry_func=data.pop('expiry_func', None)
    strike_func=data.pop('strike_func', None)
    opt_type_func=data.pop('opt_type_func', None)
    product_type=data.pop('product_type', None)
    
    cls = None
    if 'class' in data and issubclass(data['class'], MarketData):
        cls = data['class']
    elif not asset_type:
        if 'asset_class' in data:
            asset_type = data['asset_class']
        elif 'asset' in data:
            asset_type = data['asset']

    if isinstance(asset_type, type) and issubclass(asset_type, MarketData):
        cls = asset_type
    
    if cls is None:
        if asset_type is None:asset_type = 'asset'
        asset_type = str(asset_type)
        cls = _ASSETCLASS_MAP.get(asset_type)
        if cls is None:
            cls = _asset_registry.get(asset_type)
        if cls is None:
            raise NotImplementedError(f"Unknown asset type: {asset_type}")
            
    if 'instrument_type' in data:
        instrument_type = data['instrument_type']
        original_instrument_type = instrument_type
        if isinstance(instrument_type, str):
            instrument_type = instrument_type.upper()
            instrument_type = _instrument_type_map.get(instrument_type)
        
        try:
            instrument_type = InstrumentType(instrument_type)
        except Exception:
            raise AssetsException(f'illegal instrument type {original_instrument_type}')
        else:
            data['instrument_type'] = instrument_type
            original_cls = cls
            cls = _infer_asset_cls(cls, instrument_type)
            if cls is None:
                msg = f"Asset class {original_cls} does not support instrument type {instrument_type}"
                raise NotImplementedError(msg)
    
    args_specs = _get_class_attribs(cls)
    for attrib in _INTERNAL_ATTRIBUTES:
        if attrib in args_specs:args_specs.remove(attrib)

    if cls == Equity:
        args_specs.append('instrument_type')

    sid = data.pop('sid',-1)
    parsed = _parse_symbol(symbol, cls,
                          sym_func=sym_func,
                          name_func=name_func,
                          exchange_func=exchange_func,
                          calendar_func=calendar_func,
                          mult_func=mult_func,
                          tick_size_func=tick_size_func,
                          qoute_ccy_func=qoute_ccy_func,
                          base_ccy_func=base_ccy_func,
                          ccy_pair_func=ccy_pair_func,
                          root_func=root_func,
                          expiry_func=expiry_func,
                          strike_func=strike_func,
                          opt_type_func=opt_type_func,
                          **data)

    for key in list(data.keys()):
        if key not in args_specs: data.pop(key)

    kwargs = {**parsed, **data}
    obj = cls(sid, **kwargs) # type: ignore

    if product_type:
        obj = convert_asset(obj, product_type=product_type) # type: ignore
        if not obj:
            msg = f'no asset found for symbol {symbol} and '
            msg += f'product type {product_type}'
            raise SymbolNotFound(msg)
        return obj
    return obj

def list_assets() -> list[str]:
    """ registered asset class names. """
    return list(_asset_registry)

def supported_assets() -> list[Type[MarketData]]:
    """ registered asset classes. """
    return list(_asset_registry.values())

class IAssetFinder(ABC):
    '''
        Interface for asset finders. This class interacts with an 
        asset database/ source and provide standard interface to interact, 
        search and manipulate them.
    '''
    
    def __init__(self, name, *args, **kwargs):
        self._name = name
    
    @property
    def name(self) -> str:
        return self._name

    def __str__(self) -> str:
        return "AssetFinder [%s]" % self._name

    def __repr__(self) -> str:
        return self.__str__()
    
    @property
    def assets(self) -> dict[str, MarketData]:
        """ Returns a dict of symbols vs asset objects. """
        return {}
    
    @abstractmethod
    def refresh_data(self, *args, **kwargs) -> None:
        """ Refresh the database. """
        raise NotImplementedError

    @abstractmethod
    def fetch_asset(self, sid:int) -> MarketData:
        """ Fetch asset object given a SID. """
        raise NotImplementedError
        
    def fetch_assets(self, sids:list[int]) -> list[MarketData]:
        """ Fetch asset object given an iterable of SID. """
        return [self.fetch_asset(sid) for sid in sids]
    
    def convert_asset(self, asset:Asset, product_type:str|ProductType|None=None, **kwargs) -> Asset|None:
        """ convert asset product type if possible. """
        raise NotImplementedError
    
    def retrieve_asset(self, sid:int) -> MarketData:
        """ Resolve and return asset given an asset SID. """
        return self.fetch_asset(sid)
    
    def retrieve_all(self, sids:list[int]) -> list[MarketData]:
        """ Resolve and return assets given an iterable of asset SIDs. """
        return [self.retrieve_asset(sid) for sid in sids]

    @abstractmethod
    def symbol(self, sym:str, dt:pd.Timestamp|None=None, *args, **kwargs) -> MarketData:
        """ Fetch asset object given a symbol. """
        raise NotImplementedError
        
    def to_base_asset(self, asset:MarketData) -> MarketData:
        """
            default implementation of converting given asset to a asset 
            that will be used for data fetching. This, for example, may 
            remove information like product type (e.g. margin vs delivery)
            to treat them same from the point of view of market data.
        """
        return asset
    
    def symbols_to_assets(self, syms:list[str], dt:pd.Timestamp|None=None, *args, **kwargs) -> list[MarketData]:
        """ Fetch asset object given an iterable of symbols. """
        return [self.symbol(sym, dt=dt, *args, **kwargs) for sym in syms]
    
    def symbols(self, syms:list[str], dt:pd.Timestamp|None=None, *args, **kwargs) -> list[MarketData]:
        """ Fetch asset object given an iterable of symbols. """
        return self.symbols_to_assets(syms, dt=dt, *args, **kwargs)
    
    @abstractmethod
    def exists(self, syms:str|list[str]) -> bool|list[bool]:
        """ Check and returns True if the symbols exist in the DB. """
        raise NotImplementedError
        
    @abstractmethod
    def lifetimes(self, dates:pd.DatetimeIndex, assets:list[MarketData]):
        """ 
            Returns a DataFrame with dates as index and all assets in 
            the DB as columns, filled with zeros (asset not available on 
            the date) or ones.
        """
        raise NotImplementedError
        
    @abstractmethod
    def can_trade(self, asset:Asset, dt:pd.Timestamp) -> bool:
        """ True if the asset was tradeable at the given date-time. """
        try:
            self.symbol(asset.symbol)
        except SymbolNotFound:
            return False
        
        return asset.can_trade
        
    def is_alive(self, asset:Asset, dt:pd.Timestamp) -> bool:
        return self.can_trade(asset, dt)
    
    def get_asset(self, sym:str, **kwargs) -> MarketData:
        """ 
            extended version of the symbol function for 
            convenience (implementation specific).
        """
        return self.symbol(sym)
    
    def infer_asset(self, **kwargs) -> MarketData:
        """
            Extended version of the symbol function for 
            convenience. Used to construct asset object 
            from implementation specific details fields.
        """
        raise NotImplementedError
        
    def from_broker_symbol(self, sym:str, **kwargs) -> MarketData:
        """
            map the blueshift symbol from the broker symbol. This is 
            implementation specific. If a trading venue/ broker uses 
            symbols different than what blueshift uses and we need to 
            parse data containing the broker symbol (e.g. in infer_asset),
            we must implement this mapping.
        """
        return self.symbol(sym, **kwargs)
        
    def get_asset_details(self, asset:MarketData, **kwargs) -> dict[str, Any]:
        """ 
            Resolve asset to details. Returns a dict which is implementation specific.
        """
        raise NotImplementedError

_finder_registry: Dict[str, Type[IAssetFinder]] = {}
_builtin_finder_loader: Callable[[], None] | None = None
_builtins_finder_loaded = False

def register_asset_finder(name: str, cls: Type[IAssetFinder]):
    """Register a new Asset Finder type."""
    _finder_registry[name] = cls

def set_builtin_asset_finder_loader(loader: Callable[[], None]):
    """Register a callable that will lazily load built-in asset finder types."""
    global _builtin_finder_loader
    _builtin_finder_loader = loader

def _ensure_builtin_finder_loaded():
    global _builtins_finder_loaded
    if not _builtins_finder_loaded and _builtin_finder_loader:
        _builtin_finder_loader()
        _builtins_finder_loaded = True

def asset_finder_factory(finder_type: str, *args, **kwargs) -> IAssetFinder:
    """ create asset finder instance by type name. """
    from inspect import getfullargspec

    if finder_type not in _finder_registry:
        _ensure_builtin_finder_loaded()  # lazy load builtins
    cls = _finder_registry.get(finder_type)

    if finder_type not in _finder_registry or finder_type is None:
        try:
            load_plugins('blueshift.plugins.assets')
        except Exception:
            pass

    cls = _finder_registry.get(finder_type)
    if cls is None:
        raise NotImplementedError(f"Unknown asset finder type: {finder_type}")
    
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

def list_asset_finders() -> list[str]:
    """ registered asset finder class names. """
    _ensure_builtin_finder_loaded()
    return list(_finder_registry)

class DefaultAssetFinder(IAssetFinder):
    """ a default asset finder. """
    def refresh_data(self, *args, **kwargs):
        raise NotImplementedError

    def fetch_asset(self, sid):
        raise NotImplementedError
        
    def symbol(self, sym, dt=None, *args, **kwargs):
        return asset_factory(symbol=sym, **kwargs)
    
    def exists(self, syms):
        raise NotImplementedError
        
    def lifetimes(self, dates, assets):
        raise NotImplementedError
        
    def can_trade(self, asset, dt):
        raise NotImplementedError
    
register_asset_finder('default', DefaultAssetFinder)

__all__ = [
    'MarketData',
    'Asset',
    'IAssetFinder',
    'register_asset',
    'get_asset_type',
    'asset_factory',
    'list_assets',
    'supported_assets',
    'convert_asset',
    'register_asset_finder',
    'set_builtin_asset_finder_loader',
    'asset_finder_factory',
    'list_asset_finders',
    ]