from __future__ import annotations
from typing import TYPE_CHECKING, Any, cast, Literal
import numpy as np

from blueshift.calendar import get_calendar
from blueshift.calendar.date_utils import combine_date_time
from blueshift.calendar.expiry_dispatch import get_expiry_dispatch, ExchangeExpiry
from blueshift.config import BLUESHIFT_DIR
from blueshift.lib.common.constants import ANNUALIZATION_FACTOR
import blueshift.lib.trades._order_types as ORDER_TYPES
from blueshift.lib.models.vols import bs_delta_to_strike, bs_price_to_strike, interpolate_atmf
from blueshift.interfaces.assets._assets import (
    OptionType, MarketData, Asset, Option, Futures)
from blueshift.interfaces.assets.assets import IAssetFinder, convert_asset
from blueshift.interfaces.assets.utils import parse_sym, get_occ_sym
from blueshift.interfaces.assets.opt_chains import OptionChains, ExpiryType
from blueshift.interfaces.data.data_portal import MarketDepth
from blueshift.interfaces.data.library import ILibrary
from blueshift.interfaces.logger import get_logger
from blueshift.lib.exceptions import ValidationError, SymbolNotFound, ServerError, InitializationError

from .config.config import APIBrokerConfig
from .conversions import create_asset
from .master import fetch_master_data

if TYPE_CHECKING:
    import pandas as pd
    from blueshift.interfaces.logger import BlueshiftLogger
    from .broker import RestAPIBroker
else:
    import blueshift.lib.common.lazy_pandas as pd


class RestAPIBrokerAssetFinder(IAssetFinder):
    '''
    Asset finder implemented in memory, without any database
    '''
    
    def __init__(self, name: str, config: APIBrokerConfig, broker: RestAPIBroker,
                 supported_products:list[ORDER_TYPES.ProductType]=[], library:ILibrary|None=None, 
                 logger:BlueshiftLogger|None=None, *args, **kwargs):
        if not isinstance(config, APIBrokerConfig):
            msg = f'expected a config object, got {config} of type {type(config)}'
            raise ValidationError(msg)
        
        super().__init__(name, *args, **kwargs)
        self.broker = broker
        self.config = config
        self.library = library
        self.logger = logger or get_logger()
        self.exchanges = []
        self.supported_assets = self.config.broker.assets
        self.supported_products = supported_products
        self.columns = self.config.broker.options.data_columns
        self._last_asset_initialize = None
        
        expiry_dispatch = config.broker.options.expiry_dispatch
        self._expiry_dispatch:dict[str, ExchangeExpiry] = {}
        for k,v in expiry_dispatch.items():
            try:
                self._expiry_dispatch[k] = get_expiry_dispatch(v)
            except Exception as e:
                self.logger.error(f'failed to create expiry dispatcher {v} for exchange {k}:{str(e)}')

        self._reset_asset_mapping()

    def _reset_asset_mapping(self):
        self._assets:dict[str,dict[str, MarketData]] = {}
        self._security_id_map:dict[str, MarketData] = {}
        self._broker_symbol_map:dict[str, MarketData] = {}
        self._details_map:dict[MarketData, dict[str, Any]] = {}
        self._underlying_exchange_map:dict[str, str] = {}
        self._market_depth:MarketDepth = MarketDepth()
        self._opt_chains:OptionChains = OptionChains()

    def update_asset_mapping(self, asset:MarketData, details:dict[str, Any], only_cache:bool=False):
        if asset.is_rolling():
            return
        
        exchange = asset.exchange_name
        if exchange not in self.exchanges:
            self.exchanges.append(exchange)

        if exchange not in self._assets:
            self._assets[exchange] = {}
        self._assets[exchange][asset.symbol] = asset

        if asset.security_id:
                self._security_id_map[str(asset.security_id)] = asset
        if asset.broker_symbol:
            self._broker_symbol_map[asset.broker_symbol] = asset

        if asset.is_futures() or asset.is_opt():
            if 'underlying_exchange' in details and asset.exchange_name:
                self._underlying_exchange_map[asset.exchange_name] = details['underlying_exchange']

        if only_cache:
            return

        if 'expiry_types' in details:
            asset = cast(Option, asset)
            expiry_types = details['expiry_types']
            self._opt_chains.add_expiry_mapping(
                asset.underlying, exchange, expiry_types, [asset.expiry_date])

        if asset.is_futures():
            asset = cast(Futures, asset)
            self._opt_chains.add_futures_expiries(asset.underlying, exchange, [asset.expiry_date])

        if asset.is_opt():
            asset = cast(Option, asset)
            if 'security_id' not in details and asset.security_id:
                details['security_id'] = asset.security_id
                
            self._opt_chains.add_chain(
                asset,
                asset.exchange_name,
                details,
                [float(asset.strike)]
            )
            
    def _initialize(self, assets:dict[MarketData, dict[str,Any]]|None=None, 
                    opt_chains:OptionChains|None=None):
        if self._last_asset_initialize and self._last_asset_initialize >= pd.Timestamp.now(
            tz=self.broker.tz).normalize():
            return
        
        self._reset_asset_mapping()

        if assets is None or opt_chains is None:
            try:
                self.load_assets_from_source()
            except Exception as e:
                raise InitializationError(f'failed to load assets:{str(e)}') from e
                    
        self._last_asset_initialize = pd.Timestamp.now(tz=self.broker.tz).normalize()

    def __str__(self):
        return "Blueshift Broker AssetFinder [%s]" % self._name
    
    @property
    def assets(self):
        """ Returns a dict of symbols vs asset objects. """
        self._initialize()
        d:dict[str, MarketData] = {}
        for exchange in self._assets:
            d = {**d, **self._assets[exchange]}
        
        return d
    
    def refresh_data(self, *args, **kwargs):
        """ Refresh the database. """
        if 'forced' in kwargs and kwargs.get('forced'):
            mode = kwargs.get('mode')
            logger = kwargs.get('logger') or self.broker.logger
            self.load_assets_from_source()
            
    def _search(self, symbol:str, exchange_name:str|None=None) -> MarketData|None:
        """Search for an asset by symbol and optionally by exchange_name."""
        if ':' in symbol:
            exchange_name, symbol = tuple(symbol.split(':'))
            
        if exchange_name:
            if exchange_name in self._assets:
                return self._assets[exchange_name].get(symbol)
        else:
            if self.exchanges:
                # search in order of exchanges
                for exchange_name in self.exchanges:
                    if exchange_name in self._assets:
                        asset = self._assets[exchange_name].get(symbol)
                        if asset:
                            return asset
            else:
                # else return the first match
                for exchange_name in self._assets:
                    asset = self._assets[exchange_name].get(symbol)
                    if asset:
                        return asset
                
        return None
    
    def search(self, symbols:str|list[str]|None=None, exchange_name:str|None=None, **kwargs
               ) -> list[MarketData]:
        self._initialize()

        if not symbols:
            return list(self.assets.values())
        
        if isinstance(symbols, str):
            asset = self._search(symbols, exchange_name=exchange_name)
            if not asset:
                return []
            return [asset]
        
        assets = [self._search(sym, exchange_name=exchange_name) for sym in symbols]
        return [asset for asset in assets if asset is not None]
    
    def fetch_asset(self, sid):
        """ Fetch asset object given a SID. """
        self._initialize()
        asset = self._security_id_map.get(str(sid))
        if not asset:
            asset = self._opt_chains.get_from_security_id(str(sid))
            if not asset:
                raise SymbolNotFound(f'no asset found for id {sid}')
            
        return cast(MarketData, asset)
    
    def sid(self, sid):
        return self.fetch_asset(sid)
    
    def convert_asset(self, asset, product_type=None, **kwargs):
        """ 
            Convert an asset to a product type.
        """
        return convert_asset(asset, product_type=product_type)
    
    def symbol(self, symbol, dt=None, product_type=None,
               use_spot=False, exchange_name=None, *args, **kwargs):
        """
        Get asset object by symbol name

        Args:
            ``symbol (str)``: Symbol name.

        Returns:
            Asset - the asset object matching the symbol.
        """
        if 'research' in kwargs and kwargs['research']:
            kwargs.pop('research', None)
            if not self.library:
                msg = f'Cannot use research data source, no source defined'
                raise ValidationError(msg)
            return self.library.symbol(
                    symbol, dt=dt, product_type=product_type,
                    use_spot=use_spot, exchange_name=exchange_name, *args, **kwargs)
        
        try:
            self._initialize()
            symbol, exchange, asset_type, underlying, expiry, offset, option_type, strike, strike_offset, expiry_type \
                = parse_sym(symbol, exchange_name)
        except Exception:
            raise SymbolNotFound(f'Illegal symbol {symbol}')
        
        if not exchange_name and exchange:
            exchange_name = exchange

        logger = kwargs.get('logger', self.broker.logger)

        if underlying:
            orig = underlying
            underlying = underlying
            symbol = symbol.replace(orig, underlying)

        return self.get_asset(symbol, underlying, expiry, offset, 
                              option_type, strike, strike_offset, expiry_type,
                              exchange_name, product_type, use_spot, logger=logger)
        
    def to_base_asset(self, asset:MarketData):
        """
        Convert an asset to a base type.
        """
        if not asset.is_asset():
            return asset
        
        asset = cast(Asset, asset)
        if self.supported_products and ORDER_TYPES.ProductType.DELIVERY in self.supported_products:
            new = self.convert_asset(asset, product_type=ORDER_TYPES.ProductType.DELIVERY)
            if new:
                return new
            
        return asset
    
    def _exists(self, symbol:str):
        try:
            self.symbol(symbol)
        except SymbolNotFound:
            return False
        else:
            return True
        
    def exists(self, syms:str|list[str]) -> bool|list[bool]:
        if isinstance(syms, list):
            return [self._exists(sym) for sym in syms]
        
        return self._exists(syms)
        
    def lifetimes(self, dates, assets):
        """ 
        Returns a DataFrame with dates as index and all assets in 
        the DB as columns, filled with zeros (asset not available on 
        the date) or ones.
        """
        raise NotImplementedError
        
    def can_trade(self, asset, dt):
        return asset.can_trade
    
    def _bs_price_to_asset(self, atmf:float, price:float, vol:float, t:float, option_type:OptionType, 
                         underlying:str, expiry:pd.Timestamp, incr:float, exchange_name:str|None=None, 
                         logger:BlueshiftLogger|None=None):
        """Price to asset conversion (matches old 5Paisa implementation)"""
        logger = logger or self.logger

        max_iter = 20
        min_diff = 0.05
        asset = None
        last_asset = None
        last_price = None
        opt_type = OptionType.CALL if option_type == 'CE' else OptionType.PUT
        
        try:
            for i in range(max_iter):
                last_asset = asset
                strike = bs_price_to_strike(atmf, price, vol, t, OptionType(opt_type))
                if np.isnan(strike):
                    raise ValueError(f'strike search did not converge.')
                strike = round(strike/incr)*incr
                sym = get_occ_sym('','option',underlying, expiry, option_type.name, strike)
                asset = self.symbol(sym, exchange_name=exchange_name)
                px = cast(float, self.broker.current(
                    asset, 'close', check_stale=True, do_subscribe=False, logger=logger))
                if abs(px-price)/price < min_diff:
                    return asset
                
                if asset == last_asset:
                    # we got stuck, return
                    return asset
                
                if last_price and last_asset and \
                    np.sign(last_price-price) != np.sign(px - price):
                    # we got stuck perhaps oscillating
                    if abs(last_price-price) > abs(px-price):
                        return asset
                    else:
                        return last_asset
                
                last_price = px
                vol = cast(float, self.broker.current(
                    asset, 'implied_vol', check_stale=True, logger=logger, do_subscribe=False))
                
            return asset
        except Exception as e:
            msg = f'failed to fetch strike from premium: {str(e)}.'
            raise SymbolNotFound(msg)
        
    def get_underlying_exchange(self, underlying:str, exchange_name:str|None=None) -> str|None:
        if exchange_name in self._underlying_exchange_map:
            return self._underlying_exchange_map[exchange_name]
        
        asset = self._search(underlying)
        if not asset:
            return
        
        return asset.exchange_name
    
    def _get_asset(self, sym:str, exchange_name:str|None=None, 
                        product_type:ORDER_TYPES.ProductType|None=None, **kwargs) -> MarketData:
        assets = self.search(sym, exchange_name=exchange_name)

        if not assets:
            raise SymbolNotFound(f'symbol not found {sym}.')
        
        asset = assets[0]
        if exchange_name and asset.exchange_name != exchange_name:
            raise SymbolNotFound(f'symbol {sym} not found for exchange {exchange_name}.')
        
        if asset.security_id and asset.security_id not in self._security_id_map:
            self._security_id_map[str(asset.security_id)] = asset
        
        if product_type is None or not isinstance(asset, Asset):
            return asset
        else:
            try:
                asset = self.convert_asset(asset, product_type)
                if asset:
                    return asset
                raise SymbolNotFound(f'Asset not found {sym} for type {product_type}.')
            except Exception:
                raise SymbolNotFound(f'symbol not found {sym} for type {product_type}.')
            
    def _get_asset_futures(self, sym:str, underlying:str, expiry:pd.Timestamp, 
                           exchange_name:str|None=None, 
                           product_type:ORDER_TYPES.ProductType|None=None) -> Futures:
        try:
            sym = get_occ_sym(sym, 'futures', underlying, expiry)
            fut = self.search(sym, exchange_name=exchange_name)
            if not fut:
                msg = f'Futures not found for underlying {underlying}, exchange {exchange_name}'
                msg += f' and expiry {expiry}.'
                raise SymbolNotFound(msg)
            fut = cast(Futures,fut[0])
            
            if product_type is None:
                return fut
            try:
                fut = cast(Futures, self.convert_asset(fut, product_type))
                if fut is not None:
                    return fut
                raise ValueError(f'cannot conver futures instrument to product type {product_type}')
            except Exception as e:
                msg = f'Futures not found for underlying {underlying}, exchange {exchange_name}'
                msg += f', expiry {expiry} and product type {product_type}.'
                raise SymbolNotFound(msg) from e
        except Exception as e:
            msg = f'Symbol {sym} not found:{str(e)}.'
            raise SymbolNotFound(msg) from e
        
    def _get_asset_opt(self, sym:str, underlying:str, expiry:pd.Timestamp, 
                           option_type:OptionType, strike:float, exchange_name:str|None=None,
                           product_type:ORDER_TYPES.ProductType|None=None) -> Option|None:
        sym = get_occ_sym(sym, 'option', underlying, expiry, option_type, strike)
        opt = self.search(sym, exchange_name=exchange_name)
        
        if not opt:
            return
        
        opt = cast(Option,opt[0])
        if product_type is None:
            return opt
        
        try:
            return cast(Option, self.convert_asset(opt, product_type))
        except Exception as e:
            msg = f'Option not found for underlying {underlying}, exchange {exchange_name}'
            msg += f', expiry {expiry}, strike {strike}, type {option_type} and product type {product_type}.'
            raise SymbolNotFound(msg) from e
        
    def find_expiry(self, underlying:str, exchange_name:str|None, expiry_type:ExpiryType, offset:int, 
                    asset_type:Literal['futures','option']):
        expiry = self._opt_chains.find_expiry(underlying, exchange_name, expiry_type, int(offset))
        if expiry:
            if asset_type != 'futures':
                return expiry
            exp = self._opt_chains.get_futures_expiry(
                underlying, exchange_name, expiry_type=expiry_type, offset=offset)
            if exp == expiry:
                return expiry
        
        if exchange_name is not None:
            exchanges = [exchange_name]
        else:
            exchanges = list(self._expiry_dispatch.keys())

        for exchange in exchanges:
            dispatcher = self._expiry_dispatch.get(exchange)
            if dispatcher:
                if asset_type=='futures':
                    chunker = dispatcher.get_futures_chunker(underlying, expiry_type)
                else:
                    chunker = dispatcher.get_options_chunker(underlying, expiry_type)

                if chunker:
                    return chunker.get_expiry(pd.Timestamp.now().normalize(), offset=offset)
            
        return
    
    def get_futures_expiry(self, underlying:str, exchange_name:str|None, expiry:pd.Timestamp):
        exp = self._opt_chains.get_futures_expiry(underlying, exchange_name, expiry)

        if exp:
            return exp
        
        if exchange_name is not None:
            exchanges = [exchange_name]
        else:
            exchanges = list(self._expiry_dispatch.keys())

        for exchange in exchanges:
            dispatcher = self._expiry_dispatch.get(exchange)
            if dispatcher and dispatcher.futures_dispatch:
                chunker = dispatcher.futures_dispatch.get(underlying, expiry=expiry)
                if chunker:
                    return chunker.get_expiry(pd.Timestamp.now().normalize())
                
        return
    
    def check_expiry(self, underlying:str, exchange_name:str|None, expiry:pd.Timestamp):
        return self._opt_chains.check_expiry(underlying, exchange_name, expiry)
    
    def get_asset(self, sym:str, underlying:str|None=None, expiry:pd.Timestamp|None=None, 
                  offset:int|None=None, option_type:OptionType|None=None,  strike:float|None=None, 
                  strike_offset:str|int|float|None=None, expiry_type:ExpiryType|None=None, 
                  exchange_name:str|None=None, product_type:ORDER_TYPES.ProductType|str|None=None, 
                  use_spot:bool=False, logger:BlueshiftLogger|None=None, **kwargs) -> MarketData:
        self._initialize()
        logger = logger or self.logger
        original_sym = sym
        
        if product_type is not None:
            if isinstance(product_type, str):
                try:
                    # blueshift version
                    product_type = ORDER_TYPES.ProductType[product_type.upper()]
                except Exception:
                    try:
                        # broker version
                        product_type = self.config.broker.mappings.product_type.to_blueshift(product_type)
                    except Exception:
                        raise SymbolNotFound(f'unknown product type {product_type}')
                    
            product_type = ORDER_TYPES.ProductType(product_type)
                
        if expiry is None and offset is None:
            # Use search function to find asset
            return self._get_asset(sym, exchange_name, product_type=product_type)
        
        if not underlying:
            raise SymbolNotFound(f'no underlying found for symbol {original_sym}')
        
        # Get underlying exchange and FNO exchange name
        underlying_exchange = self.get_underlying_exchange(underlying, exchange_name=exchange_name)

        if expiry is None:
            try:
                if offset is None or expiry_type is None:
                    raise ValueError(f'offset or expiry type cannot be missing')
                
                asset_type = 'futures' if option_type is None else 'option'
                expiry = self.find_expiry(
                        underlying, exchange_name, expiry_type, int(offset), asset_type)
                if expiry is None:
                    raise ValueError('no expiry from option chains.')
            except Exception:
                msg = f'No expiry found for underlying {underlying}, exchange {exchange_name}'
                msg += f', series {expiry_type} and offset {offset}.'
                raise SymbolNotFound(msg)
        else:
            # Validate expiry based on asset type being requested
            if option_type is None:
                # For FUTURES: Check if expiry exists in futures expiries
                try:
                    fut_exp = self.get_futures_expiry(underlying, exchange_name, expiry)
                    if fut_exp is None:
                        raise Exception("No futures expiry found")
                except Exception:
                    msg = f'Invalid futures expiry {expiry} for underlying {underlying} and exchange {exchange_name}.'
                    raise SymbolNotFound(msg)
            else:
                # For OPTIONS: Check if expiry exists in option chains
                if not self.check_expiry(underlying, exchange_name, expiry):
                    msg = f'Invalid option expiry {expiry} for underlying {underlying} and exchange {exchange_name}.'
                    raise SymbolNotFound(msg)
        
        # Handle futures
        if option_type is None:
            return self._get_asset_futures(sym, underlying, expiry, exchange_name, product_type)
        
        # already cached options - not rolling/ relative
        if expiry and option_type is not None and strike is not None and strike_offset is None:
            opt = self._get_asset_opt(
                sym, underlying, expiry, option_type, strike, exchange_name, product_type)
            if opt:
                return opt

        try:
            fut_exp = self.get_futures_expiry(underlying, exchange_name, expiry)
            if fut_exp is None:
                raise ValueError(f'no matching futures expiry found.')
            fut_sym = get_occ_sym(sym, 'futures', underlying, fut_exp)
            fut = self.search(fut_sym, exchange_name=exchange_name)
            
            if fut is None:
                raise ValueError(f"No futures for symbol {original_sym} and exchange {exchange_name}")
            
            fut = cast(Futures, fut[0])
        except Exception as e:
            msg = f'Symbol {original_sym} not found: {str(e)}.'
            raise SymbolNotFound(msg) from e
        
        # Handle strike offset calculations
        if strike_offset is not None:
            if isinstance(strike_offset, str) and \
                (strike_offset.endswith('D') or strike_offset.endswith('P')):
                use_spot = False
            
            try:
                if use_spot:
                    underlying_asset = self.search(underlying, exchange_name=underlying_exchange)
                    if underlying_asset is None:
                        raise ValueError(f"No asset for underlying {underlying} and exchange {exchange_name} found.")
                    base_price = self.broker.current(
                            underlying_asset[0], 'close', check_stale=True, 
                            logger=logger)
                    base_price = cast(float, base_price)
                else:
                    fut_price = cast(float, self.broker.current(
                            fut, 'close', check_stale=True, logger=logger))
                    if fut_exp > expiry:
                        now = pd.Timestamp.now(tz=self.broker.tz).tz_localize(None)
                        underlying_asset = self.search(underlying, exchange_name=underlying_exchange)
                        if not underlying_asset:
                            raise ValueError(
                                f"No asset for underlying {underlying} and exchange {exchange_name} found.")
                        underlying_asset = underlying_asset[0]
                        
                        spot_price = cast(float, self.broker.current(
                                underlying_asset, 'close', check_stale=True,
                                logger=logger))
                        
                        fut_exp_t = combine_date_time(fut_exp, self.broker.calendar.close_time)
                        exp_t = combine_date_time(expiry, self.broker.calendar.close_time)
                        
                        T = (fut_exp_t - now).total_seconds()/86400
                        t = (exp_t - now).total_seconds()/86400
                        base_price = interpolate_atmf(fut_price, spot_price, float(t), float(T))
                    else:
                        base_price = fut_price
                
                strike_incr = self._opt_chains.get_strike_interval(underlying, exchange_name, expiry)
                
                if isinstance(strike_offset, str):
                    if strike_offset.endswith('D'):
                        # Handle delta to strike conversion
                        base_sym = sym.replace(strike_offset, '+0')
                        base_asset = self.symbol(base_sym, exchange_name=exchange_name)
                        vol = cast(float, self.broker.current(
                                base_asset, 'implied_vol', check_stale=True,
                                logger=logger))
                        delta = int(strike_offset[:-1])/100 # delta is specified in X100
                        now = pd.Timestamp.now(tz=self.broker.tz).tz_localize(None)
                        exp = combine_date_time(expiry, self.broker.calendar.close_time)
                        t = (exp - now).total_seconds()/86400/ANNUALIZATION_FACTOR
                        strike = bs_delta_to_strike(
                                base_price, delta, vol, t)
                        msg = f'Delta to strike {strike} with {base_price}, {delta}, '
                        msg += f'{vol}, {t}.'
                        logger.info(msg)
                        strike_offset = strike - base_price
                    elif strike_offset.endswith('P'):
                        # Handle premium to strike conversion
                        base_sym = sym.replace(strike_offset, '+0')
                        base_asset = self.symbol(base_sym, exchange_name=exchange_name)
                        vol = cast(float, self.broker.current(
                                base_asset, 'implied_vol', check_stale=True,
                                logger=logger))
                        price = float(strike_offset[:-1])/100
                        now = pd.Timestamp.now(tz=self.broker.tz).tz_localize(None)
                        exp = combine_date_time(expiry, self.broker.calendar.close_time)
                        t = (exp - now).total_seconds()/86400/ANNUALIZATION_FACTOR
                        asset = self._bs_price_to_asset(
                                base_price, price, vol, t, option_type,
                                underlying, expiry, strike_incr, exchange_name=exchange_name, logger=logger)
                        msg = f'price to asset {asset} with {base_price}, {price}, {vol}, '
                        msg += f'{t}, {underlying}, {expiry}.'
                        logger.info(msg)
                        if not asset:
                            msg = f'asset not found for {sym}, no match '
                            msg += f'found for option premium.'
                            raise SymbolNotFound(msg)
                        return asset
                    elif strike_offset.endswith('ITM'):
                        strike_offset = int(strike_offset[:-3])
                        if option_type == OptionType.CALL:
                            strike_offset = -int(strike_offset * strike_incr)
                        else:
                            strike_offset = int(strike_offset * strike_incr)
                    elif strike_offset.endswith('OTM'):
                        strike_offset = int(strike_offset[:-3])
                        if option_type == OptionType.CALL:
                            strike_offset = int(strike_offset * strike_incr)
                        else:
                            strike_offset = -int(strike_offset * strike_incr)
                
                strike = base_price + float(strike_offset)
                strike = round(strike/strike_incr) * strike_incr
            except Exception as e:
                msg = f'Failed to find strike price for strike offset '
                msg += f'{strike_offset}, underlying {underlying}, exchange {exchange_name} and '
                msg += f'expiry {expiry}:{str(e)}.'
                raise SymbolNotFound(msg)
        
        if not strike:
            msg = f'No strike specified for options on underlying {underlying} '
            raise SymbolNotFound(msg)
        
        # Get option from OptionChains
        opt = self._opt_chains.get(underlying, exchange_name, option_type, strike, expiry=expiry)
        if not opt:
            msg = f'No option found for exchange {exchange_name}, underlying {underlying}, strike {strike}, '
            msg += f'and expiry {expiry}.'
            raise SymbolNotFound(msg)
        
        if product_type is None:
            return opt
        
        opt = self.convert_asset(opt, product_type)
        if not opt:
            msg = f'No option found underlying {underlying}, strike {strike}, '
            msg += f'expiry {expiry}, product {product_type} and exchange {exchange_name}.'
            raise SymbolNotFound(msg)
        
        self.update_asset_mapping(opt, {})
        return opt
                
    def get_occ_option_details(self, asset:Option):
        if not asset.is_opt():
            raise SymbolNotFound(f'asset {asset} is not an option')
        
        if asset.is_rolling():
            raise SymbolNotFound(f'rolling asset is not supported: {asset}')
        
        underlying = asset.underlying
        exchange_name = asset.exchange_name
        expiry = asset.expiry_date
        option_type = OptionType(asset.option_type)
        strike = asset.strike
        
        fut_exp = self.get_futures_expiry(underlying, exchange_name, expiry)
        
        if fut_exp is None:
            msg = f'no matching futures found for underlying {underlying}'
            raise SymbolNotFound(msg)
        
        sym = get_occ_sym(underlying, 'futures', underlying, fut_exp)
        fut = self._search(sym, None)
        
        if not fut:
            msg = f'no futures found for underlying {underlying}'
            raise SymbolNotFound(msg)
        
        fut = cast(Futures, fut)
        return underlying, fut, expiry, option_type, strike
    
    def from_broker_symbol(self, sym, **kwargs):
        asset = self._broker_symbol_map.get(sym, None)
        
        if asset:
            return asset
        
        raise SymbolNotFound(f'no asset found for broker symbol {sym}.')
        
    def get_asset_details(self, asset, **kwargs):
        """ 
        fetch extra asset details not stored in the asset object.
        """
        return self._details_map.get(asset, {})
    
    ###################### asset interface methods ######################
    def infer_asset(self, **kwargs):
        """
        Generic infer_asset using security_id (token-based lookup).
        
        Works with standardized fields from request_get_orders():
        - security_id: str (REQUIRED) - Broker token for token-based lookup
        
        Returns:
            Asset object
            
        Raises:
            SymbolNotFound: If asset cannot be inferred
        """
        if 'security_id' in kwargs and kwargs['security_id']:
            return self.sid(kwargs['security_id'])
        elif 'symbol' in kwargs and kwargs['symbol']:
            return self.symbol(kwargs['symbol'])
        else:
            try:
                return create_asset(**kwargs)
            except Exception:
                raise SymbolNotFound(f"Cannot infer asset from data: {kwargs}")

    def load_assets_from_source(self):
        return fetch_master_data(self.broker)