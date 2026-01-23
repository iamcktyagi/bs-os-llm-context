from __future__ import annotations
from typing import TYPE_CHECKING, Any, cast
import numpy as np
from abc import ABC, abstractmethod
import bisect

from blueshift.lib.exceptions import ValidationError
from blueshift.lib.common.functions import to_int_or_float, listlike
from blueshift.lib.common.enums import ExpiryType

from ._assets import Asset, OptionType, Option
from .assets import asset_factory
from .utils import get_opt_occ_sym

if TYPE_CHECKING:
    import pandas as pd
else:
    import blueshift.lib.common.lazy_pandas as pd

_DEFAULT_EXCHANGE = 'default'

class IOptionChains(ABC):
    def __init__(self):
        self.expiry_mapping:_ExpiryMapping = _ExpiryMapping()
        self.exchanges:list[str] = []
        self.chains:dict[str,dict[pd.Timestamp,dict[str,_Chainlet]]] = {}  # [exchange][expiry][underlying] = chainlet
        self.futures:dict[str, dict[str, pd.DatetimeIndex]] = {}  # [exchange][underlying] = expiries
        
    @abstractmethod
    def get_expiries(self, underlying:str, exchange:str|None) -> pd.DatetimeIndex|list[pd.Timestamp]:
        """ get all valid expiries for the underlying. """
        raise NotImplementedError
        
    @abstractmethod
    def find_expiry(self, underlying:str, exchange:str|None, expiry_type:ExpiryType, 
                    offset:int) -> pd.Timestamp | None:
        """ find expiry for options for the underlying given expiry type and offset. """
        raise NotImplementedError
        
    @abstractmethod
    def check_expiry(self, underlying:str, exchange:str|None, expiry:pd.Timestamp) -> bool:
        """ check if a given expiry for the underlying exists. """
        raise NotImplementedError
        
    @abstractmethod
    def get_strike_interval(self, underlying:str, exchange:str|None, expiry:pd.Timestamp, 
                            *args, **kwargs) -> float:
        """ get the strike interval given an underlying and expiry. """
        raise NotImplementedError
        
    @abstractmethod
    def get_lot_size(self, underlying:str, exchange:str|None, expiry:pd.Timestamp, 
                     *args, **kwargs) -> float:
        """ get the lot size of options given an underlying and expiry. """
        raise NotImplementedError
        
    @abstractmethod
    def get_tick_size(self, underlying:str, exchange:str|None, expiry:pd.Timestamp, 
                      *args, **kwargs) -> float:
        """ get the tick size of options given an underlying and expiry. """
        raise NotImplementedError
    
    @abstractmethod
    def get_chain(self, underlying:str, exchange:str|None, expiry:pd.Timestamp, 
                  *args, **kwargs) -> tuple[list[Option],list[Option]]:
        """ get all options assets given an underlying and expiry. """
        raise NotImplementedError
    
    @abstractmethod
    def get(self, underlying:str, exchange:str|None, option_type:str, strike:str, expiry:pd.Timestamp, 
            *args, **kwargs) -> Option|None:
        """ get an option asset given an underlying, expiry, type and strike. """
        raise NotImplementedError
        
    @abstractmethod
    def get_futures_expiry(self, underlying:str, exchange:str|None, expiry:pd.Timestamp, 
                           *args, **kwargs) -> pd.Timestamp|None:
        """ get the matching futures expiry given an underlying and expiry. """
        raise NotImplementedError
        
    @abstractmethod
    def add_futures_expiries(self, underlying:str, exchange:str|None, 
                             expiries:pd.DatetimeIndex|list[pd.Timestamp]|pd.Timestamp,
                             expiry_type:ExpiryType|None=None):
        """ add futures expiries. """
        raise NotImplementedError
        
    @abstractmethod
    def add_expiry_mapping(self, underlying:str, exchange:str|None, expiry_type:ExpiryType, 
                           expiries:pd.DatetimeIndex, *args, **kwargs):
        """ add option chain expiry type mapping details. """
        raise NotImplementedError
        
    @abstractmethod
    def add_chain(self, contract:Option, exchange:str|None, data, strikes, futures=False, 
                  expiry_types=None, update=False):
        """ add option chain details. """
        raise NotImplementedError

class _Chainlet:
    """ A part of the chain with a given option type. """
    __slots__ = ['contract', '_opts','_incr']
    def __init__(self, contract:Option):
        if not isinstance(contract, Asset) or not contract.is_opt():
            raise ValidationError(f'expected a option asset as contract spec.')
            
        self.contract = contract
        self._opts:dict[OptionType,dict[float,dict[str,Any]]] = {}
        self._incr:float = np.nan
        
    def _calc_strike_incr(self):
        if not self._opts:
            return
        
        strikes:list[float] = []
        for kind in self._opts:
            strikes.extend(self._opts[kind].keys())
            
        if not strikes:
            return
        
        self._incr = np.nanmin(np.diff(np.array(sorted(set(strikes)))))
    
    def get_strike_incr(self) -> float:
        if not np.isnan(self._incr):
            return self._incr
        
        self._calc_strike_incr()
        return self._incr
    
    def get(self, option_type, strike, lot_size=None, tick_size=None):
        try:
            if option_type=='CE':
                option_type = OptionType.CALL
            elif option_type=='PE':
                option_type = OptionType.PUT
            elif isinstance(option_type, str):
                option_type = OptionType[option_type.upper()]
            else:
                option_type = OptionType(option_type)
        except Exception:
            raise ValidationError(f'not a valid option type.')
            
        try:
            strike = to_int_or_float(strike)
        except Exception:
             raise ValidationError(f'not a valid option strike.')
        
        if option_type not in self._opts or strike not in self._opts[option_type]:
            return None
        else:
            sec_info = self._opts[option_type][strike]
        
        opt_type = 'CE' if option_type==OptionType.CALL else 'PE'
        asset_data = self.contract.to_dict()
        asset_data = {**asset_data, **sec_info}
        asset_data['symbol'] = get_opt_occ_sym(
                self.contract.underlying, self.contract.expiry_date, # type: ignore
                opt_type, strike)
        asset_data['name'] = asset_data['name'] + ' Option'
        asset_data['option_type'] = opt_type
        asset_data['strike'] = strike
        asset_data['class'] = self.contract.__class__
        
        if lot_size:
            asset_data['mult'] = to_int_or_float(lot_size)
        
        if tick_size:
            asset_data['tick_size'] = to_int_or_float(1/tick_size)
        
        asset = asset_factory(**asset_data)
        return cast(Option, asset)
    
    def get_all(self, lot_size:float|None=None, tick_size:float|None=None):
        calls = []
        puts = []

        if OptionType.CALL in self._opts:
            calls = [self.get(
                    OptionType.CALL, k, lot_size=lot_size, tick_size=tick_size) \
                    for k in self._opts[OptionType.CALL]]
            
        if OptionType.PUT in self._opts:
            puts = [self.get(
                    OptionType.PUT, k, lot_size=lot_size, tick_size=tick_size) \
                    for k in self._opts[OptionType.PUT]]
        
        return [c for c in calls if c is not None], [p for p in puts if p is not None]
    
    def add(self, option_type:OptionType|str, data:dict[str, Any], strikes:list[float]):
        opt_type = None
        try:
            if option_type=='CE':
                opt_type = OptionType.CALL
            elif option_type=='PE':
                opt_type = OptionType.PUT
            elif isinstance(option_type, str):
                opt_type = OptionType[option_type.upper()]
            
            opt_type = OptionType(option_type)
        except Exception:
            raise ValidationError(f'not a valid option type.')
            
        if not listlike(strikes):
            strikes = [strikes] # type: ignore
            
        try:
            #assert isinstance(data, dict), 'input data must be a dictionary'
            if not isinstance(data, dict):
                raise ValueError('input data must be a dictionary')
            
            for key in data:
                if not listlike(data[key]):
                    data[key] = [data[key]]
                #assert len(data[key])==len(strikes), 'data sizes and strikes should match.'
                if not len(data[key])==len(strikes):
                    raise ValueError('data sizes and strikes should match.')
        except Exception as e:
            raise ValidationError(str(e))
            
        options:dict[float, dict[str, Any]] = {}
        try:
            for idx, k in enumerate(strikes):
                obj = {}
                for key in data:
                    obj[key] = data[key][idx]
                    
                options[to_int_or_float(k)] = obj
        except Exception:
            raise ValidationError(f'invalid strike(s), must be number.')
            
        if not options:
            return
        
        if opt_type in self._opts:
            existing = self._opts[opt_type]
            options = {**existing, **options}
        
        options = dict(sorted(options.items()))
        self._opts[opt_type] = options
        self._incr = np.nan # re-calc strike interval
        
class _ExpiryMapping:
    def __init__(self):
        # [expiry_type][exchange][underlying] = expiries
        self._dispatch:dict[ExpiryType,dict[str,dict[str,pd.DatetimeIndex]]] = {}
        
    def get(self, underlying:str, exchange:str, expiry_type:ExpiryType|str, offset:int):
        if not exchange:
            raise ValidationError(f'exchange is required.')

        try:
            if isinstance(expiry_type, str):
                expiry_type = ExpiryType[expiry_type.upper()]
            
            expiry_type = ExpiryType(expiry_type)
        except Exception:
            raise ValidationError(f'illegal expiry type {expiry_type}')
            
        if expiry_type in self._dispatch and exchange in self._dispatch[expiry_type]:
            if underlying in self._dispatch[expiry_type][exchange]:
                try:
                    return self._dispatch[expiry_type][exchange][underlying][int(offset)]
                except Exception:
                    pass
        return None
        
    def add(self, underlying:str, exchange:str, expiry_type:ExpiryType, 
            expiries:pd.DatetimeIndex|list[pd.Timestamp]|pd.Timestamp):
        if not exchange:
            raise ValidationError(f'exchange is required.')

        try:
            if isinstance(expiry_type, str):
                expiry_type = ExpiryType[expiry_type.upper()]
            
            expiry_type = ExpiryType(expiry_type)
        except Exception:
            raise ValidationError(f'illegal expiry type {expiry_type}')
            
        try:
            if not listlike(expiries):
                expiries = [expiries] # type: ignore
            expiries = pd.DatetimeIndex(sorted(pd.to_datetime(expiries))) # type: ignore
            expiries = expiries.dropna()
        except Exception as e:
            raise ValidationError(f'failed to parse expiries: {str(e)}')
            
        if len(expiries) == 0:
            return
            
        if expiry_type not in self._dispatch:
            self._dispatch[expiry_type] = {}
            
        if exchange not in self._dispatch[expiry_type]:
            self._dispatch[expiry_type][exchange] = {}
            
        if underlying in self._dispatch[expiry_type][exchange]:
            existing = self._dispatch[expiry_type][exchange][underlying]
            self._dispatch[expiry_type][exchange][underlying] = existing.union(expiries)
        else:
            self._dispatch[expiry_type][exchange][underlying] = expiries
                
class OptionChains(IOptionChains):
    def __init__(self):
        super().__init__()
        self._security_id_map:dict[str,tuple] = {}
        
    def get_expiries(self, underlying:str, exchange:str|None):
        if exchange:
            return self._get_expiries(underlying, exchange)
        
        for exchange in self.exchanges:
            exps =  self._get_expiries(underlying, exchange)
            if exps:
                return exps
            
        exps:list[pd.Timestamp] = []
        return exps
    
    def _get_expiries(self, underlying:str, exchange:str):
        expiries:list[pd.Timestamp] = []
        for exp in self.chains.get(exchange, {}):
            if underlying in self.chains[exchange][exp]:
                expiries.append(exp)
        return sorted(expiries)
        
    def find_expiry(self, underlying:str, exchange:str|None, expiry_type:ExpiryType, offset:int):
        if exchange:
            return self.expiry_mapping.get(underlying, exchange, expiry_type, offset)
        
        for exchange in self.exchanges:
            exp = self.expiry_mapping.get(underlying, exchange, expiry_type, offset)
            if exp is not None:
                return exp
            
        return
    
    def check_expiry(self, underlying:str, exchange:str|None, expiry:pd.Timestamp) -> bool:
        if exchange:
            return self._check_expiry(underlying, exchange, expiry)
        
        for exchange in self.exchanges:
            exists = self._check_expiry(underlying, exchange, expiry)
            if exists:
                return exists
            
        return False

    
    def _check_expiry(self, underlying:str, exchange:str, expiry:pd.Timestamp) -> bool:
        try:
            expiry = pd.Timestamp(expiry)
        except Exception:
            raise ValidationError(f'not a valid expiry {expiry}')
            
        return (exchange in self.chains and 
               expiry in self.chains[exchange] and 
               underlying in self.chains[exchange][expiry])
        
    def _get_chain(self, underlying:str, exchange:str, expiry:pd.Timestamp|None=None, 
                   expiry_type:ExpiryType|None=None, offset:int|None=None):
        if expiry:
            try:
                expiry = pd.Timestamp(expiry)
            except Exception:
                raise ValidationError(f'not a valid expiry {expiry}')
        else:
            if expiry_type is None or offset is None:
                raise ValidationError(f'valid expiry type and offset required.')
            expiry = self.find_expiry(underlying, exchange, expiry_type, offset)
            
        if not expiry:
            return
        
        if (exchange in self.chains and 
            expiry in self.chains[exchange] and 
            underlying in self.chains[exchange][expiry]):
            return self.chains[exchange][expiry][underlying]
        return None
    
    def get_strike_interval(self, underlying:str, exchange:str|None, expiry:pd.Timestamp|None=None, 
                            expiry_type:ExpiryType|None=None, offset:int|None=None) -> float:
        if exchange:
            return self._get_strike_interval(
                underlying, exchange, expiry=expiry, expiry_type=expiry_type, offset=offset
            )
        
        for exchange in self.exchanges:
            interval = self._get_strike_interval(
                underlying, exchange, expiry=expiry, expiry_type=expiry_type, offset=offset
            )
            if not np.isnan(interval):
                return interval
            
        return np.nan
    
    def _get_strike_interval(self, underlying:str, exchange:str, expiry:pd.Timestamp|None=None, 
                            expiry_type:ExpiryType|None=None, offset:int|None=None) -> float:
        chain = self._get_chain(underlying, exchange=exchange, expiry=expiry, expiry_type=expiry_type, offset=offset)
        if not chain:
            return np.nan
        return chain.get_strike_incr()
    
    def get_lot_size(self, underlying:str, exchange:str|None, expiry:pd.Timestamp|None=None, 
                     expiry_type:ExpiryType|None=None, offset:int|None=None) -> float:
        if exchange:
            return self._get_lot_size(
                underlying, exchange, expiry=expiry, expiry_type=expiry_type, offset=offset
            )
        
        for exchange in self.exchanges:
            lot = self._get_lot_size(
                underlying, exchange, expiry=expiry, expiry_type=expiry_type, offset=offset
            )
            if not np.isnan(lot):
                return lot

        return np.nan
    
    def _get_lot_size(self, underlying:str, exchange:str, expiry:pd.Timestamp|None=None, 
                     expiry_type:ExpiryType|None=None, offset:int|None=None) -> float:
        chain = self._get_chain(underlying, exchange=exchange, expiry=expiry, expiry_type=expiry_type, offset=offset)
        if not chain:
            return np.nan
        return chain.contract.mult
    
    def get_tick_size(self, underlying:str, exchange:str|None, expiry:pd.Timestamp|None=None, 
                     expiry_type:ExpiryType|None=None, offset:int|None=None) -> float:
        if exchange:
            return self._get_tick_size(
                underlying, exchange, expiry=expiry, expiry_type=expiry_type, offset=offset)
        
        for exchange in self.exchanges:
            tick = self._get_tick_size(
                underlying, exchange, expiry=expiry, expiry_type=expiry_type, offset=offset)
            if not np.isnan(tick):
                return tick
            
        return np.nan
    
    def _get_tick_size(self, underlying:str, exchange:str, expiry:pd.Timestamp|None=None, 
                     expiry_type:ExpiryType|None=None, offset:int|None=None) -> float:
        chain = self._get_chain(underlying, exchange=exchange, expiry=expiry, expiry_type=expiry_type, offset=offset)
        return chain.contract.get_tick_size() if chain else np.nan
    
    def get_futures_expiry(self, underlying:str, exchange:str|None, expiry:pd.Timestamp|None=None, 
                           expiry_type:ExpiryType|None=None, offset:int|None=None) -> pd.Timestamp|None:
        if exchange:
            return self._get_futures_expiry(
                underlying, exchange, expiry=expiry, expiry_type=expiry_type, offset=offset
            )
        
        for exchange in self.exchanges:
            exp = self._get_futures_expiry(
                underlying, exchange, expiry=expiry, expiry_type=expiry_type, offset=offset
            )
            if exp:
                return exp
            
        return
    
    def _get_futures_expiry(self, underlying:str, exchange:str, expiry:pd.Timestamp|None=None, 
                           expiry_type:ExpiryType|None=None, offset:int|None=None) -> pd.Timestamp|None:
        if underlying not in self.futures.get(exchange, {}):
            return None
        futures_expiries = self.futures[exchange][underlying]
        
        if expiry:
            try:
                expiry = pd.Timestamp(expiry)
            except Exception:
                raise ValidationError(f'not a valid expiry {expiry}')
        else:
            if expiry_type is None or offset is None:
                raise ValidationError(f'valid expiry type and offset is required.')
            expiry = self.find_expiry(underlying, exchange, expiry_type, offset)
            
        if not expiry:
            return
        
        idx = bisect.bisect_left(futures_expiries, expiry)
        
        if idx < len(futures_expiries):
            return futures_expiries[idx]
        else:
            return futures_expiries[-1]
        
    def get_chain(self, underlying:str, exchange:str|None, expiry:pd.Timestamp|None=None, 
                  expiry_type:ExpiryType|None=None, offset:int|None=None, lot_size:float|None=None, 
                  tick_size:float|None=None) -> tuple[list[Option],list[Option]]:
        if exchange:
            return self._get_chain_for_exchange(
                underlying, exchange, expiry=expiry, expiry_type=expiry_type, offset=offset, 
                lot_size=lot_size, tick_size=tick_size
            )
        
        for exchange in self.exchanges:
            calls, puts = self._get_chain_for_exchange(
                underlying, exchange, expiry=expiry, expiry_type=expiry_type, offset=offset, 
                lot_size=lot_size, tick_size=tick_size
            )
            if calls or puts:
                return calls, puts
            
        return [], []
    
    def _get_chain_for_exchange(self, underlying:str, exchange:str, expiry:pd.Timestamp|None=None, 
                  expiry_type:ExpiryType|None=None, offset:int|None=None, lot_size:float|None=None, 
                  tick_size:float|None=None) -> tuple[list[Option],list[Option]]:
        chain = self._get_chain(underlying, exchange, expiry=expiry, expiry_type=expiry_type, offset=offset)
        if chain:
            return chain.get_all(lot_size=lot_size, tick_size=tick_size)
        else:
            calls:list[Option] = []
            puts:list[Option] = []
        return calls, puts
    
    def get(self, underlying:str, exchange:str|None, option_type:OptionType|str, strike:float, 
            expiry:pd.Timestamp|None=None, expiry_type:ExpiryType|None=None, offset:int|None=None, 
            lot_size:float|None=None, tick_size:float|None=None) -> Option|None:
        if exchange:
            return self._get(
                underlying, exchange, option_type, strike, expiry=expiry, expiry_type=expiry_type, 
                offset=offset, lot_size=lot_size, tick_size=tick_size)
        
        for exchange in self.exchanges:
            opt = self._get(
                underlying, exchange, option_type, strike, expiry=expiry, expiry_type=expiry_type, 
                offset=offset, lot_size=lot_size, tick_size=tick_size)
            if opt:
                return opt
            
        return
    
    def _get(self, underlying:str, exchange:str, option_type:OptionType|str, strike:float, 
            expiry:pd.Timestamp|None=None, expiry_type:ExpiryType|None=None, offset:int|None=None, 
            lot_size:float|None=None, tick_size:float|None=None) -> Option|None:
        if expiry:
            try:
                expiry = pd.Timestamp(expiry)
            except Exception:
                raise ValidationError(f'not a valid expiry {expiry}')
        else:
            if expiry_type is None or offset is None:
                raise ValidationError(f'valid expiry type and offset required.')
            expiry = self.find_expiry(underlying, exchange, expiry_type, offset)
            
        if not expiry or expiry not in self.chains.get(exchange, {}):
            return
        if underlying not in self.chains[exchange][expiry]:
            return
        
        chain = self.chains[exchange][expiry][underlying]
        return chain.get(option_type, strike, lot_size=lot_size, tick_size=tick_size)
    
    def get_from_security_id(self, security_id:str) -> Option|None:
        security_id = str(security_id)
        
        if security_id in self._security_id_map:
            exchange, expiry, underlying, option_type, strike = self._security_id_map[security_id]
            return self.get(underlying, exchange, option_type, strike, expiry)
        
    def add_chain(self, contract:Option, exchange:str|None, data:dict[str, Any], strikes:list[float], 
                  futures:bool=False, expiry_types:list[ExpiryType]|None=None, update:bool=False):
        if exchange is None:
            exchange = _DEFAULT_EXCHANGE

        if exchange not in self.exchanges:
            self.exchanges.append(exchange)

        if not isinstance(contract, Asset) or not contract.is_opt():
            raise ValidationError(f'expected a option asset as contract spec.')
            
        try:
            expiry = pd.Timestamp(contract.expiry_date) # type: ignore
        except Exception:
            raise ValidationError(f'illegal expiry date for contract {contract}.')
        
        # Initialize exchange level
        if exchange not in self.chains:
            self.chains[exchange] = {}
            
        # Initialize expiry level
        if expiry not in self.chains[exchange]:
            self.chains[exchange][expiry] = {}
            
        # Initialize underlying level
        if contract.underlying not in self.chains[exchange][expiry]: # type: ignore
            self.chains[exchange][expiry][contract.underlying] = _Chainlet(contract) # type: ignore
            
        chainlet = self.chains[exchange][expiry][contract.underlying] # type: ignore
        if update:
            chainlet.contract = contract
        
        chainlet.add(contract.option_type, data, strikes) # type: ignore

        # update the security_id mapping if present in data:
        if 'security_id' in data:
            security_ids = data['security_id']
            maps = {str(k):(
                exchange, expiry, contract.underlying, contract.option_type, strike) \
                    for k, strike in zip(security_ids, strikes)}
            self._security_id_map.update(maps)
        
        if futures:
            self.add_futures_expiries(contract.underlying, exchange, expiry) # type: ignore
        
        if expiry_types:
            self.add_expiry_mapping(contract.underlying, exchange, expiry_types, [expiry]) # type: ignore
        
    def add_expiry_mapping(self, underlying:str, exchange:str|None, expiry_types:list[ExpiryType]|ExpiryType, 
                           expiries:pd.DatetimeIndex|list[pd.Timestamp]|pd.Timestamp):
        if exchange is None:
            exchange = _DEFAULT_EXCHANGE
        if exchange not in self.exchanges:
            self.exchanges.append(exchange)

        if expiry_types is None:
            return
        
        if not listlike(expiry_types):
            expiry_types = [expiry_types] # type: ignore
            
        expiry_types = cast(list, expiry_types)
        for expiry_type in expiry_types:
            if pd.isnull(expiry_type): # type: ignore
                continue
            self.expiry_mapping.add(underlying, exchange, expiry_type, expiries)
            
    def add_futures_expiries(self, underlying:str, exchange:str|None, 
                             expiries:pd.DatetimeIndex|list[pd.Timestamp]|pd.Timestamp,
                             expiry_type:ExpiryType|None=None):
        
        if exchange is None:
            exchange = _DEFAULT_EXCHANGE
        if exchange not in self.exchanges:
            self.exchanges.append(exchange)

        try:
            if not listlike(expiries):
                expiries = [expiries] # type: ignore
            expiries = pd.DatetimeIndex(sorted(pd.to_datetime(expiries))) # type: ignore
            expiries = expiries.dropna()
        except Exception as e:
            raise ValidationError(f'failed to parse expiries: {str(e)}')
            
        if len(expiries) == 0:
            return
        
        if exchange not in self.futures:
            self.futures[exchange] = {}
            
        if underlying in self.futures[exchange]:
            existing = self.futures[exchange][underlying]
            self.futures[exchange][underlying] = existing.union(expiries)
        else:
            self.futures[exchange][underlying] = expiries

        if expiry_type:
            self.expiry_mapping.add(underlying, exchange, expiry_type, expiries)