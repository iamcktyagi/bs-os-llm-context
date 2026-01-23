from __future__ import annotations
import re
from typing import TYPE_CHECKING, Literal
from functools import lru_cache

from blueshift.lib.common.functions import as_numeric, to_int_or_float
from blueshift.lib.common.enums import ExpiryType
from blueshift.lib.exceptions import AssetsException

from ._assets import OptionType

if TYPE_CHECKING:
    import pandas as pd
else:
    import blueshift.lib.common.lazy_pandas as pd

IMM_MONTHS = {'F': 1,
 'G': 2,
 'H': 3,
 'J': 4,
 'K': 5,
 'M': 6,
 'N': 7,
 'Q': 8,
 'U': 9,
 'V': 10,
 'X': 11,
 'Z': 12}
IMM_MATCH = '|'.join(list(IMM_MONTHS.keys()))


def symbol_function(sym:str, dt:str|pd.Timestamp|None=None) -> str:
    """ sanitize import symbol. """
    sym = sym.strip()
    sym = sym.replace(' ','')
    
    replace_list = ['&','-']
    for r in replace_list:
        if r in sym:
            sym = sym.replace(r,'_')
        
    return sym

def get_sym_exchange(sym:str) -> tuple[str|None, str]:
    """ get exchange name from {EXCHG}:{SYM} format. """
    symbol = sym
    exchange = None
    
    s = sym.split(":")
    if len(s)==2:
        symbol = s[1]
        exchange = s[0]
        
    return exchange, symbol

def get_clean_sym(sym:str, *args, **kwargs) -> str:
    """Default function to return symbol from a string."""
    s = sym.split(":")
    return s[1] if len(s) > 1 else s[0]

def get_name_from_sym(sym:str, *args, **kwargs) -> str:
    """ Default function to return asset name from a string. """
    return sym

def get_exchange_from_sym(sym:str, *args, **kwargs) -> str|None:
    """ Default function to return exchange name from a string. """
    s = sym.split(":")
    return s[0] if len(s) > 1 else None

def get_calendar_from_sym(sym:str, *args, **kwargs) -> str|None:
    """ Default function to return calendar name from a string. """
    s = sym.split(":")
    return s[0] if len(s) > 1 else None

def get_quote_ccy_from_sym(sym:str, *args, **kwargs) -> str:
    """ Default function to return quote currency from a string. """
    s = sym.split("/")
    if len(s) < 2:
        raise AssetsException("Illegal symbol.")
    return s[1]

def get_base_ccy_from_sym(sym:str, *args, **kwargs) -> str:
    """ Default function to return base currency from a string. """
    s = sym.split(":")
    s = s[1] if len(s) > 1 else s[0]
    s = s.split("/")
    if len(s) < 2:
        raise AssetsException("Illegal symbol.")
    return s[0]

def get_ccy_pair_from_sym(sym:str, *args, **kwargs) -> str:
    """ Default function to return currency pair name from a string. """
    s = sym.split(":")
    s = s[1] if len(s) > 1 else s[0]
    s = s.split("/")
    if len(s) <2 :
        raise AssetsException("Illegal symbol.")
    return s[0] + "/" + s[1]

def get_opt_type_from_sym(sym:str, *args, **kwargs) -> str:
    """ Default function to return options type from a string. """
    return sym[-2:]

def get_fut_occ_sym(underlying:str, expiry_date:pd.Timestamp) -> str:
    """ create occ like futures symbol - `{UNDERLYING}{YYYYMMDD}FUT`. """
    return underlying+expiry_date.strftime('%Y%m%d')+'FUT'

def get_opt_occ_sym(underlying:str, expiry_date:pd.Timestamp, option_type:str|OptionType, strike:int|float) -> str:
    """ create OCC like option symbol - `{UNDERLYING}{YYYYMMDD}{CE|PE}{STRIKE}`. """
    if isinstance(option_type, OptionType):
        option_type = 'CE' if option_type==OptionType.CALL else 'PE'
    strike = to_int_or_float(strike)
    return underlying+expiry_date.strftime('%Y%m%d')+option_type+str(strike)

def get_occ_sym(symbol:str, asset_type:Literal['option','futures'],
                underlying:str, expiry_date:pd.Timestamp, option_type:str|OptionType|None=None, 
                strike:int|float=0) -> str:
    """ 
        get instrument details and generate OCC like symbol. For futures, 
        `{UNDERLYING}{YYYYMMDD}FUT`, for options `{UNDERLYING}{YYYYMMDD}{CE|PE}{STRIKE}`
    """
    if isinstance(option_type, OptionType):
        option_type = 'CE' if option_type==OptionType.CALL else 'PE'
        
    if option_type and option_type.lower() in ('call','c'):
        option_type = 'CE'
    if option_type and option_type.lower() in ('put','p'):
        option_type = 'PE'
        
    if asset_type =='option':
        if not option_type:
            raise ValueError(f'option type missing from inputs')
        strike = to_int_or_float(strike)
        ticker = underlying+expiry_date.strftime('%Y%m%d')+\
                        option_type+str(strike)
    elif asset_type == 'futures':
        ticker = underlying+expiry_date.strftime('%Y%m%d')+'FUT'
    else:
        ticker = symbol
    return ticker.upper()

@lru_cache()
def reverse_occ_sym(sym:str) -> tuple[str, str|None, str, pd.Timestamp|None, OptionType|None, int|float]:
    """ parse sym and return tuple of [sym, asset type, underlying, expiry date, option type, strike]. """
    asset_type = option_type = underlying = expiry_date = None
    strike = 0
    
    match = re.search('[0-9]+CE|[0-9]+PE',sym)
    if match:
        asset_type = 'option'
        underlying = sym[:(match.span()[1]-10)]
        expiry_date = pd.Timestamp(
                sym[(match.span()[1]-10):(match.span()[1]-2)])
        option_type = sym[(match.span()[1]-2):(match.span()[1])]
        
        if option_type=='CE':
            option_type = OptionType.CALL
        elif option_type=='PE':
            option_type = OptionType.PUT
        else:
            raise ValueError(f'unknown option type {option_type}')

        strike = as_numeric(sym[(match.span()[1]):])
        return sym, asset_type, underlying, expiry_date, OptionType(option_type), strike
        
    match = re.search('[0-9]+FUT',sym)
    if match:
        asset_type = 'futures'
        underlying = sym[:(match.span()[1]-11)]
        expiry_date = pd.Timestamp(
                sym[(match.span()[1]-11):(match.span()[1]-3)])
        return sym, asset_type, underlying, expiry_date, option_type, strike
        
    asset_type = 'asset'
    underlying =  sym
    return sym, asset_type, underlying, expiry_date, option_type, strike

@lru_cache()
def get_occ_fut_details(sym:str) -> tuple[str, pd.Timestamp|None]:
    """ parse sym and return tuple of [underlying, expiry]. """
    parts=sym.split(':')
    if len(parts)>1:sym=parts[1]
    
    underlying = sym
    expiry_date = None
    match = re.search('[0-9]+FUT',sym)
    if match:
        underlying = sym[:(match.span()[1]-11)]
        expiry_date = pd.Timestamp(
                sym[(match.span()[1]-11):(match.span()[1]-3)])
        
    return underlying, expiry_date

@lru_cache()
def get_fut_details(sym:str) -> tuple[str|None, pd.Timestamp|None, int|None, ExpiryType]:
    """ parse sym and return tuple of [underlying, expiry, offset, expiry type]. """
    underlying = expiry = offset = None
    
    if '-I' in sym and sym.endswith('I'):
        parts = sym.split('-I')
        if len(parts) == 2:
            underlying, offset = parts[0], len(parts[1])
            return underlying, expiry, offset, ExpiryType.MONTHLY
        
    match = re.search('-W\d$', sym) # type: ignore
    if match:
        underlying = sym[:match.span()[0]]
        offset = int(as_numeric(sym[match.span()[0]+2]))
        return underlying, expiry, offset, ExpiryType.WEEKLY
    
    match = re.search(f'-[{IMM_MATCH}]$', sym)
    if match:
        underlying = sym[:match.span()[0]]
        offset = sym[match.span()[0]+1]
        offset = IMM_MONTHS[offset]
        return underlying, expiry, offset, ExpiryType.IMM
    
    underlying, expiry = get_occ_fut_details(sym)
    if not expiry:
        underlying = None
            
    return underlying, expiry, offset, ExpiryType.OCC

def get_occ_opt_details(sym:str) -> tuple[str, pd.Timestamp|None, OptionType|None, int|float]:
    """ parse option syms and return tuple of [underlying, expiry date, option type, strike]. """
    parts=sym.split(':')
    if len(parts)>1:sym=parts[1]
    
    strike=0
    underlying = sym
    expiry_date = None
    option_type = None
    
    match = re.search('[0-9]+CE|[0-9]+PE',sym)
    if match:
        underlying = sym[:(match.span()[1]-10)]
        expiry_date = pd.Timestamp(
                sym[(match.span()[1]-10):(match.span()[1]-2)])
        option_type = sym[(match.span()[1]-2):(match.span()[1])]

        if option_type=='CE':
            option_type = OptionType.CALL
        elif option_type=='PE':
            option_type = OptionType.PUT
        else:
            raise ValueError(f'unknown option type {option_type}')
        
        strike = sym[(match.span()[1]):]
        strike = as_numeric(strike)

    if option_type is not None:
        option_type = OptionType(option_type)
        
    return underlying, expiry_date, option_type, strike

def _get_opt_details_weeklies(sym:str) -> tuple[str, pd.Timestamp|None, int|None, OptionType|None, float|None, str|float|None]:
    """ 
        match NIFTY-W0CE17000 or NIFTY-W4CE+100 or NIFTY-W5CE+25D . Format 
        is {UNDERLYING}-W{offset}{option-type}{strike}. Offset must be 
        single digit and less than 5 (although not enforced here).
    """
    strike = None
    strike_offset = None
    underlying = sym
    offset = None
    expiry_date = None
    option_type = None
    
    match = re.search('-W\d+CE|-W\d+PE',sym) # type: ignore
    if not match:
        return underlying, expiry_date, offset, option_type, strike, strike_offset
    
    underlying = sym[:match.span()[0]]
    offset = int(as_numeric(sym[match.span()[0]+2]))
    option_type = sym[(match.span()[1]-2):(match.span()[1])]

    if option_type=='CE':
        option_type = OptionType.CALL
    elif option_type=='PE':
        option_type = OptionType.PUT
    else:
        raise ValueError(f'unknown option type {option_type}')
    
    k = sym[(match.span()[1]):]
    k = k.upper()
    
    if '+' in k or '-' in k:
        if k.endswith('D'):
            strike_offset = k
        elif k.endswith('ITM') or k.endswith('OTM'):
            strike_offset = k
        else:
            strike_offset = as_numeric(k)
    elif k.endswith('D'):
        strike_offset = k
    elif k.endswith('P'):
        strike_offset = k
    elif k.endswith('ITM') or k.endswith('OTM'):
        strike_offset = k
    else:
        strike = as_numeric(k)
    
    return underlying, expiry_date, offset, OptionType(option_type), strike, strike_offset

def _get_opt_details_monthlies(sym) -> tuple[str, pd.Timestamp|None, int|None, OptionType|None, float|None, str|float|None]:
    """ match NIFTY-ICE17000 or NIFTY-ICE+100 or NIFTY-ICE+25D """
    strike = None
    strike_offset = None
    underlying = sym
    offset = None
    expiry_date = None
    option_type = None
    
    match = re.search('-[I]+CE|-[I]+PE',sym)
    if not match:
        return underlying, expiry_date, offset, option_type, strike, strike_offset
    
    underlying = sym[:match.span()[0]]
    offset = len(sym[(match.span()[0]+1):(match.span()[1]-2)]) - 1
    option_type = sym[(match.span()[1]-2):(match.span()[1])]

    if option_type=='CE':
        option_type = OptionType.CALL
    elif option_type=='PE':
        option_type = OptionType.PUT
    else:
        raise ValueError(f'unknown option type {option_type}')
    
    k = sym[(match.span()[1]):]
    k = k.upper()
    if '+' in k or '-' in k:
        if k.endswith('D'):
            strike_offset = k
        elif k.endswith('ITM') or k.endswith('OTM'):
            strike_offset = k
        else:
            strike_offset = as_numeric(k)
    elif k.endswith('D'):
        strike_offset = k
    elif k.endswith('P'):
        strike_offset = k
    elif k.endswith('ITM') or k.endswith('OTM'):
        strike_offset = k
    else:
        strike = as_numeric(k)
    return underlying, expiry_date, offset, OptionType(option_type), strike, strike_offset

def _get_opt_details_imms(sym) -> tuple[str, pd.Timestamp|None, int|None, OptionType|None, float|None, str|float|None]:
    """ match NIFTY-HCE17000, or NIFTY-UCE+100 or NIFTY-ZCE_25D """
    strike = None
    strike_offset = None
    underlying = sym
    offset = None
    expiry_date = None
    option_type = None
    
    match = re.search(f'-[{IMM_MATCH}]CE',sym)
    if not match:
        match = re.search(f'-[{IMM_MATCH}]PE',sym)
        
    if not match:
        return underlying, expiry_date, offset, option_type, strike, strike_offset
    
    underlying = sym[:match.span()[0]]
    offset = sym[match.span()[0]+1]
    offset = IMM_MONTHS[offset]
    option_type = sym[(match.span()[1]-2):(match.span()[1])]

    if option_type=='CE':
        option_type = OptionType.CALL
    elif option_type=='PE':
        option_type = OptionType.PUT
    else:
        raise ValueError(f'unknown option type {option_type}')
    
    k = sym[(match.span()[1]):]
    k = k.upper()
    if '+' in k or '-' in k:
        if k.endswith('D'):
            strike_offset = k
        elif k.endswith('ITM') or k.endswith('OTM'):
            strike_offset = k
        else:
            strike_offset = as_numeric(k)
    elif k.endswith('D'):
        strike_offset = k
    elif k.endswith('P'):
        strike_offset = k
    elif k.endswith('ITM') or k.endswith('OTM'):
        strike_offset = k
    else:
        strike = as_numeric(k)
    return underlying, expiry_date, offset, OptionType(option_type), strike, strike_offset

@lru_cache()
def get_opt_details(sym:str) ->tuple[str, pd.Timestamp|None, int|None, OptionType|None, int|float|None, int|float|str|None, ExpiryType]:
    """ 
        Match the following: NIFTY-ICE17000 (for monthly offsets), 
        or NIFTY-W0CE17000 (for weekly offsets), or NIFTY-HCE17000 (
        for IMM offsets) or NIFTY20220331CE17000 for occ spec. Returns 
        tuple for [underlying, expiry date, offset, option type, strike, strike offset, expiry type]. 
    """
    underlying, expiry_date, offset, option_type, strike, strike_offset = \
        _get_opt_details_weeklies(sym)
    
    if option_type is not None:
        return underlying, expiry_date, offset, option_type, strike, strike_offset, ExpiryType.WEEKLY
    
    underlying, expiry_date, offset, option_type, strike, strike_offset = \
        _get_opt_details_monthlies(sym)
    
    if option_type is not None:
        return underlying, expiry_date, offset, option_type, strike, strike_offset, ExpiryType.MONTHLY
    
    underlying, expiry_date, offset, option_type, strike, strike_offset = \
        _get_opt_details_imms(sym)
    
    if option_type is not None:
        return underlying, expiry_date, offset, option_type, strike, strike_offset, ExpiryType.IMM
    
    strike = None
    strike_offset = None
    underlying = sym
    offset = None
    expiry_date = None
    option_type = None
    
    match = re.search('CE\+|PE\-',sym) # type: ignore
    if match:
        k = sym[(match.span()[1]):]
        k = k.upper()
        if k.endswith('D'):
            strike_offset = k
        elif k.endswith('P'):
            strike_offset = k
        elif k.endswith('ITM') or k.endswith('OTM'):
            strike_offset = k
        else:
            strike_offset = as_numeric(k)

    underlying, expiry_date, option_type, strike = get_occ_opt_details(sym)
    
    if strike_offset is not None:strike=0
    return underlying, expiry_date, offset, option_type, strike, strike_offset, ExpiryType.OCC

@lru_cache()
def parse_sym(symbol, exchange:str|None=None) -> tuple[str, str|None, str, str|None, pd.Timestamp|None, int|None, OptionType|None, int|float|None, int|float|str|None, ExpiryType|None]:
    """ parse seems and return tuple for [symbol, exchange, asset type, underlying, expiry, offset, option type, strike, strike offset, expiry type]. """
    if ':' in symbol:
        exchange, symbol = tuple(symbol.split(':'))
        
    strike = None
    strike_offset = None
    underlying = symbol
    offset = None
    expiry = None
    option_type = None
    type_ = None
    
    # try futures first
    underlying, expiry, offset, type_ = get_fut_details(symbol)
    if expiry or offset is not None: # 0 is a valid offset
        asset_type='futures'
        
    else:
        # not a futures instrument, try option
        underlying, expiry, offset, option_type, strike, strike_offset, type_ = \
            get_opt_details(symbol)
        if option_type is not None:
            asset_type='option'
        else:
            asset_type='asset'
    
    return symbol, exchange, asset_type, underlying, expiry, offset, option_type, strike, strike_offset, type_

@lru_cache()
def get_ccy_details(sym:str) -> tuple[str|None, str|None, str|None]:
    """ parse currency-like sym and return tuple for [base ccy, quote ccy, ccy pair]. """
    parts=sym.split(':')
    if len(parts)>1:sym=parts[1]
    
    pair = base = quote = None
    parts = sym.split("/")
    if len(parts)==2:
        pair = sym
        base, quote = parts[0], parts[1]
        
    return base, quote, pair


__all__ = [
    'symbol_function',
    'get_sym_exchange',
    'get_clean_sym',
    'get_name_from_sym',
    'get_exchange_from_sym',
    'get_calendar_from_sym',
    'get_quote_ccy_from_sym',
    'get_base_ccy_from_sym',
    'get_ccy_pair_from_sym',
    'get_opt_type_from_sym',
    'get_fut_occ_sym',
    'get_opt_occ_sym',
    'get_occ_sym',
    'reverse_occ_sym',
    'get_occ_fut_details',
    'get_fut_details',
    'get_occ_opt_details',
    'get_opt_details',
    'parse_sym',
    'get_ccy_details'
    ]