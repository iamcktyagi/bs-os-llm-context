from __future__ import annotations
from typing import TYPE_CHECKING
from enum import Enum

from blueshift.lib.trades._order_types import ProductType
from blueshift.lib.common.constants import Currency

if TYPE_CHECKING:
    import pandas as pd

class AssetClass(Enum):
    EQUITY:int
    FOREX:int
    COMMODITY:int
    RATES:int
    CASH:int
    CRYPTO:int
    VOL:int

class InstrumentType(Enum):
    SPOT:int
    FUTURES:int
    OPT:int
    MARGIN:int
    FUNDS:int
    CFD:int
    STRATEGY:int

class MktDataType(Enum):
    PRICING:int
    SERIES:int
    GENERAL:int

class OptionType(Enum):
    CALL:int
    PUT:int
    
class StrikeType(Enum):
    ABS:int
    REL:int
    DEL:int
    INCR:int
    PREMIUM:int

class MarketData:
    sid:int
    hashed_id:int
    mktdata_type:MktDataType
    symbol:str
    name:str
    security_id:str
    broker_symbol:str
    upper_circuit:float
    lower_circuit:float
    exchange_ticker:str
    start_date:pd.Timestamp
    end_date:pd.Timestamp
    ccy:str
    exchange_name:str
    calendar_name:str
    can_trade:bool

    def __int__(self) -> int:...
    def __hash__(self) -> int:...
    def __index__(self) -> int:...
    def __eq__(self,y):...
    def __lt__(self,y):...
    def __gt__(self,y):...
    def __ne__(self,y):...
    def __str__(self) -> str:...
    def __repr__(self):...
    def to_dict(self) -> dict:...
    def __reduce__(self):...
    
    @classmethod
    def from_dict(cls, data:dict) -> MarketData:...
    def update_from_dict(self, data:dict) -> None:...        
    def fx_rate(self, to_ccy:str|Currency, data_portal, dt:pd.Timestamp|None=None) -> float:...
    def is_asset(self) -> bool:...
    def is_funded(self) -> bool:...
    def is_index(self)-> bool:...
    def is_opt(self)-> bool:...
    def is_futures(self)-> bool:...
    def is_rolling(self)-> bool:...
    def is_margin(self)-> bool:...
    def is_intraday(self)-> bool:...
    def is_equity(self)-> bool:...
    def is_forex(self)-> bool:...
    def is_commodity(self)-> bool:...
    def is_etfs(self)-> bool:...
    def is_crypto(self)-> bool:...
    def is_call(self)-> bool:...
    def is_put(self)-> bool:...

class Asset(MarketData):
    asset_class:AssetClass
    instrument_type:InstrumentType
    mult:float
    tick_size:float
    precision:int
    auto_close_date:pd.Timestamp
    fractional:bool
        
    def get_tick_size(self) -> float:...
    def get_product_type(self) -> ProductType:...
    ...
    

class Equity(Asset):
    shortable:bool
    borrow_cost:float
    ...
        
class EquityMargin(Equity):...
    
class EquityIntraday(Equity):...

class CFDAsset(Asset):
    initial_margin:float
    maintenance_margin:float
    ...

class Futures(Asset):
    underlying:str
    root:str
    roll_day:int
    expiry_date:pd.Timestamp
    initial_margin:float
    maintenance_margin:float
    ...

class EquityFutures(Futures):
    ...

class Forex(Asset):
    ccy_pair:str
    base_ccy:str
    quote_ccy:str
    buy_roll:float
    sell_roll:float
    initial_margin:float
    maintenance_margin:float
    ...
            
class Crypto(Asset):
    ccy_pair:str
    base_ccy:str
    quote_ccy:str
    buy_roll:float
    sell_roll:float
    initial_margin:float
    maintenance_margin:float
    ...

class FXFutures(Forex):...

class SpotFX(Forex):...

class Option(Futures):
    strike:float
    strike_type:int
    option_type:int
    ...

class EquityOption(Option):
    ...

