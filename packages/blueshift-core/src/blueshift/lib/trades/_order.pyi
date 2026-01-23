from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

from ._order_types import (
        ProductType,
        OrderFlag,
        OrderType,
        OrderValidity,
        OrderSide,
        OrderStatus,
        )


from blueshift.interfaces.assets._assets import Asset
from blueshift.interfaces.data.data_portal import DataPortal

class Order:
    oid:str
    hashed_oid:int
    reference_id:str
    broker_order_id:str
    exchange_order_id:str
    parent_order_id:str
    asset:Asset
    user:str
    placed_by:str
    product_type:ProductType
    order_flag:OrderFlag
    order_type:OrderType
    order_validity:OrderValidity
    quantity:float|int
    filled:float|int
    pending:float|int
    disclosed:float|int
    price:float
    average_price:float
    trigger_price:float
    stoploss_price:float
    side:OrderSide
    status:OrderStatus
    status_message:str
    exchange_timestamp:pd.Timestamp
    timestamp:pd.Timestamp
    tag:str
    fractional:bool
    price_at_entry:float
    create_latency:float
    exec_latency:float
    remark:str

    def __init__(self,
                 quantity:float,
                 side:OrderSide,
                 asset:Asset,
                 product_type:ProductType=ProductType.DELIVERY,
                 order_flag:OrderFlag=OrderFlag.NORMAL,
                 order_type:OrderType=OrderType.MARKET,
                 order_validity:OrderValidity = OrderValidity.DAY,
                 disclosed:float=0,
                 price:float=0,             # for limit prices
                 trigger_price:float=0,     # for stoplosses
                 stoploss_price:float=0,     # for stoplosses
                 user:str='',
                 placed_by:str='',   # algo ID
                 tag:str='blueshift',
                 oid:str|None=None,
                 reference_id:str|None=None,
                 broker_order_id:str|None=None,
                 exchange_order_id:str|None=None,
                 parent_order_id:str|None=None,
                 filled:float=0,
                 pending:float=0,
                 average_price:float=0,
                 status:OrderStatus=OrderStatus.OPEN,
                 status_message:str|None=None,
                 exchange_timestamp=None,
                 timestamp:pd.Timestamp|None=None,
                 fractional:bool=False,
                 price_at_entry:float=0,
                 create_latency:float=0,
                 exec_latency:float=0,
                 remark:str=''       # a remark by the user
                 ):...

    def __hash__(self) -> int:...
    def __eq__(self,y) -> bool:...
    def __str__(self) -> str:...
    def __repr__(self) -> str:...
    def to_dict(self) -> dict:...
    def to_json(self) -> dict:...
    def to_json_summary(self) -> dict:...
    @classmethod
    def from_dict(cls, data:dict) -> Order:...
    def copy_trackers_from_order(self, order:Order):...
    def update_from_dict(self, data:dict, preserve_trackers:bool=False):...
    def cash_outlay(self, to_ccy:str, data_portal:DataPortal) -> float:...
    def convert_to_limit(self, price:float):...
    def is_inactive(self)-> bool:...
    def is_open(self)-> bool:...
    def is_done(self)-> bool:...
    def is_cancelled(self)-> bool:...
    def is_rejected(self)-> bool:...
    def has_failed(self)-> bool:...
    def is_final(self)-> bool:...
    def is_buy(self) -> bool:...
    def is_triggered(self, last_price:float) -> bool:...
    def set_reference_id(self, reference_id:str):...
    def set_latency(self, create_latency:float=0, exec_latency:float=0):...
    def set_remark(self, remark:str):...
    def reject(self, reason:str):...
    def set_order_id(self, order_id:str):...
    def partial_cancel(self, reason:str='cancel'):...
    def set_timestamp(self, timestamp:pd.Timestamp):...