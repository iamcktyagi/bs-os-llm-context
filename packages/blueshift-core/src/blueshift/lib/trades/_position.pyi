from __future__ import annotations
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    import pandas as pd

from ._order_types import (
        ProductType,
        PositionSide,
        OrderSide,
        )

from blueshift.interfaces.assets._assets import Asset
from ._order import Order

class Position:
    pid:str
    instrument_id:str
    asset:Asset
    quantity:float
    buy_quantity:float
    buy_price:float
    sell_quantity:float
    sell_price:float
    pnl:float
    realized_pnl:float
    unrealized_pnl:float
    last_price:float
    last_fx:float
    underlying_price:float
    timestamp:pd.Timestamp
    entry_time:pd.Timestamp
    value:float
    cost_basis:float
    margin:float
    product_type:ProductType
    position_side:PositionSide
    fractional:bool

    def __init__(self,
                 asset:Asset,
                 quantity:float,
                 side:OrderSide|Literal[-1],
                 instrument_id:str,
                 product_type:ProductType,
                 average_price:float,
                 margin:float,
                 timestamp:pd.Timestamp,
                 exchange_timestamp:pd.Timestamp,
                 buy_quantity:float=0,
                 buy_price:float=0,
                 sell_quantity:float=0,
                 sell_price:float=0,
                 pnl:float=0,
                 realized_pnl:float=0,
                 unrealized_pnl:float=0,
                 last_price:float=0,
                 last_fx:float=1.0,
                 underlying_price:float=0,
                 fractional:bool=False):...
    def __hash__(self)  -> int:...
    def __float__(self) -> float:...
    def __eq__(self,y) -> bool:...
    def __str__(self) -> str:...
    def __repr__(self) -> str:...
    def to_dict(self) -> dict:...
    def to_json(self) -> dict:...
    def to_json_summary(self) -> dict:...
    @classmethod
    def from_dict(cls, data:dict) -> Position:...
    def update_from_dict(self, data:dict) -> None:...
    @classmethod
    def from_order(cls, o:Order, margin:float=0, last_fx:float=1.0, underlying_price:float=0) -> Position:...
    def copy(self) -> Position:...
    def add_to_position(self, pos:Position) -> None:...
    def if_closed(self) -> bool:...
    def get_exposure(self) -> float:...
    def get_value(self) -> float:...
    def upfront_cash(self, pct_margin:float=0.15) -> float:...
    def is_funded(self) -> bool:...