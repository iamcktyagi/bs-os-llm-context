from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

from blueshift.interfaces.assets._assets import Asset
from blueshift.lib.trades._order import Order
from blueshift.lib.trades._position import Position
from blueshift.interfaces.data.library import ILibrary
from blueshift.lib.trades._order_types import ProductType
from blueshift.lib.common.constants import Frequency

class ABCSlippageModel:
    """ trade simulation model interface. """
    def set_frequency(self, frequency:str|Frequency) -> None:...
    def set_library(self, library:ILibrary) -> None:...
    def simulate(self, order:Order, dt:pd.Timestamp) ->tuple[float, float, float, float]:...

class ABCChargesModel:
    """ non-brokerarge charges (e.g. regulatory charges or taxes). """
    def calculate(self, order:Order, quantity:float, price:float, 
                  commission:float=0, last_fx:float=1.0) -> float:...

class ABCCostModel:
    """ cost model interface  for calculating trading costs. """
    @classmethod
    def get_arguments(cls) -> list[str]:...
        
    def calculate(self, order:Order, quantity:float, price:float, last_fx:float=1.0) -> tuple[float, float]:...
    
    def rollcost(self, positions:dict[Asset, Position]) -> tuple:...
    
class ABCMarginModel:
    """ interface face for trading and position margin model. """
    @classmethod
    def get_arguments(cls)-> list[str]:...
    @classmethod
    def supported_products(cls) -> list[ProductType]:...
    def set_library(self, library) -> ILibrary:...
    def get_trading_margins(self, orders, positions, *args, **kwargs) -> tuple[bool, float]:...
    def calculate(self, order:Order, quantity:float, price:float, position:float, 
                  timestamp:pd.Timestamp, last_fx:float=1.0, underlying_px:float=0, 
                  pos:Position|None=None) -> tuple[float, float]:...