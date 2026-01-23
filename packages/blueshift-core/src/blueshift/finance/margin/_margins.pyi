from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

from blueshift.interfaces.trading._simulation import ABCMarginModel
from blueshift.interfaces.data.library import ILibrary
from blueshift.interfaces.assets._assets import Asset
from blueshift.lib.trades._order_types import ProductType
from blueshift.lib.trades._order import Order
from blueshift.lib.trades._position import Position

class MarginModel(ABCMarginModel):
    """
        Margin model calculates margins required (or released) for an 
        order execution. For a fully funded trade, (e.g. cash equities)
        the margin is 0. For margin trading or derivatives (futures and 
        options), each trade adds to or releases a certain margin. Margins 
        are charged from the available cash in the portfolio. On release, 
        it is added back to portfolio cash. Each order, before execution, 
        is checked for sufficient funds in the account. If sufficient funds
        are not available, the order is rejected.
        
        All margin models derive from this class and concretely 
        implement two methods, `calculate` and `exposure_margin`. The 
        method `calculate` is used to determine margin requirements for 
        a transaction. The method `exposure_margin` is used to calculate 
        daily MTM settlements at the end-of-day calculation.
        
        .. note::
            Selection of margin models has no impact on fully funded 
            assets (e.g. cash equities).
            
    """
    _library:ILibrary
    margins_collected:float
    position_wise_margins:dict[Asset, float]

    def __init__(self, library=None):...
    @classmethod
    def get_arguments(cls)-> list[str]:...
    @classmethod
    def supported_products(cls) -> list[ProductType]:...
    def set_library(self, library) -> ILibrary:...
    def get_trading_margins(self, orders, positions, *args, **kwargs) -> tuple:
        """
            Calculate required margins (incremental or total) given a list of 
            orders, and a dict of asset wise current positions. The first item 
            in the return tuple is True if the margin calculated is incremental
            (additional margin required/ to be released for the orders, given 
            the existing positions). Negative margin means margin release.
            
            :param list orders: list of order object
            :param dict positions: a dict of asset wise position objects.
            :return: A tuple of incremental flag and required margin.
            :rtype: (bool, float)

        """
        ...
    
    def calculate(self, order:Order, quantity:float, price:float, 
                    position:float, timestamp:pd.Timestamp, last_fx:float=1.0, 
                    underlying_px:float=0, pos:Position|None=None) -> tuple:
        """
            Calculate the cashflow and margin for a transaction. The 
            algo account must have sufficient cash to cover for the 
            transaction to happen.
            
            :param order: Order object to process.
            :type order: :ref:`order<Order>` object.
            :param float quantity: Amount to trade.
            :param float price: Traded price.
            :param float position: Current position in the asset with sign.
            :param pandas.Timestamp timestamp: Current timestamp.
            :param float last_fx: FX rate for conversion.
            :param float underlying_px: Underlying price for option.
            :param pos: Position object to process.
            :type pos: :ref:`position<Position>` object.
            :return: A tuple of cashflow, margin.
            :rtype: (float, float)
            
        """
        ...
    
    def exposure_margin(self, asset:Asset, exposure:float,
                          pos:Position, timestamp:pd.Timestamp, last_fx:float=1.0, 
                          underlying_px:float=0.0) -> float:
        """
            Compute the exposure margin, given an asset and position in 
            that asset. This is used for end-of-day mark-to-market 
            settlement computation and is settled against the account. 
            If the account is short of the required cash, the account is
            frozen for further trading. last_fx not needed as exposure is fx adjusted.
            
            :param asset: Asset in which margin to calculate.
            :type asset: :ref:`asset<Asset>` object.
            :param float exposure: current exposure in the asset.
            :param pandas.Timestamp timestamp: current timestamp
            :return: exposure margin
            :rtype: float
        """
        ...
        
class NoMargin(MarginModel):
    """ 
        No margin. This model allows any amount of leverage for 
        unfunded products (e.g. forex or derivatives).
    """
    ...
    
class FlatMargin(MarginModel):
    """ 
        Flat margin on total exposure as percentage. The parameter 
        `intial_margin` is used to compute the fraction of the exposure 
        required to be posted as margin for a transaction. This means 
        `1/margin` is the leverage offered. The default value is 
        10% (i.e. 10x leverage). The parameter `maintenance_margin` is 
        applied for computation of margin required for carrying 
        an overnight position. It defaults to the same value as the `initial_margin` - 
        that is, no extra margin is charged for overnight positions.
        
        :param float intial_margin: Initial margin for a trade.
        :param float maintenance_margin: Maintenance margin.
    """
    _margin:float
    _maintenance_margin:float
    
    def __init__(self, intial_margin=0.10):...
    
class NSEMargin(FlatMargin):
    """ A flat margin model (10%) for NSE/NFO. """
    def __init__(self, intial_margin=0.10):...
    ...

class RegTMargin(FlatMargin):
    """ A flat margin model (50%) for NYSE. """
    def __init__(self, intial_margin=0.50):...
    ...

class FXMargin(FlatMargin):
    """ A glat margin model for Forex. """
    ...