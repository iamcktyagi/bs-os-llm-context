from __future__ import annotations

from blueshift.interfaces.trading._simulation import ABCCostModel
from blueshift.lib.trades._order import Order
from blueshift.interfaces.assets._assets import Asset
from blueshift.lib.trades._position import Position

MAX_CHARGE:float = 1E6

class CostModel(ABCCostModel):
    """
            CostModel defines the broking commission/ brokerage cost 
            modelling (plus any exchange fees and/or tax/ regulatory 
            charges, which defaults to zero and cannot be modified 
            by the user). A concrete implementation must define the 
            `calculate` method.
            
            :param float commissions: Brokerage commission - interpretation depends on implementation.
            :param float cost_cap: Max possible brokerage.
            :param float cost_floor: Minimum brokerage levied.
            :param bool cost_on_sell_only: If brokerage only on sell leg.
            :param \**kwargs:
                For compatibility, see below.
                
            :Keyword Arguments:
                * *cost* (``float``) --
                  if supplied, overwrites the `commission` parameter.
                * *min_trade_cost* (``float``) --
                  if supplied, overwrites the `cost_floor` parameter.
                
    """
    
    def __init__(self, commissions:float=0, cost_cap:float=MAX_CHARGE, cost_floor:float=0,
                 cost_on_sell_only:bool=False, *args, **kwargs):...
    
    @classmethod
    def get_arguments(cls) -> list[str]:...
        
    def calculate(self, order:Order, quantity:float, price:float, last_fx:float=1.0) -> tuple:
        """
            Calculate the commission and charges for the transaction.
            Commission is the fees deducted by the broker for this 
            particular transaction. Charges are any exchange fees 
            and/ or government or regulatory charges on top.
            
            :param order: Order object to process.
            :type order: :ref:`order<Order>` object.
            :param float quantity: Traded amount.
            :param float price: Traded price.
            :param float last_fx: FX rate for conversion (defaults to 1.0).
            :return: A tuple of commission, charges.
            :rtype: (float, float)
        
        """
        ...
    
    def rollcost(self, positions:dict[Asset, Position]) -> tuple:
        """ 
            Costs for rolling positions overnight. This is usually 
            applicable for rolling margin trading positions (e.g 
            Forex CFDs).
            
            :param dict position: Open positions.
            :return: A tuple of costs, margin.
            :rtype: (float, float)
            
            .. note::
                the input is a dictionary of current open positions - 
                keyed by the :ref:`assets <Asset>` and values are 
                :ref:`position <Position>` objects. The `cost` in the returned 
                tuple is the cost to charge for the roll, and `margin` 
                is the overnight margin to settle.
        """
        ...
        
class NoCommission(CostModel):
    """ Zero commission and trading charges. """
    def __init__(self):...
    ...
    
class PerDollar(CostModel):
    """ 
        Brokerage costs based on total value traded with cap and floor. 
        This is derived from `CostModel` and takes in the 
        same parameters. The parameter `commissions` (or `cost`) is 
        multiplied with the traded value (quantity times the price) 
        to determine the cost.
    """
    ...
    
class PerShare(CostModel):
    """ 
        Brokerage costs based on total quantity traded with cap and floor.
        This is derived from `CostModel` and takes in the 
        same parameters. The parameter `commissions` (or `cost`) is 
        multiplied with the traded quantity to determine the cost.
        
    """        
    ...
    
class PerOrder(CostModel):
    """ 
        Flat brokerage costs per order. This is derived from `CostModel` 
        and takes in the same parameters. The parameter `commissions` 
        (or `cost`) is the flat rate per order. If an order results in 
        multiple trades (corresponding to multiple fills), the charge 
        is applied only once (per order).
    """
    
    def __init__(self, commissions:float=0, cost_cap:float=MAX_CHARGE, cost_floor:float=0,
                 cost_on_sell_only:float=False):...
    
class FXCommission(CostModel):
    """ 
        Typical brokerage costs for FX trading platform. All costs are 
        assumed to be included in the bid-ask spread. This cost is 
        automatically captured in the slippage simulation.
    """
    
class PipCost(FXCommission):
    """ 
        Brokerage costs based on total quantity traded with cap and floor.
        This is derived from `CostModel` and takes in the 
        same parameters. The parameter `commissions` (or `cost`) is 
        multiplied with the traded quantity to determine the cost. In 
        addition, this also implements the `rollcost` method to calculate 
        the overnight funding cost of carrying over the position.
        
        .. note::
            This cost model is suitable for Forex assets only.
    """        
    ...
    
class PipsCost(PipCost):
    """ 
        PipCost version for backward compatibility.
        
        .. note::
            This cost model is suitable for Forex assets only.
    """        
    ...

class NSECommission(CostModel):
    """ 
        Typical NSE discount brokerage costs. Brokerage is 0 for 
        equity delivery trades. Brokerage is 3bps, capped at INR 20
        for everything else (including intraday equities).
    """
    ...

class NYSECommission(CostModel):
    """ 
        Typical US discount brokerage costs. Brokerage is USD 0.005 for 
        equity trades (per share), which is capped at 1% of trade 
        value and floored at USD 1 per order. For futures, commission 
        per contract is USD 0.85. For options, USD 0.65 per contract, 
        floored at USD 1 per order. ETFs has no fees. All are assumed 
        to include any exchange and transaction fees.
    """
    ...