from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

from blueshift.interfaces.trading._simulation import ABCSlippageModel
from blueshift.interfaces.data.library import ILibrary
from blueshift.lib.trades._order import Order
from blueshift.lib.common.constants import Frequency

class SlippageModel(ABCSlippageModel):
    """ 
        Slippage model implements the backtest simulation. The core 
        method `simulate` takes in an order and simulates the fill 
        and the fill price.
    """
    _freq: str
    _library: ILibrary

    def __init__(self, frequency:str|None=None, library:ILibrary|None=None):...
    def set_frequency(self, frequency:str|Frequency) -> None:...
    def set_library(self, library:ILibrary) -> None:...
    def simulate(self, order:Order, dt:pd.Timestamp) ->tuple[float, float, float, float]:
        """
            Takes in an order (may be already partially filled), and 
            returns a trade with executed price and amount, as well 
            as the fair mid. The difference between the execution price
            and the fair mid is the slippage.
            
            :param order: Order object to process.
            :type order: :ref:`order<Order>` object.
            :param pandas.Timestamp dt: Timestamp for the order.
            :return: A tuple of volume, price, fair mid and max volume.
            :rtype: (float, float, float, float)
        """
        ...

class BarDataSlippage(SlippageModel):
    """ Trade simulation based on OHLCV data. """
    def __init__(self, max_volume:float=0.02):
        """
            At minimum, a bar (OHLCV) based trade simulator must be 
            initiated with a `library` object, that implements the 
            `get_spot_value` method to fetch an OHLCV column at a given
            timestamp. Also, optionally specify the `CostModel` for 
            calculating brokerages and other charges for this simulation.
            If not `cost_model` keyword is found, it is assumed to be 
            zero cost.
        """
        ...
        
    @classmethod
    def get_arguments(cls) -> list[str]:
        return ['frequency','library','max_volume']
    
class NoSlippage(BarDataSlippage):
    """
        Trade simulation based on OHLCV data without any slippage. The
        max volume executed at each trading bar is capped at 
        `max_volume` fraction (defaults to 0.02, i.e. 2% of available
        volume at each bar) of the available volume at the bar. Setting 
        `max_volume` to 0 will disable any cap and the full pending 
        amount will be assumed to trade successfully in a single trade.
        The impact cost is always zero, i.e. the order is executed at 
        the available 'close' price on that bar.
        
        :param float max_volume: maximum volume that can be executed at a bar.
        
    """
    ...
    
class BidAskSlippage(BarDataSlippage):
    """
        Trade simulation based on OHLCV data with bid-ask spread. The
        max volume executed is capped at `max_volume` fraction of the 
        available volume at the bar. The impact cost equals to half the
        bid-ask spread. Setting`max_volume` to 0 will disable any cap 
        and the full pending amount will be assumed to trade successfully 
        in a single trade.
        
        :param float max_volume: maximum volume that can be executed at a bar.
        
        .. warning::
            This is supported only for cases where the dataset includes 
            the 'bid' and 'ask' data. At present, only the Forex data 
            set is suitable for this slippage model.
    """
    ...

class VolumeSlippage(BarDataSlippage):
    """ 
        Trade simulation based on OHLCV data with volume based slippage. 
        In this model, the max volume that can be executed is capped 
        at a fixed percent of the volume at the bar. The price impact is
        modelled based on the below.
        
        .. math::
            \Delta P = s/2 + \alpha.\sigma.(\frac{Q}{V})^\beta
            
        where:
            :math:`\Delta P` = impact, # pyright: ignore[reportInvalidStringEscapeSequence]
            :math:`s` = spread,
            :math:`\alpha` = cost coefficient,
            :math:`\sigma` = historical volatility,
            :math:`Q` = traded volume,
            :math:`V` = available volume,
            :math:`\beta` = cost exponent
                
        :param float max_volume: maximum participation (defaults to 0.02).
        :param float cost_coeff: cost coefficient (defaults to 0.002).
        :param float cost_exponent: cost exponent (defaults to 0.5).
        :param float spread: constant spread (defaults to 0).
        :param bool spread_is_percentage: If false, spread is treated as absolute (defaults False).
        :param bool use_vol: If false, vol is set to 1.0 (defaults to False).
        :param float vol_floor: Floor value of vol (defaults to 0.05), ignored if use_vol is False.
        
        .. seealso::
            For more on the model, see |ref_link|.
            
        .. |ref_link| raw:: html
        
            <a href="https://mfe.baruch.cuny.edu/wp-content/uploads/2017/05/Chicago2016OptimalExecution.pdf" target="_blank">here</a>
            
    """
    def __init__(self, max_volume:float=0.02, cost_coeff:float=0.005, cost_exponent:float=0.5, 
                 spread:float=0.001, spread_is_percentage:bool=True, 
                 volume_is_absolute:bool=False, use_vol:bool=False, vol_floor:float=0.05):...
    
    ...
    
class FixedSlippage(VolumeSlippage):
    """
        Trade simulation based on OHLCV data with a fixed slippage. The
        max volume executed is capped at `max_volume` fraction of the 
        available volume at the bar. Slippage is half the spread.
        
        :param float spread: bid-ask spread (constant).
        :param float max_volume: maximum volume that can be executed at a bar.
    """
    def __init__(self, spread:float=0, max_volume:float=0.02):...
    
class FixedBasisPointsSlippage(VolumeSlippage):
    """
        Trade simulation based on OHLCV data with a fixed slippage 
        expressed in basis points (100th of a percentage point). The
        max volume executed is capped at `max_volume` fraction of the 
        available volume at the bar. Slippage is half the spread. The 
        actual spread applied is arrived at as :math:`spread*close/10000`, 
        where :math:`close` is the close price at that bar.
        
        :param float spread: bid-ask spread (constant in basis points).
        :param float max_volume: maximum volume that can be executed at a bar.
    """
    def __init__(self, spread:float=0, max_volum:float=0.02):...
        
    ...

class NSESlippage(VolumeSlippage):
    """ 
        approximation of NSE slippage with volume slippage - with 
        100% participation.
    """
    def __init__(self, max_volume:float=1.0, cost_coeff:float=0.005, cost_exponent:float=0.5, 
                 spread:float=0.001, spread_is_percentage:bool=True, 
                 volume_is_absolute:bool=False, use_vol:bool=False, vol_floor:float=0.05):...
    ...

class NYSESlippage(VolumeSlippage):
    """ 
        approximation of NYSE slippage with volume slippage - with 
        100% participation.
    """
    def __init__(self, max_volume:float=1.0, cost_coeff:float=0.005, cost_exponent:float=0.5, 
                 spread:float=0.001, spread_is_percentage:bool=True, 
                 volume_is_absolute:bool=False, use_vol:bool=False, vol_floor:float=0.05):...
    ...

class FXSlippage(BidAskSlippage):
    """ 
        Approximation of FX slippage with bid-ask slippage on a typical 
        FX trading platform. We assume standard lot-size is 100000. Any
        size till this is filled immedeately, and any size over this is 
        carried over to the next bar. Max volume, unlike in the parent 
        class, is **NOT** a fraction, but absolute amount of this standard 
        lot size.
    """
    def __init__(self, max_volume:int=100000):...
    ...

class CryptoSlippage(VolumeSlippage):
    """ 
        Approximation of crypto slippage with fixed basis point slippage 
        on a typical trading platform. We assume max lot-size is 
        100000 times the minimum allowed quantity. Any size till this 
        is filled immedeately, and any size over this is carried over 
        to the next bar. Max volume, unlike in the parent class, 
        is **NOT** a fraction, but absolute amount of this standard 
        lot size.
    """
    def __init__(self, spread:float=50, max_volume:int=10000):...
    ...
    