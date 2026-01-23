from __future__ import annotations
from typing import TYPE_CHECKING
from abc import ABC, abstractmethod

from blueshift.lib.common.enums import AccountEventType as EventType
from blueshift.lib.exceptions import BlueshiftException
from blueshift.interfaces.trading.blotter import IBlotter
from blueshift.interfaces.assets._assets import Asset

if TYPE_CHECKING:
    import pandas as pd

class AccountEvent(ABC):
    """
        Encapsulation of accounts events like margin calls, corporate 
        actions etc. The responsibility to emit this events is with the
        broker object of the algorithm. The responsibility to handle these
        events are with the blotter object.
    """
    def __init__(self, *args, **kwargs):
        self._applied = False
    
    @abstractmethod
    def apply(self, blotter:IBlotter):
        """ 
            Apply the event
            
            Args:                
                ``blotter (object)``: Algo blotter.
        """
        raise NotImplementedError
        
class RollOver(AccountEvent):
    """
        Roll-over applied to assets, typically Forex positions. This can 
        also include any other interest accrued.
        
        Note: 
            The rollover cost should be provided in percentage term for 
            a long position. For short positions it will be applied with 
            the sign reversed.
        
        Args:
            ``asset (object)``: Asset for which this applies.
            
            ``effective_dt (Timestamp)``: Date on which this applies.
            
            ``pct_long_cost (float)``: rollover cost for a long position in percentage.
    """
    def __init__(self, asset:Asset, effective_dt:pd.Timestamp, pct_long_cost:float):
        
        self._asset = asset
        self._effective_dt = effective_dt
        self._cost = pct_long_cost
        super(RollOver, self).__init__()
        
    def apply(self, blotter:IBlotter):
        """ reflect the cash cost in the account cash balance. """
        if self._applied:
            return
        
        if not blotter.timestamp:
            raise BlueshiftException(f'cannot apply rollover event, blotter timestamp not set yet.')
        
        if blotter.timestamp < self._effective_dt:
            return
        
        if self._asset in blotter.current_positions:
            cash = blotter.current_positions[self._asset].quantity*self._cost
            blotter.account.cashflow(cash=cash, margin=0)
        
        self._applied = True
        
class CapitalChange(AccountEvent):
    """
        Roll-over applied to assets, typically Forex positions. This can 
        also include any other interest accrued.
        
        Args:
            ``asset (object)``: Asset for which this applies.
            
            ``effective_dt (Timestamp)``: Date on which this applies.
            
            ``amount (float)``: The amount of capital injected (negative for withdrawal).
    """
    def __init__(self, asset:Asset, effective_dt:pd.Timestamp, amount:float):
        
        self._asset = asset
        self._effective_dt = effective_dt
        self._amount = amount
        super(CapitalChange, self).__init__()
        
    def apply(self, blotter):
        """ reflect the change in capital in the account cash balance. """
        if self._applied:
            return
        
        blotter.account.cashflow(cash=self._amount, margin=0)
        self._applied = True
        
class CorporateAction(AccountEvent):
    """
        Base class for corporate actions.
        
        Args:
            ``asset (object)``: Asset for which this applies.
            
            ``effective_dt (Timestamp)``: Date on which this applies.
            
            ``announcement_dt (Timestamp)``: Date of announce ments.
            
            ``args and kwargs``: Further details (see below).
    """
    def __init__(self, asset:Asset, effective_dt:pd.Timestamp, announcement_dt:pd.Timestamp, *args, **kwargs):
        self._asset = asset
        self._effective_dt = effective_dt
        self._announcement_dt = announcement_dt
        super(CorporateAction, self).__init__()
        
    @abstractmethod
    def apply(self, blotter):
        """ 
            Apply the corporate event. 
            
            Args:
                ``blotter (object)``: Algo blotter.
        """
        raise NotImplementedError
        
class StockDividend(CorporateAction):
    """
        Class for |dividend|. The dividend is determined by the dividend 
        ratio. We assume cash dividend. If there is an existing position 
        in the underlying stock, we compute the dividend amount and add 
        this to the account cash balance.
        
        Args:
            ``asset (object)``: Asset for which this applies.
            
            ``effective_dt (Timestamp)``: Date on which this applies.
            
            ``announcement_dt (Timestamp)``: Date of announce ments.
            
            ``div_ratio (float)``: Ratio of dividend payout to closing price.
        
        .. |dividend| raw:: html

           <a href="https://en.wikipedia.org/wiki/Common_stock_dividend" target="_blank">stock dividend</a>
    """
    def __init__(self, asset:Asset, effective_dt:pd.Timestamp, announcement_dt:pd.Timestamp, 
                 div_ratio:float):
        self.div_ratio = div_ratio
        super(StockDividend, self).__init__(asset, effective_dt, announcement_dt)
        
    def apply(self, blotter:IBlotter):
        """ apply the dividend to cash balance, if not already done. """
        if self._applied:
            return
        
        if not blotter.timestamp:
            raise BlueshiftException(f'cannot apply rollover event, blotter timestamp not set yet.')
        
        if blotter.timestamp < self._effective_dt:
            return
        
        if self._asset in blotter.current_positions:
            cash = blotter.current_positions[self._asset].quantity*self.div_ratio
            blotter.account.cashflow(cash=cash, margin=0)
        self._applied = True

class StockSplit(CorporateAction):
    """
        Class for |split|. The dividend is determined by the dividend 
        ratio. We assume cash dividend. If there is an existing position 
        in the underlying stock, we compute the dividend amount and add 
        this to the account cash balance.
        
        Args:
            ``asset (object)``: Asset for which this applies.
            
            ``effective_dt (Timestamp)``: Date on which this applies.
            
            ``announcement_dt (Timestamp)``: Date of announce ments.
            
            ``split_ratio (float)``: Ratio of dividend payout to closing price.
        
        .. |split| raw:: html

           <a href="https://en.wikipedia.org/wiki/Stock_split" target="_blank">stock split</a>
    """
    def __init__(self, asset:Asset, effective_dt:pd.Timestamp, announcement_dt:pd.Timestamp, 
                 split_ratio:float):
        self._split_ratio = split_ratio
        super(StockSplit, self).__init__(asset, effective_dt, announcement_dt)
        
    def apply(self, blotter:IBlotter):
        """ apply the split, update blotter, and return any fractional shares as cash. """
        if self._applied:
            return
        
        if self._asset in blotter.current_positions:
            # we want to apply this on preferably overnight position.
            # before we put on any new trade for the day.
            cash = blotter.current_positions[self._asset].apply_split(self._split_ratio) # type: ignore
            blotter.account.cashflow(cash=cash, margin=0)
        self._applied = True
            
class StockMerger(CorporateAction):
    """
        Class for |split|. The dividend is determined by the dividend 
        ratio. We assume cash dividend. If there is an existing position 
        in the underlying stock, we compute the dividend amount and add 
        this to the account cash balance.
        
        Args:
            ``asset (object)``: Asset for which this applies.
            
            ``effective_dt (Timestamp)``: Date on which this applies.
            
            ``announcement_dt (Timestamp)``: Date of announce ments.
            
            ``acquirer (object)``: The asset which is aquiring this company.
            
            ``exchange_ratio (float)``: Ratio for the merger.
            
            ``offer_price (float)``: Offer price for the merger.
            
            ``cash_pct (float)``: Cash pay percent (the rest assumed to be in stock swap).
        
        .. |merger| raw:: html

           <a href="https://en.wikipedia.org/wiki/Mergers_and_acquisitions" target="_blank">stock merger</a>
    """
    def __init__(self, asset:Asset, effective_dt:pd.Timestamp, announcement_dt:pd.Timestamp, 
                 acquirer:Asset, exchange_ratio:float, offer_price:float=0, cash_pct:float=0):
        self._exchange_ratio = exchange_ratio
        self._acquirer = acquirer
        self._offer_price = offer_price
        self._cash_pct = cash_pct
        super(StockMerger, self).__init__(asset, effective_dt, announcement_dt)
        
    def apply(self, blotter:IBlotter):
        """ compute the cash and new asset amount, and update blotter and account. """
        #TODO: This is NOT correct, fix this.
        raise NotImplementedError
        # if self._applied:
        #     return
        
        # if self._asset in blotter.current_positions:
        #     cash1 = blotter.current_positions[self._asset].quantity\
        #             *self._cash_pct*self._offer_price
            
        #     cash2 = blotter.current_positions[self._asset].apply_merger( # type: ignore
        #             self._acquirer, self._exchange_ratio, self._cash_pct)
            
        #     blotter.current_positions[self._acquirer] \
        #         = blotter.current_positions[self._asset]
            
        #     blotter.account.cashflow(cash=cash1+cash2, margin=0)
            
        #     self._applied = True
            
        
def account_event_factory(event_type, *args, **kwargs):
    if event_type == EventType.RollOver:
        return RollOver(*args, **kwargs)
    elif event_type == EventType.CapitalChange:
        return CapitalChange(*args, **kwargs)
    elif event_type == EventType.StockDividend:
        return StockDividend(*args, **kwargs)
    elif event_type == EventType.StockSplit:
        return StockSplit(*args, **kwargs)
    elif event_type == EventType.StockMerger:
        return StockMerger(*args, **kwargs)
    else:
        return None

        
        
        
        
        
        
        
        
        