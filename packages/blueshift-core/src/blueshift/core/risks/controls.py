from __future__ import annotations
from typing import TYPE_CHECKING, Callable, cast
from abc import ABC, abstractmethod 
import math

from blueshift.lib.common.sentinels import noop
from blueshift.lib.common.constants import CCY

if TYPE_CHECKING:
    import pandas as pd
    from blueshift.interfaces.assets._assets import Asset
    from blueshift.lib.trades._order import Order
    from blueshift.interfaces.context import IContext

class TradingControl(ABC):
    """
        implements trading controls and restrictions. The controls are of
        two types - flow based control (applies to each order) and stock based
        control (applies to current state). The class implements `validate`
        function which returns True if the checks are passed. Else it returns
        false. Every order generated will call the "validate" method of all
        such control registered (using `register_trading_controls` API), 
        before sending the order for execution. If the validation fails, the 
        order will not be sent and the `on_fail` handler will be invoked.
    """
    def __init__(self, *args, **kwargs):
        self._on_fail = kwargs.get("on_fail", noop)
        self._metric = 0
        self._limit = 0
        self._error = False
        self._ccy = kwargs.get("ccy", CCY.LOCAL) # type: ignore this is guranteed to be CCY struc
        self._context = kwargs.get("context", "global")
        
    def __str__(self):
        return f'TradingControl[{self.__class__.__name__}]'
        
    def __repr__(self):
        return self.__str__()
        
    def _create(self, dt, max_num_orders=100, on_fail=None):
        pass
    
    def add_control(self, *args, **kwargs):
        raise NotImplementedError
    
    @abstractmethod
    def get_error_msg(self, asset:Asset, dt:pd.Timestamp, *args, **kwargs):
        """ generate a custom error message on failure. """
        raise NotImplementedError
    
    @abstractmethod
    def validate(self, order:Order, dt:pd.Timestamp, context:IContext, on_fail:Callable=noop, **kwargs):
        """ 
            function that implements the control validation, given an 
            order object, current timestamp (dt) and context and a custom
            callback (on_fail) that is called on failure (before returning 
            False). In case the no callback is provided, the default callback 
            will simply log the associated error message as warning.
            
            .. important::
                the signature of the on_fail is f(control, asset, dt, amount, context)
                where "control" is the risk control object itself.
                
            .. seealso::
                See details in :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.register_trading_controls`.
        """
        raise NotImplementedError
        
class TCOrderQtyPerTrade(TradingControl):
    """
        Class implementing max order quantity per single order.
    """
    def __init__(self, default_max:float, max_amount_dict:dict[Asset, float]|None=None,
                 on_fail=noop, ccy=CCY.LOCAL): # type: ignore
        # pylint: disable=bad-super-call
        super(self.__class__, self).__init__(ccy=ccy, on_fail=on_fail)
        self._max_amount_dict = max_amount_dict if max_amount_dict else {}
        self._default_max = abs(default_max)
        self.add_control({})
        
    def add_control(self, max_amount_dict:dict[Asset, float]):
        self._max_amount_dict = {**self._max_amount_dict, **max_amount_dict}
        for asset in self._max_amount_dict:
            self._max_amount_dict[asset] = abs(self._max_amount_dict[asset])
    
    def validate(self, order:Order, dt:pd.Timestamp, context:IContext, on_fail:Callable=noop, 
                 dry_run:bool=False, **kwargs):
        amount = order.quantity
        asset = order.asset
        max_allowed = self._max_amount_dict.get(asset, self._default_max)
        
        if not max_allowed:
            return True
        
        if abs(amount) <= max_allowed:
            return True
        
        self._metric = amount
        self._limit = max_allowed
        
        if self._on_fail: 
            self._on_fail(self, asset, dt, amount, context)
        else:
            on_fail(self, asset, dt, amount, context)
            
        return False
    
    def get_error_msg(self, asset:Asset, dt:pd.Timestamp):
        msg = f"Failed per trade size control for {asset}"
        msg = msg + f", amount {self._metric} against limit {self._limit}"
        msg = msg + f", on {dt}"
        return msg
    
class TCOrderValuePerTrade(TradingControl):
    """
        Class implementing max order value per single order.
    """
    def __init__(self, default_max:float, max_amount_dict:dict[Asset,float]|None=None, 
                 on_fail:Callable=noop, ccy=CCY.LOCAL): # type: ignore
        # pylint: disable=bad-super-call
        super(self.__class__, self).__init__(ccy=ccy, on_fail=on_fail)
        self._max_amount_dict = max_amount_dict if max_amount_dict else {}
        self._default_max = default_max
        
    def add_control(self, max_amount_dict:dict[Asset,float]):
        self._max_amount_dict = {**self._max_amount_dict, **max_amount_dict}
    
    def validate(self, order:Order, dt:pd.Timestamp, context:IContext, on_fail:Callable=noop, 
                 dry_run:bool=False, **kwargs):
        amount = order.quantity
        asset = order.asset
        max_allowed = self._max_amount_dict.get(asset, self._default_max)
        
        if not max_allowed:
            return True
        
        price = context.data_portal.current(asset, 'close')
        price = cast(float, price)
        last_fx = asset.fx_rate(self._ccy, context.data_portal)
        
        if price is None or last_fx is None:
            self._error = True
            return False
        
        if math.isnan(price) or math.isnan(last_fx):
            self._error = True
            return False
        
        value = abs(amount*price*last_fx)
        
        if value < max_allowed:
            return True
        
        self._metric = value
        self._limit = max_allowed
        
        if self._on_fail:
            self._on_fail(self, asset, dt, amount, context)
        else:
            on_fail(self, asset, dt, amount, context)
            
        return False
    
    def get_error_msg(self, asset:Asset, dt:pd.Timestamp):
        msg = f"Failed per trade value control for {asset}"
        if self._error:
            msg = msg + f", failed to received latest price data."
        else:
            msg = msg + f", value {self._metric} against limit {self._limit}"
            msg = msg + f", on {dt}"
        self._error = False
        return msg
    
class TCOrderQtyPerDay(TradingControl):
    """
        Class implementing max order quantity per asset per day.
    """
    def __init__(self, default_max:float, max_amount_dict:dict[Asset,float]|None=None, 
                 on_fail:Callable=noop, ccy=CCY.LOCAL): # type: ignore
        # pylint: disable=bad-super-call
        super(self.__class__, self).__init__(ccy=ccy, on_fail=on_fail)
        self._max_amount_dict = max_amount_dict if max_amount_dict else {}
        self._default_max = abs(default_max)
        self.add_control({})
        self._asset_quota:dict[Asset,float] = dict((asset,0) for asset in self._max_amount_dict)
        self._current_dt = None
        
    def add_control(self, max_amount_dict:dict[Asset,float]):
        self._max_amount_dict = {**self._max_amount_dict, **max_amount_dict}
        for asset in self._max_amount_dict:
            self._max_amount_dict[asset] = abs(self._max_amount_dict[asset])
    
    def _reset_quota(self):
        for asset in self._asset_quota:
            self._asset_quota[asset] = 0
    
    def validate(self, order:Order, dt:pd.Timestamp, context:IContext, on_fail:Callable=noop, 
                 dry_run:bool=False, **kwargs):
        amount = order.quantity
        asset = order.asset
        
        if self._current_dt != dt.date():
            self._reset_quota()
            self._current_dt = dt.date()
        
        max_allowed = self._max_amount_dict.get(asset, self._default_max)
        
        if not max_allowed:
            return True
        
        max_used = self._asset_quota.get(asset, 0)
        estimated = abs(amount)+max_used
        
        if  estimated <= max_allowed:
            if not dry_run:
                self._asset_quota[asset] = estimated
            return True
        
        self._metric = max_used
        self._limit = max_allowed
        
        if self._on_fail:
            self._on_fail(self, asset, dt, amount, context)
        else:
            on_fail(self, asset, dt, amount, context)
            
        return False
    
    def get_error_msg(self, asset:Asset, dt:pd.Timestamp):
        msg = f"Failed per day trade amount control for {asset}"
        msg = msg + f", total amount {self._metric}"
        msg = msg + f" against limit {self._limit}, on {dt}"
        return msg
    
class TCOrderValuePerDay(TradingControl):
    """
        Class implementing max order value per asset per day.
    """
    def __init__(self, default_max:float, max_amount_dict:dict[Asset,float]|None=None, 
                 on_fail:Callable=noop, ccy=CCY.LOCAL): # type: ignore
        # pylint: disable=bad-super-call
        super(self.__class__, self).__init__(ccy=ccy, on_fail=on_fail)
        self._max_amount_dict = max_amount_dict if max_amount_dict else {}
        self._default_max = abs(default_max)
        self.add_control({})
        self._asset_quota:dict[Asset,float] = dict((asset,0) for asset in self._max_amount_dict)
        self._current_dt = None
        
    def add_control(self, max_amount_dict:dict[Asset,float]):
        self._max_amount_dict = {**self._max_amount_dict, **max_amount_dict}
        for asset in self._max_amount_dict:
            self._max_amount_dict[asset] = abs(self._max_amount_dict[asset])
    
    def _reset_quota(self):
        for asset in self._asset_quota:
            self._asset_quota[asset] = 0
    
    def validate(self, order:Order, dt:pd.Timestamp, context:IContext, on_fail:Callable=noop, 
                 dry_run:bool=False, **kwargs):
        amount = order.quantity
        asset = order.asset
        
        if self._current_dt != dt.date():
            self._reset_quota()
            self._current_dt = dt.date()
        
        max_allowed = self._max_amount_dict.get(asset, self._default_max)
        
        if not max_allowed:
            return True
        
        max_used = self._asset_quota.get(asset, 0)
        price = context.data_portal.current(asset, 'close')
        price = cast(float, price)
        last_fx = asset.fx_rate(self._ccy, context.data_portal)
        
        if price is None or last_fx is None:
            self._error = True
            return False
        
        if math.isnan(price) or math.isnan(last_fx):
            self._error = True
            return False
        
        value = abs(amount*price*last_fx)
        estimated = abs(value)+max_used
        
        if  estimated < max_allowed:
            if not dry_run:
                self._asset_quota[asset] = estimated
            return True
        
        self._metric = max_used
        self._limit = max_allowed
        
        if self._on_fail:
            self._on_fail(self, asset, dt, amount, context)
        else:
            on_fail(self, asset, dt, amount, context)
            
        return False
    
    def get_error_msg(self, asset:Asset, dt:pd.Timestamp):
        msg = f"Failed per day trade value control for {asset}"
        if self._error:
            msg = msg + f", failed to received latest price data."
        else:
            msg = msg + f", total value {self._metric}"
            msg = msg + f" against limit {self._limit}, on {dt}"
        self._error = False
        return msg
    

class TCOrderNumPerDay(TradingControl):
    """
        Class implementing max order number for all assets per day.
    """
    def __init__(self, max_num_orders:int, on_fail:Callable=noop, ccy=CCY.LOCAL): # type: ignore
        # pylint: disable=bad-super-call
        super(self.__class__, self).__init__(ccy=ccy, on_fail=on_fail)
        self._max_num_orders = max_num_orders
        self._used_limit = 0
        self._current_dt = None
        
    def add_control(self, max_num_orders:int):
        self._max_num_orders = max_num_orders
    
    def _reset_quota(self):
        self._used_limit = 0
    
    def validate(self, order:Order, dt:pd.Timestamp, context:IContext, on_fail:Callable=noop, 
                 dry_run:bool=False, **kwargs):
        amount = order.quantity
        asset = order.asset
        
        # TODO: a roll not matching with date rolls is not handled here.
        if self._current_dt != dt.date():
            self._reset_quota()
            self._current_dt = dt.date()
        
        if  self._used_limit+1 <= self._max_num_orders:
            if not dry_run:
                self._used_limit = self._used_limit+1
            return True
        
        self._metric = self._used_limit
        self._limit = self._max_num_orders
        
        if self._on_fail:
            self._on_fail(self, asset, dt, amount, context)
        else:
            on_fail(self, asset, dt, amount, context)
            
        return False
    
    def get_error_msg(self, asset:Asset, dt:pd.Timestamp):
        msg = f"Failed per day number of orders control"
        msg = msg + f", total orders {self._metric}"
        msg = msg + f" against limit {self._limit}, on {dt}"
        return msg
    

class TCGrossLeverage(TradingControl):
    """
        Class implementing max account gross leverage.
    """
    def __init__(self, max_leverage:float, on_fail:Callable=noop, ccy=CCY.LOCAL): # type: ignore
        # pylint: disable=bad-super-call
        super(self.__class__, self).__init__(ccy=ccy, on_fail=on_fail)
        self._max_leverage = max_leverage
        
    def add_control(self, max_leverage:float):
        self._max_leverage = max_leverage
    
    def validate(self, order:Order, dt:pd.Timestamp, context:IContext, on_fail:Callable=noop, 
                 dry_run:bool=False, **kwargs):
        amount = order.quantity
        asset = order.asset
        
        if self._context == "algo":
            current_exposure = context.blotter_account["gross_exposure"]
            liquid_value = context.blotter_account["liquid_value"]
        else:
            current_exposure = context.broker_account['gross_exposure']
            liquid_value = context.broker_account['net_liquidation']
        
        px = context.data_portal.current(asset, 'close')
        px = cast(float, px)
        
        if px is None or math.isnan(px):
            self._error = True
            return False
        
        trade_value = abs(px*amount)
        estimated_exposure = current_exposure + trade_value
        estimated_leverage = estimated_exposure/liquid_value
        
        if estimated_leverage < self._max_leverage:
            return True
        
        self._metric = estimated_leverage
        self._limit = self._max_leverage
        
        if self._on_fail:
            self._on_fail(self, asset, dt, amount, context)
        else:
            on_fail(self, asset, dt, amount, context)
            
        return False
    
    def get_error_msg(self, asset:Asset, dt:pd.Timestamp):
        msg = f"Failed per max leverage control"
        if self._error:
            msg = msg + f", failed to received latest price data."
        else:
            msg = msg + f", estimated post trade leverage {self._metric}"
            msg = msg + f" against limit {self._limit}, on {dt}"
        self._error = False
        return msg
    
class TCGrossExposure(TradingControl):
    """
        Class implementing max account gross leverage.
    """
    def __init__(self, max_exposure:float, on_fail:Callable=noop, ccy=CCY.LOCAL): # type: ignore
        # pylint: disable=bad-super-call
        super(self.__class__, self).__init__(ccy=ccy, on_fail=on_fail)
        self._max_exposure = max_exposure
        
    def add_control(self, max_exposure:float):
        self._max_exposure = max_exposure
    
    def validate(self, order:Order, dt:pd.Timestamp, context:IContext, on_fail:Callable=noop, 
                 dry_run:bool=False, **kwargs):
        amount = order.quantity
        asset = order.asset
        
        if self._context == "algo":
            current_exposure = context.blotter_account['gross_exposure']
        else:
            current_exposure = context.broker_account['gross_exposure']
        
        px = context.data_portal.current(asset, 'close')
        px = cast(float, px)
        if px is None or math.isnan(px):
            self._error = True
            return False
        
        trade_value = abs(px*amount)
        estimated_exposure = current_exposure + trade_value
        
        if estimated_exposure < self._max_exposure:
            return True
        
        self._metric = estimated_exposure
        self._limit = self._max_exposure
        
        if self._on_fail:
            self._on_fail(self, asset, dt, amount, context)
        else:
            on_fail(self, asset, dt, amount, context)
            
        return False
    
    def get_error_msg(self, asset:Asset, dt:pd.Timestamp):
        msg = f"Failed per max exposure control"
        if self._error:
            msg = msg + f", failed to received latest price data."
        else:
            msg = msg + f", estimated post trade exposure {self._metric}"
            msg = msg + f" against limit {self._limit}, on {dt}"
        self._error = False
        return msg
    
    
class TCLongOnly(TradingControl):
    """
        Class implementing max account gross leverage.
    """
    def __init__(self, on_fail:Callable=noop, ccy=CCY.LOCAL): # type: ignore
        # pylint: disable=bad-super-call
        super(self.__class__, self).__init__(ccy=ccy, on_fail=on_fail)
        
    def add_control(self, max_exposure:float):
        pass
    
    def validate(self, order:Order, dt:pd.Timestamp, context:IContext, on_fail:Callable=noop, 
                 dry_run:bool=False, **kwargs):
        side = 1 if order.is_buy() else -1
        amount = order.quantity*side
        asset = order.asset
        
        current_pos = context.blotter_portfolio.get(asset, None)
        if current_pos:
            current_pos = current_pos.quantity
        else:
            current_pos = 0
        estimated_pos = current_pos + amount
        
        if estimated_pos >=0:
            return True
        
        self._metric = estimated_pos
        
        if self._on_fail:
            self._on_fail(self, asset, dt, amount, context)
        else:
            on_fail(self, asset, dt, amount, context)
            
        return False
    
    def get_error_msg(self, asset:Asset, dt:pd.Timestamp):
        msg = f"Failed long-only control"
        msg = msg + f", estimated post trade position {self._metric}"
        msg = msg + f", on {dt}"
        return msg
    
class TCPositionQty(TradingControl):
    """
        Class implementing max position size.
    """
    def __init__(self, default_max:float, max_amount_dict:dict[Asset, float]|None=None, 
                 on_fail:Callable=noop, ccy=CCY.LOCAL): # type: ignore
        # pylint: disable=bad-super-call
        super(self.__class__, self).__init__(ccy=ccy, on_fail=on_fail)
        self._max_amount_dict = max_amount_dict if max_amount_dict else {}
        self._default_max = abs(default_max)
        self.add_control({})
        
    def add_control(self, max_amount_dict:dict[Asset, float]):
        self._max_amount_dict = {**self._max_amount_dict, **max_amount_dict}
        for asset in self._max_amount_dict:
            self._max_amount_dict[asset] = abs(self._max_amount_dict[asset])
    
    def validate(self, order:Order, dt:pd.Timestamp, context:IContext, on_fail:Callable=noop, 
                 dry_run:bool=False, **kwargs):
        side = 1 if order.is_buy() else -1
        amount = order.quantity*side
        asset = order.asset
        
        max_allowed = self._max_amount_dict.get(asset, self._default_max)
        if not max_allowed:
            return True
        
        current_pos = context.blotter_portfolio.get(asset, None)
        if current_pos:
            current_pos = current_pos.quantity
        else:
            current_pos = 0
        estimated_pos = abs(current_pos + amount)
        
        if estimated_pos <= max_allowed:
            return True
        
        self._metric = estimated_pos
        self._limit = max_allowed
        
        if self._on_fail:
            self._on_fail(self, asset, dt, amount, context)
        else:
            on_fail(self, asset, dt, amount, context)
            
        return False
    
    def get_error_msg(self, asset:Asset, dt:pd.Timestamp):
        msg = f"Failed max position size control for {asset}"
        msg = msg + f", estimated post trade position {self._metric}"
        msg = msg + f" against limit of {self._limit}, on {dt}"
        return msg

class TCPositionValue(TradingControl):
    """
        Class implementing max position size.
    """
    def __init__(self, default_max:float, max_amount_dict:dict[Asset, float]|None=None, 
                 on_fail:Callable=noop, ccy=CCY.LOCAL): # type: ignore
        # pylint: disable=bad-super-call
        super(self.__class__, self).__init__(ccy=ccy, on_fail=on_fail)
        self._max_amount_dict = max_amount_dict if max_amount_dict else {}
        self._default_max = abs(default_max)
        self.add_control({})
        
    def add_control(self, max_amount_dict:dict[Asset,float]):
        self._max_amount_dict = {**self._max_amount_dict, **max_amount_dict}
        for asset in self._max_amount_dict:
            self._max_amount_dict[asset] = abs(self._max_amount_dict[asset])
    
    def validate(self, order:Order, dt:pd.Timestamp, context:IContext, on_fail:Callable=noop, 
                 dry_run:bool=False, **kwargs):
        side = 1 if order.is_buy() else -1
        amount = order.quantity*side
        asset = order.asset
        
        max_allowed = self._max_amount_dict.get(asset, self._default_max)
        if not max_allowed:
            return True
        
        current_pos = context.blotter_portfolio.get(asset, None)
        if current_pos:
            current_pos = current_pos.quantity
        else:
            current_pos = 0
        estimated_pos = abs(current_pos + amount)
        price = context.data_portal.current(asset, 'close')
        last_fx = asset.fx_rate(self._ccy, context.data_portal)
        price = cast(float, price)
        if price is None or last_fx is None:
            self._error = True
            return False
        
        if math.isnan(price) or math.isnan(last_fx):
            self._error = True
            return False
        
        value = estimated_pos*price*last_fx
        
        if value < max_allowed:
            return True
        
        self._metric = value
        self._limit = max_allowed
        
        if self._on_fail:
            self._on_fail(self, asset, dt, amount, context)
        else:
            on_fail(self, asset, dt, amount, context)
            
        return False
    
    def get_error_msg(self, asset:Asset, dt:pd.Timestamp):
        msg = f"Failed max position size control for {asset}"
        if self._error:
            msg = msg + f", failed to received latest price data."
        else:
            msg = msg + f", estimated post trade position {self._metric}"
            msg = msg + f" against limit of {self._limit}, on {dt}"
        self._error = False
        return msg
    
    
class TCBlackList(TradingControl):
    """
        Class implementing restricted list of assets.
    """
    def __init__(self, assets:list[Asset], on_fail:Callable=noop, ccy=CCY.LOCAL): # type: ignore
        # pylint: disable=bad-super-call
        super(self.__class__, self).__init__(ccy=ccy, on_fail=on_fail)
        self._restricted_list:set[Asset] = set(assets)
        
    def add_control(self, assets:list[Asset]):
        restricted_list:set[Asset] = set(assets)
        self._restricted_list = self._restricted_list.union(restricted_list)
    
    def validate(self, order:Order, dt:pd.Timestamp, context:IContext, on_fail:Callable=noop, 
                 dry_run:bool=False, **kwargs):
        if order.asset in self._restricted_list:
            return False
        return True
    
    def get_error_msg(self, asset:Asset, dt:pd.Timestamp):
        msg = f"Failed restricted list control for {asset} on {dt}"
        return msg
    
class TCWhiteList(TradingControl):
    """
        Class implementing restricted list of assets.
    """
    def __init__(self, assets:list[Asset], on_fail:Callable=noop, ccy=CCY.LOCAL): # type: ignore
        # pylint: disable=bad-super-call
        super(self.__class__, self).__init__(ccy=ccy, on_fail=on_fail)
        self._allowed_list:set[Asset] = set(assets)
        
    def add_control(self, assets):
        allowed_list:set[Asset] = set(assets)
        self._allowed_list = self._allowed_list.union(allowed_list)
    
    def validate(self, order:Order, dt:pd.Timestamp, context:IContext, on_fail:Callable=noop, 
                 dry_run:bool=False, **kwargs):
        if order.asset in self._allowed_list:
            return True
        return False
    
    def get_error_msg(self, asset:Asset, dt:pd.Timestamp):
        msg = f"Failed white list control for {asset} on {dt}"
        return msg
    

class TCNotificationNumPerDay(TradingControl):
    """
        Class implementing max notification number for all assets per day.
    """
    def __init__(self, max_num_orders:int, on_fail:Callable=noop, ccy=CCY.LOCAL): # type: ignore
        # pylint: disable=bad-super-call
        super(self.__class__, self).__init__(ccy=ccy, on_fail=on_fail)
        self._max_num_orders = max_num_orders
        self._used_limit = 0
        self._current_dt = None
        
    def add_control(self, max_num_orders:int):
        self._max_num_orders = max_num_orders
    
    def _reset_quota(self):
        self._used_limit = 0
    
    def validate(self, order:Order, dt:pd.Timestamp, context:IContext, on_fail:Callable=noop, 
                 dry_run:bool=False, **kwargs):
        amount = order.quantity
        asset = order.asset
        
        # TODO: a roll not matching with date rolls is not handled here.
        if self._current_dt != dt.date():
            self._reset_quota()
            self._current_dt = dt.date()
        
        if  self._used_limit+1 <= self._max_num_orders:
            if not dry_run:
                self._used_limit = self._used_limit+1
            return True
        
        self._metric = self._used_limit
        self._limit = self._max_num_orders
        
        if self._on_fail:
            self._on_fail(self, asset, dt, amount, context)
        else:
            on_fail(self, asset, dt, amount, context)
            
        return False
    
    def get_error_msg(self, asset:Asset, dt:pd.Timestamp):
        msg = f"Failed per day number of notification control"
        msg = msg + f", total notifications sent {self._metric}"
        msg = msg + f" against limit {self._limit}, on {dt}"
        return msg
