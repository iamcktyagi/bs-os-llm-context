from __future__ import annotations
from abc import ABC, abstractmethod
from collections import OrderedDict
from functools import wraps
from typing import TYPE_CHECKING, Type, Callable, Dict, Any

from blueshift.lib.trades._order_types import (
        ProductType, OrderType, OrderValidity,
        OrderSide, OrderFlag)
from blueshift.lib.trades._order import Order
from blueshift.interfaces.assets._assets import Asset
from blueshift.lib.common.enums import AlgoOrderStatus
from blueshift.interfaces.context import IContext
from blueshift.interfaces.data.data_portal import DataPortal

from ..assets.assets import asset_factory
from ..plugin_manager import load_plugins

if TYPE_CHECKING:
    import pandas as pd
    from blueshift.core.algorithm.algorithm import TradingAlgorithm
else:
    import blueshift.lib.common.lazy_pandas as pd

def check_initialized(exception:Type[Exception]|None=None) -> Callable:
    def decorator(f):
        @wraps(f)
        def decorated(self, *args, **kwargs):
            if not self.is_initialized():
                msg = f'Algo order is not initialized.'
                if exception:
                    if issubclass(exception, BaseException):
                        raise exception(msg)
                raise RuntimeError(msg)
                
            return f(self, *args, **kwargs)
        return decorated
    return decorator

class IAlgoOrder(ABC):
    """
        Interface for Algo Orders. It follows all the standard 
        properties of the regular `Order` objects. During the init, 
        pass a `algo` name (should be a string), and other attribute 
        values relevant for an order object. The lifetime is started 
        once the order object is passed on to the algo order function
        (which invoke its `execute` method). After this, periodically 
        the `update` method is called, till the order reaches a 
        terminal state. The following attributes cannot be overwritten 
        - `product_type` (set to ProductType.ALGO), `order_type` (
        set to OrderType.SMART), `order_validity` (DAY), `order_flag`
        (NORMAL), `disclosed` (0) and `fractional` (False). Other 
        attributes are implemented
    """
    def __init__(self, algo, *args, **kwargs):
        import uuid
        self.__oid = uuid.uuid4().hex
        self._children = OrderedDict()
        
        self.product_type = ProductType.ALGO
        self.order_type = OrderType.SMART               # type: ignore
        self.order_validity = OrderValidity.DAY
        self.order_flag = OrderFlag.NORMAL
        self.disclosed = 0
        self.fractional = False
        
        self.status = AlgoOrderStatus.CREATED
        self.status_message = ''
        
        self._algo = algo
        self.tag = kwargs.pop('tag', '')
        self.remark = kwargs.pop('remark', '')
        self.parent_order_id = kwargs.pop('parent_order_id', '')
        price = kwargs.pop('price', 0)
        self.price = kwargs.pop('limit_price', price)
        self.trigger_price = kwargs.pop('trigger_price',0)
        self.stoploss_price = kwargs.pop('stoploss_price', 0)
        self.placed_by = kwargs.pop('placed_by','')
        self.user = kwargs.pop('user','')
        
        self.context:IContext|None = None
        self.data:DataPortal|None = None
        
    def __str__(self):
        return f"AlgoOrder[{self.oid}]"
    
    def __repr__(self):
        return self.__str__()
    
    def to_dict(self) -> dict:
        return {
                'algo':self.algo,
                'oid':self.oid,
                'parent_order_id':self.parent_order_id,
                'asset':self.asset,
                'user':self.user,
                'placed_by':self.placed_by,
                'product_type':self.product_type,
                'order_flag':self.order_flag,
                'order_type':self.order_type,
                'order_validity':self.order_validity,
                'quantity':self.quantity,
                'filled':self.filled,
                'pending':self.pending,
                'disclosed':self.disclosed,
                'price':self.price,
                'average_price':self.average_price,
                'trigger_price':self.trigger_price,
                'stoploss_price':self.stoploss_price,
                'side':self.side,
                'status':self.status,
                'status_message':self.status_message,
                'exchange_timestamp':self.exchange_timestamp,
                'timestamp':self.timestamp,
                'tag':self.tag,
                'fractional':self.fractional,
                'price_at_entry':self.price_at_entry,
                'create_latency':self.create_latency,
                'exec_latency':self.exec_latency,
                'remark':self.remark}
        
    def to_json(self) -> dict:
        qprecision=0
        pprecision=2
        
        exchange_timestamp = str(self.exchange_timestamp) if \
            self.exchange_timestamp is not None else None
        timestamp = str(self.timestamp) if self.timestamp is not None else None
        
        return {
                'algo':self.algo,
                'oid':self.oid,
                'parent_order_id':self.parent_order_id,
                'asset':self.asset.exchange_ticker,
                'user':self.user,
                'placed_by':self.placed_by,
                'product_type':ProductType(self.product_type).name,
                'order_flag':OrderFlag(self.order_flag).name,
                'order_type':OrderType(self.order_type).name,
                'order_validity':OrderValidity(self.order_validity).name,
                'quantity':round(self.quantity,qprecision),
                'filled':round(self.filled,qprecision),
                'pending':round(self.pending,qprecision),
                'disclosed':self.disclosed,
                'price':round(self.price, pprecision),
                'average_price':round(self.average_price,pprecision),
                'trigger_price':round(self.trigger_price,pprecision),
                'stoploss_price':round(self.stoploss_price,pprecision),
                'side':OrderSide(self.side).name,
                'status':AlgoOrderStatus(self.status).name,
                'status_message':self.status_message,
                'exchange_timestamp':exchange_timestamp,
                'timestamp':timestamp,
                'tag':self.tag,
                'fractional':self.fractional,
                'price_at_entry':round(self.price_at_entry,pprecision),
                'create_latency':round(self.create_latency,1),
                'exec_latency':round(self.exec_latency,1,),
                'remark':str(self.remark),}
        
    def is_initialized(self) -> bool:
        """
            The order object must be associated with its own 
            `context` and `data` variables. This is done automatically
            when the ordering function is called with this object 
            as the argument.
        """
        try:
            if self.context and self.data:
                return True
        except Exception:
            pass
        
        return False
        
    @property
    def algo(self):
        """ The algo type name. """
        return self._algo
    
    @algo.setter
    def algo(self, value):
        self._algo = value
    
    @property
    def children(self) -> dict[str, Order]:
        """ 
            Child orders of this algo. Child orders are regular orders 
            which together constitutes the execution of this algo. It 
            is not required and all possible children are created and 
            declared at start, child order can be added as and when 
            required.
        """
        if not self.is_initialized():
            return self._children
        
        children:dict[str, Order] = {}
        algo = self.context.get_algo() # type: ignore
        for oid in self._children:
            o = algo.get_order(oid)
            if o:
                children[oid] = o
        
        self._children = children
        return self._children
        
    @property
    def oid(self) -> str:
        return self.__oid
    
    @property
    def price_at_entry(self) -> float:
        if self.children:
            first_key = next(iter(self.children))
            return self.children[first_key].price_at_entry
        
        return 0
        
    @property
    def exchange_timestamp(self) -> pd.Timestamp:
        if self.children:
            last_key = next(reversed(self.children))
            return self.children[last_key].exchange_timestamp
        return pd.Timestamp(None) # type: ignore
        
    @property
    def timestamp(self) -> pd.Timestamp:
        if self.children:
            first_key = next(iter(self.children))
            return self.children[first_key].timestamp
        return pd.Timestamp(None) # type: ignore
        
    @property
    def create_latency(self) -> float:
        latency = 0
        if self.children:
            for oid in self.children:
                latency += self.children[oid].create_latency
                
        return latency
    
    @property
    def exec_latency(self) -> float:
        latency = 0
        if self.children:
            for oid in self.children:
                latency += self.children[oid].exec_latency
                
        return latency
    
    @property
    def filled(self) -> int|float:
        """
            Returns the sum of the `filled` values of the children. If 
            all child orders are not declared at the start, this will 
            not compute the fill value correctly.
        """
        fill = 0
        if self.children:
            for oid in self.children:
                fill += self.children[oid].filled
                
        return fill
    
    @property
    def average_price(self) -> float:
        px = 0
        qty = 0
        if self.children:
            for oid in self.children:
                order = self.children[oid]
                px += order.average_price*order.filled
                qty += order.filled
                
        if qty > 0:
            px = px/qty
            
        return px
    
    def is_open(self) -> bool:
        """ Is the order still open. """
        return self.status in (AlgoOrderStatus.OPEN, AlgoOrderStatus.CREATED)
    
    def is_buy(self) -> bool:
        """ Is the order a buy order. """
        return self.side == OrderSide.BUY
    
    def is_inactive(self) -> bool:
        if self.is_final():
            return False
        
        return self.status != AlgoOrderStatus.OPEN
        
    def is_done(self) -> bool:
        return self.status == AlgoOrderStatus.COMPLETE
    
    def is_cancelled(self) -> bool:
        return self.status == AlgoOrderStatus.CANCELLED
    
    def is_rejected(self) -> bool:
        return self.status == AlgoOrderStatus.REJECTED
    
    def is_final(self) -> bool:
        return self.status in (
                AlgoOrderStatus.COMPLETE, 
                AlgoOrderStatus.REJECTED, 
                AlgoOrderStatus.CANCELLED,
                AlgoOrderStatus.ERRORED,
                )
    
    def has_failed(self) -> bool:
        return self.status in (
                AlgoOrderStatus.REJECTED, 
                AlgoOrderStatus.CANCELLED,
                AlgoOrderStatus.ERRORED,
                )
        
    @property
    def pending(self) -> float|int:
        return self.quantity - self.filled
        
    @property
    @abstractmethod
    def asset(self) -> Asset:
        """
            Must return a single and valid asset object. Use the 
            asset_factory function to create dummy asset if required 
            (for e.g. for basket orders or pair orders).
        """
        raise NotImplementedError
        
    @property
    @abstractmethod
    def quantity(self) -> int|float:
        """
            Must return a number representing the quantity of the 
            order. The interpretation of quantity, filled and pending
            remains same as regular order. If required, overwrite 
            the `filled` and `pending` properties to make it consistent.
            The default implementation computes filled as sum total 
            of filled from the children orders, and pending as the 
            difference between quantity and filled.
        """
        raise NotImplementedError
        
    @property
    @abstractmethod
    def side(self) -> OrderSide:
        """
            Must defined a OrderSide enum, for either BUY or SELL.
        """
        raise NotImplementedError
    
    @abstractmethod
    def execute(self) -> None:
        """
           This method is invoked when an AlgoOrder object is passed 
           on to the algo's ordering function. It must initiate the 
           execution process.
        """
        raise NotImplementedError
    
    @abstractmethod
    def update(self) -> None:
        """
           This method is invoked periodically by the algo order 
           handler. Update the order `status` (must be a AlgoOrderStatus
           enum) to reflect the current order status.
        """
        raise NotImplementedError
        
    @abstractmethod
    def cancel(self) -> None:
        """
            Implement cancellation behaviour. After a successful 
            cancellation, the status should be marked 
            AlgoOrderStatus.CANCELLED.
        """
        raise NotImplementedError
        
    @abstractmethod
    def reject(self, reason) -> None:
        """
            Implement rejection or error behaviour. After a successful 
            handling, the status should be marked 
            AlgoOrderStatus.REJECTED.
        """
        raise NotImplementedError
    
class IAlgoOrderHandler(ABC):
    """ interface for algo order handler. """
    def __init__(self, algo_instance:TradingAlgorithm):
        pass

    @property
    @abstractmethod
    def orders(self) -> dict[str, IAlgoOrder]:
        raise NotImplementedError
    
    @property
    @abstractmethod
    def open_orders(self) -> dict[str, IAlgoOrder]:
        raise NotImplementedError

    @abstractmethod
    def reset(self):
        raise NotImplementedError
    
    @abstractmethod
    def check_orders(self):
        raise NotImplementedError
    
    @abstractmethod
    def remove_context(self, ctx:str):
        raise NotImplementedError
    
    @abstractmethod
    def place_order(self, order:IAlgoOrder) -> IAlgoOrder:
        raise NotImplementedError
    
    @abstractmethod
    def get_orders(self, ctx) -> dict[str, IAlgoOrder]:
        raise NotImplementedError
    
_algo_order_handler_registry: Dict[str, Type[IAlgoOrderHandler]] = {}
_builtin_algo_order_handler_loader: Callable[[Any], None] | None = None

def register_algo_order_handler(name: str, cls: Type[IAlgoOrderHandler]):
    """Register a new algo order handler type."""
    _algo_order_handler_registry[name] = cls

def set_builtin_algo_order_handler_loader(loader: Callable[[Any], None]):
    """Register a callable that will lazily load built-in algo order handler types."""
    global _builtin_algo_order_handler_loader
    _builtin_algo_order_handler_loader = loader

def _ensure_builtin_algo_order_loaded(handler_type:str|None=None):
    if _builtin_algo_order_handler_loader:
        _builtin_algo_order_handler_loader(handler_type)

    if (handler_type and handler_type not in _algo_order_handler_registry) or handler_type is None:
        try:
            load_plugins('blueshift.plugins.algo_order_handler')
        except Exception:
            pass

def algo_order_handler_factory(handler_type, algo:TradingAlgorithm, *args, **kwargs) -> IAlgoOrderHandler:
    """ factory function to create algo order handler. """
    from inspect import getfullargspec

    if handler_type not in _algo_order_handler_registry:
        _ensure_builtin_algo_order_loaded(handler_type)  # lazy load builtins
    cls = _algo_order_handler_registry.get(handler_type)
    if cls is None:
        raise NotImplementedError(f"Unknown algo order handler type: {handler_type}")
    
    specs = getfullargspec(cls.__init__)

    if specs.varkw:
        kw = kwargs.copy()
    else:
        args_specs = specs.args
        kw = {}
        for key in kwargs:
            if key in args_specs:
                kw[key] = kwargs[key]

    return cls(algo, *args, **kw)

def get_algo_order_handler(algo:TradingAlgorithm, **kwargs) -> IAlgoOrderHandler:
    return algo_order_handler_factory('default', algo, **kwargs)
        
__all__ = [
    'IAlgoOrder',
    'IAlgoOrderHandler',
    'check_initialized',
    'register_algo_order_handler',
    'algo_order_handler_factory',
    'set_builtin_algo_order_handler_loader',
    'get_algo_order_handler',
    ]