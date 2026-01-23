from __future__ import annotations
from typing import TYPE_CHECKING, Type, Dict, Callable, Any
from abc import ABC, abstractmethod

from blueshift.lib.exceptions import InitializationError, ValidationError
from blueshift.lib.common.types import MaxSizedOrderedDict
from blueshift.lib.common.enums import OneclickMethod, OneClickState, OneClickStatus
from blueshift.config.config import ONECLICK_EXPIRY, MAX_ONECLICK_NOTIFICATIONS

from ..plugin_manager import load_plugins

if TYPE_CHECKING:
    import pandas as pd
    from blueshift.interfaces.logger import BlueshiftLogger
    from blueshift.interfaces.context import IContext
    from blueshift.lib.trades._order import Order
    from blueshift.core.utils.environment import TradingEnvironment
    from blueshift.core.algorithm.algorithm import TradingAlgorithm
else:
    import blueshift.lib.common.lazy_pandas as pd

_TERMINAL_STATES = (OneClickState.EXPIRED,
                    OneClickState.COMPLETED,
                    OneClickState.ERRORED,
                    OneClickState.DISCARDED,
                    )

class OneClickObject(object):
    """ Oneclick service payload. """
    __slots__ = ["_id", "_type", "_expiry_time", "_status", "_state", "_order"]

    def __init__(self, id:str, type:OneclickMethod|str, expiry_time:pd.Timestamp, 
                 state:OneClickState|str, order:Order):
        self._id = id

        try:
            self._type = OneclickMethod(type)
        except Exception:
            raise ValidationError(f'illegal oneclick method.')
        
        try:
            if isinstance(state, str):
                state = state.lower()
            self._state = OneClickState(state)
        except Exception:
            raise ValidationError(f'illegal oneclick state.')

        self._expiry_time = pd.Timestamp(expiry_time)
        self._order:Order = order
        self._status:OneClickStatus = OneClickStatus.PROCESSING

    @property
    def id(self) -> str:
        """ ID of the notification """
        return self._id

    @property
    def type(self) -> OneclickMethod:
        """ type of the notification """
        return self._type

    @property
    def expiry_time(self) -> pd.Timestamp:
        """ expiry time of the notification """
        return self._expiry_time

    @property
    def order(self) -> Order:
        """ details of the notification """
        return self._order
    
    @order.setter
    def order(self, value:Order):
        """ details of the notification """
        self._order = value

    @property
    def status(self) -> OneClickStatus:
        """ processing status of the notification """
        return self._status

    @status.setter
    def status(self, value:OneClickStatus|str):
        if self._status == OneClickStatus.PROCESSED:
            return
        
        if isinstance(value, str):
            value = value.lower()

        self._status = OneClickStatus(value)

    @property
    def state(self) -> OneClickState:
        """ processing state of the notification """
        return self._state

    @state.setter
    def state(self, value:OneClickState|str):
        if self._state in _TERMINAL_STATES:
            return
        if isinstance(value, str):
            value = value.lower()

        self._state = OneClickState(value)

    def expire_order(self):
        if self._state in _TERMINAL_STATES:
            return False
        
        self.status = OneClickStatus.PROCESSED
        self.state = OneClickState.EXPIRED
        
        if isinstance(self.order, Order):
            self.order.partial_cancel('Oneclick order expired')
            
        return True
    
    def discard_order(self):
        if self._state in _TERMINAL_STATES:
            return False
        
        self.status = OneClickStatus.PROCESSED
        self.state = OneClickState.DISCARDED
        
        if isinstance(self.order, Order):
            self.order.partial_cancel('Oneclick order discarded')
            
        return True
            
    def complete_order(self, order):
        if self._state in _TERMINAL_STATES:
            return False
        
        self.status = OneClickStatus.PROCESSED
        self.state = OneClickState.COMPLETED
        
        if isinstance(order, Order):
            self.order = order
            
        return True
            
    def reject_order(self, msg='rejected'):
        if self._state in _TERMINAL_STATES:
            return False
        
        self.status = OneClickStatus.PROCESSED
        self.state = OneClickState.ERRORED
            
        if isinstance(self.order, Order):
            self.order.reject(msg)
            
        return True

class IOneClickNotifier(ABC):
    """ abstract interface for user confirmation service """
    
    def __init__(self, config:dict[str, Any], env:TradingEnvironment, *args, **kwargs):
        self._env:TradingEnvironment = env
        if not self._env:
            raise InitializationError('Missing environment.')
            
        self._processing:dict[str, OneClickObject] = {}
        self._processed:dict[str, OneClickObject] = MaxSizedOrderedDict(
                {},max_size=MAX_ONECLICK_NOTIFICATIONS,chunk_size=500)
        self._sent_orders:dict[str, str] = {}
        self._notifications_by_context:dict[IContext,dict[str,OneClickObject]] = {}
        
        self._timeout = float(kwargs.pop('timeout', ONECLICK_EXPIRY))
        self._logger = self._env.logger
        
    @property
    def logger(self) -> BlueshiftLogger:
        return self._logger
    
    @property
    def pending(self):
        self.check_expiry()
        return self._processing.copy()
    
    @property
    def confirmed(self):
        self.check_expiry()
        return self._processed
    
    @property
    def timeout(self) -> float:
        return self._timeout
    
    @timeout.setter
    def timeout(self, value:float):
        self._timeout = value
    
    @abstractmethod
    def check_expiry(self, id_=None):
        raise NotImplementedError
    
    @abstractmethod
    def notify(self, ctx:IContext, method:OneclickMethod, order:Order, name:str, broker_name:str, 
               timeout:float|None=None, **kwargs) -> str:
        raise NotImplementedError
    
    @abstractmethod
    def confirm(self, algo:TradingAlgorithm, packet:dict[str, Any], **kwargs
                ) -> tuple[None, None]|tuple[str, OneClickState]:
        raise NotImplementedError
    
    @abstractmethod
    def cancel(self, *args, **kwargs):
        raise NotImplementedError
    
    @abstractmethod
    def handle_notifications(self, data:list[dict[str, Any]], **kwargs):
        raise NotImplementedError
    
    def get_status(self, id_, ctx=None):
        self.check_expiry(id_)
        obj = None
        if ctx:
            notifications = self._notifications_by_context.get(ctx, {})
            if id_ in notifications:
                obj = notifications[id_]
        else:
            if id_ in self._processing:
                obj = self._processing[id_]
            elif id_ in self._processed:
                obj = self._processed[id_]
        
        if obj:
            return obj.state
    
    def get_open_notifications(self, ctx=None):
        self.check_expiry()
        
        if ctx:
            notifications = self._notifications_by_context.get(ctx, {})
            open_notifications = {k:self._processing[k].order for k \
                                  in notifications \
                                  if k in self._processing}
            return open_notifications
        else:
            return {k:v.order for k,v in self.pending.items()}
    
    def get_open_notifications_by_asset(self, asset, ctx=None):
        open_notifications = self.get_open_notifications(ctx)
        return {k:v for k,v in open_notifications.items() if v.asset == asset}
    
    def get_oneclick_order(self, notification_id, ctx=None):
        self.check_expiry(notification_id)
        if ctx:
            notifications = self._notifications_by_context.get(ctx, {})
            if notification_id in notifications:
                return notifications[notification_id].order
        else:
            if notification_id in self._processing:
                return self._processing[notification_id].order
            
            if notification_id in self._processed:
                return self._processed[notification_id].order
            
    def get_order_id_from_notification_id(self, notification_id:str) -> str|None:
        return self._sent_orders.get(notification_id, None)
    
    def _add_to_context(self, ctx:IContext, id_:str, obj:OneClickObject):
        if ctx in self._notifications_by_context:
            notifications = self._notifications_by_context[ctx]
            notifications[id_] = obj
        else:
            self._notifications_by_context[ctx] = MaxSizedOrderedDict(
                {},max_size=MAX_ONECLICK_NOTIFICATIONS,chunk_size=500)
            self._notifications_by_context[ctx][id_] = obj
            
    def _update_with_ctx(self, id_:str, obj:OneClickObject):
        for ctx in self._notifications_by_context:
            notifications = self._notifications_by_context[ctx]
            if id_ in notifications:
                notifications[id_] = obj
                break


_oneclick_registry: Dict[str, Type[IOneClickNotifier]] = {}
_builtin_oneclick_loader: Callable[[Any], None] | None = None

def register_oneclick(name: str, cls: Type[IOneClickNotifier]):
    """Register a new oneclick notifier type."""
    _oneclick_registry[name] = cls

def set_builtin_oneclick_loader(loader: Callable[[Any], None]):
    """Register a callable that will lazily load built-in oneclick notifier types."""
    global _builtin_oneclick_loader
    _builtin_oneclick_loader = loader

def _ensure_builtin_oneclick_loaded(oneclick_type:str|None=None):
    if _builtin_oneclick_loader:
        _builtin_oneclick_loader(oneclick_type)

    if (oneclick_type and oneclick_type not in _oneclick_registry) or oneclick_type is None:
        try:
            load_plugins('blueshift.plugins.oneclick')
        except Exception:
            pass

def oneclick_factory(oneclick_type, config, env, *args, **kwargs) -> IOneClickNotifier:
    """ factory function to create oneclick notifier. """
    from inspect import getfullargspec

    if oneclick_type not in _oneclick_registry:
        _ensure_builtin_oneclick_loaded(oneclick_type)  # lazy load builtins
    cls = _oneclick_registry.get(oneclick_type)
    if cls is None:
        raise NotImplementedError(f"Unknown oneclick notifier type: {oneclick_type}")
    
    specs = getfullargspec(cls.__init__)

    if specs.varkw:
        kw = kwargs.copy()
    else:
        args_specs = specs.args
        kw = {}
        for key in kwargs:
            if key in args_specs:
                kw[key] = kwargs[key]

    return cls(config, env, *args, **kw)

__all__ = [
    'register_oneclick',
    'set_builtin_oneclick_loader',
    'oneclick_factory',
    'OneClickObject',
    'IOneClickNotifier',
    ]