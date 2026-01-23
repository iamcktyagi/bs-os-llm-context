from __future__ import annotations
from typing import Callable
import weakref
from weakref import ReferenceType

from blueshift.lib.common.functions import  listlike
from blueshift.lib.exceptions import InternalError
from blueshift.lib.common.ctx_mgrs import TimeoutRLock
from blueshift.config import LOCK_TIMEOUT
from blueshift.interfaces.context import IContext

class OrderTrackerHandle:
    """
        handle that can be used to cancel a order tracking event.
    """
    __slots__ = ['tracker','cb','ctx', 'oids', 'handle', 'on_timeout','cancelled','triggered']
    
    def __init__(self, tracker:OrderTracker, order_ids:set[str]|list[str], cb:Callable, 
                 ctx:str, on_timeout:Callable):
        self.cb = cb
        self.ctx = ctx
        self.oids = set(order_ids).copy()
        self.tracker = weakref.ref(tracker)
        self.on_timeout = on_timeout
        self.cancelled = False
        self.triggered = False
        self.handle = None
        
    def __str__(self):
        return f'OrderCallbackHanfle[{id(self)}]'
    
    def __repr__(self):
        return self.__str__()
        
    def trigger(self, context:IContext):
        if self.cancelled or self.triggered:
            return
        
        self.cb(context, list(self.oids))
        self.triggered = True
        
    def timeout(self, context:IContext):
        if self.cancelled or self.triggered:
            return
        
        self.on_timeout(context, self.oids.copy())
        self.triggered = True
        
    def cancel(self):
        if self.cancelled or self.triggered:
            return
        
        try:
            target = self.tracker()
            if target:
                target.remove(self)
            self.cancelled = True
        except Exception as e:
            msg = f'Failed to cancel event: {str(e)}.'
            raise InternalError(msg)
        
    def update_handle(self, handle):
        if self.cancelled or self.triggered:
            return
        
        if handle:
            self.handle = handle

class OrderTracker:
    """
        Track order execution to invoke callback on order completions.
    """
    def __init__(self):
        self.callback_mapping:dict[OrderTrackerHandle,set[str]] = {}
        self._lock = TimeoutRLock(timeout=LOCK_TIMEOUT)
        
    def register(self, order_ids, callback:Callable, ctx, on_timeout) -> OrderTrackerHandle|None:
        if not order_ids:
            return
        
        if not listlike(order_ids):
            order_ids = [order_ids]
        
        order_ids = set([str(oid) for oid in order_ids])
        h = OrderTrackerHandle(self, order_ids, callback, ctx, on_timeout)
        
        with self._lock:
            self.callback_mapping[h] = order_ids
            
            return h
                
    def remove(self, handle:OrderTrackerHandle) -> set[str]|None:
        with self._lock:
            return self.callback_mapping.pop(handle, None)
                
    def trigger(self, order_ids) -> list[OrderTrackerHandle]:
        if not self.callback_mapping:
            return []
        
        if not order_ids:
            return []
        
        if not listlike(order_ids):
            order_ids = [order_ids]
            
        order_ids = set([str(oid) for oid in order_ids])
        done = []
        
        with self._lock:
            handles = list(self.callback_mapping.keys())
            
            for h in handles:
                self.callback_mapping[h] -= order_ids
                if not self.callback_mapping[h]:
                    done.append(h)
                    self.callback_mapping.pop(h, None)
                    
            return done
                
        
        
        