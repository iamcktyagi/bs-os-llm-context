from __future__ import annotations
from typing import TYPE_CHECKING, cast
import weakref

from blueshift.lib.common.enums import AlgoOrderStatus
from blueshift.lib.exceptions import NoContextError, AlgoOrderError
from blueshift.interfaces.trading.algo_orders import (
    IAlgoOrder, IAlgoOrderHandler, register_algo_order_handler)
from blueshift.interfaces.context import IContext

if TYPE_CHECKING:
    from blueshift.core.algorithm.algorithm import TradingAlgorithm
        
class AlgoOrderHandler(IAlgoOrderHandler):
    """
        Algo order handler. The `reset` method is called at before trading
        start event. The `place_order` is called when a new algo order is 
        placed, and `check_orders` is called at every handle data event 
        as well as at trade event for realtime executions.
    """
    def __init__(self, algo_instance:TradingAlgorithm):
        self.algo_instance = weakref.ref(algo_instance)
        self._open_orders:dict[str, dict[str, IAlgoOrder]] = {}
        self._closed_orders:dict[str, dict[str, IAlgoOrder]] = {}
        
    def remove_context(self, ctx:str):
        contexts = list(self._open_orders.keys())
        if ctx in contexts:
            if ctx not in self._closed_orders:
                self._closed_orders[ctx] = {}
                
            orders = self._open_orders.pop(ctx)
            for oid in orders:
                order = orders[oid]
                if not order.is_final():
                    order.reject('context destroyed')
                self._closed_orders[ctx][oid] = order
        
    def reset(self):
        algo_instance = self.algo_instance()

        if algo_instance:
            if algo_instance.is_global_context:
                contexts = list(self._open_orders.keys())
                for ctx in contexts:
                    self._reset_for_context(ctx)
            else:
                ctx = algo_instance.current_context.name
                self._reset_for_context(ctx)
            
    def _reset_for_context(self, ctx:str):
        algo_instance = self.algo_instance()
        if algo_instance is None or ctx not in self._open_orders:
            return
        
        open_orders = self._open_orders.pop(ctx)
        try:
            with algo_instance.context.switch_context(ctx):
                try:
                    for oid in open_orders:
                        order = open_orders[oid]
                        if not order.is_final():
                            open_orders[oid].reject('end of day rejected.')
                        if ctx not in self._closed_orders:
                            self._closed_orders[ctx] = {}
                        self._closed_orders[ctx][oid] = open_orders[oid]
                except Exception as e:
                    algo_instance.handle_error(e, ctx)
        except NoContextError:
            # the context is destroyed
            msg = f'Removing context {ctx} as the context is destroyed'
            self.remove_context(ctx)
            algo_instance.log_info(msg)
        
    def place_order(self, order:IAlgoOrder) -> IAlgoOrder:
        # this is ALWAYS called in the correct context, so no 
        # need for any context switch here.
        algo_instance = self.algo_instance()
        if not algo_instance:
            raise AlgoOrderError(f'cannot place algo order, no running algo found.')
        
        order.context = algo_instance.current_context
        order.data = algo_instance.context.data_portal
        order.execute()
        
        if order.context.name not in self._open_orders:
            self._open_orders[order.context.name] = {}
        self._open_orders[order.context.name][order.oid] = order
        return order
        
    def check_orders(self):
        algo_instance = self.algo_instance()
        if not algo_instance:
            return
        
        contexts = list(self._open_orders.keys())
        for ctx in contexts:
            if ctx not in self._open_orders:
                continue
            
            try:
                with algo_instance.context.switch_context(ctx):
                    try:
                        algos = list(self._open_orders[ctx].keys())
                        for algo_id in algos:
                            algo = self._open_orders[ctx][algo_id]
                            algo.update()
                            
                            if algo.status not in (
                                    AlgoOrderStatus.CREATED, AlgoOrderStatus.OPEN):
                                algo = self._open_orders[ctx].pop(algo_id)
                                if ctx not in self._closed_orders:
                                    self._closed_orders[ctx] = {}
                                self._closed_orders[ctx][algo.oid] = algo
                    except Exception as e:
                        algo_instance.handle_error(e, ctx)
            except NoContextError:
                # the context is destroyed
                msg = f'Removing context {ctx} as the context is destroyed'
                self.remove_context(ctx)
                algo_instance.log_info(msg)
    
    @property
    def orders(self):
        algo_instance = self.algo_instance()
        if not algo_instance:
            return {}
        
        ctx = algo_instance.current_context.name
        open_orders = self._open_orders.get(ctx, {})
        closed_orders = self._closed_orders.get(ctx, {})
        
        return {**open_orders, **closed_orders}
    
    @property
    def open_orders(self):
        algo_instance = self.algo_instance()
        if not algo_instance:
            return {}
        
        ctx = algo_instance.current_context.name
        open_orders = self._open_orders.get(ctx, {})
        return open_orders
    
    def get_orders(self, ctx):
        open_orders = self._open_orders.get(ctx, {})
        closed_orders = self._closed_orders.get(ctx, {})
        
        return {**open_orders, **closed_orders}
    

register_algo_order_handler('default', AlgoOrderHandler)