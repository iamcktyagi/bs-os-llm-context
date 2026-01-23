from __future__ import annotations
from typing import TYPE_CHECKING, cast
import weakref

from blueshift.interfaces.trading._tracker import Tracker
from blueshift.lib.common.enums import AlgoMode
from blueshift.providers.trading.algo_orders import AlgoOrderHandler

if TYPE_CHECKING:
    import pandas as pd
else:
    import blueshift.lib.common.lazy_pandas as pd


_NULL_RISK_REPORT = {
                        'performance':0,
                        'cumulative_returns':0,
                        'volatility':0,
                        'drawdown':0
                    }

    
        
class ExecutionTracker(Tracker):
    """
        Execution performance tracker. This tracker is used for tracking 
        strategies that primarily places algo orders (derived from the 
        `IAlgoOrder` interface). Unlike regular performance tracker, it 
        does not report the performance metrics like pnls and net equities.
        Instead it tracks the algo order (via `AlgoOrderHandler` object 
        of the global context) and checks algo order status. The tracking 
        (`update_metrics`) simply points the current performance to the 
        algo orders beging track by the above handler. Also the `on_error` 
        and `on_cancel` events, if triggered from the blotter, marks all 
        outstanding algo orders as cancelled or rejected.
        
        The performance metrics (`current_performance`) is a dictionary 
        with keys as algo order IDs and values as algo order objects. If 
        the sub-strategy being tracked has not placed any such algo orders,
        it will return an empty dictionary.
    """
    def __init__(self, name:str, mode:AlgoMode, timestamp:pd.Timestamp|None=None, tz:str='Etc/UTC',
                 algo_handler:AlgoOrderHandler|None=None, env=None):
        super(ExecutionTracker, self).__init__(name,mode,timestamp)
        self._algo_handler = weakref.ref(algo_handler) if algo_handler else None
        self._env = env
    
    @property
    def timestamp(self):
        return self._timestamp
    
    @property
    def current_performance(self):
        if not self._algo_handler and self._env:
            self._algo_handler = weakref.ref(self._env.algo._algo_order_handler)
        if self._algo_handler:
            return self._algo_handler().get_orders(self.name) # type: ignore
        
        return {}

    def reset(self, account=None, last_saved=None,
              last_reconciled=None):
        pass

    def roll(self, timestamp):
        if timestamp:
            self.set_timestamp(timestamp)

    def to_dataframe(self):
        return pd.DataFrame(self.current_performance, index=[self._timestamp])

    def to_dataframe_performance(self):
        """ Smart orders do not roll. """
        return self.to_dataframe()

    def _save(self, timestamp):
        pass

    def _read(self, timestamp):
        """ Reading not supported. """
        pass
    
    def on_cancel(self, msg=''):
        """ called via the blotter method. """
        msg = msg or 'cancelled by user'
        perf = self.current_performance
        for oid in perf:
            order = perf[oid]
            if order.is_open():
                perf[oid].cancel(msg)
        
    def on_error(self, msg):
        """ called via the blotter method. """
        perf = self.current_performance
        for oid in perf:
            order = perf[oid]
            if order.is_open():
                perf[oid].reject(msg)
    
    def update_metrics(self, account, orders, positions, timestamp):
        """
            The current performance tracks the algo orders via the algo order 
            handler object of the global context. Therefore update metrics
            does nothing.
        """
        pass
        
    def set_benchmark(self, benchmark):
        """ No benchmark supported for smart-orders. """
        pass
        
    def update_benchmark(self, timestamp=None, benchmark=None):
        """ No benchmark supported for smart-orders. """
        pass
    
    def create_current_performance(self, trackers=None):
        """ execution mode does not support substrategy. """
        return self.current_performance
    
    def create_performance(self, trackers=None):
        """ execution mode does not support substrategy. """
        return self.to_dataframe()
    
    def create_eod_report(self, quick_mode=False, trackers=None):
        """
            No risk report for smart orders.
        """        
        return _NULL_RISK_REPORT