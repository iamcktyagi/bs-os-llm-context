from __future__ import annotations
from typing import TYPE_CHECKING, Callable
import signal
from functools import partial

from blueshift.lib.common.signal_handlers import set_catchable_signal_handler
from blueshift.config import BLUESHIFT_PRODUCT

if TYPE_CHECKING:
    from .alert import BlueshiftAlertManager

class BlueshiftInterruptHandler:
    """
        An interrup handler (in context manager style) for catching `SIGINT`
        and `SIGKILL`. In both cases the signals are honoured after running 
        callbacks registered with alert manager. In case of `SIGINT`, we 
        exit with status 0 (or 1 if any of the callbacks fails). In case of 
        SIGKILL we exit with status 1 in either case.
        
        Note:
            See also: :mod:`blueshift.core.alerts.alert`.
        
        Args:
            ``algo (TradingAlgorithm)``: The algo instance
            
            ``alert_manager (object)``: The alert manager for the run.
            
    """

    def __init__(self, alert_manager:BlueshiftAlertManager, handler:Callable|None=None):
        self.name = alert_manager.name
        self.alert_manager = alert_manager
        self.mode = alert_manager.mode
        self.originals = {}
        
        if handler:
            self.handler_func = handler
        else:
            self.handler_func = partial(self.alert_manager.graceful_exit, None)

    def __enter__(self):
        self.interrupted = False
        self.terminated = False
        self.released = False
        
        self.originals = set_catchable_signal_handler(self.handler)
        
        return self

    def __exit__(self, type, value, tb):
        self._release()

    def _release(self):
        if self.released:
            return False
        
        if self.originals:
            for signum in self.originals:
                try:
                    signal.signal(signum, self.originals[signum])
                except Exception:
                    pass
        
        self.originals = {}
        self.released = True
        return True
    
    def handler(self, signum, frame):
        """
            Singnal handler. It invokes the alert_manager ``shutdown_algo`` 
            method for handling ``SIGINT`` and ``graceful_exit`` for 
            ``SIGKILL``.
        """
        # this runs in the main thread! This should not trigger any 
        # exceptions
        if signum == signal.SIGINT:
            self.interrupted = True
            msg = f"Interrupt received, {BLUESHIFT_PRODUCT} will shut down..."
            self.alert_manager.logger.warn(msg,f"{BLUESHIFT_PRODUCT}")
        elif signum == signal.SIGTERM:
            self.terminated = True
            msg = f"Algo termination signal received, {BLUESHIFT_PRODUCT} will shut down..."
            self.alert_manager.logger.warn(msg,f"{BLUESHIFT_PRODUCT}")
        else:
            msg = f'Received signal {signum}, will ignore.'
            self.alert_manager.logger.warn(msg)
            
        self.handler_func(msg=msg)
        
            
            
