import sys
import signal
from signal import Signals
import os
from typing import Callable, cast

CATCHABLE_SIGNALS = set([])

if 'win' in sys.platform:
    err_sig = [Signals.CTRL_C_EVENT,
               Signals.CTRL_BREAK_EVENT,
               Signals.SIGINT,
               Signals.SIGTERM,
               Signals.SIGBREAK,]
    CATCHABLE_SIGNALS = set(err_sig)
else:
    err_sig = [Signals.SIGHUP,
               Signals.SIGINT,
               Signals.SIGQUIT,
               Signals.SIGKILL,
               Signals.SIGXCPU,
               Signals.SIGTERM,
               Signals.SIGSTOP,
               Signals.SIGTSTP]
    CATCHABLE_SIGNALS = set(
            err_sig) - {signal.SIGKILL, signal.SIGSTOP}
    
def set_catchable_signal_handler(handler:Callable) -> dict:
    """ 
        Install handlers for catchable signals. Returns a dict of 
        existing handlers by signum.
    """
    original_handlers = {}
    
    for signum in CATCHABLE_SIGNALS:
        try:
            original_handlers[signum] = signal.signal(signum, handler)
        except Exception:
            pass
        
    return original_handlers

def send_signal_to_process(signum:int, pids:list[int]|int|None=None) -> None:
    """ pass on the signal to target processes. """
    from blueshift.lib.common.functions import listlike

    try:
        import psutil
    except ImportError:
        raise ImportError(f'psutils, which is reqiured for this feature, is not installed.')

    if pids is None:
        pids = []

    if not listlike(pids):
        pids = [cast(int, pids)]
    
    pids = set(pids) # type: ignore
    
    for proc in psutil.process_iter():
        try:
            if proc.pid in pids: # type: ignore
                os.kill(proc.pid, signum) 
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

