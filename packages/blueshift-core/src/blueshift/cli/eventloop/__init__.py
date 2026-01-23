from __future__ import annotations
from blueshift.lib.common.enums import AlgoMode

from .eventloop import BusyLoop, SingleJobBusyLoop, AsyncLoop, ICommandLoop

def eventloop_factory(name:str, quick:bool=False, *args, **kwargs) -> ICommandLoop:
    mode = kwargs.get('mode', AlgoMode.BACKTEST)
    mode = AlgoMode(mode)

    kwargs['mode'] = mode
    
    if mode == AlgoMode.BACKTEST:
        if quick:
            return SingleJobBusyLoop(name, *args, **kwargs)
        else:
            return BusyLoop(name, *args, **kwargs)
    else:
        return AsyncLoop(name, *args, **kwargs)