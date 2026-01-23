from __future__ import annotations
from typing import Any, Callable, Dict, TYPE_CHECKING, Type

from abc import ABC, abstractmethod
from enum import Enum
import numpy as np

from blueshift.lib.common.sentinels import noop


if TYPE_CHECKING:
    from blueshift.core.algorithm.algorithm import TradingAlgorithm
    from blueshift.interfaces.assets._assets import Asset
    from blueshift.interfaces.context import IContext

class ExitMethod(Enum):
    PRICE = 0
    MOVE = 1
    PERCENT = 2
    AMOUNT = 3
    PNL = 4

class IExitHandler(ABC):
    """ interface for stoploss and takeprofit handler. """
    def __init__(self, algo:TradingAlgorithm, cooloff_period:float=30):
        pass

    @abstractmethod
    def set_cooloff_period(self, value:float):raise NotImplementedError
    
    @abstractmethod
    def get_cooloff_period(self) -> float:raise NotImplementedError
    
    @abstractmethod
    def reset(self, roll:bool=True):raise NotImplementedError

    @abstractmethod
    def validate_cooloff(self, asset:Asset|None=None):raise NotImplementedError

    @abstractmethod
    def validate_squareoff(self, asset:Asset|None=None):raise NotImplementedError

    @abstractmethod
    def remove_context(self, ctx:IContext):raise NotImplementedError

    @abstractmethod
    def reset_cooloff(self, context:IContext, asset:Asset|None):raise NotImplementedError

    @abstractmethod
    def reset_stoploss(self, context:IContext, asset:Asset|None):raise NotImplementedError

    @abstractmethod
    def reset_takeprofit(self, context:IContext, asset:Asset|None):raise NotImplementedError

    @abstractmethod
    def add_stoploss(self, context:IContext, asset:Asset|None, method:ExitMethod, target:float, 
                       callback:Callable=noop, trailing:float=0, do_exit:bool=True, 
                       entry_level:float=np.nan, rolling:bool=False):
        raise NotImplementedError
    
    @abstractmethod
    def add_takeprofit(self, context:IContext, asset:Asset|None, method:ExitMethod, target:float, 
                       callback:Callable=noop, do_exit:bool=True, entry_level:float=np.nan, 
                       rolling:bool=False):
        raise NotImplementedError

    @abstractmethod
    def get_stoploss(self, context:IContext, asset:Asset|None):raise NotImplementedError

    @abstractmethod
    def get_takeprofit(self, context:IContext, asset:Asset|None):raise NotImplementedError

    @abstractmethod
    def copy_stoploss(self, source:Asset|None, target:Asset|None):raise NotImplementedError

    @abstractmethod
    def copy_takeprofit(self, source:Asset|None, target:Asset|None):raise NotImplementedError

    @abstractmethod
    def set_cooloff(self, assets:Asset|list[Asset]|None=None, squareoff:bool=False, 
                    is_global:bool=False):
        raise NotImplementedError

    @abstractmethod
    def is_empty(self, context:IContext|None=None) -> bool:raise NotImplementedError

    @abstractmethod
    def check_exits(self, reset:bool=False):raise NotImplementedError