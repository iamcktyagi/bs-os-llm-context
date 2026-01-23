from __future__ import annotations
from typing import TYPE_CHECKING
from abc import ABC, abstractmethod

from blueshift.lib.common.enums import AlgoMode, ExecutionMode
from blueshift.interfaces.trading.blotter import IBlotter
from blueshift.interfaces.trading.broker import IBroker
from blueshift.interfaces.assets.assets import IAssetFinder, Asset
from blueshift.interfaces.data.data_portal import DataPortal
from blueshift.calendar.trading_calendar import TradingCalendar
from blueshift.lib.clocks._clock import TradingClock
from blueshift.lib.trades._order import Order
from blueshift.lib.trades._position import Position
from blueshift.lib.trades._accounts import BlotterAccount, Account

if TYPE_CHECKING:
    import pandas as pd

class IContext(ABC):
    '''The algorithm context abstract interface. '''
    @property
    def parent(self) -> IContext|None:
        return self
    
    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError
        
    def get_algo(self):
        raise NotImplementedError
    
    @property
    @abstractmethod
    def mode(self) -> AlgoMode:
        raise NotImplementedError
    
    @property
    @abstractmethod
    def execution_mode(self) ->ExecutionMode:
        raise NotImplementedError

    @property
    @abstractmethod
    def blotter(self) -> IBlotter:
        raise NotImplementedError

    @property
    @abstractmethod
    def broker(self) -> IBroker:
        raise NotImplementedError
    
    @property
    @abstractmethod
    def broker_name(self) -> str:
        raise NotImplementedError
    
    @property
    @abstractmethod
    def notifier(self):
        raise NotImplementedError
    
    @property
    @abstractmethod
    def asset_finder(self) -> IAssetFinder:
        raise NotImplementedError
    
    @property
    @abstractmethod
    def data_portal(self) -> DataPortal:
        raise NotImplementedError

    @property
    @abstractmethod
    def account(self) -> IAccount:
        raise NotImplementedError

    @property
    @abstractmethod
    def blotter_account(self) -> dict:
        raise NotImplementedError

    @property
    @abstractmethod
    def broker_account(self) -> dict:
        raise NotImplementedError

    @property
    @abstractmethod
    def orders(self) -> dict[str, Order]:
        raise NotImplementedError

    @property
    @abstractmethod
    def broker_orders(self) -> dict[str, Order]:
        raise NotImplementedError

    @property
    @abstractmethod
    def open_orders(self) -> dict[str, Order]:
        raise NotImplementedError

    @property
    @abstractmethod
    def broker_open_orders(self) -> dict[str, Order]:
        raise NotImplementedError

    @property
    @abstractmethod
    def portfolio(self) -> IPortfolio:
        raise NotImplementedError

    @property
    @abstractmethod
    def blotter_portfolio(self) -> dict[Asset, Position]:
        raise NotImplementedError

    @property
    @abstractmethod
    def broker_portfolio(self) -> dict[Asset, Position]:
        raise NotImplementedError

    @property
    @abstractmethod
    def performance(self) -> dict:
        raise NotImplementedError

    @property
    @abstractmethod
    def risk_report(self) -> dict:
        raise NotImplementedError

    @property
    @abstractmethod
    def trading_calendar(self) -> TradingCalendar:
        raise NotImplementedError
    
    @property
    @abstractmethod
    def broker_calendar(self) -> TradingCalendar:
        raise NotImplementedError

    @property
    @abstractmethod
    def pnls(self) -> pd.DataFrame:
        raise NotImplementedError

    @property
    @abstractmethod
    def timestamp(self) -> pd.Timestamp|None:
        raise NotImplementedError

    @property
    @abstractmethod
    def clock(self) -> TradingClock:
        """ the algo clock object. """
        raise NotImplementedError

    @property
    @abstractmethod
    def record_vars(self):
        raise NotImplementedError
    
    @property
    @abstractmethod
    def state(self) -> dict:
        raise NotImplementedError
        
    @property
    @abstractmethod
    def is_initialized(self) -> bool:
       raise NotImplementedError
    
    @property
    @abstractmethod
    def algo_id(self) -> str|None:
        raise NotImplementedError
    
    @abstractmethod
    def open_orders_by_asset(self, asset) -> dict[str, Order]:
        raise NotImplementedError

    def save(self, timestamp:pd.Timestamp):
        pass

    def read(self, timestamp:pd.Timestamp):
        pass
    
    def reset(self, *args, **kwargs):
        pass

    def finalize(self, timestamp:pd.Timestamp):
        pass
        
    def switch_context(self, name):
        raise NotImplementedError
    
    def record(self, varname:str, value):
        raise NotImplementedError
    
    def record_state(self, varname:str):
        raise NotImplementedError
    
    def load_state(self):
        raise NotImplementedError
    

class IPortfolio:
    """
        Portfolio view of the current algorithm state (maintained by
        the blotter).
    """

    __slots__ = ['_account','_performance','_portfolio','_start_dt','_end_dt']

    def __init__(self, account:dict, portfolio:dict[Asset, Position], performance:dict, 
                 start_dt:pd.Timestamp|None=None, end_dt:pd.Timestamp|None=None):
        self._start_dt = start_dt
        self._end_dt = end_dt
        self._account = account
        self._performance = performance
        self._portfolio = portfolio

    def __str__(self):
        return "Blueshift Portfolio"

    def __repr__(self):
        return self.__str__()

    @property
    def portfolio_value(self) -> float:
        return self._account["net"]

    @property
    def positions_exposure(self) -> float:
        return self._account["gross_exposure"]

    @property
    def cash(self) -> float:
        return self._account["cash"]

    @property
    def starting_cash(self) -> float:
        return self._account["starting_cash"]

    @property
    def positions_value(self) -> float:
        return self._account["holdings"]

    @property
    def pnl(self) -> float:
        return self._account["pnls"]
    
    @property
    def pnls(self) -> float:
        return self._account["pnls"]
    
    @property
    def mtm(self) -> float:
        return self._account["mtm"]

    @property
    def positions(self) -> dict[Asset, Position]:
        return self._portfolio

    @property
    def returns(self) -> pd.Series:
        return self._performance["algo_cum_returns"]

    @property
    def start_date(self) -> pd.Timestamp|None:
        return self._start_dt

class IAccount:
    """
        Account view of the current algorithm state (maintained by
        the blotter).
    """

    __slots__ = ['_account']

    def __init__(self, account:dict):
        self._account = account

    def __str__(self):
        return "Blueshift Account"

    def __repr__(self):
        return self.__str__()

    @property
    def leverage(self) -> float:
        return self._account["gross_leverage"]

    @property
    def gross_leverage(self) -> float:
        return self._account["gross_leverage"]

    @property
    def available_funds(self) -> float:
        return self._account["cash"]

    @property
    def margin(self) -> float:
        return self._account["margin"]

    @property
    def net_liquidation(self) -> float:
        return self._account["liquid_value"]

    @property
    def net_leverage(self) -> float:
        return self._account["net_leverage"]

    @property
    def total_positions_value(self) -> float:
        return self._account["holdings"]

    @property
    def total_positions_exposure(self) -> float:
        return self._account["gross_exposure"]

    @property
    def net_exposure(self) -> float:
        return self._account["net_exposure"]

    @property
    def gross_exposure(self) -> float:
        return self._account["gross_exposure"]

    @property
    def long_exposure(self) -> float:
        return self._account["long_exposure"]

    @property
    def short_exposure(self) -> float:
        return self._account["short_exposure"]

    @property
    def long_count(self) -> int:
        return self._account["long_count"]

    @property
    def short_count(self) -> int:
        return self._account["short_count"]

    @property
    def commissions(self) -> float:
        return self._account["commissions"]

    @property
    def charges(self) -> float:
        return self._account["charges"]

    @property
    def accrued_interest(self) -> float:
        # TODO: add support for accrued interest
        return 0

    @property
    def buying_power(self) -> float:
        # TODO: add support for buying power
        return float('inf')

    @property
    def day_trades_remaining(self) -> float:
        # TODO: add support for trades remaining
        return float('inf')
