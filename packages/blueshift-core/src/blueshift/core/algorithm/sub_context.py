from __future__ import annotations
from typing import TYPE_CHECKING
import numbers
import pandas as pd

from blueshift.lib.common.enums import AlgoMode
from blueshift.lib.common.platform import sizeof
from blueshift.lib.common.types import BlueshiftPositionDict, BlueshiftOrderDict
from blueshift.lib.exceptions import RecordVarError, ContextError
from blueshift.interfaces.context import IContext, IAccount, IPortfolio
from blueshift.config.config import MAX_CTX_RECORD_VARS, MAX_CTX_STATE_VARS

if TYPE_CHECKING:
    import pandas as pd
    from blueshift.lib.common.enums import ExecutionMode, AlgoMode
    from blueshift.calendar.trading_calendar import TradingCalendar
    from blueshift.lib.clocks._clock import TradingClock
    from blueshift.interfaces.assets._assets import Asset
    from blueshift.lib.trades._position import Position
    from blueshift.lib.trades._order import Order
    from blueshift.interfaces.trading.blotter import IBlotter
    from blueshift.interfaces.trading.broker import IBroker
    from blueshift.interfaces.assets.assets import IAssetFinder
    from blueshift.interfaces.data.data_portal import DataPortal
    from .context import AlgoWrapper, AlgoContext
    from blueshift.core.utils.environment import TradingEnvironment
else:
    import blueshift.lib.common.lazy_pandas as pd

class SubContext(IContext):
    '''
        SubContext objects are created dynamically for each 
        sub-strategy launched in the main Algorithm event loop. 
        It has its own SubBlotter and associated trackers. The 
        SubContext object does not directly control or updates its
        underlying blotter. The blotter object of the parent 
        context must do it. This object just provide the standard 
        context attributes.
        
        Note:
            SubContext objects should not be created directly 
            by user. It is created by the global AlgoContext 
            object as and when required.
        
        Args:
            ``name (str)``: Name of the sub-context
            
            ``blotter (object)``: A sub-blotter object.
            
            ``parent (object)``: The global AlgoContext object.
    '''

    def __init__(self, name:str, algo_id:str|None, blotter:IBlotter, parent:AlgoContext, 
                 env:TradingEnvironment):
        self.__name = name
        self.__algo_id = algo_id
        self.__blotter = blotter
        self.__parent = parent
        self._env = env
        self.__record_vars = pd.DataFrame()
        self.__record_state_vars:set[str] = set()
        self._protected_members:list[str] = []
        
        # protect internal variables from accidental overwrites
        # within the user strategy code.
        for m in dir(self):
            if m.startswith('__') or m.startswith('_Algo') \
                or m=='timestamp':
                # dunder variables are excluded
                continue
            self._protected_members.append(m)

    def __str__(self):
        return "Blueshift SubContext [name:%s, broker:%s]" % (self.name,
                                                self.broker)

    def __repr__(self):
        return self.__str__()
    
    def __setattr__(self, key, value):
        if key == 'timestamp' or key == '_SubContext__timestamp':
            return super().__setattr__(key, value)
        
        if hasattr(self, key) and key in self._protected_members:
            raise AttributeError(
                    f'protected attribute {key}, cannot overwrite.')
            
        return super().__setattr__(key, value)
    
    @property
    def parent(self) -> AlgoContext:
        return self.__parent
    
    @property
    def name(self) -> str:
        return self.__name
    
    def get_algo(self) ->AlgoWrapper|None:
        return self.parent.get_algo()
    
    @property
    def algo_id(self):
        return self.__algo_id
    
    @property
    def mode(self) -> AlgoMode:
        return self.parent.mode
    
    @property
    def execution_mode(self) -> ExecutionMode:
        return self.parent.execution_mode

    @property
    def blotter(self) -> IBlotter:
        return self.__blotter

    @property
    def broker(self) -> IBroker:
        return self.__blotter.broker
    
    @property
    def broker_name(self) -> str:
        return self.broker.name
    
    @property
    def notifier(self):
        return self.parent.notifier
    
    @property
    def asset_finder(self) -> IAssetFinder:
        return self.parent.asset_finder
    
    @property
    def data_portal(self) -> DataPortal:
        return self.parent.data_portal

    @property
    def account(self) -> IAccount:
        return IAccount(self.blotter.account.to_dict())

    @property
    def blotter_account(self) -> dict:
        return self.blotter.account.to_dict()

    @property
    def broker_account(self) -> dict:
        try:
            return self.broker.account
        except Exception:
            acct = self.broker.get_account(
                    algo_name=self._env.name,
                    algo_user=self._env.algo_user,
                    logger=self._env.logger)
            return acct.to_dict()

    @property
    def orders(self) -> dict[str, Order]:
        return BlueshiftOrderDict(self.__blotter.orders)

    @property
    def broker_orders(self) -> dict[str, Order]:
        return BlueshiftOrderDict(
                self.broker.get_all_orders(
                algo_name=self._env.name, 
                algo_user=self._env.algo_user,
                logger=self._env.logger))

    @property
    def open_orders(self) -> dict[str, Order]:
        return BlueshiftOrderDict(self.__blotter.open_orders)
    
    @property
    def is_disconnected(self) -> bool:
        return self.parent.is_disconnected
    
    def open_orders_by_asset(self, asset) -> dict[str, Order]:
        mayberolling = {}
        if self.parent.mode == AlgoMode.BACKTEST:
            orders = self.__blotter.open_orders_by_asset(asset) # type: ignore
            mayberolling = {o.oid:o for o in orders}
        orders = self.__blotter.open_orders
        orders = {k:v for k,v in orders.items() if v.asset == asset}
        return BlueshiftOrderDict({**mayberolling, **orders})

    @property
    def broker_open_orders(self) -> dict[str, Order]:
        orders = self.broker.get_all_orders(
                    algo_name=self._env.name, 
                    algo_user=self._env.algo_user,
                    logger=self._env.logger)
        return BlueshiftOrderDict(
                {k:v for k,v in orders.items() if not v.is_final()})

    @property
    def portfolio(self) -> IPortfolio:
        return IPortfolio(self.__blotter.account.to_dict(),
                          self.blotter_portfolio,
                          self.__blotter.performance,
                          self._env.start_dt,
                          self._env.end_dt)

    @property
    def blotter_portfolio(self) -> dict[Asset, Position]:
        return BlueshiftPositionDict(self.__blotter.portfolio)

    @property
    def broker_portfolio(self) -> dict[Asset, Position]:
        positions = self.broker.get_positions(
                    algo_name=self._env.name,
                    algo_user=self._env.algo_user,
                    logger=self._env.logger)
        return BlueshiftPositionDict(positions)

    @property
    def performance(self) -> dict:
        perf = self.__blotter.performance
        if self.timestamp:
            perf['timestamp'] = self.timestamp
        return perf

    @property
    def risk_report(self) -> dict:
        return self.__blotter.risk_report

    @property
    def trading_calendar(self) -> TradingCalendar:
        return self.parent.trading_calendar
    
    @property
    def broker_calendar(self) -> TradingCalendar:
        return self.broker.calendar

    @property
    def pnls(self) -> pd.DataFrame:
        return self.__blotter.pnls

    @property
    def timestamp(self) -> pd.Timestamp|None:
        return self.parent.timestamp

    @property
    def clock(self) -> TradingClock:
        return self.parent.clock

    @property
    def record_vars(self) -> pd.DataFrame:
        return self.__record_vars
    
    @property
    def is_initialized(self) -> bool:
        """ sub-contexts are always initialized. """
        return True
    
    @property
    def is_restart(self) -> bool:
        """ If this is a restart. """
        return self.parent.is_restart
    
    def reset(self, *args, **kwargs):
        raise ContextError(
                f'Context reset not allowed within a subcontext.')
        
    def record_state(self, varname:str):
        varname = str(varname)
        if not varname.isidentifier() or varname.startswith('_'):
            return
        
        if len(self.__record_state_vars) > MAX_CTX_STATE_VARS:
            msg = f'you can add maximum {MAX_CTX_RECORD_VARS} state variables.'
            raise RecordVarError(msg=msg)
            
        self.__record_state_vars.add(varname)
        
    def load_state(self):
        states = self.parent._load_state(self.name)
        states = states or {}
            
        for name, value in states.items():
            setattr(self, name, value)
            self.record_state(name)
    
    @property
    def state(self):
        out = {}
        for name in self.__record_state_vars:
            if name.startswith('_'):
                continue
            try:
                value = getattr(self, name, None)
                # assert isinstance(value, str) or isinstance(value, bool) or \
                #     isinstance(value, numbers.Number)
                if not (
                    isinstance(value, str) 
                    or isinstance(value, bool) 
                    or isinstance(value, numbers.Number) 
                    or isinstance(value, dict) 
                    or isinstance(value, list) 
                    or isinstance(value, set)):
                    raise ValueError('illegal state data type')
                if isinstance(value, str):
                    #assert len(value) <= 128
                    if len(value) > 128:
                        raise ValueError(f'state size too large')
                if isinstance(value, dict) or isinstance(value, list) or \
                    isinstance(value, set):
                    if sizeof(value) > 51200:
                        raise ValueError(f'state size too large')
            except Exception:
                continue
            out[str(name)] = value
            
        return out

    def record(self, varname:str, value):
        dt = self.timestamp
        if not dt:
            return
        
        if len(self.__record_vars.columns) > MAX_CTX_RECORD_VARS:
            msg = f'you can add maximum {MAX_CTX_RECORD_VARS} record variables.'
            raise RecordVarError(msg=msg)

        try:
            varname = str(varname)
            value = float(value)
            self.__record_vars.loc[pd.to_datetime(dt.date()),varname] =\
                    value
        except (TypeError, ValueError):
            msg = "record variable names must be string-like "
            msg = msg + "and values float-like"
            raise RecordVarError(msg=msg)
            
    def get_record_dict(self, dt:pd.Timestamp):
        loc = pd.to_datetime(dt.date())
        
        if not loc in self.__record_vars.index:
            return {}
        
        return self.__record_vars.loc[loc,:].to_dict()