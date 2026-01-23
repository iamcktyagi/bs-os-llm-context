from __future__ import annotations
from typing import TYPE_CHECKING, Iterator
import numbers
from contextlib import contextmanager
from collections import namedtuple, deque
from sys import exc_info
from os import path as os_path
import json
import weakref

from blueshift.lib.exceptions import (
        InitializationError, ValidationError, ContextError, NoContextError,
        RecordVarError)
from blueshift.lib.common.enums import ExecutionMode, AlgoMode
from blueshift.lib.common.types import BlueshiftPositionDict, BlueshiftOrderDict
from blueshift.lib.common.platform import sizeof
from blueshift.lib.clocks._clock import TradingClock
from blueshift.lib.serialize.json import BlueshiftJSONEncoder
from blueshift.calendar.trading_calendar import TradingCalendar
from blueshift.config.defaults import get_config_oneclick
from blueshift.config.config import MAX_CTX_RECORD_VARS, MAX_CTX_STATE_VARS
from blueshift.config import blueshift_run_path

from blueshift.interfaces.trading.broker import IBroker
from blueshift.interfaces.assets.assets import IAssetFinder
from blueshift.interfaces.data.data_portal import DataPortal
from blueshift.interfaces.trading.blotter import blotter_factory
from blueshift.interfaces.context import IContext, IAccount, IPortfolio
from blueshift.interfaces.trading.oneclick import oneclick_factory

from blueshift.providers import *

from .sub_context import SubContext

if TYPE_CHECKING:
    import pandas as pd
    import datetime
    from blueshift.core.algorithm.algorithm import TradingAlgorithm
    from blueshift.interfaces.trading.blotter import IBlotter
    from blueshift.core.utils.environment import TradingEnvironment
    from blueshift.interfaces.trading.algo_orders import IAlgoOrderHandler
    from blueshift.interfaces.assets._assets import Asset
    from blueshift.lib.trades._position import Position
    from blueshift.lib.trades._order import Order
    from .strategy import Strategy
else:
    import blueshift.lib.common.lazy_pandas as pd

ContextEntry = namedtuple('ContextEntry', ['ctx','err_ctx'])

class AlgoWrapper:
    def __init__(self, algo:TradingAlgorithm):
        self.__algo = weakref.ref(algo)
        
    def __getattr__(self, name):
        msg = f"TradingAlgorithm has no attribute '{name}'."
        
        try:
            attr = getattr(self.__algo(), name)
        except Exception:
            raise AttributeError(msg)
        
        if hasattr(attr, 'is_api'):
            return attr
        
        raise AttributeError(msg)

class AlgoContext(IContext):
    '''
        The algorithm context encapsulates the context of a running 
        algorithm. This includes tracking internal objects like 
        blotter, broker interface etc, as well as account, portfolio
        and positions details (see below). The context object is also useful 
        to store user-defined variables for access anywhere in the 
        strategy code.
        
        .. warning::
            Once the context is initialised, its core attributes (i.e.
            non-user defined attributes) are read-only. Attempting to 
            overwrite them will throw ``AttributeError`` and will 
            crash the algo.
            
        .. seealso:: :ref:`create and use variables<How to create and use variables>`
    '''

    def __init__(self, env:TradingEnvironment, asset_finder:IAssetFinder|None=None, 
                 data_portal:DataPortal|None=None, 
                 calendar:TradingCalendar|None=None):
        # name of the context. Good practice is to match the algo name
        # this will be used to tag orders where supported
        self.__protected_members:list[str] = []
        self._name = env.name
        self._mode = env.mode
        self._execution_mode = env.execution_mode
        self.__algo_id = env.algo_id
        self._env = env
        self.__finalized:bool = False

        self.__timestamp:pd.Timestamp|None = None
        self.__record_vars = pd.DataFrame()
        self.__record_state_vars:set = set()
        self.__loaded_state:dict = {}

        # get the broker object and mark initialize
        self.__broker_initialized:bool = False
        self.__children:dict[str, SubContext] = {}
        self.__context_stack = deque([])
        self.__algo = None
        
        calendar = calendar or env.trading_calendar
        self._reset_broker_tuple(
                broker=env.broker, clock=env.clock, asset_finder=asset_finder, 
                data_portal=data_portal, calendar=calendar)

        # object to track transactions, performance and positions
        self.__tracker_initialized = False
        self._reset_trackers()
        
        # now setup oneclick handler
        one_click_config = get_config_oneclick()
        if self._execution_mode == ExecutionMode.ONECLICK and \
            not one_click_config['enabled']:
            msg = "Execution mode not enabled. Update config and retry."
            raise InitializationError(msg=msg)
        
        if self._execution_mode == ExecutionMode.ONECLICK:
            one_click_config['tz'] = self.__calendar.tz
            method = one_click_config.get('method','default')
            self.__notifier = oneclick_factory(method, one_click_config, env=self._env)
        else:
            self.__notifier = None
            
        self.protect_attrs()

    def __str__(self) -> str:
        return "Blueshift Context [name:%s, broker:%s]" % (self.name,
                                                self.broker)

    def __repr__(self):
        return self.__str__()
    
    def __setattr__(self, key, value):
        if key == 'timestamp' or key == '_AlgoContext__timestamp':
            return super().__setattr__(key, value)
        
        if hasattr(self, key) and key in self.__protected_members:
            raise AttributeError(
                    f'protected attribute, cannot overwrite.')
            
        return super().__setattr__(key, value)
    
    def protect_attrs(self):
        # protect internal variables from accidental overwrites
        # within the user strategy code.
        for m in dir(self):
            if m.startswith('__') or m.startswith('_Algo') \
                or m=='timestamp':
                # dunder variables are excluded
                continue
            self.__protected_members.append(m)

    @property
    def name(self) -> str:
        """ return the name (``str``) of the current algo run. """
        return self._name
    
    def get_algo(self) -> AlgoWrapper|None:
        """ return the reference to the current algo. """
        if self.__algo:
            return self.__algo
        
        if not self._env.algo:
            return
        
        self.__algo = AlgoWrapper(self._env.algo)
        return self.__algo
    
    @property
    def children(self) -> dict[str, SubContext]:
        return self.__children
    
    @property
    def algo_id(self) -> str|None:
        return self.__algo_id
    
    @property
    def mode(self) -> AlgoMode:
        """ 
            return the run mode (``enum``) of the current run. 
            
            .. seealso::
                see :ref:`Algo Modes and Other Constants` for 
                allowed values and interpretation.
        """
        return self._mode
    
    @property
    def execution_mode(self) -> ExecutionMode:
        """ 
            return the execution mode (``enum``) of the current run. 
            
            .. seealso::
                see :ref:`Algo Modes and Other Constants` for 
                allowed values and interpretation.
        """
        return self._execution_mode

    @property
    def blotter(self) -> IBlotter:
        """ return the blotter object of the current run. """
        return self.__blotter

    @property
    def broker(self) -> IBroker:
        """ return the current broker/ execution platform object. """
        return self.__broker
    
    @property
    def broker_name(self) -> str:
        """ return the name of the current broker. """
        return self.__broker.name
    
    @property
    def notifier(self):
        return self.__notifier
    
    @property
    def algo_order_handler(self) -> IAlgoOrderHandler|None:
        if self._env and self._env.algo:
            return self._env.algo._algo_order_handler
    
    @property
    def asset_finder(self) -> IAssetFinder:
        """ object implementing `AssetFinder` interface. """
        return self.__asset_finder
    
    @property
    def data_portal(self) -> DataPortal:
        """ object implementing `history` and `current` data methods. """
        return self.__data_portal

    @property
    def account(self) -> IAccount:
        """
            Return the account object (a view of the underlying 
            trading account).
            
            The account object has the following structure. All these
            attributes are read-only.

            +------------------------+------------+-------------------------------------------------------------------------+
            |Attribute               | Type       | Description                                                             |
            +========================+============+=========================================================================+
            |margin                  | ``float``  | Total margin posted with the broker                                     |
            +------------------------+------------+-------------------------------------------------------------------------+
            |leverage                | ``float``  | Gross leverage (gross exposure / liquid asset value)                    |
            +------------------------+------------+-------------------------------------------------------------------------+
            |gross_leverage          | ``float``  | Gross leverage (gross exposure / liquid asset value)                    |
            +------------------------+------------+-------------------------------------------------------------------------+
            |net_leverage            | ``float``  | Net leverage (net exposure / liquid asset value)                        |
            +------------------------+------------+-------------------------------------------------------------------------+
            |gross_exposure          | ``float``  | Gross (unsigned sum) exposure across all assets at last updated prices  |
            +------------------------+------------+-------------------------------------------------------------------------+
            |long_exposure           | ``float``  | Total exposures in long positions                                       |
            +------------------------+------------+-------------------------------------------------------------------------+
            |short_exposure          | ``float``  | Total exposures in short positions                                      |
            +------------------------+------------+-------------------------------------------------------------------------+
            |long_count              | ``int``    | Total assets count in long positions                                    |
            +------------------------+------------+-------------------------------------------------------------------------+
            |short_count             | ``int``    | Total assets count in short positions                                   |
            +------------------------+------------+-------------------------------------------------------------------------+
            |net_exposure            | ``float``  | Net (signed sum) exposure across all assets at last updated prices      |
            +------------------------+------------+-------------------------------------------------------------------------+
            |net_liquidation         | ``float``  | Sum of cash and margin                                                  |
            +------------------------+------------+-------------------------------------------------------------------------+
            |commissions             | ``float``  | Net commissions paid (if available)                                     |
            +------------------------+------------+-------------------------------------------------------------------------+
            |charges                 | ``float``  | Net trading charges paid (if available)                                 |
            +------------------------+------------+-------------------------------------------------------------------------+
            |total_positions_exposure| ``float``  | Gross (unsigned sum) exposure across all assets at last updated prices  |
            +------------------------+------------+-------------------------------------------------------------------------+
            |available_funds         | ``float``  | Net cash available on the account                                       |
            +------------------------+------------+-------------------------------------------------------------------------+
            |total_positions_value   | ``float``  | Total value of all holdings                                             |
            +------------------------+------------+-------------------------------------------------------------------------+

        .. warning:: 
            Running multiple strategies in the same account may lead to 
            misleading values of these attributes.

        """
        return IAccount(self.__blotter.account.to_dict())

    @property
    def blotter_account(self) -> dict:
        """
            Return the current algo level virtual account object as a dict.
        """
        return self.__blotter.account.to_dict()

    @property
    def broker_account(self) -> dict:
        """ Returns the underlying physical (broker) account. """
        try:
            return self.__broker.account
        except Exception:
            acct = self.__broker.get_account(
                    algo_name=self._env.name,
                    algo_user=self._env.algo_user,
                    logger=self._env.logger)
            return acct.to_dict()

    @property
    def orders(self) -> dict[str, Order]:
        """ 
            return all open and closed orders for the current
            blotter session. This is a ``dict`` with keys as order IDs 
            (``str``) and values as :ref:`order<Order>` object.
        """
        return BlueshiftOrderDict(self.__blotter.orders)

    @property
    def broker_orders(self) -> dict[str, Order]:
        """ return a list of all open and closed orders for the current
        broker session. See API documentation for details """
        return BlueshiftOrderDict(
                self.__broker.get_all_orders(
                algo_name=self._env.name, 
                algo_user=self._env.algo_user,
                logger=self._env.logger))

    @property
    def open_orders(self) -> dict[str, Order]:
        """ 
            return all orders currently open from the algorithm. This is
            a ``dict`` with keys as order IDs (``str``) and values as 
            :ref:`order<Order>` object.
        """
        return BlueshiftOrderDict(self.__blotter.open_orders)
    
    @property
    def is_disconnected(self) -> bool:
        """
            return True if the underlying broker has a streaming connection
            and is not connected at the moment. Returns False otherwise.
        """
        if hasattr(self.__broker, 'is_connected'):
            return not self.__broker.is_connected # type: ignore
        
        return False
    
    def open_orders_by_asset(self, asset) -> dict[str, Order]:
        """ 
            return all orders currently open from the algorithm for a
            given asset. This is a ``dict`` with keys as order IDs 
            (``str``) and values as :ref:`order<Order>` object.
        """
        mayberolling = {}
        if self.mode == AlgoMode.BACKTEST:
            orders = self.__blotter.open_orders_by_asset(asset) # type: ignore
            mayberolling = {o.oid:o for o in orders}
            
        orders = self.__blotter.open_orders
        orders = {k:v for k,v in orders.items() if v.asset == asset}
        return BlueshiftOrderDict({**mayberolling, **orders})

    @property
    def broker_open_orders(self) -> dict[str, Order]:
        """ return all orders currently open from the broker. See API
        documentation for details """
        orders = self.__broker.get_all_orders(
                    algo_name=self._env.name, 
                    algo_user=self._env.algo_user,
                    logger=self._env.logger)
        return BlueshiftOrderDict(
                {k:v for k,v in orders.items() if not v.is_final()})

    @property
    def portfolio(self) -> IPortfolio:
        """
            Return the current portfolio object. Portfolio is a view of the
            current state of the algorithm, including positions.
        
            The attributes (read-only) of the portfolio object are as
            below:

            +------------------+---------------+-------------------------------+
            |Attribute         | Type          |Description                    |
            +==================+===============+===============================+
            |portfolio_value   | ``float``     |Current portfolio net value    |
            +------------------+---------------+-------------------------------+
            |positions_exposure| ``float``     |Present gross exposure         |
            +------------------+---------------+-------------------------------+
            |cash              | ``float``     |Total undeployed cash          |
            +------------------+---------------+-------------------------------+
            |starting_cash     | ``float``     |Starting capital               |
            +------------------+---------------+-------------------------------+
            |returns           | ``float``     |Cumulative Algo returns        |
            +------------------+---------------+-------------------------------+
            |positions_value   | ``float``     |Total value of holdings        |
            +------------------+---------------+-------------------------------+
            |pnl               | ``float``     |Total profit or loss           |
            +------------------+---------------+-------------------------------+
            |mtm               | ``float``     |Unrealized profit or loss      |
            +------------------+---------------+-------------------------------+ 
            |start_date        | ``Timestamp`` |Start date of the algo         |
            +------------------+---------------+-------------------------------+
            |positions         | ``dict``      |Positions dict (see below)     |
            +------------------+---------------+-------------------------------+

            The ``positions`` attribute is a dictionary with the current
            positions. The keys of the dictionary are :ref:`Asset` 
            objects. The values are :ref:`Position` objects.

        """
        return IPortfolio(self.__blotter.account.to_dict(),
                          self.blotter_portfolio,
                          self.__blotter.performance,
                          self._env.start_dt,
                          self._env.end_dt)

    @property
    def blotter_portfolio(self) -> dict[Asset, Position]:
        """
            Return the current portfolio object. Portfolio is a dictionary
            of positions keyed by the asset, tracking algo positions.

            Note:
                The attributes (read-only) of the position object are as
                below:

                +---------------+--------+-------------------------------+
                |Attribute      | Type   |Description                    |
                +===============+========+===============================+
                |asset          | Asset  |Asset of the position          |
                +---------------+--------+-------------------------------+
                |quantity       | int    |Net quantity at present        |
                +---------------+--------+-------------------------------+
                |buy_quantity   | int    |Total buying quantity          |
                +---------------+--------+-------------------------------+
                |buy_price      | float  |Average buy price              |
                +---------------+--------+-------------------------------+
                |sell_quantity  | int    |Total sell quantity            |
                +---------------+--------+-------------------------------+
                |sell_price     | float  |Average selling price          |
                +---------------+--------+-------------------------------+
                |pnl            | float  |Total profit or loss           |
                +---------------+--------+-------------------------------+
                |realized_pnl   | float  |Realized part of pnl           |
                +---------------+--------+-------------------------------+
                |unrealized_pnl | float  |Unrealized (MTM) part of pnl   |
                +---------------+--------+-------------------------------+
                |last_price     | float  |Last updated price             |
                +---------------+--------+-------------------------------+
                |timestamp      | object |Timestamp of last update       |
                +---------------+--------+-------------------------------+
                |value          | float  |Total value of this position   |
                +---------------+--------+-------------------------------+
                |margin         | float  |Margin posted (if available)   |
                +---------------+--------+-------------------------------+
                |product_type   | Enum   |Product type                   |
                +---------------+--------+-------------------------------+

            Returns:
                Dict. A dictionary of position objects (keyed by assets).
        """
        return BlueshiftPositionDict(self.__blotter.portfolio)

    @property
    def broker_portfolio(self) -> dict[Asset, Position]:
        positions = self.__broker.get_positions(
                    algo_name=self._env.name, 
                    algo_user=self._env.algo_user,
                    logger=self._env.logger)
        return BlueshiftPositionDict(positions)

    @property
    def performance(self) -> dict:
        """
            Returns the current performance object. This include the latest
            snap of of all account attributes. It also include a few risk
            and pnl metrics.
        """
        perf = self.__blotter.performance
        if self.timestamp:
            perf['timestamp'] = self.timestamp
        return perf

    @property
    def risk_report(self) -> dict:
        """
            Return the latest risk report.

            Returns:
                Dict. A dictionary with following metrics and reports.

                :per_trade: per trade report, see :func:`blueshift.blotter.analytics.pertrade.compute_eod_per_trade_report`.
                :transactions: transaction report, see :func:`blueshift.blotter.analytics.pertrade.compute_eod_txn_report`.
                :performance: risk metrics report, see :func:`blueshift.blotter.analytics.stats.compute_eod_point_stats_report`.
        """
        return self.__blotter.risk_report

    @property
    def trading_calendar(self) -> TradingCalendar:
        """ 
            Returns the current trading calendar object.
            
            .. seealso::
                See documentation for :ref:`Trading Calendar`.
        """
        return self.__calendar
    
    @property
    def broker_calendar(self) -> TradingCalendar:
        """ return the broker calendar object. """
        return self.__broker.calendar
    
    @property
    def intraday_cutoff(self) -> tuple|datetime.time|None:
        """ 
            Returns the intraday cutoff time for accepting orders with 
            rolling asset definition. Order placement using rolling asset
            will be refused after this cut-off time.
        """
        return self.__broker.intraday_cutoff

    @property
    def pnls(self) -> pd.DataFrame:
        """
            Returns historical (daily) profit-and-loss information since
            inception. This is a ``pandas.Dataframe`` with the following 
            columns:

            - algo_returns: daily returns of the strategy
            - algo_cum_returns: cumulative returns of the strategy
            - algo_volatility: annualised daily volatility of the strategy
            - drawdown: current drawdown of the strategy (percentage)

            .. note:: 
                The timestamp for each day is the end-of-day, except 
                the current day with the timestamp of most recent 
                computation.
        """
        return self.__blotter.pnls

    @property
    def timestamp(self) -> pd.Timestamp|None:
        """ return the current timestamp. Use API `get_datetime` instead of
        directly using this attribute. """
        return self.__timestamp

    @timestamp.setter
    def timestamp(self, timestamp):
        # no validation check for the sake of speed!
        self.__timestamp = timestamp
        # set the blotter timestamp as well.
        self.__blotter.timestamp = timestamp

    @property
    def clock(self) -> TradingClock:
        """ the algo clock object. """
        return self.__clock

    @property
    def record_vars(self) -> pd.DataFrame:
        """
            The recorded var dataframe (``pandas.DataFrame``) as 
            generated by a call to the API function ``record``. 
            The column names are recorded variable names. Variables
            are recorded on a per-session (i.e. daily) basis. A maximum 
            of 10 recorded variables are allowed.
            
            .. warning::
                Adding recorded variables may slow down the speed of 
                a backtest run.

            .. seealso::
                See details in :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.record`.

        """
        return self.__record_vars

    def save(self, timestamp:pd.Timestamp):
        """ save the blotter. """
        if self.__blotter:
            self.__blotter.save(timestamp)
            
        self._save_state()
        
    def _save_state(self):
        out ={}
        state = self.state
        if state:
            out[self.name] = state
            
        for name in self.__children:
            context = self.__children[name]
            state = context.state
            if state:
                out[name] = state
                
        if out:
            try:
                path = os_path.join(blueshift_run_path(
                            self.name),'state.json')
                with open(path, 'w') as fp:
                    json.dump(out, fp, cls=BlueshiftJSONEncoder)
            except Exception as e:
                msg = f'Failed to save state:{str(e)}. '
                msg += f'Did you use a state variable of illegal type?'
                self._env.logger.error(msg)

    def read(self, timestamp:pd.Timestamp):
        """ read the blotter. """
        if self.__blotter:
            self.__blotter.read(timestamp)
        
    def _load_state(self, name):
        if self.__loaded_state is None:
            self.__loaded_state = {}
            try:
                path = os_path.join(blueshift_run_path(
                                self.name),'state.json')
                if os_path.isfile(path):
                    with open(path) as fp:
                        self.__loaded_state = json.load(fp)
            except Exception as e:
                msg = f'Failed to load saved algo states: {str(e)}.'
                raise InitializationError(msg)
                
        return self.__loaded_state.get(name, None)

    def _reset_broker_tuple(self, broker:IBroker|None=None, clock:TradingClock|None=None, 
                            asset_finder:IAssetFinder|None=None, data_portal:DataPortal|None=None, 
                            calendar:TradingCalendar|None=None):
        '''
            extract broker data
        '''
        if broker:
            if not isinstance(broker, IBroker):
                raise InitializationError("Broker is not valid.")
        else:
            broker = self._env.broker
        self.__broker = broker
            
        if clock:
            if not isinstance(clock, TradingClock):
                raise InitializationError("Clock is not valid.")
        else:
            clock = self._env.clock
        self.__clock = clock
        
        if asset_finder is None:
            self.__asset_finder = self.__broker
        else:
            if not isinstance(asset_finder, IAssetFinder):
                raise InitializationError("asset-finder is not valid.")
            self.__asset_finder = asset_finder
            
        if data_portal is None:
            self.__data_portal = self.__broker.data_portal
        else:
            if not isinstance(data_portal, DataPortal):
                raise InitializationError("data-portal is not valid.")
            self.__data_portal = data_portal
                
        if calendar:
            if not isinstance(calendar, TradingCalendar):
                raise InitializationError("Calendar is not valid.")
            self.__calendar = calendar
        else:
            self.__calendar = self._env.trading_calendar
        
        if broker and clock:
            self.__broker_initialized = True
        else:
            raise InitializationError("Broker or clock setup is not complete.")

    def _reset_trackers(self):
        if not self.__broker_initialized:
            return

        self.__blotter = blotter_factory(
                self._mode.name.lower(),
                name=self._name,
                asset_finder=self.__asset_finder,
                data_portal=self.__data_portal,
                broker=self.__broker,
                logger=self._env.logger,
                env = self._env,
                ccy=self.__broker.ccy,
                blotter_type=self.__broker.blotter_type)
        
        self.__tracker_initialized = True

    def reset(self, *args, **kwargs):
        self._reset_trackers()
        self.__protected_members = []
        self.__children = {}
        self.__context_stack = deque([])
        self._env.current_context = self.name
        self._env.error_context = None
        self._reset_broker_tuple(*args, **kwargs)

    def set_up(self, timestamp:pd.Timestamp):
        '''
            Setting up the context before the algo start, or at any
            re-start, initializes timestamp and performance object
        '''
        if timestamp is None:
            timestamp = self.__timestamp

        if not timestamp:
            raise InitializationError(msg="timestamp required"
                                      " to initialize"
                                      " context")
        if not isinstance(timestamp, pd.Timestamp):
            raise ValidationError(msg="timestamp must be of type"
                                  " Timestamp")
        self.__timestamp = timestamp
        
        # inject initial state
        initial_positions = {}
        if 'positions' in self._env.initial_state:
            initial_positions:dict = self._env.initial_state['positions'].copy()

        self.__blotter.reset(timestamp=None, initial_positions=initial_positions)
        self.__blotter.read(timestamp)
        
    @property
    def is_initialized(self) -> bool:
        return self.__broker_initialized and self.__tracker_initialized
    
    @property
    def is_restart(self) -> bool:
        """ If this is a restart. """
        return self._env.restart is True

    def EOD_update(self, timestamp:pd.Timestamp):
        '''
            Called end of day at after trading hours BAR. No validation
        '''
        self.__blotter.roll(timestamp)

    def BAR_update(self, timestamp:pd.Timestamp):
        '''
            Called end of every trading BAR. No validation here
        '''
        pass

    def SOB_update(self, timestamp:pd.Timestamp):
        '''
            Called at start of the day. No validation.
        '''
        self.__broker.refresh_data(timestamp=timestamp)
        for name in self.__children:
            context = self.__children[name]
            context.broker.refresh_data(timestamp=timestamp)

    def finalize(self, timestamp:pd.Timestamp):
        """
            Finalize the context - typically refresh performance metrics,
            and potentially save the results.
        """
        if self.__finalized:
            return
        
        try:
            self.set_record_vars()
            # this finalize all the children blotters
            self.__blotter.finalize(timestamp)
            self._save_state()
        finally:
            self.__finalized = True
            
    def record_state(self, varname:str):
        varname = str(varname)
        if not varname.isidentifier() or varname.startswith('_'):
            return
        
        if len(self.__record_state_vars) > MAX_CTX_STATE_VARS:
            msg = f'you can add maximum {MAX_CTX_RECORD_VARS} state variables.'
            raise RecordVarError(msg=msg)
            
        self.__record_state_vars.add(varname)
        
    def load_state(self, states=None):
        states = self._load_state(self.name)
        states = states or {}
            
        for name, value in states.items():
            setattr(self, name, value)
            self.record_state(name)
            
        # load passed on states, but do not record them by default
        if 'states' in self._env.initial_state:
            states = self._env.initial_state['states'].copy()
            for name, value in states.items():
                setattr(self, name, value)
    
    @property
    def state(self) -> dict:
        out = {}
        for name in self.__record_state_vars:
            if name.startswith('_'):
                continue
            try:
                value = getattr(self, name, None)
                # assert isinstance(value, str) or isinstance(value, bool) or \
                #     isinstance(value, numbers.Number) or \
                #     isinstance(value, dict) or isinstance(value, list) or \
                #     isinstance(value, set)
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
                    #assert sizeof(value) <= 51200 # 50 KB
                    if sizeof(value) > 51200:
                        raise ValueError(f'state size too large')
            except Exception:
                continue
            out[str(name)] = value
            
        return out

    def record(self, varname:str, value):
        """
            Record individual variables. If called before timestamp is
            initialized, return silently.
        """
        dt = self.__timestamp
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
            
    def get_record_dict(self, dt:pd.Timestamp) -> dict:
        loc = pd.to_datetime(dt.date())
        
        if not loc in self.__record_vars.index:
            return {}
        
        return self.__record_vars.loc[loc,:].to_dict()
    
    def set_record_vars(self):
        self.blotter.set_record_vars(self.record_vars)
        for name in self.__children:
            context = self.__children[name]
            context.blotter.set_record_vars(context.record_vars)
            
    def emit_perf_messages(self) -> Iterator[dict]:
        packet = {**self.blotter.get_performance(), 
                  **self.blotter.get_risk_report()}
        packet['timestamp'] = str(self.timestamp)
        packet['context'] = self.name
        yield packet
        
    def emit_risk_messages(self, ts:pd.Timestamp, progress=None) -> Iterator[dict]:
        if progress:
            packet = {**self.blotter.get_risk_report(), **{'progress':progress}}
        else:
            packet = {**self.blotter.get_risk_report(), 
                      **self.blotter.get_performance()}
        record = self.get_record_dict(ts)
        if record:
            packet['records'] = record
        packet['timestamp'] = str(self.timestamp)
        packet['context'] = self.name
        yield packet
    
    @property
    def subcontexts(self) -> dict[str, SubContext]:
        return self.__children
    
    def add_context(self, strategy:Strategy) -> tuple[SubContext, IBlotter]:
        """
            Add a sub-context to this context. This will also 
            add a sub-blotter to the parent blotter.
        """
        name = strategy.name
        if name == self.name:
            msg = f'Cannot add subcontext with same name as the '
            msg += 'global context.'
            raise ContextError(msg)
            
        if name in self.__children:
            msg = f'Cannot add subcontext, contexts already '
            msg += 'exists with same name, names must be unique.'
            raise ContextError(msg)
        
        blotter = self.__blotter.add_blotter(strategy, timestamp=self.timestamp)
        
        algo_id = getattr(strategy,'algo_id', None)
        context = SubContext(name, algo_id, blotter, self, self._env)
        
        self.__children[name] = context
        return context, blotter
        
    def remove_context(self, name:str):
        """ 
            Remove an existing sub context. This will also remove 
            the sub-blotter from the parent blotter.
        """
        if name == self.name:
            return
        
        if name in self.__children:
            try:
                self.__blotter.remove_blotter(name)
            except Exception:
                pass
            return self.__children.pop(name, None)
    
    def get_context(self, name:str):
        """
            Get a sub-context by name, if it exists.
        """
        if name == self.name:
            return self
        
        return self.__children.get(name, None)
    
    def _reset_context(self):
        self.__context_stack = deque([])
        self._env.current_context = self.name
        self._env.error_context = None

    @contextmanager
    def switch_context(self, name:str):
        """ 
            Switch the context of the current API calls. `Context` argument 
            is of type AlgoContext and `env` is of type TradingEnvironment.
            This supports nested context switches.
        """ 
        if name not in self.__children and name != self.name:
            msg = f'missing context {name}, cannot switch context.'
            raise NoContextError(msg)
            
        if name == self.name:
            context = self
        else:
            context = self.__children[name]
            
        if name != context.name:
            msg = f'Inconsistent context, names do not match. '
            msg += f'Got {name} and {context.name}'
            raise ContextError(msg)
            
        last_ctx = ContextEntry(self._env.current_context, None)
        try:
            self.__context_stack.append(last_ctx)
            self._env.current_context = name
            self._env.error_context = None
            yield context
        finally:
            last_ctx = self.__context_stack.pop()
            self._env.current_context = last_ctx.ctx
            
            # set error context if not global context, preserve it 
            # while popping the stack as well
            if name != self.name and not self._env.error_context:
                e, _, _ = exc_info()
                if e:
                    # some exceptions in the last context
                    self._env.error_context = name
                else:
                    self._env.error_context = last_ctx.err_ctx