from __future__ import annotations
from typing import (
    TYPE_CHECKING, Callable, cast, Type, AsyncGenerator, Generator, Any, TypeAlias, Literal)
from os import path as os_path
import numpy as np
from functools import partial, lru_cache
import asyncio
import random
import math
import warnings
import time
from inspect import isclass
import logging
import datetime
import re
import threading
import sys
import uuid
import weakref
from transitions import MachineError

from blueshift.config import (
        get_config, get_config_resource, BLUESHIFT_PRODUCT,
        BLUESHIFT_DISPLAY_VERSION, BLUESHIFT_EAGER_SCHEDULING, 
        BLUESHIFT_DEBUG_MODE, USE_IOC_SQUAREOFF, DEFAULT_IOC_ORDER,
        DEFAULT_COOLOFF, IOC_WAIT_TIME, MAX_ITER_IOC_EXIT, LOCK_TIMEOUT,
        ALLOW_EXTERNAL_EVENT, ORDER_CHECK_TIMEOUT, ORDER_RETRY_TIME,
        EXIT_ORDER_TIMEOUT, USE_IOC_EXIT_SQUAREOFF, USE_MULTI_SQUAREOFF)

from blueshift.calendar.trading_calendar import make_consistent_tz

from blueshift.lib.common.platform import get_exception, print_msg
from blueshift.lib.clocks._clock import SimulationClock, BARS
from blueshift.lib.clocks.realtime import RealtimeClock, ClockQueue
from blueshift.lib.trades._order import Order
from blueshift.lib.trades._order_types import (
        OrderSide, ProductType, OrderValidity, OrderType)
from blueshift.interfaces.trading._exit_handler import ExitMethod
from blueshift.lib.common.constants import ALMOST_ZERO, NANO_SECOND
from blueshift.lib.common.functions import listlike, sleep_if_not_main_thread, create_message_envelope
from blueshift.lib.common.sentinels import noop
from blueshift.lib.common.types import (
    ExpiringCache, PartialOrder, BlueshiftPositionDict, BlueshiftOrderDict)
from blueshift.lib.common.enums import (
        AlgoMode as MODE, ExecutionMode, AccountEventType as EventType,
        AlgoMessageType as MessageType, OneclickMethod,
        BlueshiftCallbackAction, AlgoCallBack, AlgoTimeoutEvent, 
        AlgoTerminateEvent, AlgoStatus, TerminalAlgoStatuses, 
        REALTIME_MODES,LIVE_MODES,SIMULATION_MODES)
from blueshift.lib.exceptions import (
        InitializationError, StateMachineError, PriceOutOfRange,
        ScheduleFunctionError, TradingControlError, UserDefinedException,
        NoSuchPipeline, ExecutionTimeout, TerminationError,
        ValidationError, ContextError, BlueshiftDataException,
        SubContextTerminate, NoContextError, UserStopError, PreTradeCheckError,
        BlueshiftException, HistoryWindowStartsBeforeData, SymbolNotFound,
        NoDataForAsset, OrderError, BlueshiftWarning, OrderAlreadyProcessed, 
        OrderNotFound, SessionOutofRange, get_clean_err_msg)
from blueshift.lib.common.decorators import ensure_state, asyncify, wrap_with_kwarg
from blueshift.lib.common.ctx_mgrs import (
        MessageBrokerCtxManager, StdioRedirect, TimeoutRLock)
from blueshift.lib.trades.utils import create_position
from blueshift.lib.trades._position import Position

from blueshift.interfaces.code.security.security import code_checker_factory
from blueshift.interfaces.assets._assets import Asset, MarketData, StrikeType
from blueshift.interfaces.context import IContext
from blueshift.interfaces.trading.broker import ILiveBroker, IBacktestBroker
from blueshift.interfaces.logger import BlueshiftLogger
from blueshift.interfaces.data.data_portal import DataPortal
from blueshift.interfaces.trading.algo_orders import IAlgoOrder, get_algo_order_handler
from blueshift.interfaces.trading.exit_handler import get_exit_handler

from ..utils.scheduler import (
        TimeRule, TimeEvent, Scheduler, date_rules, time_rules, 
        EventHandle, TriggerOnce)
from ..utils.order_tracker import OrderTracker, OrderTrackerHandle
from ..utils.events import account_event_factory
from ..risks.controls import (TradingControl,
                                      TCOrderQtyPerTrade,
                                      TCOrderValuePerTrade,
                                      TCOrderQtyPerDay,
                                      TCOrderValuePerDay,
                                      TCOrderNumPerDay,
                                      TCLongOnly,
                                      TCPositionQty,
                                      TCPositionValue,
                                      TCGrossLeverage,
                                      TCGrossExposure,
                                      TCBlackList,
                                      TCWhiteList,
                                      TCNotificationNumPerDay,)

from blueshift.providers import *

from ..risks.monitors import RiskMonitor
from ..alerts.alert import BlueshiftAlertManager
from .api_decorator import api_method,command_method
from .context import AlgoContext
from .state_machine import _AlgoStateMachine
from .strategy import Strategy, strategy_factory

from collections import namedtuple
from itertools import chain, repeat
AttachedPipeline = namedtuple(
        'AttachedPipeline', ['pipe','chunks', 'eager'])

_MAX_ITER_EXIT = 3
_WAIT_TIME_EXIT = 1
_WAIT_TIME_EXIT_MINUTE = 3/60
_WAIT_TIME_IOC_EXIT = 3

if TYPE_CHECKING:
    import pandas as pd
    from blueshift.config.config import BlueshiftConfig
    from blueshift.core.utils.environment import TradingEnvironment
else:
    import blueshift.lib.common.lazy_pandas as pd

BlueshiftCallBack: TypeAlias = Callable[[pd.Timestamp], None]
UserCallBack: TypeAlias = Callable[[IContext, DataPortal], None]
OrderCallBack: TypeAlias = Callable[[IContext, list[str]], None]
TargetCallBack: TypeAlias = Callable[[IContext, Asset|None], None]

class TradingAlgorithm(_AlgoStateMachine):

    _MAX_NOTIFICATIONS = 2000

    def _print_verbose(self, msg:str):
        if self._env.verbose and self.mode in REALTIME_MODES:
            print_msg(msg, level='info')

    def _make_bars_dispatch(self):
        """
            Dispatch dictionary for user defined functions.
        """
        self._USER_FUNC_DISPATCH = {
            BARS.ALGO_START: self.initialize,
            BARS.BEFORE_TRADING_START: self.before_trading_start,
            BARS.TRADING_BAR: self.handle_data,
            BARS.AFTER_TRADING_HOURS: self.after_trading_hours,
            BARS.HEART_BEAT: self.heartbeat,
            BARS.ALGO_END: self.analyze
        }

    def _make_broker_dispatch(self):
        """
            Dispatch dictionary for backtest broker processing.
        """
        self._BROKER_FUNC_DISPATCH = {
            BARS.ALGO_START: self._broker_algo_start,
            BARS.BEFORE_TRADING_START: self._broker_before_trading_start,
            BARS.TRADING_BAR: self._broker_trading_bar,
            BARS.DAY_END: self._broker_trading_bar,
            BARS.AFTER_TRADING_HOURS: self._broker_after_trading_hours,
            BARS.HEART_BEAT: self._broker_heart_beat,
            BARS.ALGO_END: self._broker_algo_end,
        }
        
    def _broker_algo_start(self, ts:pd.Timestamp):
        self.context.broker.algo_start(ts)
        # handle the added strategy
        strategies = list(self._strategies)
        for name in strategies:
            try:
                context = self.context.get_context(name)
                if not context:
                    continue
                if context.broker is self.context.broker:
                    continue
                context.broker.algo_start(ts)
            except Exception as e:
                self.handle_sub_strategy_error_no_ctx_switch(e, name)
            
    def _broker_before_trading_start(self, ts:pd.Timestamp):
        self.context.broker.before_trading_start(ts)
        # handle the added strategy
        strategies = list(self._strategies)
        for name in strategies:
            try:
                context = self.context.get_context(name)
                if not context:
                    continue
                if context.broker is self.context.broker:
                    continue
                context.broker.before_trading_start(ts)
            except Exception as e:
                self.handle_sub_strategy_error_no_ctx_switch(e, name)
            
    def _broker_trading_bar(self, ts:pd.Timestamp):
        self.context.broker.trading_bar(ts)
        # handle the added strategy
        strategies = list(self._strategies)
        for name in strategies:
            try:
                context = self.context.get_context(name)
                if not context:
                    continue
                if context.broker is self.context.broker:
                    continue
                context.broker.trading_bar(ts)
            except Exception as e:
                self.handle_sub_strategy_error_no_ctx_switch(e, name)
            
    def _broker_after_trading_hours(self, ts:pd.Timestamp):
        self.context.broker.after_trading_hours(ts)
        # handle the added strategy
        strategies = list(self._strategies)
        for name in strategies:
            try:
                context = self.context.get_context(name)
                if not context:
                    continue
                if context.broker is self.context.broker:
                    continue
                context.broker.after_trading_hours(ts)
            except Exception as e:
                self.handle_sub_strategy_error_no_ctx_switch(e, name)
                
    def _broker_heart_beat(self, ts:pd.Timestamp):
        self.context.broker.heart_beat(ts)
        # handle the added strategy
        strategies = list(self._strategies)
        for name in strategies:
            try:
                context = self.context.get_context(name)
                if not context:
                    continue
                if context.broker is self.context.broker:
                    continue
                context.broker.heart_beat(ts)
            except Exception as e:
                self.handle_sub_strategy_error_no_ctx_switch(e, name)
                
    def _broker_algo_end(self, ts:pd.Timestamp):
        self.context.broker.algo_end(ts)
        # handle the added strategy
        strategies = list(self._strategies)
        for name in strategies:
            try:
                context = self.context.get_context(name)
                if not context:
                    continue
                if context.broker is self.context.broker:
                    continue
                context.broker.algo_end(ts)
            except Exception as e:
                self.handle_sub_strategy_error_no_ctx_switch(e, name)

    def __init__(self, env:TradingEnvironment, *args, **kwargs):
        """
            The trading algorithm is responsible for 1. reading the input
            program, 2. collecting and running user functions in appropriate
            event functions, 3. handling commands and publishing performance
            packets as well as defining the API functions. The main loops in
            backtest and live runs are different. Backtest in implemented as
            generator. Live loop is an async generator to make use of the
            sleep time of the real clock.
        """    
        env.algo = self
        env.lock_attrs()
        super(self.__class__, self).__init__(
                env.name, env.mode, env.execution_mode)
        self._env:TradingEnvironment = env
        self.TASKS_TO_CANCEL_ON_EXCEPTIONS = set()

        # two ways to kickstart, specify the components...
        asset_finder = kwargs.get("asset_finder")
        data_portal = kwargs.get("data_portal")
        context = kwargs.get("context", None)
        
        if context and isinstance(context, AlgoContext):
            self.context:AlgoContext = context
        elif asset_finder and data_portal:
            self.context:AlgoContext = AlgoContext(self._env, asset_finder=asset_finder, data_portal=data_portal)
        else:
            raise InitializationError(msg="context not specified and cannot be created.")
        
        # reset all trackers
        self._reset_trackers()

        # we should have a proper algo context by this time
        # but this may not be initialized and ready to run.
        # we must check initialization before running the algo.
        # Most of it done in context, here we check run mode compatibility.
        if self.mode not in self.context.broker.supported_modes:
            raise InitializationError(msg="incompatible run mode and broker.")
        # check the execution mode - auto or with user confirmation
        if self.execution_mode not in self.context.broker.execution_modes:
            raise InitializationError(msg="incompatible execution mode and broker.")
        # no user confirmation mode for backtest
        if self.mode in (MODE.BACKTEST, MODE.EXECUTION) and \
            self.execution_mode == ExecutionMode.ONECLICK:
            raise InitializationError(
                    msg="mode {self.mode} does not support oneclick execution.")

        # extract the user algo
        if self._env.algo_code:
            self.algo = self._env.algo_code
        elif self._env.algo_file:
            self.algo = self._env.algo_file
        else:
            raise InitializationError(msg="algo missing or not specified.")
            
        # setup the default logger
        self.set_logger()
        # register self with the alert manager
        env.alert_manager.register_algo(self)
            
        #self.update_namespace()
        self.load_algo(None)
        self.log_debug(f'loading strategy complete.')

        # create the bars dispatch dictionaries
        self._USER_FUNC_DISPATCH = {}
        self._BROKER_FUNC_DISPATCH = {}

        self._make_bars_dispatch()
        self._make_broker_dispatch()
        
        alert_manager = self._env.alert_manager
        alert_manager.register_handler(
                self, 'smart-orders', self.smart_order)
        alert_manager.register_handler(
                self, 'broker-login', self.login)
        alert_manager.register_handler(
                self, 'priming-message', noop)
        alert_manager.register_handler(
                self, 'server-log', self.blueshift_server_log)
        alert_manager.register_handler(
                self, 'server-control', self.blueshift_server_control)
        
        if self.context.notifier:
            alert_manager.register_handler(
                    self, 'notifications', 
                    self.context.notifier.handle_notifications)
        
        if self.mode == MODE.EXECUTION:
            self._env.blueshift_callback(
                        action=BlueshiftCallbackAction.SMART_ORDER_STATUS.value,
                        status=AlgoStatus.CREATED.value,
                        msg=f'Smart order {self.name} is created successfully.')
        else:
            self._env.blueshift_callback(
                        action=BlueshiftCallbackAction.ALGO_STATUS.value,
                        status=AlgoStatus.CREATED.value,
                        msg=f'Algo {self.name} is created successfully.')
        
    def _reset_trackers(self):
        # the async loop and message queue for the realtime clock
        self._queue:ClockQueue|None = None
        
        # set up finalize behaviours
        self._status:AlgoStatus = AlgoStatus.CREATED
        self._desired_status:AlgoStatus = AlgoStatus.RUNNING
        self.completion_msg:str = ''
        self._is_finalized:bool = False
        self._is_finalize_in_progress:bool = False
        self._on_cancel_error_called:bool = False
        self._on_final_exit_called:bool = False
        self._cancel_orders_on_exit:bool = False
        self._squareoff_on_exit:bool = False
        self._skip_exit_on_kill:bool = False
        self._err_on_square_off:bool = False
        
        # set up the scheduler and event handler
        self._scheduler = Scheduler(self)
        self._order_tracker = OrderTracker()
        
        # set up the trading controllers list
        self._trading_controls:list[TradingControl] = []
        self.__freeze_trading:bool = False
        # if this flag is true, no user trading events is triggered, i.e. 
        # handle_data and handle_event
        self._is_terminated:bool = False
        
        # risk monitor
        self.monitor:RiskMonitor|None = None

        # setup pipeline
        self._pipeline_engine = None
        self._pipelines = {}
        self._pipeline_cache = ExpiringCache(method='timestamp')
        
        self._strategies:dict[str, Strategy] = {}
        self._exits_handler = get_exit_handler(self)
        # store ctx-wise reset handlers. it stores the timer  handler 
        # returned from call_later with key (context,asset), where asset 
        # can be none for context-wise cooloffs.
        self._exits_handler_scheduled_resets:dict[tuple[IContext, Asset|None],asyncio.Handle|None] = {}
        self._algo_order_handler = get_algo_order_handler(self)
        
        # update reference to the exit handler with the broker
        # this is only applicable for backtester and paper trader
        self.context.broker.exit_handler = self._exits_handler
        
        # flag to avoid repeated exit square-off, this is a global
        # flag used for all subcontexts as well
        self._exit_squareoff:bool = False
        
        # warning control flag for wrong quantity
        self._order_warning:set[Asset] = set()
        
        # event waiters for open orders and data updates
        self._order_waiters:set[threading.Event] = set()
        self._data_waiters:set[threading.Event] = set()
        self._order_lock = TimeoutRLock(timeout=LOCK_TIMEOUT)
        self._finalize_lock = TimeoutRLock(timeout=min(60, LOCK_TIMEOUT))
        self._lock = TimeoutRLock(timeout=LOCK_TIMEOUT)
        
        # tracker for cancelling scheduled events
        self._scheduled_events = {}
        # tracker for new orders
        self._new_orders = set()
        # decorated callable cache
        self._callable_cache = weakref.WeakKeyDictionary()
        
        # for tracking algo error and manual intervention
        self.manual_intervention:bool = False
        self.manual_intervention_msg:str = ''
        self._wants_to_quit = False # for quitting async in executor
        
        # for tracking coros
        self._risk_coro = None
        self._clock_coro = None
        self._algo_coro = None

    def __str__(self) -> str:
        return "Blueshift Algorithm [name:%s, broker:%s]" % (self.name,
                                                  self.context.broker)

    def __repr__(self) -> str:
        return self.__str__()

    def _bar_noop(self, timestamp:pd.Timestamp):
        """
            null operation for a bar function.
        """
        pass

    def load_algo(self, root:str|None=None, algo_id:str|None=None):
        """
            Load the algo file, inject api and extract functions.
        """
        if isinstance(self.algo, str) and not self.algo.endswith('.py'):
            algo_file = "<string>"
            self.log_debug(f'reading strategy code from input...')
        elif os_path.isfile(self.algo):
            algo_file = os_path.basename(self.algo)
            with open(self.algo) as algofile:
                self.algo = algofile.read()
            self.log_debug(f'reading strategy code...')
        else:
            raise InitializationError(msg="algo file not found.")

        self.algo_file = algo_file
        
        no_sec = False
        if not self._env.security:
            no_sec = True
            
        namespace = {}
        max_print:int = int(get_config_resource('stdout')) # type: ignore
            
        if self._env.root:
            self.log_debug(f'compiling strategy(s)...')
        else:
            self.log_debug(f'compiling strategy...')

        c = code_checker_factory(
            self._env.code_checker, self._env.pwd, self._env.root, config = self._env.security, 
            no_sec=no_sec, max_print=max_print, stdout=self._env.stdout, 
            stderr=self._env.stderr, logger=self.logger)
        self.log_debug(f'created strategy parser...')
        try:
            code = c.compile_(self.algo, algo_file)
            self.log_debug(f'initiating namespace creation...')
            c.exec_(code, namespace) #nosec
        except Exception:
            raise # raise codered errors
        else:
            self.log_debug(f'strategy code successfully compiled.')
        
        if max_print and self.mode == MODE.BACKTEST:
            msg = f"resource limits: enabled max print statements "
            msg = msg + f" at {max_print}."
            self.log_info(msg)

        # bind the API methods to this instance using old method
        # if any of them are marked for that.
        for k in namespace:
            if callable(namespace[k]):
                is_api = getattr(namespace[k],"is_api",None)
                if is_api:
                    namespace[k] = partial(namespace[k],
                                  self)
            
        if BLUESHIFT_DEBUG_MODE:
            # save a copy for debugging
            self.namespace = namespace
        
        # check if a valid strategy
        initialize_func = namespace.get('initialize', None)
        if not initialize_func:
            self.log_debug(f'loading strategy class.')
            # try to load a class based strategy
            klass = self._load_strategy(
                    self.algo, self.env.algo_name, namespace)
            if not klass or not hasattr(klass, 'initialize'):
                msg = f'No valid strategy found, a valid strategy must '
                msg += f'define, at minimum, the initialize function, '
                msg += f'or a Strategy class with a initialize method.'
                raise InitializationError(msg)
            
            try:
                params = cast(dict, self._env.parameters)
                strategy = strategy_factory(klass, self.name, **params)
            except Exception as e:
                msg = f'Failed to create strategy object: {str(e)}.'
                raise InitializationError(msg)
            else:
                # we read the class based strategy
                self._initialize = strategy.initialize
                self._handle_data = getattr(strategy, 'handle_data', noop)
                self._before_trading_start = getattr(strategy, 'before_trading_start', noop)
                self._after_trading_hours = getattr(strategy, 'after_trading_hours', noop)
                self._heartbeat = getattr(strategy, 'heartbeat', noop)
                self._analyze = getattr(strategy, 'analyze', noop)
                self._handle_event = getattr(strategy, 'handle_event', noop)
                self._on_data = getattr(strategy, 'on_data', noop)
                self._on_trade = getattr(strategy, 'on_trade', noop)
                self._on_error = getattr(strategy, 'on_error', noop)
                self._on_cancel = getattr(strategy, 'on_cancel', noop)
                self._on_exit = getattr(strategy, 'on_exit', noop)
                self._on_oneclick = getattr(strategy, 'on_oneclick', noop)
                self._on_update = getattr(strategy, 'on_update', noop)
                self._on_connect = namespace.get('on_connect', noop)
                self._on_disconnect = namespace.get('on_disconnect', noop)
                self._on_algo_error = getattr(strategy, 'on_algo_error', noop)
                self._on_stoploss = getattr(strategy, 'on_stoploss', noop)
                self._on_takeprofit = getattr(strategy, 'on_takeprofit', noop)
        else:
            # we read the function based strategy
            self._initialize = initialize_func
            self._handle_data = namespace.get('handle_data', noop)
            self._before_trading_start = namespace.get('before_trading_start', noop)
            self._after_trading_hours = namespace.get('after_trading_hours', noop)
            self._heartbeat = namespace.get('heartbeat', noop)
            self._analyze = namespace.get('analyze', noop)
            self._handle_event = namespace.get('handle_event', noop)
            self._on_data = namespace.get('on_data', noop)
            self._on_trade = namespace.get('on_trade', noop)
            self._on_error = namespace.get('on_error', noop)
            self._on_cancel = namespace.get('on_cancel', noop)
            self._on_exit = namespace.get('on_exit', noop)
            self._on_oneclick = namespace.get('on_oneclick', noop)
            self._on_update = namespace.get('on_update', noop)
            self._on_connect = namespace.get('on_connect', noop)
            self._on_disconnect = namespace.get('on_disconnect', noop)
            self._on_algo_error = namespace.get('on_algo_error', noop)
            self._on_stoploss = namespace.get('on_stoploss', noop)
            self._on_takeprofit = namespace.get('on_takeprofit', noop)
        
        # inject algo callback if live trading
        if self.mode in REALTIME_MODES and isinstance(self.context.broker, ILiveBroker):
            if getattr(self.context.broker,'set_algo_callback', None):
                msg = 'setting up callbacks for events.'
                self._print_verbose(msg)
                self.context.broker.set_algo_callback(
                        self.schedule_event, 
                        algo_name = self._env.name,
                        algo_user=self._env.algo_user,
                        logger=self.logger)
                
    def _load_strategy(self, src, algo_name, namespace=None) -> Type[Strategy]|None:
        """
            Load a strategy class
        """
        def _is_strategy(obj) -> bool:
            try:
                if isclass(obj) and issubclass(obj, Strategy) \
                    and obj.__name__ != 'Strategy':
                    return True
                else:
                    return False
            except Exception:
                return False
        
        file_name = os_path.basename(src)
        
        if not namespace:
            algo = None
            try:
                if not os_path.exists(src):
                    # assume this is the strategy code to load
                    algo = src
                else:
                    with open(src) as fp:
                        algo = fp.read()
            except Exception as e:
                msg = f'Failed to load strategy from src {src}:{str(e)}'
                raise InitializationError(msg)
                    
            no_sec = False
            if not self._env.security:
                no_sec = True
            
            namespace = {}
            max_print = get_config_resource('stdout')
            
            c = code_checker_factory(
                self._env.code_checker, self._env.pwd, self._env.root, config = self._env.security, 
                no_sec=no_sec, max_print=max_print, stdout=self._env.stdout, 
                stderr=self._env.stderr, logger=self.logger)
            try:
                code = c.compile_(algo, file_name)
                c.exec_(code, namespace) #nosec
            except Exception as e:
                msg = f'failed to load user strategy:{str(e)}.'
                raise InitializationError(msg)
            else:
                if BLUESHIFT_DEBUG_MODE:
                    # save a copy for debugging
                    self.namespace = namespace

        # bind the API methods to this instance using old method
        # if any of them are marked for that.
        for k in namespace:
            if callable(namespace[k]):
                is_api = getattr(namespace[k],"is_api",None)
                if is_api:
                    namespace[k] = partial(namespace[k],
                                  self)
                    
        # explicitly defined Strategy class registry
        registry = namespace.get('strategies', None)
        if registry and algo_name:
            try:
                strategy = registry.get(algo_name)
                if _is_strategy(strategy):
                    strategy = cast(Type[Strategy], strategy)
                    return strategy
            except Exception:
                pass
        
        # or find the first class def that works
        strategy = None
        for k in namespace:
            # find the first available subclass of Strategy
            obj = namespace[k]
            if _is_strategy(obj):
                obj = cast(Type[Strategy], obj)
                return obj

    def _init_pipeline(self):
        try:
            from blueshift.pipeline import setup_pipeline_engine # type: ignore
        except ImportError:
            msg = f'pipeline APIs are not supported.'
            self.logger.error(msg)
            return
        
        try:
            library = self.context.broker.library
            #assert library is not None
            if library is None:
                raise ValueError('missing library')
            daily_store = library.pipeline_store
        except Exception as e:
            msg = f'The broker has no support for pipeline:{str(e)}'
            self.log_warning(msg)
            return
        else:
            if not daily_store:
                msg = f'The broker has no eligible store for pipeline.'
                self.log_warning(msg)
                return
        
        adj_handler = daily_store.adjustments_handler
        quarterly_fundamental_store = library.quarterly_fundamental_store
        annual_fundamental_store = library.annual_fundamental_store

        self._pipeline_engine = setup_pipeline_engine(
            library, daily_store, adj_handler, quarterly_fundamental_store,
            annual_fundamental_store)

    def initialize(self, timestamp:pd.Timestamp):
        """
            Called at the start of the algo. Or at every resume command.
        """
        try:
            self.fsm_initialize()
        except MachineError:
            msg = f"State Machine Error ({self.state}): in initialize"
            raise StateMachineError(msg=msg)

        # call hooks, if any
        self._start_hooks(
                self.context, self._env, get_config(self._env.algo_user), 
                timestamp)

        # the user function
        self._initialize(self.context)
        
        
        # handle the added strategy
        strategies = list(self._strategies)
        for name in strategies:
            strategy = self._strategies[name]
            try:
                with self.context.switch_context(name) as context:
                    try:
                        strategy._initialize(context)
                    except Exception as e:
                        self.handle_sub_strategy_error(e, name)
            except NoContextError:
                # this always run in the main thread, we should not be here
                pass

    def before_trading_start(self, timestamp:pd.Timestamp):
        """
            Called at the start of the session.
        """
        try:
            self.fsm_before_trading_start()
        except MachineError:
            msg = f"State Machine Error ({self.state}): in before_trading_start"
            raise StateMachineError(msg=msg)
        # reset warning flag
        self._order_warning = set()
        
        # reset exit handlers
        self._exits_handler.reset()
        self._algo_order_handler.reset()
        try:
            self._before_trading_start(self.context,
                                       self.context.data_portal)
            
            # handle the added strategy
            strategies = list(self._strategies)
            for name in strategies:
                strategy = self._strategies[name]
                try:
                    with self.context.switch_context(name) as context:
                        try:
                            strategy._before_trading_start(
                                    context, context.data_portal)
                        except Exception as e:
                            self.handle_sub_strategy_error(e, name)
                except NoContextError:
                    # this always run in the main thread, we should not be here
                    pass
        finally:
            # call the handle data state update here.
            if self.mode not in REALTIME_MODES:
                try:
                    self.fsm_handle_data()
                except MachineError:
                    msg = f"State Machine Error ({self.state}): in before_trading_start"
                    raise StateMachineError(msg=msg)

    def handle_data(self, timestamp:pd.Timestamp):
        """
            Called at the start of each trading bar. We call the state
            machine function only for live mode here, and club with
            before trading start for backtest mode. This a bit hacky but
            speeds things up.
        """
        if self._is_terminated:
            return
        
        if self.mode in REALTIME_MODES:
            try:
                self.fsm_handle_data()
            except MachineError:
                msg = f"State Machine Error ({self.state}): in handle_data"
                raise StateMachineError(msg=msg)
                
        if self.is_paused():
            self._exits_handler.check_exits(reset=True)
            return
                
        # check for sl/tp exits first
        self._exits_handler.check_exits(reset=True)
        self.check_algo_orders()

        # run scheduled tasks next, this handles both
        # global context and subcontexts
        self._scheduler.trigger_events(self.context,
                                       self.context.data_portal,
                                       timestamp.value)
        # followed by on_data if in backtest mode
        if self.mode == MODE.BACKTEST:
            self._on_data(self.context, self.context.data_portal)
            
        # followed by on_trade trigger for backtest mode
        # based on the events flag set on the broker object
        bt_trade_event = False
        if self.mode == MODE.BACKTEST:
            if AlgoCallBack.TRADE in self.context.broker.events:
                bt_trade_event = self.context.broker.events.pop(AlgoCallBack.TRADE)
                self._on_trade(self.context, self.context.data_portal)
                
            if AlgoCallBack.RECONCILIATION in self.context.broker.events:
                oids = list(self.context.broker.events.pop(AlgoCallBack.RECONCILIATION, []))
                self.handle_reconciliation_event(oids, timestamp)
        
        # followed by user defined handle_data
        self._handle_data(self.context, self.context.data_portal)
        
        
        # handle the added strategy
        strategies = list(self._strategies)
        for name in strategies:
            strategy = self._strategies[name]
            try:
                with self.context.switch_context(name) as context:
                    try:
                        if self.mode == MODE.BACKTEST:
                            strategy._on_data(
                                    context, context.data_portal)
                            if bt_trade_event:
                                strategy._on_trade(context, context.data_portal)
                        strategy._handle_data(
                                context, context.data_portal)
                    except Exception as e:
                        self.handle_sub_strategy_error(e, name)
            except NoContextError:
                # this always run in the main thread, we should not be here
                pass
            
        # put idle packe if event loop if empty
        if self.mode in REALTIME_MODES:
            self.schedule_idle()
        
    def send_completion_msg(self, msg:str, 
                            msg_type:Literal['info', 'error', 'warning', 'success', 'info2'] ='error'):
        self.log_platform(msg)
        
        if self.mode in REALTIME_MODES:
            self.context.blotter.emit_notify_msg(msg, msg_type=msg_type)
        
            
    def set_intervention_msg(self, msg:str):
        self.manual_intervention:bool = True
        self.manual_intervention_msg = msg
        self.log_platform(msg)
        
        self._desired_status = AlgoStatus.ERRORED
        self._status = AlgoStatus.ERRORED
        self.completion_msg = msg
        
        if self.mode in REALTIME_MODES:
            self.context.blotter.emit_notify_msg(msg, msg_type='error')
            
    def check_algo_orders(self):
        self._algo_order_handler.check_orders()
    
    def wakeup_order_waiters(self):
        try:
            with self._lock:
                for event in self._order_waiters:
                    event.set()
        except TimeoutError:
            pass
        else:
            self._order_waiters = set()
        
    def wakeup_data_waiters(self):
        try:
            with self._lock:
                for event in self._data_waiters:
                    event.set()
        except TimeoutError:
            pass
        else:
            self._data_waiters = set()
        
    def add_waiter(self, event:threading.Event):
        if self.mode not in REALTIME_MODES:
            return
        if self.mode in LIVE_MODES:
            return self._order_waiters.add(event)
        return self._data_waiters.add(event)
        
    def remove_waiter(self, event:threading.Event):
        if event in self._order_waiters:
            return self._order_waiters.remove(event)
        if event in self._data_waiters:
            return self._data_waiters.remove(event)
        
    def _run_recon_in_thread(self):
        with self._order_lock:
            try:
                if self.mode in LIVE_MODES:
                    self.reconcile_all(
                            timestamp=None, forced=True, exit_time=False, 
                            fetch_orders=False, only_txns=True, simulate=False)
            except Exception as e:
                msg = f'failed to run reconciliation on trade event: {str(e)}'
                self.log_error(msg)
            finally:
                # wake up the waiters
                self.wakeup_order_waiters()

    def schedule_event(self, event_type:AlgoCallBack, param:list|None=None):
        if self.mode not in REALTIME_MODES:
            return

        if not self.is_TRADING_BAR() and event_type != AlgoCallBack.TRADE:
            return

        if not isinstance(event_type, AlgoCallBack):
            return
            
        if event_type == AlgoCallBack.DISCONNECT:
            # we got disconnected, so no point waiting on events
            self.wakeup_order_waiters()
            self.wakeup_data_waiters()
                    
        if param and event_type == AlgoCallBack.DATA:
            if not listlike(param):
                param = [param]
            if all([asset not in self.current_context.broker.subscribed_assets \
                 for asset in param]):
                return
            # add to monitor so that we wake up after recon
            if self.monitor:
                self.monitor.add_event(AlgoCallBack.DATA)
            
        if param and event_type == AlgoCallBack.TRADE:
            if not listlike(param):
                param = [param]
                
            try:
                with self._order_lock:
                    if all([oid not in self.current_context.blotter.orders_no_reconcile() \
                         for oid in param]):
                        self.log_warning(f'skipping order event update for {param}')
                        return
                    
                    if_exit = self._risk_coro is not None and self._risk_coro.done()
                    if_exit = if_exit or not self.is_TRADING_BAR()
                    
                    if if_exit:
                        # the risk monitor is not running anymore, do a 
                        # blotter reconcile here for live trading modes
                        executor = self._env.alert_manager.executor
                        if executor:
                            executor.submit(self._run_recon_in_thread)
                    else:
                        if self.monitor:
                            self.monitor.add_event(AlgoCallBack.TRADE)
            except TimeoutError:
                # timeout exception is not fatal here
                pass
            
        # no check required for AlgoCallback.RECONCILIATION as it is 
        # triggered from within the algo itself

        if self.loop and self._queue:
            func = self._queue.put_no_repeat
            if event_type == AlgoCallBack.DATA:
                # allow discard on queue backpressure for data events
                # as we can afford to miss some. Non-data events must
                # succeed or raise ClockError exception
                func = self._queue.put_if_possible
            
            packet = (param, event_type)
            self.loop.call_soon_threadsafe(func, packet, self)
            
    def _schedule_order_check(self, order_id:str):
        if self.mode not in LIVE_MODES:
            return
        
        if not order_id:
            return
        
        def check_order_update(order_id):
            if order_id in self._new_orders:
                oo = self.get_order(order_id, algo=False)
                if oo and not oo.is_open():
                    self._new_orders.remove(order_id)
                else:
                    msg = f'No update yet received for order ID {order_id}, '
                    msg += f'suspected problems in order streaming (or not enabled).'
                    self.log_warning(msg)
        
        func = partial(check_order_update, order_id)
        if self.loop and self._queue:
            self._new_orders.add(order_id)
            self.run_later(ORDER_CHECK_TIMEOUT, func)
            
            
    def handle_terminate(self, event_type:AlgoTerminateEvent, timestamp:pd.Timestamp, callback:BlueshiftCallBack):
        if callable(callback):
            try:
                callback(timestamp)
            except Exception as e:
                msg = f'Failed to run callback for termination event '
                msg += f'{event_type.value}:{str(e)}.'
                self.log_error(msg)
        
    def handle_timeout(self, event_type:AlgoTimeoutEvent, timestamp:pd.Timestamp, context_name:str, 
                       callback:UserCallBack, id_:str|None=None):
        """
            Run function in a given context for set-time out API calls.
        """
        if self._is_terminated or self.is_paused():
            return
        
        if self.mode not in REALTIME_MODES:
            return

        if not self.is_TRADING_BAR():
            msg = f'Skipping callback {callback.__name__} as '
            msg += f'the algo is not in trading state.'
            self.log_warning(msg)
            return
        
        if not isinstance(event_type, AlgoTimeoutEvent):
            return
        
        if id_ and id_ not in self._scheduled_events:
            # already cancelled
            return
            
        try:
            with self.context.switch_context(context_name) as context:
                try:
                    callback(context, context.data_portal)
                except Exception as e:
                    self.handle_sub_strategy_error(e, context_name)
        except NoContextError:
            # This runs in teh same thread as asyncio call_later
            pass
        finally:
            if id_:
                self._scheduled_events.pop(id_, None)
        
    def schedule_idle(self, kind='idle'):
        if not self._queue:
            return
        
        packet = (None, AlgoCallBack.IDLE)
        
        try:
            if kind=='possible':
                self._queue.put_if_possible(packet)
            else:
                self._queue.put_if_idle(packet)
        except Exception as e:
            e.algo = self # type: ignore
            raise e
            
    def run_soon(self, func):
        if self.loop and self.loop.is_running():
            self.loop.call_soon_threadsafe(func)
            
    def run_later(self, delay:float, func:Callable, id_:str|None=None) -> asyncio.Handle|None:
        def call_later():
            h = self.loop.call_later(abs(delay), func)
            if id_ and id_ in self._scheduled_events:
                self._scheduled_events[id_].update_handle(h)
            
        h = None
        if self.loop and self.loop.is_running():
            h = self.loop.call_soon_threadsafe(call_later)
            if id_ and id_ in self._scheduled_events:
                self._scheduled_events[id_].update_handle(h)

        return h
                
    def reconcile_all(self, timestamp:pd.Timestamp|None=None, emit_log:bool=True, forced:bool=False,
                  exit_time:bool=False, fetch_orders:bool=True, only_txns:bool=False, 
                  simulate:bool=True) -> bool:
        if not timestamp:
            if self.mode in REALTIME_MODES:
                timestamp = pd.Timestamp.now(tz=self.context.trading_calendar.tz)
            else:
                timestamp = self.context.timestamp
            
        if simulate and self.mode in SIMULATION_MODES:
            self.context.blotter.simulate(timestamp)
            if self.mode == MODE.PAPER:
                self.context.blotter.needs_reconciliation = True
                
        return self.context.blotter.reconcile_all(
                timestamp, 
                emit_log=emit_log, 
                forced=forced, 
                exit_time=exit_time, 
                internal=False, 
                fetch_orders=fetch_orders, 
                only_txns=only_txns)
        
    def run_idle(self, timestamp:pd.Timestamp):
        """
            Call idle callback.
        """
        if not self.is_TRADING_BAR():
            return
        
        if self.mode == MODE.PAPER:
            # trigger trade simulation for paper mode
            if self.context.blotter.get_open_orders():
                # run simulation to fill orders
                self.reconcile_all(timestamp, forced=True, simulate=True)
                    
        if self.mode in REALTIME_MODES:
            self.context.blotter.update_valuation(timestamp, timeout=10)
            # check sl/ tp exits in live run modes
            if not self._exits_handler.is_empty():
                self._exits_handler.check_exits(reset=False)
                
            # increased frequency of scheduler checks
            if BLUESHIFT_EAGER_SCHEDULING and not self.is_paused():
                self._scheduler.trigger_events(
                        self.context, self.context.data_portal,
                        timestamp.value)
                
                # put idle packe if we have a pending scheduled event
                seconds = max(1,int(self._env.frequency.nano/NANO_SECOND))
                dt = timestamp + pd.Timedelta(seconds=seconds)
                dt = self._scheduler.next_event_dt(dt.value)
                if self.loop and dt:
                    time_left = (dt - timestamp.value)/NANO_SECOND
                    if time_left > 0:
                        self.run_later(
                                time_left, self.schedule_idle)
                
    def handle_oneclick_event(self, notifications:list[dict], timestamp:pd.Timestamp):
        """
            Called when the user has confirmed a oneclick notification.
        """
        if self._is_terminated or self.is_paused():
            return
        
        if self.mode == MODE.BACKTEST:
            return
        
        if self.execution_mode != ExecutionMode.ONECLICK:
            return
        
        try:
            self._on_oneclick(self.context, notifications)
        except Exception as e:
            self.log_error(f'Failed to run on update routine:{str(e)}.')
            
    def handle_reconciliation_event(self, order_ids:list[str], timestamp:pd.Timestamp):
        """
            Called when the an reconciliation event is triggered.
        """
        if not order_ids:
            return
        
        try:
            if self.mode in (MODE.LIVE, MODE.PAPER):
                self.context.blotter.emit_valuation_packets(timestamp)
                self.context.blotter.emit_positions_packets(timestamp, True) 
                self.context.blotter.emit_txns_packets(timestamp, True)
            elif self.mode == MODE.EXECUTION:
                self.context.blotter.emit_perf_msg(timestamp)
        except Exception as e:
            self.logger.info(f'failed to emit valuation packets: {str(e)}')
        
        exc = None # run all and then the raise exc from main ctx if any
        handles = self._order_tracker.trigger(order_ids)
        for h in handles:
            try:
                with self.context.switch_context(h.ctx) as context:
                    try:
                        h.trigger(context)
                    except Exception as e:
                        self.log_error(f'Failed to run order callbacks in context {h.ctx}:{str(e)}.')
                        try:
                            self.handle_sub_strategy_error(e, h.ctx)
                        except Exception as e:
                            exc = e
            except NoContextError:
                pass
            
        if exc:
            # raise in err in main ctx
            raise exc
        
    def handle_external_event(self, params:dict, timestamp:pd.Timestamp):
        """
            Called when the an external event is triggered. This is done 
            only in the global context.
        """
        if self._is_terminated or self.is_paused():
            return
        
        if self.mode == MODE.BACKTEST:
            return
        
        try:
            self._on_update(self.context, params)
        except Exception as e:
            self.log_error(f'Failed to run on update routine:{str(e)}.')
            
    def handle_algo_error(self, error:Exception|str):
        """
            Called when the algo has encountered an error but has recovered 
            to continue. This handler function can choose to take additional 
            action in this case.
        """
        try:
            self._on_algo_error(self.context, error)
        except Exception as e:
            self.log_error(f'Failed to run on algo error routine:{str(e)}.')
        
    def on_error(self, error:Exception|str):
        if self._on_cancel_error_called:
            return
        self._on_cancel_error_called = True
        last = self.__freeze_trading
        
        try:
            self.__freeze_trading = True
            strategies = list(self._strategies)
            for name in strategies:
                strategy = self._strategies[name]
                try:
                    with self.context.switch_context(name) as context:
                        try:
                            strategy._on_error(context, error)
                        except Exception as e:
                            msg = f'Failed to run error routine for {context.name}:{str(e)}.'
                            self.log_error(msg)
                except NoContextError:
                    pass
                
            try:
                self._on_error(self.context, error)
            except Exception as e:
                self.log_error(f'Failed to run algo error routine:{str(e)}.')
                
            try:
                self.context.blotter.on_error(error)
            except Exception as e:
                self.log_error(f'Failed to run blotter error routine:{str(e)}.')
        finally:
            self.__freeze_trading = last
        
    def on_cancel(self):
        if self._on_cancel_error_called:
            return
        self._on_cancel_error_called = True
        last = self.__freeze_trading
        
        try:
            self.__freeze_trading = True
            strategies = list(self._strategies)
            for name in strategies:
                strategy = self._strategies[name]
                try:
                    with self.context.switch_context(name) as context:
                        try:
                            strategy._on_cancel(context)
                        except Exception as e:
                            msg = f'Failed to run on cancel routine for {context.name}:{str(e)}.'
                            self.log_error(msg)
                except NoContextError:
                    pass
                    
            try:
                self._on_cancel(self.context)
            except Exception as e:
                self.log_error(f'Failed to run algo cancel routine:{str(e)}.')
                
            try:
                self.context.blotter.on_cancel()
            except Exception as e:
                self.log_error(f'Failed to run blotter cancel routine:{str(e)}.')
        finally:
            self.__freeze_trading = last
            
    def on_exit(self):
        if self._on_final_exit_called:
            return
        
        self._on_final_exit_called = True
        last = self.__freeze_trading
        try:
            self.__freeze_trading  = True
            strategies = list(self._strategies)
            for name in strategies:
                strategy = self._strategies[name]
                try:
                    with self.context.switch_context(name) as context:
                        try:
                            strategy._on_exit(context)
                        except Exception as e:
                            msg = f'Failed to run on exit routine for {name}:{str(e)}.'
                            self.log_error(msg)
                except NoContextError:
                    pass
                    
            try:
                self._on_exit(self.context)
            except Exception as e:
                self.log_error(f'Failed to run algo on exit routine:{str(e)}.')
        finally:
            self.__freeze_trading = last

    def handle_event(self, event_type:AlgoCallBack, timestamp:pd.Timestamp, param:list):
        """
            Called at specific updates from the broker connection. This
            requires streaming data support from the broker and is only
            valid in live mode.
        """
        if self._is_terminated:
            return
        
        if self.mode not in REALTIME_MODES:
            return

        if not self.is_TRADING_BAR():
            return

        if not isinstance(event_type, AlgoCallBack):
            return
        
        if event_type == AlgoCallBack.RECONCILIATION:
            if param and listlike(param):
                for oid in param:
                    if oid in self._new_orders:
                        self._new_orders.remove(oid)
            return self.handle_reconciliation_event(param, timestamp)
        
        if event_type in (AlgoCallBack.IDLE, AlgoCallBack.ONECLICK, 
                          AlgoCallBack.EXTERNAL):
            # this is not a user function, we explicitly handle this
            # through the run_idle function internally or call on_oneclick
            # or on_update explicitly in the event loop
            return
        
        if event_type == AlgoCallBack.CONNECT:
            try:
                self.log_warning(f'Broker re-connect event received on channels {param}.')
                self._on_connect(self.context)
            except Exception as e:
                self.log_error(f'Failed to run on on connect routine:{str(e)}.')
            return
        
        if event_type == AlgoCallBack.DISCONNECT:
            try:
                self.log_warning(f'Broker disconnect event received on channels {param}.')
                self._on_disconnect(self.context)
            except Exception as e:
                self.log_error(f'Failed to run on on disconnect routine:{str(e)}.')
            return
            
        if param and event_type == AlgoCallBack.TRADE:
            for oid in param:
                if oid in self._new_orders:
                    self._new_orders.remove(oid)
            self.log_info(f'{self.name}: got event update for order ID {param}')
        
        # mark require recon for paper blotter for data packets
        if self.mode == MODE.PAPER and event_type==AlgoCallBack.DATA:
            self.context.blotter.needs_reconciliation = True
            
        # put an idle packet if the event loop is empty. This is used
        # for running idle jobs like paper simulate or sl/tp checks
        if self.mode in REALTIME_MODES and event_type!=AlgoCallBack.IDLE:
            self.schedule_idle()
            
        if self.is_paused():
            return

        # in case of a trade update, force a blotter reconciliation
        # TODO: check and and verify thread lock makes this safe
        if event_type == AlgoCallBack.TRADE:
            self.log_debug(f'got blueshift order event update -> {param}')
            # this is identical to risk monitor reconcile, and expected to be
            # a nothing op -> but it is here just in case the order of 
            # monitor reconcile and handle event is not as intended
            if self.mode in LIVE_MODES:
                self.reconcile_all(timestamp, emit_log=False, forced=True,
                    exit_time=False, fetch_orders=False, only_txns=True, 
                    simulate=False)
                
            self.check_algo_orders()

        # call the user defined function
        self._handle_event(
                self.context, self.context.data_portal,
                event_type.name.lower())
        
        if event_type == AlgoCallBack.DATA:
            self._on_data(self.context, self.context.data_portal)
        elif event_type == AlgoCallBack.TRADE:
            self._on_trade(self.context, self.context.data_portal)
        
        # handle the added strategy
        strategies = list(self._strategies)
        for name in strategies:
            strategy = self._strategies[name]
            try:
                with self.context.switch_context(name) as context:
                    try:
                        if event_type == AlgoCallBack.TRADE:
                            strategy._on_trade(
                                    context, context.data_portal)
                        elif event_type == AlgoCallBack.DATA:
                            strategy._on_data(
                                    context, context.data_portal)
                    except Exception as e:
                        self.handle_sub_strategy_error(e, name)
            except NoContextError:
                # this always run in the main thread, we should not be here
                pass

    def after_trading_hours(self, timestamp:pd.Timestamp):
        """
            Called at the end of the session.
        """
        try:
            self.fsm_after_trading_hours()
        except MachineError:
            msg = f"State Machine Error ({self.state}): in after_trading_hours"
            raise StateMachineError(msg=msg)
            
        self._scheduler.eod_reset()

        self._after_trading_hours(self.context,
                                  self.context.data_portal)
        
        # handle the added strategy
        strategies = list(self._strategies)
        for name in strategies:
            strategy = self._strategies[name]
            try:
                with self.context.switch_context(name) as context:
                    try:
                        strategy._after_trading_hours(
                                context, context.data_portal)
                    except Exception as e:
                        self.handle_sub_strategy_error(e, name)
            except NoContextError:
                # this always run in the main thread, we should not be here
                pass

    def analyze(self,timestamp:pd.Timestamp):
        """
            Called at the end of the algo run.
        """
        try:
            self.fsm_analyze()
        except MachineError:
            msg=f"State Machine Error ({self.state}): in analyze"
            raise StateMachineError(msg=msg)

        self._end_hooks(
                self.context, self._env, get_config(self._env.algo_user))
        perfs = self.context.blotter.perfs_history
        self._analyze(self.context, perfs)
        
        # handle the added strategy
        strategies = list(self._strategies)
        for name in strategies:
            strategy = self._strategies[name]
            try:
                with self.context.switch_context(name) as context:
                    try:
                        perfs = context.blotter.perfs_history
                        strategy._analyze(context, perfs)
                    except Exception as e:
                        self.handle_sub_strategy_error(e, name)
            except NoContextError:
                # this always run in the main thread, we should not be here
                pass

    def heartbeat(self, timestamp:pd.Timestamp):
        """
            Called when we are not in a session.
        """
        try:
            self.fsm_heartbeat()
        except MachineError:
            msg=f"State Machine Error ({self.state}): in heartbeat"
            raise StateMachineError(msg=msg)

        self._heartbeat(self.context)
        
        # handle the added strategy
        strategies = list(self._strategies)
        for name in strategies:
            strategy = self._strategies[name]
            try:
                with self.context.switch_context(name) as context:
                    try:
                        strategy._heartbeat(context)
                    except Exception as e:
                        self.handle_sub_strategy_error(e, name)
            except NoContextError:
                # this always run in the main thread, we should not be here
                pass

    def _start_hooks(self, context:IContext, env:TradingEnvironment, config:BlueshiftConfig, 
                     timestamp:pd.Timestamp):
        risk_management = config.risk_management
        
        if risk_management.get('max_order_per_day'):
            qty = risk_management['max_order_per_day']
            try:
                qty = int(qty)
            except (ValueError, TypeError):
                pass
            else:
                self.set_max_order_count(qty)
                msg = f"risk management: enabled max order count limit"
                msg = msg + f" at {qty} per day."
                self.log_info(msg)

        if risk_management.get('max_order_qty'):
            qty = risk_management['max_order_qty']
            try:
                qty = int(qty)
            except (ValueError, TypeError):
                pass
            else:
                self.set_max_order_size(assets=None, max_quantity=qty)
                msg = f"risk management: enabled max order size limit"
                msg = msg + f" at {qty} units per order."
                self.log_info(msg)
                
        # cool is floored at 1 minute
        cooloff = risk_management.get('cooloff_period', DEFAULT_COOLOFF)
        cooloff = max(1, cooloff)
        self._exits_handler.reset()
        self._exits_handler.set_cooloff_period(cooloff)
        msg = f"risk management: enabled exit cool-off period "
        msg += f"for {cooloff} minutes."
        self.log_info(msg)
        
        if env.timeout and self.mode == MODE.BACKTEST:
            msg = f"resource limits: enabled max run-time "
            msg += f"of {env.timeout} seconds."
            self.log_info(msg)

        if self.execution_mode == ExecutionMode.ONECLICK:
            self._notification_control = TCNotificationNumPerDay(self._MAX_NOTIFICATIONS)

        if env.restart:
            msg = "restarting algo with last saved context, "
            msg = msg + "if available."
            self.log_info(msg)
            context.read(timestamp)
            context.blotter.check_on_restart()

        # mark blueshift is in ready mode - this is for health check.
#        update_blueshift_state('ready',self.name)

    def _end_hooks(self, context, env, config):
        pass

    def _back_test_generator(self, quick:bool=False, alert_manager:BlueshiftAlertManager|None=None,
                             show_progress:bool=False) -> Generator[tuple[MessageType, Any, Any]]:
        """
            The entry point for backtest run. This generator yields
            the current day performance.
        """
        if self.mode != MODE.BACKTEST:
            raise StateMachineError(msg="mode must be back-test")

        if not self.context.is_initialized:
            raise InitializationError(msg="context is not "
                                      "properly initialized")

        if not isinstance(self.context.clock, SimulationClock):
            raise ValidationError(msg="clock must be simulation clock")

        self._make_broker_dispatch() # only useful for backtest

        dts, bars = tuple(list(zip(*[(t,bar) for t,bar in self.context.clock])))
        dts = pd.DatetimeIndex(pd.to_datetime(dts))
        dts = dts.tz_localize('Etc/UTC').tz_convert(
                self.context.trading_calendar.tz)
        
        if show_progress:
            try:
                import tqdm # type: ignore -> optional dependency
            except ImportError:
                show_progress = False

        if show_progress:
            import tqdm # type: ignore -> optional dependency
            events = tqdm.tqdm(zip(dts, bars), total=len(dts))
        else:
            events = zip(dts, bars)
        
        total_sessions = len(self.context.clock.session_nanos)
        session_count = 0
        
        # timeout is only implemented for backtest, the stop time 
        # is given by the end date in case of live execution
        start_time = 0
        timeout = self._env.timeout
        if timeout != 0:
            start_time = time.time()
            
        # no minute level performance packets for backtest
        # packets are emitted only end-of-day
        for ts, bar in events:
            try:
                self._BROKER_FUNC_DISPATCH.get(bar,self._bar_noop)(ts)
                self.context.timestamp = ts
                
                if self.is_STOPPED():
                    msg = "Algorithm stopped, will exit."
                    self.log_platform(msg,timestamp=ts)
                    raise TerminationError(msg)
                
                if self.context.clock.is_terminated:
                    # clock terminated, process algo end
                    bar = BARS.ALGO_END
                    if self._desired_status == AlgoStatus.CANCELLED:
                        msg = "Algorithm cancelled by user, will exit."
                        raise UserStopError(msg)
                    elif self._desired_status in (AlgoStatus.STOPPED, AlgoStatus.REJECTED):
                        msg = "Algorithm stopped, will exit."
                        raise TerminationError(msg)

                if bar == BARS.ALGO_START:
                    self.context.set_up(timestamp=ts)
                elif bar == BARS.BEFORE_TRADING_START:
                    self.context.SOB_update(ts)
                elif bar == BARS.TRADING_BAR or bar == BARS.DAY_END:
                    self.context.BAR_update(ts)
                elif bar == BARS.AFTER_TRADING_HOURS:
                    self.context.EOD_update(ts)
                    
                    progress = None
                    if quick:
                        session_count += 1
                        progress = session_count/total_sessions
                    for packet in self.context.emit_risk_messages(ts, progress):
                        yield MessageType.DAILY, None, packet
                elif bar == BARS.HEART_BEAT:
                    pass
                elif bar == BARS.ALGO_END:
                    msg = "Algo run complete."
                    self._print_verbose(msg)
                    self.log_platform(msg)
                    # we need to finalize the context to send the last packet
                    with np.errstate(invalid='ignore',divide='ignore'):
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore", category=RuntimeWarning)
                            self.context.finalize(ts)
                    
                    for perf in self.context.emit_perf_messages():
                        yield MessageType.COMPLETE, None, perf
                
                # check timeout
                if start_time != 0:
                    elapsed = time.time() - start_time
                    if elapsed > timeout:
                        raise ExecutionTimeout('backtest run has timed out.')
                
                if bar != BARS.DAY_END:
                    # do not run user program on day close
                    with np.errstate(invalid='ignore',divide='ignore'):
                        try:
                            self._USER_FUNC_DISPATCH.get(bar,self._bar_noop)(ts)
                        except (HistoryWindowStartsBeforeData, NoDataForAsset):
                            continue
                if self.context.clock.is_terminated:
                    break
            except ExecutionTimeout:
                if self._env.quick:
                    msg = f'{BLUESHIFT_PRODUCT} run timed-out.'
                    yield MessageType.PLATFORM, msg, None
                msg = self._env.alert_manager.handle_execution_timeout(
                        self)
                raise TerminationError(msg)
            except UserStopError:
                self.set_algo_status(AlgoStatus.CANCELLED)
                msg = f"Algo {self.name} run stopped by user."
                raise TerminationError(msg)
            except TerminationError:
                raise
            except (BlueshiftException, UserDefinedException) as e:
                self._env.alert_manager.handle_backtest_blueshift_error(
                        self, e)
                self.handle_algo_error(e)
                continue
            except Exception as e:
                self._env.alert_manager.handle_backtest_generic_error(
                        self, e)
                self.handle_algo_error(e)

    def _back_test_run(self, quick:bool=False, publish_packets:bool=False,
                      show_progress:bool=False) -> pd.DataFrame:
        alert_manager = self._env.alert_manager
        self.context.clock.reset()
        perfs = []

        publisher_handle = alert_manager.publisher

        with MessageBrokerCtxManager(
                publisher_handle, enabled=publish_packets) as publisher:
            if self._env.redirect:
                show_progress = False
            if publisher is None:
                publish_packets = False
            
            self._env.blueshift_callback(
                    action=BlueshiftCallbackAction.ALGO_STATUS.value,
                    status=AlgoStatus.RUNNING.value,
                    msg=f'Algo {self.name} started running.')
            for message_type, msg, data in self._back_test_generator(quick=quick,
                alert_manager=alert_manager, show_progress=show_progress):
                if message_type == MessageType.DAILY:
                    perfs.append(data)
                if publish_packets:
                    level = 'error' if message_type == MessageType.PLATFORM else None
                    packet = create_message_envelope(
                        self.name, message_type, msg=msg, data=data, level=level)
                    publisher.send_to_topic(self.name, packet) # type: ignore
                    
        if not perfs:
            return pd.DataFrame()
        
        try:
            perfs = pd.DataFrame(perfs)
            perfs = perfs.set_index('timestamp')
            perfs.index = pd.DatetimeIndex(pd.to_datetime(perfs.index, utc=True))
            perfs.index = perfs.index.tz_convert(self.context.trading_calendar.tz) # type: ignore
            return perfs
        except Exception as e:
            msg = f'Failed generating performance after algo run:{str(e)}.'
            self.log_error(msg)
            return pd.DataFrame()

    def _reset_clock(self):
        """
            Reset the realtime clock
        """
        self._queue = ClockQueue(logger=self.logger)
        self.context.clock.reset(self._queue)

    async def _process_tick(self, alert_manager=None) -> AsyncGenerator[tuple[MessageType,Any,Any], None]:
        """
            Process ticks from real clock asynchronously. This generator
            receives a command from the channel, and if it is `continue`,
            continues normal algo loop. Else either pause or stop the algo.
            It can also invoke any member function based on command.
        """
        def _asyncify(f):
            return asyncify(
                    loop=self.loop, executor=self._env.alert_manager.executor)(f)
            
        async_handle_terminate = _asyncify(self.handle_terminate)
        async_run_idle = _asyncify(self.run_idle)
        async_handle_oneclick_event = _asyncify(self.handle_oneclick_event)
        async_handle_external_event = _asyncify(self.handle_external_event)
        async_handle_event = _asyncify(self.handle_event)
        async_handle_timeout = _asyncify(self.handle_timeout)
        
        BROKER_FUNC_ASYNC_DISPATCH = {}
        for k,f in self._BROKER_FUNC_DISPATCH.items():
            BROKER_FUNC_ASYNC_DISPATCH[k] = _asyncify(f)
            
        USER_FUNC_ASYNC_DISPATCH = {}
        for k,f in self._USER_FUNC_DISPATCH.items():
            USER_FUNC_ASYNC_DISPATCH[k] = _asyncify(f)
        
        while True:
            """
                start from the beginning, process all ticks except
                TRADING_BAR or HEART_BEAT. For these we skip to the last.
                See implement this logic in ClockQueue.
            """
            try:
                self._queue = cast(ClockQueue, self._queue)
                try:
                    t, bar = await asyncio.wait_for(self._queue.get_last(), 2*LOCK_TIMEOUT)
                except asyncio.TimeoutError:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    err_msg = get_exception(exc_type, exc_value, exc_traceback)
                    self.logger.info(err_msg)
                    raise TerminationError(f'Timed out fetching events from clock.')
                    
                if t is None and bar is None:
                    continue
                
                ts = pd.Timestamp.now(
                        tz=self.context.trading_calendar.tz)

                if bar != BARS.AFTER_TRADING_HOURS and \
                    not isinstance(bar, (AlgoCallBack, AlgoTimeoutEvent, AlgoTerminateEvent)):
                    # call after trading hours after algo functions
                    f = BROKER_FUNC_ASYNC_DISPATCH.get(bar,None)
                    if f:
                        try:
                            await asyncio.wait_for(f(ts), 2*LOCK_TIMEOUT)
                        except asyncio.TimeoutError:
                            exc_type, exc_value, exc_traceback = sys.exc_info()
                            err_msg = get_exception(exc_type, exc_value, exc_traceback)
                            self.logger.info(err_msg)
                            raise TerminationError(f'Timed out running broker dispatch.')
                    
                self.context.timestamp = ts

                if self.is_STOPPED():
                    msg = "Algorithm stopped, will exit."
                    self.log_platform(msg,ts)
                    raise TerminationError(msg)
                
                if isinstance(bar, AlgoTerminateEvent):
                    callback = t
                    f = async_handle_terminate
                    try:
                        await asyncio.wait_for(f(bar, ts, callback), 2*LOCK_TIMEOUT)
                    except asyncio.TimeoutError:
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        err_msg = get_exception(exc_type, exc_value, exc_traceback)
                        self.logger.info(err_msg)
                        raise TerminationError(f'Timed out handling terminate event.')
                    if bar == AlgoTerminateEvent.KILL:
                        self._skip_exit_on_kill = True
                        msg = "Algorithm killed, will exit."
                        self.log_platform(msg,ts)
                        raise TerminationError(msg)
                    if bar == AlgoTerminateEvent.STOP:
                        msg = "Algo run stopped, will exit."
                        self.log_platform(msg,ts)
                        raise TerminationError(msg)
                    if bar == AlgoTerminateEvent.REJECT:
                        msg = "Smart order rejected, will exit."
                        self.log_platform(msg,ts)
                        raise TerminationError(msg)
                    if bar == AlgoTerminateEvent.DONE:
                        # give a chance to complete scheduled tasks
                        await asyncio.sleep(0)
                        if self.context.clock.is_terminated:
                            self._desired_status = AlgoStatus.DONE
                        else:
                            msg = "Algorithm cancelled by user, will exit."
                            raise UserStopError(msg)
                    else:
                        msg = "Algo run cancelled, will exit."
                        self.log_platform(msg,ts)
                        raise TerminationError(msg)
                        
                if self.context.clock.is_terminated:
                    # clock terminated, process algo end
                    #await asyncio.sleep(0)
                    bar = BARS.ALGO_END
                    if self._desired_status == AlgoStatus.CANCELLED:
                        msg = "Algorithm cancelled by user, will exit."
                        raise UserStopError(msg)
                    elif self._desired_status in (AlgoStatus.STOPPED, AlgoStatus.REJECTED):
                        msg = "Algorithm stopped, will exit."
                        raise TerminationError(msg)
                        
                if bar == BARS.ALGO_START:
                    msg = "Algorithm setup complete. Executing initialize routines."
                    self._print_verbose(msg)
                    self.log_platform(msg)
                    self.context.set_up(timestamp=ts)
                    if self.mode == MODE.EXECUTION and self._env.server_mode:
                        # callback to say we are ready to handle
                        # smart orders requests now
                        self._env.blueshift_callback(
                                action=BlueshiftCallbackAction.SMART_ORDER_STATUS.value,
                                status=AlgoStatus.OPEN.value,
                                msg=f'Smart order {self.name} is now open.')
                    elif self._env.server_mode:
                        self._env.blueshift_callback(
                                action=BlueshiftCallbackAction.ALGO_STATUS.value,
                                status=AlgoStatus.RUNNING.value,
                                msg=f'Algo {self.name} started running.')
                    yield MessageType.INIT, None, None
                    await asyncio.sleep(0)
                elif bar == BARS.BEFORE_TRADING_START:
                    msg = "Executing before trading start routines."
                    self._print_verbose(msg)
                    self.log_platform(msg)
                    self.context.SOB_update(ts)
                    yield MessageType.SOB, None, None
                    await asyncio.sleep(0)
                elif bar == BARS.TRADING_BAR:
                    self.context.BAR_update(ts)
                    for packet in self.context.emit_perf_messages():
                        yield MessageType.BAR, None, packet
                    await asyncio.sleep(0)
                elif bar == AlgoCallBack.IDLE:
                    f = async_run_idle
                    try:
                        await asyncio.wait_for(f(ts), 2*LOCK_TIMEOUT)
                    except asyncio.TimeoutError:
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        err_msg = get_exception(exc_type, exc_value, exc_traceback)
                        self.logger.info(err_msg)
                        raise TerminationError(f'Timed out running idle routine.')
                    continue
                elif bar == BARS.AFTER_TRADING_HOURS:
                    msg = "Executing after trading hours routines"
                    self._print_verbose(msg)
                    self.log_platform(msg)
                    self.context.EOD_update(ts)
                    
                    # call after trading hours now
                    f = BROKER_FUNC_ASYNC_DISPATCH.get(bar,None)
                    if f:
                        try:
                            await asyncio.wait_for(f(ts), 2*LOCK_TIMEOUT)
                        except asyncio.TimeoutError:
                            exc_type, exc_value, exc_traceback = sys.exc_info()
                            err_msg = get_exception(exc_type, exc_value, exc_traceback)
                            self.logger.info(err_msg)
                            raise TerminationError(f'Timed out running broker after trading routine.')
                    
                    if self._env.server_mode:
                        # callback to local blueshift server
                        self._env.blueshift_callback(
                                action=BlueshiftCallbackAction.EOD_UPLOAD.value,
                                status=AlgoStatus.RUNNING.value,
                                msg=f'Algo {self.name} requested EOD data upload.')
                      
                    for packet in self.context.emit_risk_messages(ts):
                        yield MessageType.DAILY, None, packet
                    
                    await asyncio.sleep(0)
                elif bar == BARS.ALGO_END:
                    await asyncio.sleep(0)
                    msg = "Algorithm run complete. Will shut down."
                    self._print_verbose(msg)
                    self.log_platform(msg)
                    # we need to finalize the context to send the last packet
                    with np.errstate(invalid='ignore',divide='ignore'):
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore", category=RuntimeWarning)
                            self.context.finalize(ts)
                    
                    for packet in self.context.emit_risk_messages(ts):
                        yield MessageType.COMPLETE, None, packet
                else:
                    yield MessageType.HEARTBEAT, None, None

                # be nice a give a change to others
                await asyncio.sleep(0)
                
                # run user defined handlers, no run if paused
                if bar == AlgoCallBack.ONECLICK and self.is_running():
                    notifications = t
                    f = async_handle_oneclick_event
                    with np.errstate(invalid='ignore',divide='ignore'):
                        try:
                            await asyncio.wait_for(f(notifications, ts), 2*LOCK_TIMEOUT)
                        except asyncio.TimeoutError:
                            exc_type, exc_value, exc_traceback = sys.exc_info()
                            err_msg = get_exception(exc_type, exc_value, exc_traceback)
                            self.logger.info(err_msg)
                            raise TerminationError(f'Timed out while handling oneclick notifications')
                elif bar == AlgoCallBack.EXTERNAL and self.is_running():
                    params = t
                    f = async_handle_external_event
                    with np.errstate(invalid='ignore',divide='ignore'):
                        try:
                            await asyncio.wait_for(f(params, ts), 2*LOCK_TIMEOUT)
                        except asyncio.TimeoutError:
                            exc_type, exc_value, exc_traceback = sys.exc_info()
                            err_msg = get_exception(exc_type, exc_value, exc_traceback)
                            self.logger.info(err_msg)
                            raise TerminationError(f'Timed out while handling user event')
                elif isinstance(bar, AlgoCallBack):
                    # pausing handled in handle_event
                    param = t
                    f = async_handle_event
                    with np.errstate(invalid='ignore',divide='ignore'):
                        try:
                            await asyncio.wait_for(f(bar, ts, param), 2*LOCK_TIMEOUT)
                        except asyncio.TimeoutError:
                            exc_type, exc_value, exc_traceback = sys.exc_info()
                            err_msg = get_exception(exc_type, exc_value, exc_traceback)
                            self.logger.info(err_msg)
                            raise TerminationError(f'Timed out while handling events')
                            
                elif isinstance(bar, AlgoTimeoutEvent):
                    if len(t) == 4 and callable(t[0]): # type: ignore -> t is not None for timeout evt
                        callback, context, delay, id_ = t # type: ignore
                        if delay in (None, 0) and self.is_running():
                            # no running of callbacks if paused
                            f = async_handle_timeout
                            with np.errstate(invalid='ignore',divide='ignore'):
                                try:
                                    await asyncio.wait_for(f(bar, ts, context, callback, id_), 2*LOCK_TIMEOUT)
                                except asyncio.TimeoutError:
                                    exc_type, exc_value, exc_traceback = sys.exc_info()
                                    err_msg = get_exception(exc_type, exc_value, exc_traceback)
                                    self.logger.info(err_msg)
                                    raise TerminationError(f'Timed out while handling scheduled events')
                                    
                        else:
                            # but allow adding callback even if paused
                            delay = cast(float, delay)
                            f = partial(
                                    self._tickle, context, callback, id_)
                            self.run_later(abs(delay), f, id_)
                else:
                    # pause handled in handle data, else user func
                    # can run
                    f = USER_FUNC_ASYNC_DISPATCH.get(bar,None)
                    if f:
                        with np.errstate(invalid='ignore',divide='ignore'):
                            try:
                                await asyncio.wait_for(f(ts), 2*LOCK_TIMEOUT)
                            except asyncio.TimeoutError:
                                exc_type, exc_value, exc_traceback = sys.exc_info()
                                err_msg = get_exception(exc_type, exc_value, exc_traceback)
                                self.logger.info(err_msg)
                                raise TerminationError(f'Timed out while running user routine.')
                            
                # allow other handlers
                await asyncio.sleep(0)
                
                if bar == BARS.ALGO_END:
                    # add some random sleep to unsync algos stopping at 
                    # market close, not applicable for smart orders
                    if self.mode != MODE.EXECUTION:
                        await asyncio.sleep(random.randint(0,10))
                    return
            except asyncio.CancelledError:
                msg = await self._env.alert_manager.handle_asyc_cancelled(
                        self)
                yield MessageType.DEBUG, msg, None
                raise TerminationError(msg)
            except TerminationError:
                raise
            except (BlueshiftException, UserDefinedException) as e:
                msg = await self._env.alert_manager.handle_blueshift_async_exception(
                        self, e)
                yield MessageType.DEBUG, msg, None
                msg = f'Algo {self.name} encountered an error, but recovered to continue.'
                self.log_warning(msg)
                self.handle_algo_error(e)
                continue
            except Exception as e:
                msg = await self._env.alert_manager.handle_async_exception(
                        self, e)
                yield MessageType.DEBUG, msg, None
                msg = f'Algo {self.name} encountered an exception, but recovered to continue.'
                self.log_warning(msg)
                self.handle_algo_error(e)
                continue # continue unless re-raised
            else:
                # a clean iteration of the loop. reset exception trackers
                if bar == BARS.TRADING_BAR and alert_manager:
                    alert_manager.reset_error_tracker(self.name)

    async def _run_live(self, publish_packets=False):
        """
            Function to run the main algo loop generator.
        """
        publisher_handle = self._env.alert_manager.publisher
        
        try:
            with MessageBrokerCtxManager(publisher_handle,
                                         enabled=publish_packets) as publisher:
                if publisher is None: 
                    publish_packets = False
                g = self._process_tick(self._env.alert_manager)
                
                async for message_type, msg, data in g:
                    try:
                        if publish_packets:
                            packet = create_message_envelope(self.name, message_type, msg=msg, data=data)
                            publisher.send_to_topic(self.name, packet) # type: ignore -> publisher not None
                    except StopAsyncIteration:
                        break
        except Exception as err:
            try:
                self._live_run_handle_error(err)
            except Exception as e:
                msg = f'Algo run for {self.name} terminated:{str(e)}.'
                if not self.is_finalized:
                    if not self.completion_msg:
                        self.completion_msg = get_clean_err_msg(msg)
                        
                    # status cannot be done here with error
                    if self._desired_status == AlgoStatus.DONE:
                        # status cannot be done here with error
                        self.set_algo_status(AlgoStatus.ERRORED)
        finally:
            if not self.is_finalized:
                if not self.completion_msg:
                    self.completion_msg = f'Algo {self.name} run complete.'
                    
                # try algo finalize async
                try:
                    await asyncio.wait_for(self.async_finalize(self.context.timestamp), 2*LOCK_TIMEOUT)
                except asyncio.TimeoutError:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    err_msg = get_exception(exc_type, exc_value, exc_traceback)
                    self.logger.info(err_msg)
                    self.log_error(f'algo {self.name} finalize timed out.')
                except Exception as e:
                    self.log_error(f'Failed to finalize algo {self.name}:{str(e)}.')
                finally:
                    self._handle_algo_exit(None)

    def _live_run(self, publish_packets:bool=False, skip_wait:bool=False):
        """
            The entry point for a live run.
        """
        if self.mode not in REALTIME_MODES:
            raise StateMachineError(msg="mode must be live, paper or execution.")

        if not self.context.is_initialized:
            raise InitializationError(msg="context is not "
                                      "properly initialized")

        if not isinstance(self.context.clock, RealtimeClock):
            raise ValidationError(msg="clock must be real-time clock")
        self._reset_clock()
        
        risk_management = get_config(self._env.algo_user).risk_management
        self.monitor = RiskMonitor(
                self, 
                alert_manager=self._env.alert_manager,
                risk_management=risk_management)

        # initialize the coroutines
        clock_coro = self.context.clock.tick()
        algo_coro = self._run_live(publish_packets)
        risk_coro = self.monitor.watch()
        coros_to_run = [algo_coro, clock_coro, risk_coro]
        
        tasks = [asyncio.ensure_future(
                coro, loop=self.loop) for coro in coros_to_run]
    
        self._algo_coro = tasks[0]
        self._clock_coro = tasks[1]
        self._risk_coro = tasks[2]
        
        for t in tasks:
            self.TASKS_TO_CANCEL_ON_EXCEPTIONS.add(t)

        try:
            task = asyncio.gather(
                    *tasks, return_exceptions=False)
            
            if not skip_wait and not self.loop.is_running():
                # we wait for the task to compete here
                self.loop.run_until_complete(task)
                msg = f"Algo {self.name} run completed."
            else:
                # task is run in the loop, add the done callback
                task.add_done_callback(self._live_run_callback)
                msg = f"Algo {self.name} run has started."
            self.log_info(msg)
        except BaseException as e:
            self._live_run_handle_error(e)
    
    def _live_run_cancel_tasks(self):
        try:
            for task in self.TASKS_TO_CANCEL_ON_EXCEPTIONS:
                try:
                    task.cancel()
                except Exception:
                    pass
        except Exception:
            pass
                
    def _live_run_handle_error(self, e:BaseException):
        if isinstance(e, asyncio.CancelledError):
            self.set_algo_status(AlgoStatus.CANCELLED)
            msg = f"Algo run for {self.name} terminated."
            raise TerminationError(msg)
        elif isinstance(e, UserStopError):
            self.set_algo_status(AlgoStatus.CANCELLED)
            msg = f"Algo {self.name} run stopped by user."
            raise TerminationError(msg)
        elif isinstance(e, TerminationError):
            self.set_algo_status(AlgoStatus.ERRORED)
            msg = f"Algo run for {self.name} terminated:{str(e)}"
            raise TerminationError(msg)
        elif isinstance(e, BaseException):
            # TODO: do a proper exception handling here
            if str(e) == "0":
                # we have a sys exit with status 0
                # we should not be here
                msg = f"Algo run for {self.name} terminated."
                self.log_error(msg)
                raise TerminationError(msg)
            else:
                msg = f"Algo {self.name} run failed in error: {str(e)}"
                raise TerminationError(msg)
                
    def _live_run_callback(self, task):
        asyncio.create_task(self._async_live_run_callback(task))
                
    async def _async_live_run_callback(self, task:asyncio.tasks.Task):
        try:
            # just in case some exception
            try:
                task.exception()
            except Exception:
                # this can raise asyncio.exceptions.InvalidStateError
                pass
            
            # this will raise exception if the gather raised any
            try:
                task.result()
            except BaseException as e:
                self._live_run_handle_error(e)
            else:
                msg = f"Algo {self.name} run completed."
                self.log_info(msg)
        except Exception as e:
            msg = f'Algo run for {self.name} terminated:{str(e)}.'
            if not self.is_finalized:
                if not self.completion_msg:
                    self.completion_msg = get_clean_err_msg(msg)
                    
                # status cannot be done here with error
                if self._desired_status == AlgoStatus.DONE:
                    # status cannot be done here with error
                    self.set_algo_status(AlgoStatus.ERRORED)
        finally:
            # algo context is already finalized in the ALGO_END bar
            if not self.is_finalized:
                if not self.completion_msg:
                    self.completion_msg = f'Algo {self.name} run complete.'
                    
                # try algo finalize
                try:
                    await asyncio.wait_for(self.async_finalize(self.context.timestamp), 2*LOCK_TIMEOUT)
                except asyncio.TimeoutError:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    err_msg = get_exception(exc_type, exc_value, exc_traceback)
                    self.logger.info(err_msg)
                    self.log_error(f'algo {self.name} finalize timed out.')
                except Exception as e:
                    self.log_error(f'Failed to finalize algo {self.name}:{str(e)}.')
                finally:
                    self._handle_algo_exit(None)

    def run(self, show_progress:bool=False, skip_wait:bool=False):
        """
            Single entry point for both backtest and live runs.
            
            :param bool show_progress: Turns on or off a progress bar (only for backtesting on console).
            :param bool skip_wait: Whether to start the eventloop.
        """
        if self._status == AlgoStatus.RUNNING:
            msg = f'Algo {self.name} already running.'
            self.log_warning(msg)
            return
        
        if self.is_finalized:
            msg = f'Algo {self.name} already terminated, cannot run again.'
            self.log_error(msg)
            return
            
        publish = self._env.publish
        self._status = AlgoStatus.RUNNING
        self._desired_status = AlgoStatus.DONE
        
        algo_name = self.env.algo_moniker or os_path.basename(self._env.algo_file)
        
        try:
            if self.mode in REALTIME_MODES:
                if self.mode == MODE.LIVE:
                    msg = "starting LIVE with algo:"+ algo_name +\
                            ", engine:" + BLUESHIFT_DISPLAY_VERSION +\
                            ", broker:" + self.context.broker.name + ", timezone:" +\
                            str(self.context.trading_calendar.tz) + "\n"
                elif self.mode == MODE.PAPER:
                    msg = "starting simulalted paper trading with algo:"+ algo_name +\
                        ", engine:" + BLUESHIFT_DISPLAY_VERSION +\
                        ", broker:" + self.context.broker.name + ", timezone:" +\
                        str(self.context.trading_calendar.tz) + "\n"
                else:
                    msg = "Smart Execution with algo:"+ algo_name +\
                        ", engine:" + BLUESHIFT_DISPLAY_VERSION +\
                        ", broker:" + self.context.broker.name + ", timezone:" +\
                        str(self.context.trading_calendar.tz) + "\n"
                
                self._env.alert_manager.print_info(
                        msg, name=self.name, level="info2", msg_type=MessageType.PLATFORM)
                self.log_info(msg)
                # this will return immediately if skip_wait is False
                self._live_run(publish,skip_wait=skip_wait) # no progress bar for live
            elif self.mode == MODE.BACKTEST:
                if self._env.quick:
                    msg = "starting quick backtest with algo:"+ algo_name +\
                            ", engine:" + BLUESHIFT_DISPLAY_VERSION +\
                            ", broker:" + self.context.broker.name + ", timezone:" +\
                            str(self.context.trading_calendar.tz) + "\n"
                else:
                    msg = "starting backtest with algo:"+ algo_name +\
                            ", engine:" + BLUESHIFT_DISPLAY_VERSION +\
                            ", broker:" + self.context.broker.name + ", timezone:" +\
                            str(self.context.trading_calendar.tz) + "\n"
                
                clock = cast(SimulationClock, self.context.clock)
                sessions = clock.session_nanos
                length = len(sessions)
                tz = self.context.trading_calendar.tz
                
                self._env.alert_manager.print_info(msg, name=self.name, level='info2')
                self.log_info(msg)
                t1 = time.time()
                
                perfs = self._back_test_run(
                        quick=self._env.quick, publish_packets=publish, 
                        show_progress=show_progress)
                
                t2 = time.time()
                elapsed = round(t2-t1,2)
                elapsed = str(datetime.timedelta(seconds=elapsed))
                msg = f"backtest run complete, time taken {elapsed}."
                self._env.alert_manager.print_info(msg, name=self.name, level='info')
                self.log_info(msg)
                
                simulated = len(perfs)      # one packet for every session
                self.logger.set_levels(logging.INFO)
                msg = f'Simulated {simulated} sessions out of {length}.' 
                self.logger.info(msg, mode=MODE.BACKTEST, timestamp=pd.Timestamp.now())
                msg = f'first open: {pd.Timestamp(sessions[0], tz=tz)}'
                self.logger.info(msg, mode=MODE.BACKTEST, timestamp=pd.Timestamp.now())
                msg = f'last simulated: {pd.Timestamp(sessions[simulated-1], tz=tz)}'
                self.logger.info(msg, mode=MODE.BACKTEST, timestamp=pd.Timestamp.now())
                self.logger.restore_levels()
                
                return perfs
            else:
                self.set_algo_status(AlgoStatus.ERRORED)
                raise StateMachineError(msg="undefined mode, cannot run algo.")
        except Exception as e:
            msg = f'Algo run for {self.name} terminated:{str(e)}.'
            if not self.is_finalized:
                skip_wait = False
                if not self.completion_msg:
                    self.completion_msg = get_clean_err_msg(msg)
                if self._desired_status == AlgoStatus.DONE:
                    # status cannot be done here with error
                    self.set_algo_status(AlgoStatus.ERRORED)
        finally:
            if skip_wait:
                # the task done callback handles this
                return
            
            # algo is already finalized in the ALGO_END bar
            if not self.is_finalized:
                if not self.completion_msg:
                    self.completion_msg = f'Algo {self.name} run complete.'
                    
                # try algo finalize
                try:
                    self.finalize(self.context.timestamp)
                except Exception as e:
                    self.log_error(f'Failed to finalize algo {self.name}:{str(e)}.')
                finally:
                    self._handle_algo_exit(None)

    def set_logger(self, logger:BlueshiftLogger|None=None):
        """ set up the logger. """
        if logger and isinstance(logger, BlueshiftLogger):
            self._logger = logger
        else:
            self._logger = self._env.logger

        self._logger.tz = self.context.trading_calendar.tz
        
    @api_method
    def log_info(self, msg:str, timestamp:pd.Timestamp|None=None):
        """ 
            Utility function for logging user message with log level 
            set at INFO.
        """
        if not timestamp and self.mode == MODE.BACKTEST:
            timestamp = self.context.timestamp
        
        if timestamp:
            self.logger.info(msg,"algorithm", timestamp=timestamp,
                              mode=self.mode)
        else:
            self.logger.info(msg,"algorithm")
            
    def log_debug(self, msg:str, timestamp:pd.Timestamp|None=None):
        if BLUESHIFT_DEBUG_MODE:
            self.log_info(msg, timestamp=timestamp)

    def log_platform(self, msg:str, timestamp:pd.Timestamp|None=None):
        if not timestamp and self.mode == MODE.BACKTEST:
            timestamp = self.context.timestamp
        if timestamp:
            self.logger.platform(msg,"algorithm", timestamp=timestamp,
                              mode=self.mode)
        else:
            self.logger.platform(msg,"algorithm")
            
    @api_method
    def log_warning(self, msg:str, timestamp:pd.Timestamp|None=None):
        """ 
            Utility function for logging user message with log level 
            set at WARNING.
        """
        if not timestamp and self.mode == MODE.BACKTEST:
            timestamp = self.context.timestamp
        if timestamp:
            self.logger.warning(msg,"algorithm", timestamp=timestamp,
                                 mode=self.mode)
        else:
            self.logger.warning(msg,"algorithm")

    @api_method
    def log_error(self, msg, timestamp=None):
        """ 
            Utility function for logging user message with log level 
            set at ERROR.
        """
        if not timestamp and self.mode == MODE.BACKTEST:
            timestamp = self.context.timestamp
        if timestamp:
            self.logger.error(msg,"algorithm", timestamp=timestamp,
                               mode=self.mode)
        else:
            self.logger.error(msg,"algorithm")
            
    def close(self, timestamp:pd.Timestamp|None, *args, **kwargs):
        msg = f'Closing the algo {self.name} and releasing resources.'
        self.finalize(timestamp, msg=msg, *args, **kwargs)
        
    def _handle_algo_exit(self, timestamp:pd.Timestamp|None, *args, **kwargs):
        # skip running if is_finalized True or is_finalize_in_progress > 0
        # the later means some finaluze is already going on
        if self._is_finalized or self._is_finalize_in_progress > 0:
            self.log_info(f'skipping algo exit routine as it is already run or running.')
            return
        
        self._wants_to_quit:bool = True # try to exit pending executor jobs
        
        if not timestamp and self.mode in REALTIME_MODES:
            timestamp = pd.Timestamp.now(tz=self.context.trading_calendar.tz)
            
        if timestamp:
            try:
                self.context.blotter.finalize(timestamp, exit_time=True)
                try:
                    if self.mode in (MODE.LIVE, MODE.PAPER):
                        self.context.blotter.emit_valuation_packets(timestamp)
                        self.context.blotter.emit_positions_packets(timestamp, True) 
                        self.context.blotter.emit_txns_packets(timestamp, True)
                    elif self.mode == MODE.EXECUTION:
                        self.context.blotter.emit_perf_msg(timestamp)
                except Exception as e:
                    self.logger.info(f'failed to emit final valuation packets: {str(e)}')
            except Exception as e:
                msg = f'Failed to finalize algo blotter: {str(e)}.'
                self.log_error(msg)
            else:
                msg = f'Updated blotter for exit cancellation/ square-off (if any).'
                self.log_info(msg)
        
        # make sure both clock and algo coro is done
        self._live_run_cancel_tasks()
        
        # shutdown the clock
        try:
            self.context.clock.terminate()
        except Exception:
            pass
        
        # shutdown risk monitor
        if self.monitor:
            try:
                self.monitor.stop()
            except Exception:
                pass
        
            try:
                self.monitor.finalize()
            except Exception:
                pass
        
        # run the callbacks that others might have schedule, this 
        # must run BEFORE the context is finalized on error. Else, 
        # the context is finalized on ALGO END anyways
        try:
            self.log_info(f'executing registered callbacks before exit...')
            self._env.alert_manager.run_callbacks(self, timestamp)
        except Exception as e:
            msg = f'Failed to run registered callbacks for algo: {str(e)}.'
            self.log_error(msg)
        
        # update the exit status and call on_cancel if required
        try:
            if self._desired_status == AlgoStatus.ERRORED:
                # on error is triggered by alert manager, but it is
                # idempotent, so we can call again
                self._status = AlgoStatus.ERRORED
                if timestamp:
                    self.on_error(self.completion_msg)
            elif self._desired_status == AlgoStatus.KILLED:
                self._status = AlgoStatus.KILLED
            elif self._desired_status == AlgoStatus.STOPPED:
                # no on_error routine for kill
                self._status = AlgoStatus.STOPPED
                if timestamp:
                    self.on_error(self.completion_msg)
            elif self._desired_status == AlgoStatus.CANCELLED:
                self._status = AlgoStatus.CANCELLED
                if timestamp:
                    self.on_cancel()
            elif self._desired_status == AlgoStatus.REJECTED:
                self._status = AlgoStatus.REJECTED
                if timestamp:
                    self.on_error(self.completion_msg)
            else:
                self._status = AlgoStatus.DONE
                self.on_exit()
            
            # finalize the context, this is idempotent
            if timestamp:
                with np.errstate(invalid='ignore',divide='ignore'):
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore", category=RuntimeWarning)
                            try:
                                self.context.finalize(
                                        timestamp, *args, **kwargs)
                            except Exception as e:
                                msg = f'Failed to finalize context for algo:{str(e)}.'
                                self.log_error(msg)
            
            # make sure the algo state is stopped
            try:
                self.fsm_stop()
            except Exception:
                pass
                                
            # finally finalize the env, this is also idempotent
            try:
                if self.mode in REALTIME_MODES:
                    msg_type='info'
                    if self._status in (AlgoStatus.REJECTED, 
                                        AlgoStatus.ERRORED,
                                        AlgoStatus.KILLED,
                                        AlgoStatus.STOPPED):
                        msg_type='error'
                        
                    # update the algo manual intervention check
                    if not self.manual_intervention:
                        try:
                            open_orders = self.context.blotter.get_open_orders()
                            open_positions = self.context.blotter.get_open_positions()
                            case1 = self._squareoff_on_exit and open_positions
                            case2 = self._cancel_orders_on_exit and open_orders
                            case3 = self._squareoff_on_exit and open_orders
                            case4 = self._err_on_square_off
                            
                            if case1 or case2 or case3:
                                msg = f'the algo exited but have some open '
                                msg += f'orders and/or positions - please take required '
                                msg += f'actions from your broker account.'
                                msg_type='error'
                                self.set_intervention_msg(msg)
                            elif case4:
                                msg = f'some square-off order(s) failed - please take required '
                                msg += f'actions from your broker account.'
                                msg_type='error'
                                self.set_intervention_msg(msg)
                        except Exception:
                            pass
                    
                    self.send_completion_msg(self.completion_msg, msg_type=msg_type)
                
                self._env.finalize(
                        self.context.timestamp, msg=self.completion_msg)
            except Exception as e:
                msg = f'Failed to finalize env and send status update:{str(e)}.'
                self.log_error(msg)
        finally:
            self._is_finalized = True
            
    def finalize(self, timestamp:pd.Timestamp|None, *args, **kwargs):
        # idempotent behaviour
        if self._is_finalized or self._is_finalize_in_progress:
            # guard to avoid multiple finalize
            self.log_info(f'skipping algo finalize routine as it is already run or running.')
            return
        
        self.log_info(f'Finalizing algo and running exit routines.')
        # update completion msg if supplied
        completion_msg = kwargs.pop('msg', None)
        if completion_msg and not self.completion_msg:
            self.completion_msg = get_clean_err_msg(completion_msg)
            
        timestamp = timestamp or self.context.timestamp
        if self.mode != MODE.BACKTEST:
            timestamp = pd.Timestamp.now(tz=self.context.trading_calendar.tz)
        
        # run algo exit routines if timestamp is valid
        try:
            with self._finalize_lock:
                # guard to avoid multiple finalization from threading
                try:
                    self._is_finalize_in_progress = True
                    if timestamp:
                        self._finalize(timestamp, *args, **kwargs)
                except Exception as e:
                    msg = f'Failed to run finalize routines for algo: {str(e)}.'
                    self.log_error(msg)
                finally:
                    self._is_finalize_in_progress = False
                    self._handle_algo_exit(timestamp, *args, **kwargs)
        except Exception:
            msg = f'failed to acquired finalize lock, skipping it.'
            self.log_info(msg)
            
    async def async_finalize(self, timestamp:pd.Timestamp|None, *args, **kwargs):
        if self._is_finalized or self._is_finalize_in_progress:
            # guard to avoid multiple finalize
            self.log_info(f'skipping algo finalize routine as it is already run or running.')
            return
        
        self.log_info(f'Finalizing algo and running exit routines.')
        # update completion msg if supplied
        completion_msg = kwargs.pop('msg', None)
        if completion_msg and not self.completion_msg:
            self.completion_msg = get_clean_err_msg(completion_msg)
            
        timestamp = timestamp or self.context.timestamp
        if self.mode != MODE.BACKTEST:
            timestamp = pd.Timestamp.now(tz=self.context.trading_calendar.tz)
        
        # run algo exit routines if timestamp is valid
        try:
            with self._finalize_lock:
                try:
                    self._is_finalize_in_progress = True
                    if timestamp:
                        f = asyncify(
                                loop=self.loop, 
                                executor=self._env.alert_manager.executor)(
                                        self._finalize)
                        await asyncio.wait_for(f(timestamp, *args, **kwargs), 2*LOCK_TIMEOUT)
                except asyncio.TimeoutError:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    err_msg = get_exception(exc_type, exc_value, exc_traceback)
                    self.logger.info(err_msg)
                    self.log_error(f'algo {self.name} exit routine timed out.')
                except Exception as e:
                    msg = f'Failed to run exit routines for algo: {str(e)}.'
                    self.log_error(msg)
                finally:
                    self._is_finalize_in_progress = False
                    self._handle_algo_exit(timestamp, *args, **kwargs)
        except Exception:
            msg = f'failed to acquire finalize lock, skipping it.'
            self.log_info(msg)
                
    def _finalize(self, timestamp:pd.Timestamp|None, *args, **kwargs):
        is_main = threading.get_ident()==threading.main_thread().ident
        
        self.log_info(f'Checking and running exit routine.')
        cancel_orders:bool|None = kwargs.get('cancel_orders', None)
        square_off:bool|None = kwargs.get('square_off', None)
        
        if cancel_orders is None:
            cancel_orders = self._cancel_orders_on_exit
            
        if square_off is None:
            square_off = self._squareoff_on_exit
        
        square_off = cast(bool, square_off)
        cancel_orders = cast(bool, cancel_orders)

        if not cancel_orders and not square_off:
            # nothing to do
            return True
        
        if self._skip_exit_on_kill:
            msg = f'Algo encountered an error and was killed, skipping exit policy square-off or cancellation. '
            msg += f'Please check for any unexpected open positions or orders'
            msg += f' after the algo exit.'
            self.set_intervention_msg(msg)
            return False
        
        if is_main and self.mode in REALTIME_MODES and self._env.server_mode:
            msg = f'Exit policy square-off or cancellation will run with '
            msg += f'reduced timeouts. Please check for any unexpected open '
            msg += f'positions or orders after the algo exit.'
            self.log_platform(msg)
        
        is_market_open:bool = False
        try:
            ts = self.context.timestamp
            if ts:
                is_market_open = self.context.trading_calendar.is_open(ts)
        except Exception:
            pass
        
        if not is_market_open:
            if self.mode in REALTIME_MODES:
                msg = f'Cannot execute square-off or cancellation, '
                msg += f'market not open: {self.context.timestamp}.'
                self.log_warning(msg)
            return False
        
        ret:bool = True
        if cancel_orders and not square_off:
            self.log_info(f'Exiting with open order cancellation ...')
            open_orders = self.get_open_orders(algo=False)
            for order_id in open_orders:
                try:
                    self._validate_and_cancel_order(
                            order_id, open_orders[order_id])
                except OrderAlreadyProcessed:
                    self.log_info(f'order {order_id} is already processed.')
                except Exception as e:
                    ret = False
                    msg = f'Failed to cancel open order {order_id} in exit policy:{str(e)}.'
                    self.log_error(msg)
            
            try:
                if self.execution_mode == ExecutionMode.ONECLICK:
                    for id_, obj in self.get_open_notifications(algo=False).items():
                        self._validate_and_cancel_notification(id_, obj)
            except Exception as e:
                msg = f'Failed to cancel open notification in exit policy:{str(e)}.'
                self.log_error(msg)
        elif square_off:
            self.log_info(f'Exiting with square-off...')
            self.reset_cooloff() # override any cool-off in effect
            # squareoff includes order cancellation as well
            if not self._exit_squareoff:
                status = self.square_off(
                        algo=False, ioc=USE_IOC_EXIT_SQUAREOFF, remark='algo-exit',
                        algo_exit=True) # global square-off
                
                if status:
                    self._exit_squareoff = True
            else:
                msg = f'skipping square-off as it is already triggered.'
                self.log_info(msg)
                
        if self.mode != MODE.BACKTEST:
            timestamp = pd.Timestamp.now(tz=self.context.trading_calendar.tz)
        
        max_iter=_MAX_ITER_EXIT
        if self.mode in SIMULATION_MODES:
            # ensure we get execution right away instead of waiting 
            # for the next event loop cycle
            self.context.blotter.simulate(timestamp)
            for i in range(max_iter):
                if self.wants_to_quit:
                    self.log_platform(f'exiting exit policy execution as the algo exited.')
                    break
                if self.context.blotter.get_open_orders():
                    self.context.blotter.simulate(timestamp)
                    if self.mode == MODE.PAPER:
                        # delay for price update in case last price
                        # has 0 volume data and hence no fill
                        sleep_if_not_main_thread(_WAIT_TIME_EXIT)
                        timestamp = pd.Timestamp.now(tz=self.context.trading_calendar.tz)
                else:
                    break
            if self.mode == MODE.PAPER:
                self.context.blotter.needs_reconciliation = True
        else:
            # for live modes check for pending orders
            if is_main:
                for i in range(max_iter):
                    if self.wants_to_quit:
                        self.log_platform(f'exiting exit policy execution as the algo exited.')
                        break
                    if self.context.blotter.get_open_orders():
                        time.sleep(_WAIT_TIME_EXIT)
                    else:
                        break
            else:
                try:
                    open_orders = list(self.context.blotter.get_open_orders().keys())
                    self._wait_for_trade(open_orders, EXIT_ORDER_TIMEOUT/60)
                except Exception as e:
                    self.log_error(f'exit policy execution failed:{str(e)}.')
        
        # run a reconciliation to ensure the blotter reflects the
        # square off and order cancellation transactions
        if self.mode != MODE.BACKTEST:
            timestamp = pd.Timestamp.now(tz=self.context.trading_calendar.tz)
        
        self.context.blotter.reconcile_all(
                timestamp, forced=True, exit_time=True, only_txns=True)
        
        open_orders = self.context.blotter.get_open_orders()
        positions = self.context.blotter.get_open_positions()
        
        case1:bool = square_off and len(positions)>0
        case2:bool = square_off and len(open_orders)>0
        case3:bool = cancel_orders and len(open_orders)>0
        case4:bool = self._err_on_square_off
        
        if case1 or case2 or case3:
            ret = False
            msg = f'Exit square-off attempt failed. Some orders and '
            msg += f'positions may still be open and may get executed after the algo '
            msg += f'exits. Please check your broker account.' 
            if open_orders:
                msg += f' Pending orders are {open_orders}.'
            self.set_intervention_msg(msg)
        elif case4:
            ret = False
            msg = f'One or more square-off attempt(s) failed. Some orders and '
            msg += f'positions may still be open. Please check your broker account.'
            self.set_intervention_msg(msg)
        else:
            msg = f'Strategy exit square-off or cancellation sucessfully executed.'
            self.log_info(msg)
            
        return ret
                
    def blueshift_server_log(self, payload:dict[str, Any]):
        if not self._env.server_mode:
            self.log_warning(f'Server logging not supported.')
            return
            
        if self.mode == MODE.BACKTEST:
            timestamp = self.context.timestamp
        else:
            timestamp = pd.Timestamp.now(tz=self.context.trading_calendar.tz)
            
        
        if 'message' in payload:
            msg = payload['message']
        else:
            msg = str(payload)
            
        self.logger.platform(msg,"server", timestamp=timestamp, mode=self.mode)
            
    def blueshift_server_control(self, payload:dict[str, Any]):
        if not self._env.server_mode:
            self.log_warning(f'Server control methods not supported.')
            return
        
        if not self._queue:
            self.log_warning(f'Server control methods not available yet.')
            return
        
        if 'method' in payload and payload['method'].lower() == 'cancel':
            self.cancel(msg='Algo cancel request received, will exit.')
        if 'method' in payload and payload['method'].lower() == 'squareoff-cancel':
            if self.status in (AlgoStatus.CREATED,AlgoStatus.RUNNING,AlgoStatus.PAUSED):
                self._cancel_orders_on_exit = True
                self._squareoff_on_exit = True
                msg = 'Algo squareoff and cancel request received, will exit.'
            else:
                msg = 'Algo squareoff and cancel request received, '
                msg += f'but it is already terminating or done. '
                msg += f'Will initiate regular exit.'
            self.cancel(msg=msg)
        elif 'method' in payload and payload['method'].lower() == 'stop':
            self.stop(msg='Algo stop request received, will exit.')
        elif 'method' in payload and payload['method'].lower() == 'pause':
            if self.mode == MODE.BACKTEST:
                msg = f'Algo {self.name} cannot be pause request '
                msg += f'will be ignored.'
                self.log_warning(msg)
            if self.is_running():
                self.pause(msg='Algo pause request received.')
            else:
                msg = f'Algo {self.name} is not running, pause request '
                msg += f'will be ignored.'
                self.log_warning(msg)
        elif 'method' in payload and payload['method'].lower() == 'resume':
            if self.is_paused():
                self.resume(msg='Algo resume request received.')
            else:
                msg = f'Algo {self.name} is not paused, resume request '
                msg += f'will be ignored.'
                self.log_warning(msg)
        elif 'method' in payload and payload['method'].lower() == 'external-event':
            if not ALLOW_EXTERNAL_EVENT:
                msg = f'External update event not allowed.'
                self.log_warning(msg)
                return
            try:
                data = payload['data']
                packet = (data, AlgoCallBack.EXTERNAL)
                self.loop.call_soon_threadsafe(
                        self._queue.put_no_repeat, packet, self)
            except Exception as e:
                msg = f'Failed to update algo: {str(e)}.'
                self.log_warning(msg)

    """
        A list of methods for processing commands received on the command
        channel during a live run.
    """
    def process_command(self, cmd):
        """
            API function to dispatch user command on command channel. The
            function accepts a command object (a tuple of function name,
            arguments list and keyword arguments list), checks if the
            function name is a valid command. If part of the command API,
            it is executed and results are returned.
            
            :param namedtuple cmd: A command to execute.

            .. seealso:: py:attr:`blueshift.utils.types.Command`
        """
        fn = getattr(self, cmd.cmd, None)
        if fn:
            is_cmd_fn = getattr(fn, "is_command", False)
            if is_cmd_fn:
                try:
                    if 'async' in cmd.kwargs:
                        cmd.kwargs.pop('async')
                        f = partial(
                                self._async_command_wrapper, fn,
                                *cmd.args, **cmd.kwargs)
                        if self.loop.is_running():
                            self.loop.call_soon_threadsafe(f)
                            msg = f'running {fn.__name__} in background'
                            msg = cmd.kwargs["message"] if 'message' in cmd.kwargs else msg
                            return {"status":"OK","results":msg}
                        else:
                            msg = 'cannot process command, event loop is not running'
                            return {"status":"Error","results":msg}
                    else:
                        results = fn(*cmd.args, **cmd.kwargs)
                except Exception as e:
                    if isinstance(e, TerminationError):
                        raise
                    msg = f"error in {fn.__name__}: {str(e)}"
                    return {"status":"Error","results":msg}
                else:
                    return {"status":"OK","results":results}
            else:
                msg = f"{fn} is not a supported command, will be ignored."
                self.log_warning(msg)
                return {"status":"Error","results":msg}
        else:
            msg = f"unknown command {cmd.cmd}, will be ignored."
            self.log_warning(msg)
            return {"status":"Error","results":msg}

    def _async_command_wrapper(self, f, *args, **kwargs):
        try:
            f(*args, **kwargs)
        except Exception as e:
            if isinstance(e, BlueshiftException):
                raise e
            else:
                raise BlueshiftException(msg=f'{str(e)}')
                
    def _finalize_strategy(self, timestamp:pd.Timestamp|None, name:str, *args, **kwargs):
        is_main = threading.get_ident()==threading.main_thread().ident
        # can be called either in its own context or in a global context
        # for own context, cancel open orders if required and return.
        # for global context, do the same plus also run the analyze 
        # function before quitting.
        cancel_orders = kwargs.get('cancel_orders', None)
        square_off = kwargs.get('square_off', None)
            
        strategy = self._strategies.get(name)
        if not strategy:
            # no strategy with this context, do nothing
            return
        
        if not cancel_orders and not square_off:
            return
        
        is_market_open = False
        try:
            ts = self.context.timestamp
            if ts:
                is_market_open = self.context.trading_calendar.is_open(ts)
        except Exception:
            pass
        
        if not is_market_open:
            if self.mode in REALTIME_MODES:
                msg = f'Cannot execute square-off or cancellation for sub-strategy {name},'
                msg += f', market not open.'
                self.log_warning(msg)
            return
        
        try:
            with self.context.switch_context(name) as context:
                try:
                    if cancel_orders and not square_off:
                        open_orders = self.get_open_orders()
                        for order_id in open_orders:
                            try:
                                self._validate_and_cancel_order(
                                        order_id, open_orders[order_id])
                            except OrderAlreadyProcessed:
                                self.log_info(f'order {order_id} is already processed.')
                            except Exception:
                                msg = f'Failed cancelling order {order_id} '
                                msg += f'from sub-strategy {name}.'
                                self.log_error(msg)
                    elif square_off:
                        status = self.square_off(algo=True, remark='algo-exit') # context square-off
                        if not status:
                            msg = f'Failed to square-off on sub-strategy exit'
                            self.log_error(msg)
                        
                    if self.execution_mode == ExecutionMode.ONECLICK:
                        for id_, obj in self.get_open_notifications().items():
                            self._validate_and_cancel_notification(id_, obj)
                except Exception as e:
                    msg = f'Failed in finalizing sub-strategy {name}:{str(e)}.'
                    self.log_error(msg)
        except NoContextError:
            # this always run in the main thread, we should not be here
            pass
            
        max_iter=_MAX_ITER_EXIT
        if self.mode in SIMULATION_MODES:
            try:
                with self.context.switch_context(name) as context:
                    self.context.blotter.simulate(timestamp)
                    for i in range(max_iter):
                        if self.wants_to_quit:
                            self.log_platform(f'exiting exit routine for sub strategy as the algo exited.')
                            break
                        if self.get_open_orders():
                            self.context.blotter.simulate(timestamp)
                            if self.mode == MODE.PAPER:
                                # delay for price update in case last price
                                # has 0 volume data and hence no fill
                                sleep_if_not_main_thread(_WAIT_TIME_EXIT)
                                timestamp = pd.Timestamp.now(tz=self.context.trading_calendar.tz)
                        else:
                            break
                    if self.mode == MODE.PAPER:
                        context.blotter.needs_reconciliation = True
            except NoContextError:
                pass
        else:
            try:
                with self.context.switch_context(name) as context:
                    if is_main:
                        for i in range(max_iter):
                            if self.wants_to_quit:
                                self.log_platform(f'exiting exit routine for sub strategy as the algo exited.')
                                break
                            if self.get_open_orders():
                                time.sleep(_WAIT_TIME_EXIT)
                            else:
                                break
                    else:
                        open_orders = list(self.get_open_orders().keys())
                        self._wait_for_trade(open_orders, EXIT_ORDER_TIMEOUT/60)
            except NoContextError:
                pass
        
        # run a reconciliation to ensure the blotter reflects the
        # square off and order cancellation transactions
        try:
            with self.context.switch_context(name) as context:
                context.blotter.reconcile_all(
                        timestamp, forced=True, exit_time=True, only_txns=True)
                
                if self.get_open_orders():
                    msg = f'Sub-strategy exit pending order timed out for strategy '
                    msg += f'{name}, Some orders are '
                    msg += f'still open and may get executed after the strategy '
                    msg += f'exits. Please check your broker account.'
                    self.log_warning(msg)
                else:
                    msg = f'Sub-strategy exit square-off or cancellation sucessfully executed.'
                    self.log_info(msg)
        except NoContextError:
            pass
                
    @api_method
    def get_context(self, name:str|None=None) -> IContext|None:
        """
            The the algo context by name. If the current algo is running 
            with sub-strategies added, fetch the corresponding sub 
            context by the name. Omitting the `name` argument will 
            return the main context.
            
            :param str name: The name of the sub strategy to fetch.
        """
        if name is None:
            return self.context
        
        try:
            return self.context.get_context(name)
        except Exception:
            pass
    
    @api_method
    def add_strategy(self, strategy:Strategy):
        """
            Add a sub strategy to the current algo. Sub strategies 
            allow a modular approach to incorporate multiple independent 
            rules in a single strategy. For more see :ref: `sub-strategies<Sub-Strategies>`.
            
            .. note:: 
                Sub strategies can only be added in regular modes (not 
                in EXECUTION mode).
            
            :param strategy: The strategy to add.
            :type strategy: :ref:`Strategy<Defining a sub-strategy>`.
            
            .. seealso:: :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.cancel_strategy`
            
        """
        if self.mode == MODE.EXECUTION:
            msg = f'Cannot add strategy, not supported for smart orders.'
            raise ValidationError(msg)
            
        if not isinstance(strategy, Strategy):
            self.log_error(f'Illegal strategy type, cannot add.')
            return
        
        if not self.context.is_initialized:
            msg = f'Cannot add strategy, global context not initialized.'
            raise ValidationError(msg)
            
        if strategy.name==self.context.name or strategy.name in self._strategies:
            msg = f'Cannot add strategy {strategy.name}, name must be unique, '
            msg += f'a strategy with this name already exists.'
            raise ValidationError(msg)
        
        context, blotter = self.context.add_context(strategy)
        if self.mode not in context.broker.supported_modes:
            msg = f"incompatible run mode and broker. Cannot add strategy."
            raise ValidationError(msg)
            
        self._strategies[strategy.name] = strategy
        if context.broker is not self.context.broker and isinstance(context.broker, ILiveBroker):
            # this is only applicable for backtester and paper trader
            context.broker.exit_handler = self._exits_handler
            if self.mode in REALTIME_MODES:
                # we schedule set algo callback, this should be safe 
                # as AlgoCallback event are on no-repeat basis.
                if getattr(self.context.broker,'set_algo_callback', None):
                    msg = 'setting up callbacks for events for '
                    msg += f'broker {context.broker}'
                    self.log_info(msg)
                    context.broker.set_algo_callback(
                            self.schedule_event,
                            algo_name = self._env.name,
                            algo_user=self._env.algo_user,
                            logger=self.logger)
        
        self.log_platform(f'Successfully added strategy {strategy.name}.')
      
    def remove_strategy(self, name:str, cancel_orders:bool=True, square_off:bool=False, 
                        cancel:bool=False, error:Exception|str|None=None):
        if self.mode == MODE.EXECUTION:
            return
            
        if not self.is_global_context and name != self.current_context.name:
            msg = f'Strategy can only be removed in global context. '
            msg += f'or from itself - illegal attempt to terminate.'
            raise ValidationError(msg)
            
        if name not in self._strategies:
            return
            
        
        self._finalize_strategy(
                self.context.timestamp, name, 
                cancel_orders=cancel_orders, 
                square_off=square_off)
        
        # context and scheduler independently check for strategy existence
        ctx = self.context.get_context(name)
        if not ctx:
            # strategy already removed.
            return
        
        strategy = self._strategies[name]
        if error:
            try:
                strategy._on_error(ctx, error)
            except Exception as e:
                msg = f'Failed to run on error routine for strategy '
                msg += f'{name}: :{str(e)}.'
                self.log_error(msg)
        elif cancel:
            try:
                strategy._on_cancel(ctx)
            except Exception as e:
                msg = f'Failed to run on cancel routine for strategy '
                msg += f'{name}: :{str(e)}.'
                self.log_error(msg)
        else:
            try:
                perfs = ctx.blotter.perfs_history
                strategy._analyze(ctx, perfs)
            except Exception as e:
                msg = f'Failed to run analyze routine for strategy '
                msg += f'{name}: :{str(e)}.'
                self.log_error(msg)
                
        # on exit runs no matter what
        try:
                strategy._on_exit(ctx)
        except Exception as e:
            msg = f'Failed to run on exit routine for strategy '
            msg += f'{name}: :{str(e)}.'
            self.log_error(msg)
        
        self._strategies.pop(name, None)
        
        # remove from stoploss handler and from algo order handlers
        self._exits_handler.remove_context(ctx)
        self._algo_order_handler.remove_context(name)
        self._scheduler.remove_context(name)
        self.context.remove_context(name)
        self.log_platform(f'Successfully removed strategy {name}.')
        
    @api_method
    def cancel_strategy(self, name:str, cancel_orders:bool=True, square_off:bool=False):
        """
            Cancel (remove) a sub strategy previously added. For more 
            on sub-strategies, see :ref: `sub-strategies<Sub-Strategies>`.
            
            :param str name: Name of the sub-strategy to remove.
            :param bool cancel_orders: Cancel open orders on removal.
            :param bool square_off: Square off on removal.
            
            .. seealso:: :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.add_strategy`
        """
        return self.remove_strategy(
                name, cancel_orders=cancel_orders, square_off=square_off, 
                cancel=True)
        
    def handle_sub_strategy_error(self, e:Exception, name:str):
        if name==self.context.name or self.is_global_context:
            raise e
            
        alert_manager = self._env.alert_manager
        alert_manager.handle_subcontext_error(self, e, name)

    def handle_error(self, e, name:str):
        return self.handle_sub_strategy_error(e, name)
        
    def handle_sub_strategy_error_no_ctx_switch(self, e:Exception, name:str):
        if name==self.context.name:
            raise e
            
        alert_manager = self._env.alert_manager
        alert_manager.handle_subcontext_error(self, e, name)
        
    @property
    def wants_to_quit(self) -> bool:
        # flag to suppress order functions
        return self._wants_to_quit
    
    @property
    def is_stopped(self) -> bool:
        return self._status in TerminalAlgoStatuses
    
    @property
    def current_context(self) -> IContext:
        name = self._env.current_context
        ctx = self.context.get_context(name)
        if not ctx:
            msg = f'The context {name} is destroyed.'
            raise ContextError(msg)
        return ctx
    
    def get_current_state(self):
        if self.is_global_context:
            return self.state
        
        try:
            strategy = self._strategies.get(self.current_context.name)
        except Exception:
            return self.state
        else:
            if strategy:
                return strategy.state
            return self.state
        
    def set_algo_status(self, value:AlgoStatus):
        if self._status in TerminalAlgoStatuses:
            return
        
        if self._desired_status in (AlgoStatus.CANCELLED, AlgoStatus.STOPPED,
                            AlgoStatus.ERRORED, AlgoStatus.KILLED):
            return
            
        self._desired_status = AlgoStatus(value)
        
    @property
    def logger(self) -> BlueshiftLogger:
        return self._logger
    
    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._env.alert_manager.loop
    
    @property
    def status(self) -> AlgoStatus:
        return self._status
    
    @property
    def is_finalized(self) -> bool:
        return self._is_finalized
    
    @property
    def env(self) -> TradingEnvironment:
        return self._env
    
    @property
    def context_name(self) -> str:
        return self.current_context.name
    
    @property
    def is_global_context(self) -> bool:
        return self._env.current_context == self.context.name
    
    @property
    def queue(self) -> ClockQueue|None:
        return self._queue

    @command_method
    def debug(self, *args, **kwargs):
        """
            Command method to run arbitrary python code and return
            the result. Only allowed if the algo is running with the 
            `no-sec` (skip security enforcement) flag set.
        """
        if self._env.security is not None:
            return ''
        
        from io import StringIO
        stdiobuffer = StringIO(newline='\n')
        src = "\n".join(args)
        with StdioRedirect(stdiobuffer, logger=self.logger):
            try:
                code = compile(src,"<DEBUG>","exec") #nosec
                exec(code) #nosec
            except Exception:
                pass
        return stdiobuffer.getvalue()

    @command_method
    def current_state(self):
        """
            Command method to return current state.
        """
        return self.get_current_state()

    
    @command_method
    def start(self, show_progress=False, skip_wait=False):
        """
            start the current algorithm. This is an alias for the 
            `run` method.
        """
        self.run(show_progress=show_progress, skip_wait=skip_wait)
        
    @command_method
    def restart(self, show_progress=False):
        """
            Restart the current algorithm.
            
        """
        self.reset()
        self._desired_status = AlgoStatus.RUNNING
        self.run(show_progress=show_progress)
    
    
    @command_method
    def pause(self, *args, **kwargs):
        """
            Command method to pause the algorithm. This command will NOT
            suspend the event loop execution but any trading activity 
            and user defined functions will be suspended. Backtests cannot 
            be paused once started.

            .. seealso:: :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.resume`

        """
        if self._status in TerminalAlgoStatuses:
            self.log_warning(f'Cannot pause, algo is terminated.')
            return
        
        if self.mode not in REALTIME_MODES:
            self.log_warning(f'Pause is allowed only in realtime run mode.')
            return
        
        try:
            self._desired_status = AlgoStatus.PAUSED
            self.set_pause()
            msg = "Algorithm paused. No further processing till resumed."
            self.log_warning(msg)
            self._status = AlgoStatus.PAUSED
            self._desired_status = AlgoStatus.RUNNING
            
            if self.mode == MODE.EXECUTION:
                self._env.blueshift_callback(
                            action=BlueshiftCallbackAction.SMART_ORDER_STATUS.value,
                            status=AlgoStatus.PAUSED.value,
                            msg=f'Smart order {self.name} pause initiated.')
            else:
                self._env.blueshift_callback(
                        action=BlueshiftCallbackAction.ALGO_STATUS.value,
                        status=AlgoStatus.PAUSED.value,
                        msg=f'Algo {self.name} pause initiated.')
            
            if self.env.publish and self.env.alert_manager.publisher:
                packet = create_message_envelope(self.name, MessageType.PAUSED, msg=msg)
                self.env.alert_manager.publisher.send_to_topic(self.name, packet)

            return msg
        except MachineError:
            self._desired_status = AlgoStatus.ERRORED
            msg=f"State Machine Error ({self.state}): attempt to pause."
            raise StateMachineError(msg=msg)

    @command_method
    def resume(self, *args, **kwargs):
        """
            Resume an algo that has been paused earlier. This will start 
            processing user defined functions (callbacks) and resume any 
            trading activities.

            .. seealso:: :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.pause`
        """
        if not self.is_paused():
            self.log_warning(f'Cannot resume, algo not on pause.')
            return
        
        if self._status in TerminalAlgoStatuses:
            self.log_warning(f'Cannot resume, algo is terminated.')
            return
        
        if self.mode not in REALTIME_MODES:
            self.log_warning(f'Resume is allowed only in realtime run mode.')
            return
        
        try:
            self._desired_status = AlgoStatus.RUNNING
            self.reset_pause()
            msg = "Algorithm resumed."
            self.log_warning(msg)
            self._status = AlgoStatus.RUNNING
            self._desired_status = AlgoStatus.DONE
            
            if self.mode == MODE.EXECUTION:
                self._env.blueshift_callback(
                            action=BlueshiftCallbackAction.SMART_ORDER_STATUS.value,
                            status=AlgoStatus.OPEN.value,
                            msg=f'Smart order {self.name} resume initiated.')
            else:
                self._env.blueshift_callback(
                        action=BlueshiftCallbackAction.ALGO_STATUS.value,
                        status=AlgoStatus.RUNNING.value,
                        msg=f'Algo {self.name} resume initiated.')
                
            if self.env.publish and self.env.alert_manager.publisher:
                packet = create_message_envelope(self.name, MessageType.RESUMED, msg=msg)
                self.env.alert_manager.publisher.send_to_topic(self.name, packet)
                
            return msg
        except MachineError:
            self._desired_status = AlgoStatus.ERRORED
            msg=f"State Machine Error ({self.state}): attempt to resume."
            raise StateMachineError(msg=msg)
            
    @api_method
    @command_method
    def cancel(self, *args, **kwargs):
        """
            Cancel command will cause the main event loop to raise an
            exception, which will cause the main program to shut-down
            gracefully. This will execute the algo exit policy.

            .. seealso:: 
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.stop`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.reject`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.kill`
        """
        try:
            self.set_algo_status(AlgoStatus.CANCELLED)
            msg = kwargs.get('msg', None)
            if msg:
                msg = f"Algo run cancelled, will shutdown:{msg}"
            else:
                msg = f"Algo run cancelled, will shutdown."
                
            self.context.clock.terminate()
            self._is_terminated = True
            self.log_warning(msg)
            self.completion_msg = get_clean_err_msg(msg)
            
            if self.mode in REALTIME_MODES and self._queue:
                # put an algo terminate event to trigger immediately
                f = lambda ts:self.on_cancel()
                packet = (f, AlgoTerminateEvent.CANCEL)
                self.loop.call_soon_threadsafe(
                    self._queue.put_no_repeat, packet, self)
        except MachineError:
            # this should never fail.
            self._desired_status = AlgoStatus.ERRORED
            msg=f"State Machine Error ({self.state}): attempt to stop."
            raise StateMachineError(msg=msg)
            
    @command_method
    def stop(self, *args, **kwargs):
        """
            Stop command will cause the main event loop to raise an
            exception, which will cause the main program to shut-down
            gracefully. This will still run the algo exit policy.

            .. seealso:: 
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.cancel`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.reject`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.kill`
        """
        if self._status in TerminalAlgoStatuses:
            self.log_warning(f'Cannot stop, algo is already terminated.')
            return
        
        try:
            self.set_algo_status(AlgoStatus.STOPPED)
            msg = kwargs.get('msg',None)
            if msg:
                msg = f"Algo stopped, will shutdown:{msg}"
            else:
                msg = f"Algo stopped, will shutdown."
                
            self.context.clock.terminate()
            self.log_warning(msg)
            self._status = AlgoStatus.STOPPED
            self.completion_msg = get_clean_err_msg(msg)
            
            if self.mode != MODE.BACKTEST and self._queue:
                # put an algo terminate event to trigger immediately
                f = lambda ts:noop()
                packet = (f, AlgoTerminateEvent.STOP)
                self.loop.call_soon_threadsafe(
                    self._queue.put_no_repeat, packet, self)
        except MachineError:
            # this should never fail.
            self._desired_status = AlgoStatus.ERRORED
            msg=f"State Machine Error ({self.state}): attempt to stop."
            raise StateMachineError(msg=msg)
            
    @api_method
    @command_method
    def reject(self, *args, **kwargs):
        """
            Reject triggers graceful exit from the current run with after 
            calling the on_error method. This is applicable for the 
            `EXECUTION` mode only. This will execute the algo exit policy.
            
            .. seealso:: 
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.cancel`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.stop`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.kill`
        """
        if self.mode != MODE.EXECUTION:
            msg = f'Reject is only valid for smart orders.'
            raise BlueshiftException(msg)
            
        if self._status in TerminalAlgoStatuses:
            self.log_warning(f'Cannot rejet, smart order is already terminated.')
            return
        
        try:
            msg = kwargs.get('msg',None)
            self.set_algo_status(AlgoStatus.REJECTED)
            if msg:
                msg_str = f"Smart order rejected, will exit:{msg}"
            else:
                msg_str = f"Smart order rejected, will exit."
                
            self.context.clock.terminate()
            self.log_warning(msg_str)
            self._status = AlgoStatus.REJECTED
            self.completion_msg = get_clean_err_msg(msg_str)
            
            if self.mode != MODE.BACKTEST and self._queue:
                # put an algo terminate event to trigger immediately
                f = lambda ts:self.on_error(msg_str)
                packet = (f, AlgoTerminateEvent.STOP)
                self.loop.call_soon_threadsafe(
                    self._queue.put_no_repeat, packet, self)
        except MachineError:
            # this should never fail.
            self._desired_status = AlgoStatus.ERRORED
            msg=f"State Machine Error ({self.state}): attempt to stop."
            raise StateMachineError(msg=msg)

    @command_method
    def kill(self, *args, **kwargs):
        """
            Kill command will cause the main event loop to raise an
            exception, which will cause the main program to shut-down
            gracefully. This will bypass the exit policy of the algo 
            and will terminate as soon as possible.
            
            .. seealso:: 
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.cancel`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.stop`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.reject`
        """
        if self._status in TerminalAlgoStatuses:
            self.log_warning(f'Cannot kill, algo is already terminated.')
            return
        
        try:
            self.set_algo_status(AlgoStatus.KILLED)
            msg = kwargs.get('msg', None)
            if msg:
                msg = f"Algo killed, will shutdown. Any exit routine will be skipped:{msg}"
            else:
                msg = f"Algo killed, will shutdown. Any exit routine will be skipped."
                
            self.fsm_stop()
            self.log_warning(msg)
            self._status = AlgoStatus.KILLED
            self.completion_msg = get_clean_err_msg(msg)
            self._skip_exit_on_kill = True
            
            if self.mode != MODE.BACKTEST and self._queue:
                # put an algo terminate event to trigger immediately
                f = lambda ts:noop()
                packet = (f, AlgoTerminateEvent.KILL)
                self.loop.call_soon_threadsafe(
                    self._queue.put_no_repeat, packet, self)
        except MachineError:
            # this should never fail.
            self._desired_status = AlgoStatus.ERRORED
            msg=f"State Machine Error ({self.state}): attempt to stop."
            raise StateMachineError(msg=msg)
            
    def reset(self):
        try:
            self._desired_status = AlgoStatus.RUNNING
            self.reset_state()
            self.context.reset()
            self._reset_trackers()
        except Exception as e:
            self._desired_status = AlgoStatus.ERRORED
            msg=f"Error resetting the algo:{str(e)}"
            raise StateMachineError(msg=msg)

    @api_method
    @command_method
    def login(self, **kwargs):
        """
            This triggers a login on the broker authentication object.
            The processing depends on the particular implementation.
        """
        self.log_info(f'broker login command with {kwargs}')
        broker = self.context.broker
        broker.login(**kwargs)
        msg = "executed broker login."
        self.log_info(msg)
        return msg

    @api_method
    @command_method
    def refresh_asset_db(self, *args, **kwargs):
        """
            trigger a refresh of the broker asset database. The 
            processing depends on the particular implementation.
        """

        broker = self.context.broker
        broker.refresh_data(*args, **kwargs)
        msg = "Relaoded asset database."
        self.log_warning(msg)
        return msg

    @api_method
    @command_method
    def stop_trading(self):
        """
            Disable placing orders. For square-off and other cases 
            where trading control validation is bypassed, those orders 
            will still go through.
            
            .. seealso:: :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.resume_trading`
        """
        self._freeze_trading()
        msg = "Stop trading invoked, all new orders will be ignored. Square-offs will still work."
        self.log_warning(msg)
        return msg

    @api_method
    @command_method
    def resume_trading(self, *args, **kwargs):
        """
            Start an algo which has trading stopped earlier.
            
            .. seealso:: :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.stop_trading`
        """

        self._unfreeze_trading()
        msg = "Resume trading command: Trading resumed. Orders will be sent to broker."
        self.log_warning(msg)
        return msg

    @command_method
    def apply_event(self, *args, **kwargs):
        """
            Command method to apply an event to the underlying blotter and
            trading account. The events are either account events or some
            corporate events.
        """
        keywords = {}

        try:
            event_type = kwargs.pop("event_type")
            event_type = EventType(event_type)

            keywords["asset"] = self.context.asset_finder.symbol(
                    kwargs.pop("asset"), logger=self.logger)
            keywords["effective_dt"] = pd.Timestamp(
                    kwargs.pop("effective_dt"))
            keywords["announcement_dt"] = pd.Timestamp(
                    kwargs.pop("announcement_dt", None))
            keywords["pct_long_cost"] = float(
                    kwargs.pop("pct_long_cost", 0))
            keywords["amount"] = float(kwargs.pop("amount", 0))
            keywords["div_ratio"] = float(kwargs.pop("div_ratio", 0))
            keywords["split_ratio"] = float(kwargs.pop("split_ratio", 0))
            keywords["exchange_ratio"] = float(
                    kwargs.pop("exchange_ratio", 0))
            keywords["offer_price"] = float(kwargs.pop("offer_price", 0))
            keywords["cash_pct"] = float(kwargs.pop("cash_pct", 0))
            acquirer = kwargs.pop("acquirer", None)
            if acquirer:
                keywords["acquirer"] = self.context.asset_finder.symbol(
                        acquirer, logger=self.logger)

            evt = account_event_factory(event_type, **keywords)
            if evt is None:
                raise ValueError("could not construct event to apply.")
            evt.apply(self.context.blotter)
        except (BlueshiftException, KeyError, ValueError\
                , AttributeError, TypeError)\
                as e:
            err = str(e)
            msg = f"Apply event command: Could not apply the specified event: {err}."
            self.log_warning(msg)
            return msg
        else:
            msg = "Apply event command: event successfully applied"
            return msg

    @command_method
    def notify(self, *args, **kwargs):
        """
            Command method to handle oneclick service.
            Returns:
                None
        """
        if self.execution_mode != ExecutionMode.ONECLICK or self.context.notifier is None:
            msg = 'not running in compatible mode for this command.'
            self.log_warning(msg)
            return

        self.context.notifier.confirm(self, kwargs)

    @command_method
    def smart_order(self, orders):
        """
            Depreicated.
        """    
        msg = f'This method no longer supported and will raise '
        msg += f'error in a future version.'
        warnings.warn(msg, DeprecationWarning)
        return

    """
        All API functions related to algorithm objects should go in this
        space. These are accessible from the user code directly.
        
    """
    @api_method
    def quit(self, cancel_orders=True):
        """
            Exit from the current run by stopping the algo.

            .. note::
                This will cause the algorithm to exit execution in the next
                clock tick. This will NOT trigger the ``analyze`` API function
                as it exits. However, the current state of the blotter,
                including the performance will be saved, if possible.
                
            :param bool cancel_orders: If open orders should be cancelled.

            .. danger:: This function only initiates outstanding orders
                        cancellation, and does NOT guarantee it. Be cautious.
        """
        msg = f'This method should not be used by user program and '
        msg += f'will raise error in a future version. Use `finish` '
        msg += f'or `terminate` or `exit_while_done` instead'
        warnings.warn(msg, DeprecationWarning)
        
        try:
            if cancel_orders:
                self._cancel_orders_on_exit = True
                msg = "Quitting the algorithm now. "
                msg = msg + "pending orders will be cancelled."
            else:
                msg = "Quitting the algorithm now."
                
            self.fsm_stop()
            self.log_platform(msg)
            self._status = AlgoStatus.STOPPED
            if not self.completion_msg:
                self.completion_msg = get_clean_err_msg(msg)
            raise TerminationError(msg)
        except MachineError:
            # this should never fail.
            msg=f"State Machine Error ({self.state}): attempt to stop."
            raise StateMachineError(msg=msg)
            
    @api_method
    def finish(self, msg:str|None=None):
        """
            Exit from the current run by scheduling the algo end event. 
            This will cause the algorithm to exit execution as soon as 
            possible. This will also trigger the ``analyze`` API 
            function as it exits. This method can be called from any 
            subcontext as well, and will cause the whole algo to exit. It 
            differs from the ``terminate`` method in that this causes the algo 
            to terminate after processing already queued up events. Use 
            ``terminate`` for immediate exits (signifying some error 
            conditions as determined by the strategy).

            :param str msg: A message to print for the algo completion.
            
            .. warning::
                This function, unlike the `terminate` function, does not 
                cause an immediate termination of execution flow, but 
                schedules a graceful exit instead. This exit is only 
                affected as soon as the strategy function that invokes this 
                API method returns. So make sure this is the last line 
                of that function if you want to finish the algo run as 
                soon as possible.

            .. warning::
                This function only initiates outstanding orders
                cancellation and position square-off, if specified 
                in the exit policy and does NOT guarantee their 
                successful completion.
                
            .. seealso::
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.terminate`
        """
        if self._status in TerminalAlgoStatuses:
            self.log_error(f'Cannot stop, algo is already terminated.')
            return
        
        try:
            self.set_algo_status(AlgoStatus.DONE)
            self._status = AlgoStatus.DONE
            if msg:
                msg = f"Algo run finish triggered:{msg}"
            else:
                msg = f"Algo run finish triggered."
                
            self.context.clock.terminate()
            self.log_info(msg)
            if not self.completion_msg:
                self.completion_msg = get_clean_err_msg(msg)
            
            if self.mode != MODE.BACKTEST and self._queue:
                # put an algo terminate event to trigger immediately
                f = lambda ts:noop()
                packet = (f, AlgoTerminateEvent.DONE)
                self.loop.call_soon_threadsafe(
                    self._queue.put_no_repeat, packet, self)
        except MachineError:
            # this should never fail.
            self._desired_status = AlgoStatus.ERRORED
            msg=f"State Machine Error ({self.state}): attempt to stop."
            raise StateMachineError(msg=msg)

    @api_method
    def terminate(self, error:Exception|str|None=None, cancel_orders:bool=True, is_error:bool=False):
        """
            Exit from the current context/ run by scheduling the algo 
            end event. This will cause the algorithm to exit execution 
            as soon as possible. This will also trigger the ``analyze`` 
            API function as it exits. If called from within a sub-context, 
            will terminate only that subcontext. If `error` is an 
            exception object or if `is_error` is True, the exit is 
            assumed to be with error and the `on_error` handler 
            will be invoked on exit. Else `on_cancel` will be invoked. It 
            differs from the ``finish`` method in that this causes the algo 
            to terminate immediately, ignoring any queued up events. For 
            regular algo exit, use ``finish``. User ``terminate`` for 
            immediate exits (signifying some error condition as determined 
            by the strategy).

            :param str error: An error message (or an exception object).
            :param bool cancel_orders: If open orders should be cancelled.
            :param bool is_error: If exiting on error.
            
            .. note::
                If called with is_error=True, this will also invoke the 
                :ref:`on_error<On Error>` handler of the algo.

            .. danger::
                This function only initiates outstanding orders
                cancellation, and does NOT guarantee it.
                
            .. seealso::
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.finish`
        """
        if not self.is_global_context:
            ctx = self.context_name
            self.remove_strategy(
                    self.context_name, cancel_orders=cancel_orders,
                    error=str(error))
            # raise exception to immediately stop further execution
            # of user strategy.
            raise SubContextTerminate(
                    f'Substrategy {ctx} terminated')
        
        msg = ''
        try:
            if cancel_orders:
                self._cancel_orders_on_exit = True
                msg = "terminating the algorithm now, "
                msg = msg + "pending orders will be cancelled."
            else:
                msg = "terminating the algorithm now."
            
            if msg:
                self.log_warning(msg)
                
            f = lambda ts:self.on_cancel()
            
            if isinstance(error, Exception) or is_error:
                msg = f'Strategy reported termination error message:{str(error)}'
                self.log_error(msg)
                f = lambda ts:self.on_error(msg)
            else:
                msg = f'Strategy reported termination message:{str(error)}'
                self.log_info(msg)
            
            self._desired_status = AlgoStatus.CANCELLED
            self.context.clock.terminate()
            self._is_terminated = True
            if not self.completion_msg:
                self.completion_msg = get_clean_err_msg(msg)
            
            if self.mode != MODE.BACKTEST and self._queue:
                # put an algo terminate event to trigger immediately
                packet = (f, AlgoTerminateEvent.STOP)
                self.loop.call_soon_threadsafe(
                    self._queue.put_no_repeat, packet, self)
            raise TerminationError(msg)
        except MachineError:
            # this should never fail.
            msg=f"State Machine Error ({self.state}): attempt to stop."
            raise StateMachineError(msg=msg)
            
    @api_method
    def exit_when_done(self, completion_msg:str|None=None, check_orders:bool=True, 
                       check_positions:bool=False, timeout:float|None=None):
        """
            Exit from the current run by scheduling the algo end event, 
            after checking if all open orders are complete and/ or all 
            open positions are closed out. If both `check_orders` and 
            `check_positions` are False, it is equivalent to the `finish` 
            API function. Use `wait_for_exit` if you do not want to force 
            and exit after timeout and want to wait again instead.

            :param str completion_msg: A completion message to display.
            :param bool check_orders: Wait for open orders to finalize.
            :param bool check_positions: Wait for open positions to close.
            :param float timeout: max wait time (in minutes).
            
            .. note::
                If `check_positions` is True, it will wait for all open 
                positions to close out, but will not initiate (square-off)
                on its own. You must do that (call square-off or equivalent)
                before you call this API function with `check_positions` 
                as True, otherwise it can potentially wait a long time and 
                /or exit with an error. Also, if `check_positions` is 
                True, `check_orders` is automatically set to True (as 
                otherwise the wait may not be meaningful).

            .. danger::
                This function only initiates termination but does NOT 
                guarantee it.
                
            .. seealso:: 
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.square_off`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.wait_for_exit`
        """
        if not check_orders and not check_positions:
            return self.finish(completion_msg)
        
        if check_positions:
            check_orders = True
        
        if not timeout:
            timeout = _WAIT_TIME_EXIT_MINUTE*5 # to match deprecated _exit_when_done
        
        status = self._wait_for_exit(
                None, check_orders=check_orders, 
                check_positions=check_positions, timeout=timeout)
        
        if status:
            completion_msg = completion_msg or 'algo run finished.'
            self.finish(completion_msg)
        else:
            msg = f'Timed out while waiting (timeout)minutes for open '
            msg += f'orders to execute and/or open positions to '
            msg += f'close, will force termination.'
            self.terminate(msg, cancel_orders=False, is_error=True)
            return True
    
    def _exit_when_done(self, completion_msg:str|None=None, check_orders:bool=True, 
                       check_positions:bool=False, attempts:int=5, wait:float|None=None):
        # TODO: this is deprecated -> delete
        if not check_orders and not check_positions:
            return self.finish(completion_msg)
        
        if not wait:
            wait = _WAIT_TIME_EXIT_MINUTE
        
        _MAX_ITER = max(1, int(attempts))
        timeout = wait
        iter_var = 0
        
        def _try_and_terminate(check_orders, check_positions):
            nonlocal iter_var
            open_orders = {}
            positions = {}
            done = False
            
            iter_var += 1
            
            if iter_var >= _MAX_ITER:
                # we are at last iteration, trigger an exit recon
                self.reconcile_all(
                        timestamp=None, forced=True, exit_time=True, 
                        fetch_orders=True, only_txns=True, simulate=True)
            
            if check_positions:
                check_orders = True
            
            if check_orders:
                open_orders = self.get_open_orders(algo=False)
                
            if check_positions:
                positions = self.get_open_positions(algo=False)
            
            if not open_orders and not positions:
                done = True
            
            if done:
                self.finish(completion_msg)
                return True
            
            if iter_var >= _MAX_ITER:
                msg = f'Max iteration exceeded while waiting for open '
                msg += f'orders to execute and/or open positions to '
                msg += f'close, will force termination.'
                self.finish(msg)
                return True
            
            return False
            
        def _exit_when_done(context=None, data=None):
            # context and data are dummy variables here as we do not 
            # use them but required for the schedule_once call signature
            if not _try_and_terminate(check_orders, check_positions):
                # the termination was not successful, some orders or positions
                # are still open. Try again later. We mode to schedule 
                # later with total 3*6=18s potential delay
                self.schedule_later(_exit_when_done, timeout)
                
        _exit_when_done()
        
    @api_method
    def square_off_and_exit(self):
        """
            Utility function that calls square-off and then wait for 
            all positions to close before exiting the algo run.
        """
        try:
            status = self.square_off()
            #assert status, "please check your broker account"
            if not status:
                raise ValueError("please check your broker account")
        except Exception as e:
            self._logger.error(f'Error in square-off:{str(e)}.')
            
        self.exit_when_done(check_positions=True)
        
        
    @api_method
    @ensure_state(states=["TRADING_BAR"],
                  err_msg="can only be called during a trading session.")
    def wait_for_trade(self, order_ids:list[str]|str, timeout:float=1, exit_time:bool=False) ->bool:
        """
            Wait for a list of orders to either complete or fail. A single
            order id is also accepted. Missing order IDs (None) value will 
            be automatically discarded. Returns True if none of the orders 
            are open, else returns False. For backtesting, timeout will be 
            rounded to integer and that many simulation attempts will be 
            made. For live, the timeout signifies number of minutes before 
            giving up and returning False (in case the orders are still 
            outstanding).
            
            Use this method if you want to wait for multiple pending 
            orders synchronously. If you just want to wait for a single 
            order, another alternative is to use the `order_with_retry` 
            API method.

            :param list order_ids: A list of order IDs.
            :param float timeout: Maximum time (in minutes) to wait.
            
            .. note::
                A True value does not indicate all orders are executed, 
                rather it means none of the orders are open anymore.

            .. warning::
                For backtest, this API function will attempt to fill the 
                order entirely in the current minute bar (a total `timeout` 
                attempts will be made). This is different than the regular
                simulation model, where the fill happens in the next bar 
                (and any remaining amount is attempted in the subsequent 
                trading minutes).
                
            .. warning::
                If the underlying broker does not support order streaming 
                (in live trading) or data streaming (in paper mode), this 
                will block the algo execution for the amount of time 
                specified in `timeout`. It is not recommended to use this 
                API in such cases.
                
            .. danger::
                This API blocks the execution of the algo till the orders 
                are not open anymore or the timeout triggers. A large 
                timeout value can block the algo for a long time.
                
            .. seealso:: 
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.order`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.order_with_retry`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.schedule_order_callback`
        """
        return self._wait_for_trade(
                order_ids, timeout=timeout, exit_time=exit_time)
    
    def _wait_for_trade(self, order_ids:list[str]|str, timeout:float=1, exit_time:bool=False) -> bool:
        if not listlike(order_ids):
            order_ids = [order_ids] # type: ignore
        
        try:
            order_ids = list(set([oid for oid in order_ids if oid is not None]))
        except Exception:
            raise ValidationError(f'expected a list of order Ids, got {order_ids}')
            
        if not order_ids:
            # nothing to wait for
            return True
        
        orders = [self.get_order(oid, algo=False) for oid in order_ids]
        if None in orders:
            msg = f'Some of the order IDs returned no corresponding order.'
            raise OrderNotFound(msg)
        if all([o.is_final() for o in orders]): # type: ignore -> already None checked
            return True
        
        if self.mode in REALTIME_MODES:
            return self._wait_for_trade_live(order_ids, timeout*60, exit_time=exit_time)
        else:
            return self._wait_for_trade_backtest(order_ids, timeout, exit_time=exit_time)
        
    def _wait_for_trade_backtest(self, order_ids:list[str], timeout:float, exit_time:bool=False) -> bool:
        timeout = max(1, round(timeout))
        
        for i in range(timeout):
            self.context.blotter.simulate(self.context.timestamp)
            orders = [self.get_order(oid, algo=False) for oid in order_ids]
            if None in orders:
                msg = f'Some of the order IDs returned no corresponding order.'
                raise OrderNotFound(msg)
            done = all([o.is_final() for o in orders]) # type: ignore -> already None checked
            if done:
                return True
            
        return False
        
    def _wait_for_trade_live(self, order_ids:list[str], timeout:float, exit_time:bool=False) -> bool:
        event = threading.Event()
        all_done = False
        time_left = timeout
        
        def _check_orders(wait=True):
            event.clear()
            orders = [self.get_order(oid, algo=False) for oid in order_ids]
            if None in orders:
                details = {oid:o for oid,o in zip(order_ids, orders)}
                msg = f'some of the order IDs returned no corresponding order:{details}'
                raise OrderNotFound(msg)
            
            if all([o.is_final() for o in orders]): # type: ignore -> already None checked
                # we are already done
                return True
            
            if wait:
                self.add_waiter(event)
                return False
            
            timestamp = pd.Timestamp.now(tz=self.context.trading_calendar.tz)
            if self.mode not in REALTIME_MODES:
                # backtest is single threaded, cannot support this api
                msg = f'waiting for trade is not supported in mode {self.mode.name}.'
                raise ValidationError(msg)
            else:
                self.reconcile_all(
                        timestamp, forced=True, exit_time=exit_time, 
                        fetch_orders=exit_time, only_txns=True, simulate=True)
                
                orders = [self.get_order(oid, algo=False) for oid in order_ids]
                if None in orders:
                    details = {oid:o for oid,o in zip(order_ids, orders)}
                    msg = f'no order(s) found for some of the order ID(s):{details}'
                    raise OrderNotFound(msg)
                done = all([o.is_final() for o in orders]) # type: ignore -> already None checked
                
                if done:
                    self.remove_waiter(event)
                
                return done
            
        start_time = time.time()
        while time_left > 0:
            all_done = _check_orders()
            if all_done:
                self.remove_waiter(event)
                return True
            
            try:
                # event is already added in the waiter, wait on it
                event.wait(timeout=time_left)
            except TimeoutError:
                # if timed out return the final check result
                return _check_orders(wait=False)
            else:
                time_left -= time.time() - start_time
                
        # if timed out return the final check result
        return _check_orders(wait=False)
    
    @api_method
    @ensure_state(states=["TRADING_BAR"],
                  err_msg="can only be called during a trading session.")
    def wait_for_exit(self, assets:list[Asset]|Asset|None=None, check_orders:bool=True, 
                      check_positions:bool=True, timeout:float=1) -> bool:
        """
            Wait for positions/ orders on a list of assets to exit (or timeout). If
            `assets` is `None`, will wait for the whole portfolio in the 
            current context to exit. Returns True if position(s)/ order(s) for the 
            specified assets are exited, else returns False. For backtesting, 
            timeout will be rounded to integer and that many simulation 
            attempts will be made. For live, the timeout signifies number of 
            minutes before giving up and returning False (in case the positions
            are still outstanding).
            
            Use this method if you want to wait to ensure an exit to succeed
            (either following an `square_off` or exit order call) synchronously.
            Note: for square_off call with ioc=True, the square_off itself 
            will return the success as its returned value (True or False) in a 
            synchronous manner. Also for order functions, you can have a 
            similar effect with a following call of `wait_for_trade` (that 
            waits for the order(s) to succeed).

            :param list assets: A list of assets, or a single asset or None.
            :param bool check_order: wait for open orders only
            :param bool check_positions: wait for open positions only
            :param float timeout: Maximum time (in minutes) to wait.

            .. warning::
                For backtest, this API function will attempt to fill any 
                outstanding order(s) entirely in the current minute bar 
                (a total `timeout` attempts will be made). This is different 
                than the regular simulation model, where the fill happens in 
                the next bar (and any remaining amount is attempted in the 
                subsequent trading minutes).
                
            .. warning::
                If the underlying broker does not support order streaming 
                (in live trading) or data streaming (in paper mode), this 
                will block the algo execution for the amount of time 
                specified in `timeout`. It is not recommended to use this 
                API in such cases, especially with a large timeout.
                
            .. danger::
                This API blocks the execution of the algo till the positions 
                are not open anymore or the timeout triggers. A large 
                timeout value can block the algo for a long time.
                
            .. seealso:: 
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.square_off`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.wait_for_trade`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.exit_when_done`
        """
        return self._wait_for_exit(assets=assets, check_orders=check_orders, 
                      check_positions=check_positions, timeout=timeout)
    
    def _wait_for_exit(self, assets=None, check_orders=True, 
                      check_positions=True, timeout:float=1):
        if assets is None:
            assets = []
        elif assets and not listlike(assets):
            assets = [assets]
        
        if self.mode in REALTIME_MODES:
            return self._wait_for_exit_live(
                    assets, timeout*60, check_orders, check_positions)
        else:
            return self._wait_for_exit_backtest(
                    assets, timeout, check_orders, check_positions)
        
    def _wait_for_exit_backtest(self, assets:list[Asset], timeout:float, check_order:bool,
                                check_positions:bool) -> bool:
        timeout = max(1, round(timeout))
        assets = set(assets) # type: ignore
        
        for i in range(timeout):
            self.context.blotter.simulate(self.context.timestamp)
            done = True
            positions = {}
            orders = {}
            
            if check_positions:
                positions = self.context.blotter.get_open_positions()
            
            if check_order:
                orders = self.context.blotter.get_open_orders()
            
            if assets:
                if check_positions:
                    done = all([asset not in positions for asset in assets]) and done
                if check_order:
                    done = all([o.is_final() for oid,o in orders.items() if o.asset in assets]) and done
            else:
                if check_positions:
                    done = not positions and done
                if check_order:
                    done = all([o.is_final() for oid,o in orders.items()]) and done
            
            if done:
                return True
            
        return False
    
    def _wait_for_exit_live(self, assets:list[Asset], timeout:float, check_order:bool,
                            check_positions:bool) -> bool:
        event = threading.Event()
        all_done = False
        time_left = timeout
        assets = set(assets) # type: ignore
        
        def _check_positions(wait=True):
            event.clear()
            done = True
            positions = {}
            orders = {}
            
            if check_positions:
                positions = self.context.blotter.get_open_positions()
            if check_order:
                orders = self.context.blotter.get_open_orders()
            
            if assets:
                if check_positions:
                    done = all([asset not in positions for asset in assets]) and done
                if check_order:
                    done = all([o.is_final() for oid,o in orders.items() if o.asset in assets]) and done
            else:
                if check_positions:
                    done = not positions and done
                if check_order:
                    done = all([o.is_final() for oid,o in orders.items()]) and done
                
            if done:
                return True
            
            if wait:
                self.add_waiter(event)
                return False
            
            timestamp = pd.Timestamp.now(tz=self.context.trading_calendar.tz)
            if self.mode not in REALTIME_MODES:
                # backtest is single threaded, cannot support this api
                msg = f'waiting for trade is not supported in mode {self.mode.name}.'
                raise ValidationError(msg)
            else:
                self.reconcile_all(
                        timestamp, forced=True, exit_time=True, 
                        fetch_orders=True, only_txns=True, simulate=True)
                
                positions = self.context.blotter.get_open_positions()
                orders = self.context.blotter.get_open_orders()
                done = True
                
                if assets:
                    if check_positions:
                        done = all([asset not in positions for asset in assets]) and done
                    if check_order:
                        done = all([o.is_final() for oid,o in orders.items() if o.asset in assets]) and done
                else:
                    if check_positions:
                        done = not positions and done
                    if check_order:
                        done = all([o.is_final() for oid,o in orders.items()]) and done
                
                if done:
                    self.remove_waiter(event)
                
                return done
            
        start_time = time.time()
        while time_left > 0:
            all_done = _check_positions()
            if all_done:
                self.remove_waiter(event)
                return True
            
            try:
                event.wait(timeout=time_left)
            except TimeoutError:
                # if timed out return the final check result
                return _check_positions(wait=False)
            else:
                time_left -= time.time() - start_time
                
        # if timed out return the final check result
        return _check_positions(wait=False)

    @api_method
    def get_datetime(self) -> pd.Timestamp|None:
        """
            Get the current date-time of the algorithm context. For live 
            trading, this returns the current real-time. For a backtest, 
            this will return the simulation time at the time of the 
            call.

            :return: current date-time (Timestamp) in the algo loop.
            :rtype: pandas.Timestamp
            
        """
        if self.mode == MODE.BACKTEST:
            dt = self.context.timestamp
        else:
            dt = pd.Timestamp.now(tz=self.context.trading_calendar.tz)
        return dt

    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def register_trading_controls(self, control:TradingControl):
        """
            Register a custom trading control instance to check before each
            order is created.
            
            :param control: control to implement.
            :type control: see :py:class:`blueshift.core.risks.controls.TradingControl`
            
            .. seealso::
                See details in :py:class:`blueshift.core.risks.controls.TradingControl`.
        """
        if not isinstance(control, TradingControl):
            raise TradingControlError(msg="invalid control type.")

        self._trading_controls.append(control)
        
    @api_method
    def record_state(self, *args):
        """
            Add one or more context variables (user defined context 
            attributes) names as the user defined algo state. Such 
            variables will be persisted across a algo re-start and 
            can be useful for strategies that require tracking of 
            algo specific states (other than orders, positions and 
            performances which are tracked automatically).

            .. note::
                The recorded variables are saved when a context 
                terminates, or when the algo exits. To load the states 
                after a restart, use the `load_state` API function.
                
            .. warning::
                The name of a state must be a string and a valid Python 
                identifier. Also the values must be of type boolean, 
                string, number, list or dict. Maximum number of state 
                variables that can be saved is 10. For a string, maximum 
                allowed number of characters is 128. For a list or a dict,
                maximum size should not exceed 50KB.

            .. seealso::
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.load_state`
                :py:attr:`blueshift.core.algorithm.context.AlgoContext.state`.
        """
        for name in args:
            if not name.isidentifier():
                continue
            self.current_context.record_state(str(name))
            
    @api_method
    def load_state(self):
        """
            Load saved state variables (usually after a restart). This 
            will load the saved state variables for the current context 
            and set the save values as context attributes.

            .. note::
                The recorded values are tracked within the context, as
                ``state`` variable.
                
            .. warning::
                If the last run fails to save the states (illegal variable 
                name or value, or some system error), the loaded state 
                may have missing state variables.

            .. seealso::
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.record_state`
                :py:attr:`blueshift.core.algorithm.context.AlgoContext.state`.
        """
        self.current_context.load_state()

    @api_method
    def record(self, *args, **kwargs):
        """
            Record a list of var-name, value pairs for each day.

            :param kwargs: the names and values to record. Must be in pairs.

            .. note::
                The recorded values are tracked within the context, as
                ``record_vars`` variable. Also, any variable record is kept
                as only one point per day (session). If the user script records
                values at multiple points in a day, only the last value
                will be retained. Use of this function can slow down a 
                backtest.
                
            .. seealso::
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.record_state`.
                :py:attr:`blueshift.core.algorithm.context.AlgoContext.record_vars`.
        """
        args_iter = iter(args)
        var_dict = dict(zip(args_iter,args_iter))
        var_dict = {**var_dict, **kwargs}

        for varname, value in var_dict.items():
            self.current_context.record(varname, value)
            
    def _order_callback_timeout(self, h:OrderTrackerHandle):
        handle = self._order_tracker.remove(h)
        if not handle:
            return
        
        try:
            with self.context.switch_context(h.ctx) as context:
                try:
                    h.timeout(context)
                except Exception as e:
                    self.log_error(f'failed to run order callback timeout routine in context {h.ctx}:{str(e)}')
                    self.handle_sub_strategy_error(e, h.ctx)
        except NoContextError:
            pass
            
    def _order_callback_soon(self, h:OrderTrackerHandle, context:IContext, data:DataPortal):
        if not h:
            return
        
        try:
            with self.context.switch_context(h.ctx) as ctx:
                try:
                    h.trigger(ctx)
                except Exception as e:
                    self.log_error(f'failed to run order callback routine in context {h.ctx}:{str(e)}')
                    self.handle_sub_strategy_error(e, h.ctx)
        except NoContextError:
            pass
    
    @api_method
    def schedule_order_callback(self, callback:OrderCallBack, order_ids:list[str]|str, timeout:float=1, 
                                on_timeout:OrderCallBack|None=None) -> OrderTrackerHandle:
        """
            Add a callback to be called as soon as a list of order IDs are 
            finished. The callback (as well as the timeout handler) must have 
            the call signature of ``f(context, order_ids)``. The handler will 
            be called when none of the orders are open i.e. all of them are 
            either completely filled, or (fully or partially) cancelled or 
            rejected.
            
            On success, returns a handle object that supports "cancel()" method 
            to cancel the callback.
            
            :param callable callback: Callback function to run.
            :param list order_ids: A list of order IDs to wait for.
            :param number timeout: Timeout for wait (in minutes).
            :param callable on_timeout: A callback if timed out.
            
            .. note::
                
                - if all the orders are already in finished state (cancelled, 
                  completed or rejected), the callback will be invoked 
                  immediately. If the orders are not in finished state within 
                  the specified `timeout`, the `on_timeout` callback will be 
                  invoked. Both callbacks must have signature of 
                  `f(context, order_ids)`.
                  
            .. warning::
                This callback will not be invoked as long as any order 
                remains open - which can be technically never. Use the 
                timeout handler, or a schedule_later call to check the order 
                status, to avoid an indefinite wait.
                  
            .. seealso::
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.wait_for_trade`
        """
        if not order_ids:
            order_ids = []
            
        if not listlike(order_ids):
            order_ids = [order_ids] # type: ignore
            
        if not on_timeout:
            on_timeout = noop
            
        try:
            order_ids = list(set([oid for oid in order_ids if oid is not None]))
        except Exception:
            raise ValidationError(f'expected a list of order Ids, got {order_ids}')
            
        # raise OrderNotFound error if missing
        status = [self.current_context.orders[k].is_open() for k in order_ids]
            
        if not status or not any(status):
            # scheduled right away, no need to register with the tracker
            h = OrderTrackerHandle(
                    self._order_tracker, order_ids, callback, 
                    self.current_context.name, on_timeout)
            func = partial(self._order_callback_soon, h)
            self.schedule_soon(func)
            return h
        
        h = self._order_tracker.register(
                order_ids, callback, self.current_context.name, on_timeout)
        h = cast(OrderTrackerHandle, h)
        f = lambda context, data:self._order_callback_timeout(h)
        self.schedule_later(f, timeout)
            
        return h
            
    @api_method
    def schedule_soon(self, callback:UserCallBack) -> EventHandle:
        """
            Add a callback to be called as soon as possible. The callback 
            must have the standard call signature of ``f(context, data)``. 
            The handler will be called for live modes as soon as 
            possible, if in trading hours, else will be called in the 
            next trading bar for the day. For backtest, it will always 
            be next trading bar (exactly same behaviour as 
            ``schedule_once``).
            
            On success, returns a handle object that supports "cancel()" method 
            to cancel the callback.
            
            :param callable callback: Callback function to run.
            
            .. note::
                
                - This schedules the callback to run one time only. For 
                  repetitive callbacks, use ``schedule_function`` below
                  
                - The callback can use this function to schedule itself 
                  recursively, if needed.
                  
            .. seealso::
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.schedule_once`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.schedule_later`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.schedule_at`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.schedule_function`
        """
        if not callable(callback):
            raise ValidationError(f'Invalid callback, expected a callable.')
            
        if self.mode != MODE.BACKTEST and self.is_TRADING_BAR() and \
            self.loop and self._queue:
            id_ = uuid.uuid4().hex
            packet = ((callback, self.current_context.name, None, id_), AlgoTimeoutEvent.TIMEOUT)
            h = self.loop.call_soon_threadsafe(
                    self._queue.put_no_repeat, packet, self)
            handle = EventHandle(self._scheduler, handle=h, id_=id_)
            self._scheduled_events[id_] = handle
            return handle
        else:
            return self.schedule_once(callback)
            
    @api_method
    def schedule_once(self, callback:UserCallBack) -> EventHandle:
        """
            Add a callback to be called once at the next event processing 
            cycle. The callback must have the standard call signature of 
            ``f(context, data)``. The handler will be called in the next
            trading bar - for both backtest and live modes.
            
            On success, returns a handle object that supports "cancel()" method 
            to cancel the callback.
            
            :param callable callback: Callback function to run.
            
            .. note::
                
                - This schedules the callback to run one time only. For 
                  repetitive callbacks, use ``schedule_function`` below
                  
                - The callback can use this function to schedule itself 
                  recursively, if needed.
                  
            .. seealso::
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.schedule_soon`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.schedule_later`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.schedule_at`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.schedule_function`
        """
        if not callable(callback):
            raise ValidationError(f'Invalid callback, expected a callable.')
            
        return self._scheduler.call_soon(callback, self.current_context.name)
        
    def _tickle(self, context, callback, id_=None):
        try:
            packet = ((callback, context, None, id_), AlgoTimeoutEvent.TIMEOUT)
            if self.loop and self._queue:
                h = self.loop.call_soon_threadsafe(
                        self._queue.put_no_repeat, packet, self)
                if id_ and id_ in self._scheduled_events:
                    self._scheduled_events[id_].update_handle(h)
        except Exception as e:
            e.algo = self # type: ignore
            raise e
    
    @api_method
    def schedule_later(self, callback:UserCallBack, delay:float) -> EventHandle:
        """
            Add a callback to be called once after a specified delay (in 
            minutes). The callback must have the signature 
            ``f(context, data)``. The callback will be triggered during 
            the market hour only.
            
            On success, returns a handle object that supports "cancel()" method 
            to cancel the callback.
            
            :param callable callback: Callback function to run.
            :param number delay: Delay in minutes (can be fractional).
            
            .. note::
                
                - This schedules the callback to run one time only. For 
                  repetitive callbacks, use ``schedule_function`` below
                  
                - The callback can use this function to schedule itself 
                  recursively, if needed.
                  
                - You cannot use this method from the `initialize` or the 
                  `before_trading_start` methods. This function must be 
                  called during trading hours.
                  
                - You can use a fractional number to run a function at 
                  higher frequency resolution than minute. For example 
                  specifying delay=0.1 will run the callback after 6 seconds.
                  This is only applicable for live runs. For backtests, it 
                  will fall back to one minute minimum. The minimum delay 
                  that can be specified is 1 second.
                  
            .. warning::
                
                There is no guarantee the function will be called at the 
                exact delay, but if it is called, it will be called 
                at least after the specified delay amount. Also, scheduling 
                are not carried forward over the end-of-day.
                
            .. seealso::
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.schedule_soon`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.schedule_once`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.schedule_at`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.schedule_function`
                  
        """
        if not callable(callback):
            raise ValidationError(f'Invalid callback, expected a callable.')
                
        if self.mode == MODE.BACKTEST:
            # we do not have a realtime clock. Use the scheduler for this
            delay = math.ceil(delay) # integer minute
            now = self.get_datetime()
            if now is None:
                raise ValidationError(f'current timestamp not yet set.')
            target = now + pd.Timedelta(minutes=delay)
            rule = TriggerOnce(target)
            e = TimeEvent(self._env.current_context, rule, callback)
            return self._scheduler.add_event(e, eod_reset=True)
        
        try:
            float(delay)
        except Exception:
            raise ValidationError(f'Invalid delay, expected a number, got {delay}.')
        
        try:
            if not self.loop or not self._queue:
                raise ScheduleFunctionError(f'Failed to schedule callback, no event loop.')
            
            context_name = self.current_context.name
            delay = delay*60 # minutes to second
            id_ = uuid.uuid4().hex
            packet = ((callback, context_name, delay, id_), AlgoTimeoutEvent.TIMEOUT)
            h = self.loop.call_soon_threadsafe(
                    self._queue.put_no_repeat, packet, self)
            handle = EventHandle(self._scheduler, handle=h, id_=id_)
            self._scheduled_events[id_] = handle
            return handle
        except Exception as e:
            raise ScheduleFunctionError(f'Failed to schedule callback:{str(e)}.')

    @api_method
    def schedule_function(self, callback:UserCallBack, date_rule:Callable|None=None, 
                          time_rule:Callable|None=None) -> EventHandle:
        """
            Schedule a callable to be executed repeatedly by a set of date 
            and time based rules. Schedule function can only be triggered 
            during trading hours. The callable in the schedule function 
            will be run before `handle_data` for that trading bar. The 
            callback must accept two arguments - :ref:`context <Context Object>` 
            and :ref:`data<Data Object>`.
            
            On success, returns a handle object that supports "cancel()" method 
            to cancel the scheduled function.

            :param function callback: A function with signature ``f(context, data)``.
            :param date_rule: Defines schedules in terms of dates.
            :type date_rule: see :py:class:`blueshift.api.date_rules`
            :param time_rule: Defines schedules in terms of time.
            :type time_rule: see :py:class:`blueshift.api.time_rules`
                
            .. warning::
                
                - This method can only be used within the 
                  :ref:`initialize<Initialize>` function. Attempting to set a 
                  scheduled callback anywhere else will raise error and 
                  crash the algo.
                
                - The offset should be meaningful and always non-negative.
                  For e.g. although the hours offset can be maximum 23, 
                  using such an offset is not meaningful for shorter trading 
                  hours (unless it is a 24x7 market).
                
                - In live trading, there is no guarantee that the scheduled 
                  function will be called at exactly at the scheduled 
                  date and time. It may be delayed if the algorithm is 
                  busy with some other function. The function is guaranteed 
                  to be called no sooner than the scheduled date and time,
                  and as soon as possible after that.
                
            .. seealso::
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.schedule_soon`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.schedule_once`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.schedule_at`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.schedule_later`

        """
        date_rule = date_rule or date_rules.every_day()
        time_rule = time_rule or time_rules.every_nth_minute()

        if self.context.timestamp is None:
            raise ScheduleFunctionError(msg="cannot schedule, algo setup not complete.")

        if not hasattr(date_rule, 'date_rule'):
            raise ScheduleFunctionError(msg="not a valid date rule.")

        if not hasattr(time_rule, 'time_rule'):
            raise ScheduleFunctionError(msg="not a valid time rule.")

        if not callable(callback):
            raise ScheduleFunctionError(msg="callback must be a callable.")

        if self.mode == MODE.BACKTEST:
            clock = cast(SimulationClock, self.context.clock)
            start_dt = pd.Timestamp(clock.start_nano, tz=self.context.trading_calendar.tz)
            end_dt = pd.Timestamp(clock.end_nano, tz=self.context.trading_calendar.tz)
            rule = TimeRule(self.context.timestamp, date_rule, time_rule, 
                            start_dt=start_dt, end_dt=end_dt,
                            trading_calendar = self.context.trading_calendar)
        else:
            rule = TimeRule(self.context.timestamp, date_rule, time_rule,
                            trading_calendar = self.context.trading_calendar,
                            mode = self.mode)
                
        e = TimeEvent(self._env.current_context, rule, callback)
        return self._scheduler.add_event(e)
        
    @api_method
    def schedule_at(self, callback:UserCallBack, at:str|tuple[int,int]|datetime.time) -> EventHandle:
        """
            Add a callback to be called once at a specified time (in 
            hh:mm format or as a hour,minute tuple). The callback must have the 
            signature ``f(context, data)``. The callback will be triggered during 
            the market hour only.
            
            On success, returns a handle object that supports "cancel()" method 
            to cancel the callback.
            
            :param callable callback: Callback function to run.
            :param number at: Time as hh:mm or as hour,minute tuple.
            
            .. note::
                
                - This schedules the callback to run one time only. For 
                  repetitive callbacks, use ``schedule_function`` below
                  
                - The callback can use this function to schedule itself 
                  recursively, if needed.
                  
                - You cannot use this method from the `initialize` or the 
                  `before_trading_start` methods. This function must be 
                  called during trading hours.
                  
            .. warning::
                
                There is no guarantee the function will be called at the 
                exact time, but if it is called, it will be called 
                at least at or after the scheduled time. Also, scheduling 
                will not be carried forward over the end-of-day.
                
            .. seealso::
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.schedule_soon`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.schedule_once`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.schedule_later`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.schedule_function`
                  
        """
        if isinstance(at, str):
            # look for format hh:mm
            try:
                hh, mm = at.split(':')
                at = (int(hh), int(mm))
            except Exception:
                msg = f'Invalid time string, expected in hh:mm format.'
                raise ScheduleFunctionError(msg=msg)
        
        try:
            if not isinstance(at, datetime.time):
                at = datetime.time(*at)
        except Exception:
            msg = f'Invalid argument, expected in hh:mm format or a tuple '
            msg += f'of integer (hours, minutes). Got {at}.'
            raise ScheduleFunctionError(msg=msg)
            
        now = self.get_datetime()
        if now is None:
            raise ScheduleFunctionError(msg="cannot schedule, algo setup not complete.")
        
        target = datetime.datetime.combine(now.date(), at)
        delay = target - now.tz_localize(None).to_pydatetime()
        delay = delay.total_seconds()/60
        
        if delay == int(delay):
            delay = int(delay)
        
        if delay > 0:
            return self.schedule_later(callback, delay)
        return self.schedule_soon(callback)

    @api_method
    def symbol(self,symbol_str:str, dt:pd.Timestamp|None=None, *args, **kwargs) -> MarketData:
        """
            API function to resolve a symbol string to an asset. See 
            :ref:`symbology<Asset Symbology>` for more details.

            :param str symbol_str: The symbol of the asset to fetch.
            :param timestamp dt: Optional datetime argument.
            :return: the :ref:`asset<Asset>` object.
            :raises: ``SymbolNotFound`` exception if no matching asset found.
            
            .. important::
                By passing a keyword argument `dt`, you can force it to 
                resolve a rolling symbol to a dated asset in backtests. In 
                live trading, it is ignored and assets are always resolved 
                as dated.
            
            .. important::
                In live trading, passing a keyword argument `research=True` 
                will trigger a query to the saved research data, instead of 
                realtime data. In case the live broker implementation does 
                not support it, it will raise an error.
        """
        if isinstance(symbol_str, MarketData):
            return symbol_str
        
        kwargs['logger'] = self.logger
        asset = self.context.asset_finder.symbol(
                symbol_str, dt=dt, *args, **kwargs)
        
        if asset.is_opt() and asset.strike_type in (StrikeType.DEL, StrikeType.PREMIUM): # type: ignore
            dt = self.get_datetime()
            asset = self.context.asset_finder.symbol(
                symbol_str, dt=dt, *args, **kwargs)
        
        return asset
    
    @api_method
    def get_dated_asset(self, asset:MarketData) -> MarketData:
        """
            API function to fetch dated asset for a rolling asset.
            
            :param asset: asset to convert to corresponding dated asset.
            :type asset: :ref:`asset<Asset>` object.
        """
        if asset.is_rolling():
            return self.symbol(asset.exchange_ticker, dt=self.context.timestamp)
        return asset

    @api_method
    def symbols(self, symbols:list[str], dt:pd.Timestamp|None=None, *args, **kwargs)->list[MarketData]:
        """
            API function to resolve a list of symbols to assets.

            :param list symbols: The list of symbols.
            :return: A ``list`` of :ref:`asset<Asset>` objects.
            :raises: ``SymbolNotFound`` exception if no matching asset found.
            
        """
        kwargs['logger'] = self.logger
        return [self.context.asset_finder.symbol(
                sym, dt=dt, *args, **kwargs) for sym in symbols]

    @api_method
    @lru_cache(maxsize=4096)
    def sid(self, sec_id:int) -> MarketData:
        """
            API function to resolve a symbol identifier to an asset. This
            method resolves the ID to an asset object corresponding to the 
            underlying pipeline asset store (usually the daily store). If 
            no such store available, this will raise SymbolNotFoundError.
            
            .. important::
                Blueshift stores assets in multiple stores with different 
                frequency resolutions and sources. There is no guarantee 
                that the security ID of the same asset will be the same 
                across different stores. In general, you should never use 
                this API to fetch assets. This is only intended to be used 
                in defining custom pipeline implementation where you need a 
                resolution from asset id returned by the pipeline engine 
                in the `compute` function.
            
            :param int sec_id: The symbol ID of the asset to fetch.
            :return: the :ref:`asset<Asset>` object.
            :raises: ``SymbolNotFound`` exception if no matching asset found.
            
            .. seealso:: meth:`.symbol`.
            
        """
        try:
            lib = self.context.broker.library
            if lib and lib.pipeline_store:
                return lib.pipeline_store.sid(sec_id)
            raise SymbolNotFound(f'sid is supported only if pipeline is supported as well.')
        except Exception as e:
            raise SymbolNotFound from e

    @api_method
    def can_trade(self, assets:Asset|list[Asset]) -> bool:
        """
            Function to check if asset can be traded at current dt. 
            This requires the asset to be alive as well as we should 
            be within trading hours.

            :param list assets: List of assets to check
            :return: ``True`` if all assets in the list can be traded, 
                else ``False``.
            :rtype: bool
            
            .. seealso:: :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.is_alive`
        """
        if not self.is_TRADING_BAR():
            return False

        return self.is_alive(assets)
    
    @api_method
    def is_alive(self, assets:Asset|list[Asset]) -> bool:
        """
            Function to check if asset is alive (not expired or 
            expelled otherwise).

            :param list assets: List of assets to check
            :return: ``True`` if all assets in the list can be traded, 
                else ``False``.
            :rtype: bool
            
            .. seealso:: :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.can_trade`
        """
        if not listlike(assets):
            assets = [assets] # type: ignore
        
        assets = cast(list[Asset], assets)
        dt = self.get_datetime()
        if not dt:
            raise ValidationError(f'Can you check if asset(s) alive - algo not setup yet.')

        _can_trade:list[bool] = []
        for asset in assets:
            if asset.auto_close_date:
                exp_dt = asset.auto_close_date
                closes = self.context.trading_calendar.close_time
                exp_dt = datetime.datetime.combine(exp_dt.date(), closes)
                exp_dt = exp_dt - datetime.timedelta(hours=0, minutes=1)
                exp_dt = make_consistent_tz(exp_dt,self.context.trading_calendar.tz)
                _can_trade.append(exp_dt >= dt)

        return all(_can_trade)
    
    def _maybe_get_converted_asset(self, asset:Asset) -> Asset:
        """
            Convert asset to position assets if required.
        """
        if asset.is_opt() and asset.is_rolling():
            return self.get_dated_asset(asset) # type: ignore
        
        return asset

    def _control_fail_handler(self, control:TradingControl, asset:Asset, dt:pd.Timestamp, amount:float, 
                              context:IContext):
        """
            Default control validation fail handler, logs a warning.
        """
        msg = control.get_error_msg(asset, dt)
        self.log_warning(msg, timestamp=self.context.timestamp)
        
    def _is_entry_order(self, asset:Asset, order:Order) -> bool:
        positions = self.get_open_positions(algo=False)
        
        if asset not in positions:
            return True
        
        side = 1 if order.side == OrderSide.BUY else -1
        exposure = positions[asset].quantity
        order_exposure = order.quantity*side
        
        if abs(exposure) < abs(exposure + order_exposure):
            return True
        
        return False

    def _validate_trading_controls(self, order:Order, dry_run:bool=False) -> bool:
        """
            Validate all controls. Return false with the first fail.
        """
        asset = self.get_asset_from_order(order)
        if self._is_entry_order(asset, order) and not self._exits_handler.validate_cooloff(asset):
            msg = f'Cannot place order for asset {asset}, '
            msg += 'in cool-off period after exit.'
            self.log_warning(msg, timestamp=self.context.timestamp)
            return False
        
        for control in self._trading_controls:
            if not control.validate(order, self.context.timestamp, # type: ignore
                                    self.context,
                                    self._control_fail_handler,
                                    dry_run=dry_run):
                if self.mode != MODE.BACKTEST:
                    msg = f'Cannot place order for {order.asset.symbol}, '
                    msg += f'a trading control check failed.'
                    self.log_warning(msg, timestamp=self.context.timestamp)
                return False
        return True


    def _freeze_trading(self):
        """ set trading to freeze. """
        self.__freeze_trading:bool = True

    def _unfreeze_trading(self):
        """ reset frozen trading. """
        self.__freeze_trading:bool = False

    # list of trading control APIs affecting order functions

    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def set_max_order_size(self, assets:dict[Asset,float]|list[Asset]|None=None, 
                           max_quantity:float|None=None, max_notional:float|None=None, 
                           on_fail:Callable|None=None):
        """
            Set a limit on the order size - either in terms of quantity,
            or value. Any order exceeding this limit will not be 
            processed and a warning will be raised.

            :param list assets: List of assets for this control. If
                assets is a dict, it should be in asset:value format, and
                will only apply to the assets mentioned. If assets is a
                list, the same value will apply to all assets. If assets is
                None, the control value will apply to ALL assets.
            
            :param int max_quantity: Maximum quantity allowed (unsigned).
            :param float max_notoinal: Maximum value at current price.
            
            .. note:
                Only one control of this type can be live in the system
                at any point in time. Registering another control of this
                type will silently overwrite the existing one.
        """
        default:float=0

        if not max_quantity and not max_notional:
            msg = "must specify either max quantity or "
            msg = msg + "max notional"
            raise TradingControlError(msg=msg)
        elif max_quantity and max_notional:
            msg = "cannot have both controls:"
            msg = msg + " max quantity and max notional"
            raise TradingControlError(msg=msg)
        elif max_quantity:
            ctrl = 0
            value = cast(float, max_quantity)
        else:
            ctrl = 1
            value = cast(float, max_notional)

        limit_dict:dict[Asset,float] = {}
        if isinstance(assets, dict):
            limit_dict = assets
            default = min(limit_dict.values())
        elif listlike(assets):
            limit_dict = dict(zip(assets,[value]*len(assets))) # type: ignore
            default = value
        else:
            default = value

        if on_fail is None:
            on_fail = noop

        if ctrl == 0:
            control = TCOrderQtyPerTrade(
                    default, limit_dict, on_fail, ccy=self._env.ccy)
        else:
            control = TCOrderValuePerTrade(
                    default, limit_dict, on_fail, ccy=self._env.ccy)

        self.register_trading_controls(control)

    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def set_max_daily_size(self, assets:dict[Asset,float]|list[Asset]|None=None, 
                           max_quantity:float|None=None, max_notional:float|None=None, 
                           on_fail:Callable|None=None):
        """
            Set a limit on the order size - in terms of total daily 
            limits. If ``assets`` is None, it is applied for all assets,
            else only for assets in the list.

            :param list assets: A list of assets for position control.
            :param int max_quantity: Maximum order quantity allowed.
            :param float max_notional: Maximum order value allowed.
            
            .. warning::
                Specifying both max_quantity and max_notional will raise
                exception.
                
            .. important::
                This control will count an order as success on a successful
                check and will add its quantity or order value to its daily
                quota, irrespective of whether the order is actually 
                executed. For example and order that passed the check but 
                later cancelled by the user or rejected by the broker will 
                still count towards the daily quota.
            
        """
        default:float = 0
        if not max_quantity and not max_notional:
            msg = "must specify either max quantity or "
            msg = msg + "max notional"
            raise TradingControlError(msg=msg)
        elif max_quantity and max_notional:
            msg = "cannot have both controls:"
            msg = msg + " max quantity and max notional"
            raise TradingControlError(msg=msg)
        elif max_quantity:
            ctrl = 0
            limit = cast(float,max_quantity)
        else:
            ctrl = 1
            limit = cast(float, max_notional)

        limit_dict:dict[Asset,float] = {}
        if isinstance(assets, dict):
            limit_dict = assets
            default = min(limit_dict.values())
        elif listlike(assets):
            limit_dict = dict(zip(assets,[limit]*len(assets))) # type: ignore
            default = limit
        else:
            default = limit

        on_fail = noop if on_fail is None else on_fail

        if ctrl == 0:
            control = TCOrderQtyPerDay(
                    default, limit_dict, on_fail, ccy=self._env.ccy)
        else:
            control = TCOrderValuePerDay(
                    default, limit_dict, on_fail, ccy=self._env.ccy)

        self.register_trading_controls(control)

    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def set_max_position_size(self, assets:dict[Asset,float]|list[Asset]|None=None, 
                              max_quantity:float|None=None, max_notional:float|None=None, 
                              on_fail:Callable|None=None):
        """
            Set a limit on the position size (as opposed to order size).
            Any order that can exceed this position (at current prices)
            will be refused (and will raise a warning). If ``assets`` 
            is None, it is applied for all assets, else only for assets 
            in the list.

            :param list assets: A list of assets for position control.
            :param int max_quantity: Maximum position quantity allowed.
            :param float max_notional: Maximum position exposure allowed.
            
            .. warning::
                Specifying both max_quantity and max_notional will raise
                exception.
        """
        default:float = 0
        if not max_quantity and not max_notional:
            msg = "must specify either max quantity or "
            msg = msg + "max notional"
            raise TradingControlError(msg=msg)
        elif max_quantity and max_notional:
            msg = "cannot have both controls:"
            msg = msg + " max quantity and max notional"
            raise TradingControlError(msg=msg)
        elif max_quantity:
            ctrl = 0
            limit = cast(float, max_quantity)
        else:
            ctrl = 1
            limit = cast(float, max_notional)

        limit_dict:dict[Asset, float] = {}
        if isinstance(assets, dict):
            limit_dict = assets
            default = min(limit_dict.values())
        elif listlike(assets):
            limit_dict = dict(zip(assets,[limit]*len(assets))) # type: ignore
            default = limit
        else:
            default = limit

        on_fail = noop if on_fail is None else on_fail

        if ctrl == 0:
            control = TCPositionQty(
                    default, limit_dict, on_fail, ccy=self._env.ccy)
        else:
            control = TCPositionValue(
                    default, limit_dict, on_fail, ccy=self._env.ccy)

        self.register_trading_controls(control)

    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def set_max_order_count(self, max_count:int, on_fail:Callable|None=None):
        """
            Set a limit on the maximum number of orders generated in a day.
            Any order that can exceed this limit will be refused (and
            will raise a warning).

            :param int max_count: Maximum number of orders allowed per 
                session (day).
                
            .. important::
                This control will count an order as success on a successful
                check and will add it to the daily quota, irrespective of 
                whether the order is actually executed. For example and 
                order that passed the check but later cancelled by the user 
                or rejected by the broker will still count towards the 
                daily quota.
        """
        on_fail = noop if on_fail is None else on_fail
        control = TCOrderNumPerDay(max_count, on_fail, ccy=self._env.ccy)
        self.register_trading_controls(control)

    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def set_long_only(self, on_fail:Callable|None=None):
        """
            Set a flag for long only algorithm. Any short-selling order 
            (attempt to sell without owning the assets) will be refused 
            (and a warning raised).
            
        """
        on_fail = noop if on_fail is None else on_fail
        control = TCLongOnly(on_fail, ccy=self._env.ccy)
        self.register_trading_controls(control)

    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def set_max_leverage(self, max_leverage:float, on_fail:Callable|None=None):
        """
            Set a limit on the account gross leverage . Any order that 
            can potentially exceed this limit will be refused 
            (with a warning).

            :param float max_leverage: Maximum allowed leverage.
        """
        on_fail = noop if on_fail is None else on_fail
        control = TCGrossLeverage(max_leverage, on_fail, ccy=self._env.ccy)
        self.register_trading_controls(control)

    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def set_max_exposure(self, max_exposure:float, on_fail:Callable|None=None):
        """
            Set a limit on the account gross exposure. Any order that 
            can potentially exceed this limit will be refused 
            (with a warning).
            
            :param float max_exposure: Maximum allowed exposure.
        """
        on_fail = noop if on_fail is None else on_fail
        control = TCGrossExposure(max_exposure, on_fail, ccy=self._env.ccy)
        self.register_trading_controls(control)

    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def set_do_not_order_list(self, assets:Asset|list[Asset], on_fail:Callable|None=None):
        """
            Defines a list of assets not to be ordered. Any order on
            these assets will be refused (with a warning).
            
            :param list assets: A list of assets.

            .. seealso:: :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.set_allowed_list`
        """
        on_fail = noop if on_fail is None else on_fail
        if not listlike(assets):
            assets = [assets] # type: ignore

        assets = cast(list, assets)
        control = TCBlackList(assets, on_fail, ccy=self._env.ccy)
        self.register_trading_controls(control)

    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def set_allowed_list(self, assets:Asset|list[Asset], on_fail:Callable|None=None):
        """
            Defines a whitelist of assets to be ordered. Any order
            outside these assets will be refused (with a warning). 
            Usually, the user script will use either this function or 
            the blacklist function ``set_do_not_order_list``, but 
            not both.
            
            :param list assets: A list of assets.

            .. seealso:: :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.set_do_not_order_list`
        """
        on_fail = noop if on_fail is None else on_fail
        if not listlike(assets):
            assets = [assets] # type: ignore
        assets = cast(list, assets)
        control = TCWhiteList(assets, on_fail, ccy=self._env.ccy)
        self.register_trading_controls(control)
        
    def _is_fractional(self, asset:Asset) -> bool:
        # fractional if broker default settings is fractional
        fractional1 = self.current_context.broker.is_default_fractional
        # elif asset supports fractional AND mult is fraction
        fractional2 = asset.fractional and asset.mult < 1
        # otherwise "fractional=True" must be specified
        fractional = fractional1 or fractional2
            
        return fractional
    
    def _infer_product_type(self, asset:Asset, **kwargs) -> tuple[Asset, ProductType]:
        product_type = kwargs.get('product_type')
        
        if product_type and isinstance(product_type, str):
            product_type = kwargs['product_type'].lower()
            if product_type not in ('delivery','intraday','margin'):
                msg = f"Product type {product_type} is not supported."
                raise ValidationError(msg)
            else:
                kwargs[product_type] = True
        
        if 'intraday' in kwargs and kwargs['intraday']:
            product_type = ProductType.INTRADAY
        elif 'margin' in kwargs and kwargs['margin']:
            product_type = ProductType.MARGIN
        elif 'delivery' in kwargs and kwargs['delivery']:
            product_type = ProductType.DELIVERY
        else:
            asset_type = asset.get_product_type()
            if asset_type in (ProductType.MARGIN, ProductType.INTRADAY):
                product_type = asset_type

        if product_type:
            try:
                product_type = ProductType(product_type)
            except Exception:
                raise ValidationError(f'Illegal product type {product_type}')
        
        if product_type == ProductType.ALGO:
            msg = f'For algo order, create an AlgoOrder instance and pass '
            msg += f'it to the order function.'
            raise ValidationError(msg)
        
        if product_type is None:
            product_type = self.current_context.broker.supported_products[0]
            
        if product_type not in self.current_context.broker.supported_products:
            type_ = product_type.name
            msg = f"Product type {type_} is not supported"
            msg += " by the broker."
            raise ValidationError(msg)
            
        if asset.get_product_type() != product_type:
            converted = self.current_context.broker.convert_asset(
                    asset, product_type)
            if not converted:
                msg = f"Product type {product_type} not compatible"
                msg = msg + " with asset {asset}."
                raise ValidationError(msg) 
            asset = converted
        
        return asset, product_type

    def _create_and_validate_orders(self, asset:Asset, quantity:float,
              limit_price:float=0, stop_price:float=0, bypass_control:bool=False,
              style=None, dry_run:bool=False, **kwargs) -> tuple[bool, Order|None, float, str]:
        if self.__freeze_trading and not bypass_control:
            msg = f'Cannot place order, trading flag is disabled for this algo.'
            return False, None, 0, msg

        if not isinstance(asset, Asset):
            msg = f"can't place order for {asset.symbol},"
            msg += " not a tradable asset."
            return False, None, 0, msg
        
        try:
            float(quantity)
        except Exception:
            msg = f"Cannot place order for {asset.symbol}, not a valid quantity {quantity}"
            return False, None, 0, msg
            
        if math.isnan(quantity):
            msg = f"Cannot place order for {asset.symbol}, quantity is NaN."
            return False, None, 0, msg
        
        mult = asset.mult
        fractional = kwargs.get('fractional',False) or self._is_fractional(asset)
        
        if fractional:
            mult = float(kwargs.get('min_quantity',0)) or mult
            if mult < 1:
                # make sure quantity is multuple of "mult"
                quantity = round(quantity/mult)*mult
            else:
                # an arbitray 2 decimal place precision
                quantity = round(quantity, 2)
        else:
            quantity = int(round(quantity/mult)*mult)
            
        if quantity == 0:
            msg = '0'
            if asset not in self._order_warning and self.mode != MODE.BACKTEST:
                self._order_warning.add(asset)
                msg = f'Cannot place order for asset {asset.symbol}, 0 quantity implied. Check '
                msg += f'requested quantity is multiple of lot size. If '
                msg += f'you are using order sizing based on capital, '
                msg += f'make sure enough capital is allocated.'
                
            return False, None, 0, msg

        side = OrderSide.BUY if quantity > 0 else OrderSide.SELL

        order_type = 0
        if style:
            order_type = style
        else:
            if limit_price > 0:
                order_type = 1
            if stop_price > 0:
                order_type = order_type|2

        try:
            order_type = OrderType(order_type)
        except Exception:
            raise ValidationError(f'illegal order type {order_type}')
        
        try:
            asset, product_type = self._infer_product_type(asset, **kwargs)
        except ValidationError as e:
            msg = f'Cannot place order for asset {asset.symbol}:{str(e)}.'
            return False, None, 0, msg

        validity = OrderValidity.IOC if DEFAULT_IOC_ORDER else OrderValidity.DAY
        if 'validity' in kwargs:
            validity = kwargs['validity']
            values = dict(OrderValidity.__members__)
            if isinstance(validity, str):
                key = validity.upper()
                if key in values:
                    validity = values[key]

            try:
                validity = OrderValidity(validity)
            except Exception:
                msg = f'Cannot place order - illegal order validity {validity}.'
                return False, None, 0, msg
                
        t1 = time.time_ns()
        if 'last_price' in kwargs:
            price_at_entry = kwargs['last_price']
        else:
            try:
                price_at_entry = self.context.data_portal.current(
                        asset, "close")
            except Exception:
                # should not fail the order for this
                price_at_entry = 0 # missing price at entry
        delay = (time.time_ns()-t1)/1000000
        price_at_entry = cast(float,price_at_entry)
        
        user = str(self._env.platform)
        placed_by = self.current_context.algo_id or ''
        tag = self.context.name
        parent_order_id = self.current_context.name
        remark = str(kwargs.pop('remark',''))
        reference_id = kwargs.pop('reference_id', None)
        
        o = Order(quantity=abs(quantity), side=side, asset=asset, product_type=product_type,
                  order_type = order_type, order_validity=validity,
                  price=limit_price, stoploss_price=stop_price,
                  user=user, placed_by=placed_by, tag=tag,
                  parent_order_id=parent_order_id, 
                  timestamp=self.get_datetime(), fractional=fractional,
                  price_at_entry=price_at_entry, 
                  create_latency=0, remark=remark)
        
        if reference_id:
            o.set_reference_id(str(reference_id))

        if not bypass_control:
            checks_ok = self._validate_trading_controls(o, dry_run=dry_run)
            if not checks_ok:
                return False, None, 0, 'trading control check failed'
            
        create_latency=0
        if 'latency' in kwargs:
            create_latency = (time.time_ns() - kwargs.pop('latency'))/1000000
            create_latency = max(0, create_latency - delay)
            create_latency = round(create_latency,3)
            o.set_latency(create_latency, 0)
        
        return True, o, delay, 'success'

    def _handle_backtest_order(self, asset:Order|Asset, quantity:float,
              limit_price:float=0, stop_price:float=0, bypass_control:bool=False,
              style=None, **kwargs) -> str|None:
        if not isinstance(asset, Order):
            success, obj, _, msg = self._create_and_validate_orders(
                    asset, quantity, limit_price, stop_price, bypass_control,
                    style, dry_run=False, **kwargs)
    
            if not success or not obj:
                if msg !='0':
                    self.log_warning(msg, timestamp=self.context.timestamp)
                return
        else:
            obj = asset
            if not bypass_control:
                checks_ok = self._validate_trading_controls(obj, dry_run=False)
                if not checks_ok:
                    self.log_warning('trading control check failed')
                    return

        if obj.is_final():
            raise OrderError(f'attempt to place an order which is not open.')

        # Backtest should always return order ID or raise exception
        broker = cast(IBacktestBroker, self.current_context.broker)
        order_id = broker.place_order(obj, **kwargs)
        if order_id:
            # order ID is already updated
            self.current_context.blotter.add_transactions(order_id, obj, 0, 0)

        return order_id

    def _handle_notification_order(self, asset:Order|Asset, quantity:float,
              limit_price:float=0, stop_price:float=0, bypass_control:bool=False,
              style=None, timeout:float|None=None, **kwargs) -> str|None:
        if not isinstance(asset, Order):
            success, obj, _, msg = self._create_and_validate_orders(
                    asset, quantity, limit_price, stop_price, bypass_control,
                    style, dry_run=True, **kwargs)
    
            if not success or not obj:
                if msg=='0':msg = '0 order quantity implied.'
                self.log_warning(msg, timestamp=self.context.timestamp)
                return
        else:
            obj = asset
            if not bypass_control:
                checks_ok = self._validate_trading_controls(obj, dry_run=False)
                if not checks_ok:
                    self.log_warning('trading control check failed')
                    return
                
        if obj.is_final():
            raise OrderError(f'attempt to place an order which is not open.')

        # notifications are never with bypass control,
        # so we always check if we have exceeded the limits.
        ts = self.context.timestamp
        if not ts or not self.context.notifier:
            self.log_warning('Cannot create order notification, algo setup not done yet.')
            return

        if not self._notification_control.validate(
                obj, ts, self.context,
                self._control_fail_handler, dry_run=False):
            return
        
        try:
            notification_id = self.context.notifier.notify(
                    ctx=self.current_context,
                    method=OneclickMethod.PLACE_ORDER,
                    order=obj,
                    name=self.context.name,
                    broker_name=self.context.broker_name,
                    timeout=timeout)
        except Exception as e:
            raise OrderError(f'failed to send onclick notification:{str(e)}') from e

        msg = f"sending notification:{obj.to_json()}"
        self.log_info(msg, self.context.timestamp)

        return notification_id
    
    def _send_order_to_broker(self, order:Order, **kwargs) -> str|None:
        kwargs['algo_name'] = self._env.name
        kwargs['algo_user'] = self._env.algo_user
        kwargs['logger'] = self.logger
        do_not_reject:bool = kwargs.get('do_not_reject', False)
        
        delay = kwargs.get('order_delay',0)
        if_market = order.order_type == OrderType.MARKET
        
        start_time = time.time_ns()
        
        # use lock to ensure we do not skip order update as the update 
        # comes before this function exits. No timeout error handling 
        # as this is not expected to fail.
        with self._order_lock:
            try:
                order = self.current_context.broker.pretrade(order, logger=self.logger)
                order_id = self.current_context.broker.place_order(order, **kwargs)
            except Exception as e:
                msg = f'Failed to place order for {order.asset.symbol}:{str(e)}.'
                self.context.blotter.emit_notify_msg(msg, msg_type='error')
                
                if not do_not_reject and self.mode in REALTIME_MODES:
                    # smart execution expected to handle rejections explictly
                    if not order.remark:
                        try:
                            remark = str(e.__class__.__name__)
                            parts = re.sub( r"([A-Z])", r" \1", remark).split()
                            remark = ' '.join(parts).lower()
                            order.set_remark(remark)
                        except Exception:
                            pass
                    
                    order.reject(get_clean_err_msg(str(e)))
                    self.current_context.blotter.emit_transactions_msg(
                            {order.oid:order})
                    
                raise e
                
            end_time = time.time_ns()
            # we may have done str() on None!!
            order_id = None if order_id=='None' else order_id
            new_order_id = None
            
            exec_latency = (end_time-start_time)/1000000 + delay
            exec_latency = max(0, exec_latency)
            exec_latency = round(exec_latency,3)
            
            if order_id and not listlike(order_id): # unwinds can result in lists
                # this works as strings are explicitly considered  not "listlike", 
                # but partialOrder, a namedtuple, is listlike
                order_id = str(order_id)
                order.set_order_id(order_id)
                order.set_latency(0,exec_latency)
                self.current_context.blotter.add_transactions(
                        order_id, order, 0, 0, if_market=if_market)
                self._schedule_order_check(order_id)
            elif order_id and isinstance(order_id, PartialOrder):
                # this works as named tuples are non-string iterables
                if order_id.quantity < order.quantity:
                    msg = f'Order filled partially via unwinding for quantity {order_id.quantity}.'
                    self.log_warning(msg)
                order = Order.from_dict(order.to_dict())
                order.update_from_dict({'quantity': order_id.quantity})
                order.set_order_id(order_id.oid)
                order.set_latency(0,exec_latency)
                order_id = str(order_id.oid)
                self.current_context.blotter.add_transactions(
                        order_id, order, 0, 0, if_market=if_market)
                self._schedule_order_check(order_id)
            elif order_id and listlike(order_id):
                # we assume the last order id is the new order, and the rest 
                # one or more are unwinds. We need to match only the new order
                order_id = list(order_id)
                if not isinstance(order_id[-1], PartialOrder):
                    new_order_id = str(order_id.pop())
                else:
                    new_order_id = None
                original_qty = order.quantity
                
                for oid in order_id:
                    if isinstance(oid, PartialOrder):
                        unwind = Order.from_dict(order.to_dict())
                        unwind.update_from_dict({'quantity': oid.quantity})
                        unwind.set_order_id(oid.oid)
                        unwind.set_latency(0,exec_latency)
                        self.current_context.blotter.add_transactions(
                            oid.oid, unwind, 0, 0, if_market=if_market)
                        self._schedule_order_check(oid.oid)
                        original_qty -= oid.quantity
                    else:
                        msg = f'Illegal order IDs in {order_id}, expected Partial orders.'
                        raise ValidationError(msg)
                        
                if original_qty < 0:
                    msg = f'Negative order quantity {original_qty} after unwinds, '
                    msg += f'original quantity {order.quantity}.'
                    raise ValidationError(msg)
                
                if new_order_id:
                    order.update_from_dict({'quantity': original_qty})
                    order.set_order_id(new_order_id)
                    order.set_latency(0,exec_latency)
                    self.current_context.blotter.add_transactions(
                            new_order_id, order, 0, 0, if_market=if_market)
                    self._schedule_order_check(new_order_id)
                    order_id = new_order_id
                else:
                    try:
                        order_id = str(order_id[-1].oid) # type: ignore
                    except Exception:
                        msg = f'Empty order IDs {order_id}, expected Partial orders.'
                        raise ValidationError(msg)
                
        if order_id and self.mode == MODE.PAPER:
            # put an idle packet for order simulation
            self.schedule_idle(kind='possible')
        
        if order_id:
            msg = f"sent order successfully, order id:{order_id}"
            self.log_platform(msg, self.context.timestamp)
            msg = f'New order {order_id} placed for {order.asset}'
            self.context.blotter.emit_notify_msg(msg)
        
        return str(order_id)

    def _handle_live_order(self, asset:Order|Asset, quantity:float,
              limit_price:float=0, stop_price:float=0, bypass_control:bool=False,
              style=None, **kwargs) -> str |None:
        if not isinstance(asset, Order):
            success, obj, delay, msg = self._create_and_validate_orders(
                    asset, quantity, limit_price, stop_price, bypass_control,
                    style, dry_run=False, **kwargs)
    
            if not success or not obj:
                if msg=='0':msg = '0 order quantity implied.'
                self.context.blotter.emit_notify_msg(msg, msg_type='error')
                self.log_warning(msg, timestamp=self.context.timestamp)
                return
        else:
            obj = asset
            delay = 0
            if not bypass_control:
                checks_ok = self._validate_trading_controls(obj, dry_run=False)
                if not checks_ok:
                    self.log_warning('trading control check failed')
                    return
                
        if obj.is_final():
            raise OrderError(f'attempt to place an order which is not open.')
            
        kwargs['order_delay'] = delay
        msg = f"sending order to broker:{obj.to_json()}"
        self.log_platform(msg, self.context.timestamp)
        return self._send_order_to_broker(obj, **kwargs)

    def confirm_order(self, order:Order, bypass_control:bool=False,
              style=None):
        if self.execution_mode != ExecutionMode.ONECLICK:
            return

        if not bypass_control:
            checks_ok = self._validate_trading_controls(order, dry_run=False)
            if not checks_ok:
                return None

        msg = f"sending order confirmation to broker:{order.to_json()}"
        self.log_platform(msg, self.context.timestamp)
        return self._send_order_to_broker(order)

    # TODO: cythonize the creation of order
    @api_method
    def create_order(self, asset:Asset, quantity:float, limit_price:float=0, stop_price:float=0, 
                     **kwargs) -> Order:
        """
            Create order object - which can be passed on to an ordering 
            function. See the `order` API method for parameters. User this 
            to speed up order placement by pre-creating order.
            
            .. seealso:: 
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.order`
        """
        kwargs.pop('bypass_control', None)
        kwargs.pop('dry_run', None)
        
        return self._create_order(
                asset, quantity, limit_price=limit_price, 
                stop_price=stop_price, bypass_control=True,
                dry_run=False, **kwargs)
    
    def _create_order(self, asset:Asset, quantity:float, limit_price:float=0, stop_price:float=0, 
                     **kwargs) -> Order:
        """
            Create order object - which can be passed on to an ordering 
            function. See the `order` API method for parameters. User this 
            to speed up order placement by pre-creating order.
            
            .. seealso:: 
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.order`
        """
        bypass_control:bool = kwargs.pop('bypass_control', True)
        dry_run:bool = kwargs.pop('dry_run', False)
        kwargs.pop('style', None)
        
        success, obj, _, msg = self._create_and_validate_orders(
                asset, quantity, limit_price, stop_price, 
                bypass_control=bypass_control, style=None, dry_run=dry_run, **kwargs)
        
        if not success or not obj:
            side = 'buy' if quantity >=0 else 'sell'
            qty = abs(quantity)
            if msg=='0':msg = '0 order quantity implied'
            msg = f'failed to create {side} order for {qty}@{asset}:{msg}'
                
            raise OrderError(msg)
            
        return obj
    
    @api_method
    @ensure_state(states=["TRADING_BAR"],
                  err_msg="can only be called during a trading session.")
    def order(self, asset:Asset|IAlgoOrder|Order|list[Order], quantity:float|None=None,
              limit_price:float=0, stop_price:float=0, style=None, 
              **kwargs) -> str|list[str]|None:
        """
            Place a new order. This is the interface to the underlying broker
            for ALL order related API functions. The first argument can be 
            an order object (created through `create_order`), in which case 
            all some order related parameters may be ignore if passed. The 
            first argument can also be an execution algo object (an instance 
            of a subclass of IAlgoOrder).In this case, any other arguments 
            will be ignored.

            The handling of limit and stop price specification is totally
            implementation dependent. In case the broker supports limit
            orders, limit_price will be effective.
            
            .. note::
                You can pass an AlgoOrder object to trade execution algorithm.
                See :ref:`Execution Algorithms<Execution Algorithms>`.

            .. important::
                - A successful call of this method will return an order ID, 
                  or a list of order IDs (in case of `max_retries`, see below).  
                  The method will return just after placing the order, and 
                  will not wait for the execution to complete. You must use 
                  the returned order ID to check the status of the order.
                  If no order ID is returned, it means the order placement 
                  failed. However, no exception will be raised. Only errors 
                  reported by the broker while placing the order will be 
                  raised as exception (e.g. `OrderError` or `APIError`).
                
                - Orders can be placed during active trading hours only. 
                  This means only :ref:`handle_data<Handle Data>` or 
                  :ref:`scheduled callback<Scheduled Callback Functions>` 
                  functions, or appropriate trade or data callback functions
                  are eligible to place orders.
                  
                - Order with zero implied quantity (e.g. order size less 
                  than lot-size) will silently fail. No order will be 
                  sent to the broker.
                
                - At present only limit and market orders are supported. 
                  Stop loss specification will be ignored. 
                  
                - You can also specify `max_retries`, an integer, to signify 
                  an order with retries. In this case, the validity will be 
                  assumed to be `IOC` and `quantity` must be a whole number 
                  and `fractional` must be false. If not, the retry option 
                  will be ignored and regular order will be placed. The 
                  return values will be a list of order IDs (instead of a 
                  single order ID). Since the validity is `IOC`, the method
                  will return after the execution is complete.
                  
                - For order using rolling assets, see the caveats 
                  :ref:`here<How to place orders>`.
                  
                - For intraday products, order placement will be refused 
                  if the broker follows an intraday cut-off time (
                  usually 15 mins from the end of trading day).
                
                - Always check if the return value is None or a valid 
                  order id. For some live brokers, successful orders 
                  that result in an existing position unwind may return 
                  a ``None`` value. Also a rejected order or an order 
                  that failed validation checks may return a ``None`` 
                  value (e.g. order with 0 quantity).
                  
                - If running in `oneclick` execution mode, the returned 
                  notification ID can be exchanged to an order ID using 
                  the API function `get_order_by_notification_id`. This 
                  will return None if the notification is cancelled or not yet 
                  confirmed by the user or is expired. Will return an order
                  ID if the user confirmed the order.
                  
                - The asset supplied to the order function may not be the 
                  same asset in the order object. Use `get_asset_for_order`
                  to determine what the asset in the order object will be.
                  
                - The asset supplied to the order function, or the asset in 
                  the order object may not be the same as the asset in the 
                  position resulting from the order execution. Refer to the 
                  `get_asset_from_order` API function for this.

            :param asset: asset on which the order to be placed (or and AlgoOrder).
            :type asset: :ref:`asset<Asset>` object.
            :param int quantity: units to order (> 0 is buy, < 0 is sale).
            :param float limit_price: limit price for limit order
            :param float stop_price: Stop-loss price (currently ignored).
            :param kwargs: Extra keyword parameters passed on to the broker.
            :return: order ID or a list of order IDs or None. This can be either 
                an order ID, or a notification ID (when running in `oneclick` 
                execution mode), or `None` (in case the order did not generate 
                any actual orders to be sent to the broker, e.g. order with 0 
                quantity) or a list of order IDs (in case of order with retries, 
                i.e. when `max_retries` is specified).
            :rtype: str or list or None.
            
            Recognized keyword arguments are `max_retries` (see above), 
            `fractional`, `validity`, `product_type` and 'reference_id'. 
            If `fractional` is true, `quantity`can be a fraction - if both 
            the asset and the broker supports fractional trading. If specified, `
            validity` must be of type :ref:`OrderValidity<OrderValidity>` 
            and `product_type` should be of type :ref:`ProductType<ProductType>` 
            (names are also accepted, instead of `enums`). User 'reference_id' 
            to add a user defined identifier for the order (not necessary 
            unique for each order).
            
            .. seealso:: 
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.create_order`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.wait_for_trade`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.get_order`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.get_open_orders`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.get_order_by_notification_id`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.get_asset_for_order`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.get_asset_from_order`
                :py:attr:`blueshift.lib.execution.algo_orders.IAlgoOrder`

            .. danger:: 
                No ordering function will check the capacity of the 
                account to validate the order (e.g. cash, margin
                requirements etc.). You must check before placing 
                any order.
        """
        kwargs.pop('bypass_control', None)
        
        if not self.is_TRADING_BAR():
            msg = f"can't place order for {asset},"
            msg += " market not open."
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='warning')
            self.log_warning(msg, timestamp=self.context.timestamp)
            return
        
        if isinstance(asset, IAlgoOrder):
            if 'max_retries' in kwargs:
                msg = f'Algo order type cannot be used with max retry parameter.'
                if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                        msg, msg_type='warning')
                self.log_warning(msg, timestamp=self.context.timestamp)
                return
            else:
                 return self._order(asset, quantity=quantity, limit_price=limit_price, 
                                    stop_price=stop_price, bypass_control=False, style=style, **kwargs)
        
        if listlike(asset):
            asset = list(asset) # type: ignore
        
        if isinstance(asset, list):
            # multi orders
            retries = timeout = 0
            exceptions = None
            try:
                retries = 0
                if 'retries' in kwargs:
                    retries = int(kwargs.pop('retries'))
                elif 'max_retries' in kwargs:
                    retries = int(kwargs.pop('max_retries'))
                
                timeout = float(kwargs.pop('timeout', 1))
                wait = kwargs.pop('wait', False)
                exceptions = kwargs.pop('exceptions', None)
                
                if exceptions and not listlike(exceptions):
                    exceptions = [exceptions]
                
                #assert retries >= 0
                #assert timeout > 0
                if retries < 0 or timeout <= 0:
                    raise ValueError(f'negative retry or timeout parameter.')
            except Exception:
                msg = f"Illegal parameter for multi orders -> "
                msg += f"retries: {retries}, timeout: {timeout} or "
                msg += f"exceptions: {exceptions}, will ignore."
                self.log_warning(msg, timestamp=self.context.timestamp)
                return []
            else:
                asset = cast(list[Order], asset)
                return self._multi_orders(
                        asset, wait=wait, retries=retries, timeout=timeout,
                        exceptions=exceptions, **kwargs)
            
        if isinstance(asset, Order):
            return self._order(asset, quantity=quantity,
                               limit_price=limit_price, stop_price=stop_price, 
                               bypass_control=False, style=style, **kwargs)
        
        if quantity is None:
            msg = f'no order quantity specified.'
            self.log_warning(msg, timestamp=self.context.timestamp)
            return
        
        if 'max_retries' in kwargs and not kwargs.get('fractional',False):
            try:
                max_retries = int(kwargs.pop('max_retries'))
                #assert max_retries >=0
                #assert quantity == int(quantity)
                if max_retries < 0 or quantity != int(quantity):
                    raise ValueError(f'illegal retry or quantity')
            except Exception:
                msg = f"Illegal parameter for order with retries -> "
                msg += f"max retries: {kwargs['max_retries']}, "
                msg += f"quantity: {quantity}, will ignore."
                self.log_warning(msg, timestamp=self.context.timestamp)
            else:
                return self._order_with_retries(
                        asset, quantity, max_retries=max_retries, 
                        limit_price=limit_price, stop_price=stop_price, 
                        style=style, **kwargs)
        
        return self._order(asset, quantity=quantity,
              limit_price=limit_price, stop_price=stop_price, 
              bypass_control=False, style=style, **kwargs)
        
    @api_method
    def _order(self, asset:Asset|IAlgoOrder|Order, quantity:float|None=None,
              limit_price:float=0, stop_price:float=0, bypass_control:bool=False,
              style=None, **kwargs) -> str|None:
        """ Internal order api method. """
        if self.wants_to_quit:
            msg = f'aborting order placement for {quantity}@{asset} as '
            msg +=f'algo is about to exit.'
            raise OrderError(msg)
            
        if 'latency' not in kwargs:
            kwargs['latency'] = time.time_ns()
            
        if isinstance(asset, IAlgoOrder):
            # an instance of AlgoOrder
            return self._algo_order(asset)
        
        oid = None
        
        if self.mode == MODE.BACKTEST:
            return self._handle_backtest_order(
                    asset, quantity,limit_price, stop_price, bypass_control, style, # type:ignore
                    **kwargs)
        else:
            # mode LIVE/ PAPER/ EXECUTION
            if self.execution_mode == ExecutionMode.AUTO or bypass_control:
                # for auto or for bypass control orders, send directly
                oid = self._handle_live_order(
                        asset, quantity,limit_price=limit_price, stop_price=stop_price, # type: ignore
                        bypass_control=bypass_control, 
                        style=style, **kwargs)
            elif self.execution_mode == ExecutionMode.ONECLICK:
                timeout = kwargs.pop('timeout', None)
                # else go through user confirmation route
                oid = self._handle_notification_order(
                        asset, quantity,limit_price=limit_price, stop_price=stop_price, # type: ignore
                        bypass_control=bypass_control, style=style, timeout=timeout, **kwargs)
                
        if oid and self.mode == MODE.PAPER:
            # put an idle packet for order simulation
            self.schedule_idle(kind='possible')
            
        return oid

    @api_method
    def order_value(self, asset:Asset, value:float, limit_price:float=0, stop_price:float=0, 
                    style=None, **kwargs) -> str|list[str]|None:
        """
            Place a new order sized to achieve a certain dollar value, 
            given the current price of the asset.

            :param asset: asset on which the order to be placed.
            :type asset: :ref:`asset<Asset>` object.
            :param float value: dollar value (> 0 is buy, < 0 is sale).
            :param float limit_price: limit price for limit order
            :param float stop_price: Stop-loss price (currently ignored).
            :param kwargs: Extra keyword parameters passed on to the broker.
            :return: order ID.
            :rtype: str or list or None.
            
            .. important::
                See the documentation of the `order` API for other 
                important details.

            .. danger:: This will take into account the current price of 
                the asset, not actual execution price. The total value 
                of execution can exceed the specified target value.
                
            .. seealso:: :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.order`
        """
        if 'latency' not in kwargs:
            kwargs['latency'] = time.time_ns()
            
        if not self.is_TRADING_BAR():
            msg = f"can't place order for {asset.symbol},"
            msg += " market not open."
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='warning')
            self.log_warning(msg, timestamp=self.context.timestamp)
            return
            
        if not isinstance(asset, Asset):
            msg = f'Cannot place order for {asset}, invalid type, '
            msg += f'expected an Asset, got {type(asset)}.'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            raise ValidationError(msg)
            
        try:
            float(value)
        except Exception:
            msg = f'Cannot place order for {asset.symbol}, invalid '
            msg += f'value, expected a number, got {type(value)}.'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            raise ValidationError(msg)
            
        last_price = self.context.data_portal.current(asset, "close")
        last_price = cast(float, last_price)

        if math.isnan(last_price):
            msg = f"Cannot place order for {asset}, cannot place order "
            msg += f"for {asset.symbol}, no price data."
            self.context.blotter.emit_notify_msg(msg, msg_type='error')
            self.log_info(msg)
            return

        last_fx = asset.fx_rate(
                self.context.blotter.ccy, self.context.data_portal)
        if math.isnan(last_fx):
            msg = f"cannot place order for {asset.symbol}, no fx data."
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(msg)
            self.log_info(msg)
            return
        
        # add last price to kwargs for slippage calculation
        kwargs['last_price'] = last_price
        fractional = kwargs.get('fractional',False) or self._is_fractional(asset)
        
        if fractional:
            qty = value/(last_price*last_fx)
        else:
            qty = int(value/(last_price*last_fx))
            
        if not fractional and 'max_retries' in kwargs:
            try:
                max_retries = int(kwargs.pop('max_retries'))
                #assert max_retries >=0
                #assert qty == int(qty)
                if max_retries < 0 or qty != int(qty):
                    raise ValueError(f'illegal retry or quantity')
            except Exception:
                msg = f"Illegal parameter for order with retries -> "
                msg += f"max retries: {kwargs['max_retries']}, "
                msg += f"quantity: {qty}, will ignore."
                self.log_warning(msg, timestamp=self.context.timestamp)
            else:
                return self._order_with_retries(
                        asset, qty, max_retries=max_retries, 
                        limit_price=limit_price, stop_price=stop_price, 
                        style=style, **kwargs)
            
        return self._order(
                asset,qty,limit_price,stop_price,False,style, **kwargs)

    @api_method
    def order_percent(self, asset:Asset, percent:float, limit_price:float=0, stop_price:float=0, 
                      style=None, **kwargs) -> str|list[str]|None:
        """
            Place a new order sized to achieve a certain percentage of 
            the algo net equity, given the current price of the asset. This 
            method applies a haircut of 2% on the current portfolio value 
            for computing the order value, in case of market order.

            :param asset: asset on which the order to be placed.
            :type asset: :ref:`asset<Asset>` object.
            :param float percent: fraction of portfolio value (> 0 is buy, < 0 is sale).
            :param float limit_price: limit price for limit order
            :param float stop_price: Stop-loss price (currently ignored).
            :param kwargs: Extra keyword parameters passed on to the broker.
            :return: order ID.
            :rtype: str or list or None.
            
            .. important::
                See the documentation of the `order` API for other 
                important details.

            .. danger:: 
                This will take in to account current price of the asset 
                and current algo net equity, not actual execution price.
                The total value of execution can exceed the specified 
                percent.
                
            .. seealso:: :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.order`
        """
        if 'latency' not in kwargs:
            kwargs['latency'] = time.time_ns()
            
        if not isinstance(asset, Asset):
            msg = f'Cannot place order for {asset}, invalid type, '
            msg += f'expected an Asset, got {type(asset)}.'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            raise ValidationError(msg)
            
        try:
            float(percent)
        except Exception:
            msg = f'Cannot place order for {asset.symbol}, invalid '
            msg += f'percent, expected number, got {type(percent)}.'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            raise ValidationError(msg)
            
        if self.mode != MODE.BACKTEST:
            timestamp = pd.Timestamp.now(
                    tz=self.context.trading_calendar.tz)
            self.context.blotter.reconcile_all(
                    timestamp, forced=True, fetch_orders=False, exit_time=False)
            
        # apply a margin of 2% for price volatility for market orders
        MARGIN = 0.02 if limit_price==0 else 0
        net = self.current_context.blotter_account["net"]*(1-MARGIN)
        value = net*percent
            
        return self.order_value(
                asset,value,limit_price,stop_price,style,**kwargs)

    @api_method
    def order_target(self, asset:Asset, target:float, limit_price:float=0, stop_price:float=0, 
                     style=None, **kwargs) -> str|list[str]|None:
        """
            Place a new order sized to achieve a position of a certain
            quantity of the asset, taking into account the current 
            positions and outstanding open orders (including one-click 
            orders that require user confirmation).

            :param asset: asset on which the order to be placed.
            :type asset: :ref:`asset<Asset>` object.
            :param int target: units to target (> 0 is buy, < 0 is sale).
            :param float limit_price: limit price for limit order
            :param float stop_price: Stop-loss price (currently ignored).
            :param kwargs: Extra keyword parameters passed on to the broker.
            :return: order ID.
            :rtype: str or list or None.
            
            .. important::
                If the current position in the asset is X, and a target 
                order is placed for Y units, and there is no outstanding 
                open order for the asset, this will place an order for 
                Y-X units. If X>Y, this means a sell order, if X < Y, 
                a buy order. If X is exactly equal to Y, no actions are 
                taken. If there are outstanding open orders, that is 
                added to X before calculating the difference. In case of 
                a significant delay in order update from the broker, this can 
                compute wrong incremental units.
                
                Targetting order will NOT work for rolling option assets
                (as the strike may change with the underlying for a given 
                strike offset over time) and an error will be raised.
                
                Also see the documentation of the `order` API for other 
                important details.
                
            .. seealso:: :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.order`
        """
        if 'latency' not in kwargs:
            kwargs['latency'] = time.time_ns()
            
        if not self.is_TRADING_BAR():
            msg = f"can't place order for {asset.symbol},"
            msg += " market not open."
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='warning')
            self.log_warning(msg, timestamp=self.context.timestamp)
            return
            
        if not isinstance(asset, Asset):
            msg = f'Cannot place order for {asset.symbol}, invalid type, '
            msg += f'expected an Asset, got {type(asset)}.'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            raise ValidationError(msg)
        
        if asset.is_opt() and asset.is_rolling():
            msg = f'Cannot place a target order for a rolling option. '
            msg += f'Use "order" instead.'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            raise ValidationError(msg)
            
        try:
            float(target)
        except Exception:
            msg = f'Cannot place order for {asset.symbol}, invalid target, '
            msg += f'expected a number, got {type(target)}.'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            raise ValidationError(msg)
            
        try:
            asset, product_type = self._infer_product_type(
                    asset, **kwargs)
        except ValidationError as e:
            msg = f'Cannot place order for asset {asset.symbol}:{str(e)}.'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            self.log_warning(msg, timestamp=self.context.timestamp)
            return
        else:
            kwargs['product_type'] = product_type
            
        if self.mode != MODE.BACKTEST:
            timestamp = pd.Timestamp.now(
                    tz=self.context.trading_calendar.tz)
            self.context.blotter.reconcile_all(
                    timestamp, forced=True, fetch_orders=False, exit_time=False)
            
        orders = self.current_context.open_orders_by_asset(asset)
        pos = self.current_context.blotter_portfolio.get(asset, None)
        pos = pos.quantity if pos else 0
        fractional = kwargs.get('fractional',False) or self._is_fractional(asset)
        
        if fractional:
            qty = target - pos
        else:
            qty = int(target - pos)

        outstanding = 0
        for oid in orders:
            o = orders[oid]
            pending = o.pending if o.side == OrderSide.BUY else -o.pending
            outstanding += pending
            
        if self.execution_mode == ExecutionMode.ONECLICK and self.context.notifier:
            orders = self.context.notifier.get_open_notifications_by_asset(
                    asset, self.current_context)
            for oid in orders:
                o = orders[oid]
                pending = o.pending if o.side == OrderSide.BUY else -o.pending
                outstanding += pending
            
        qty = qty-outstanding

        if qty == 0 or abs(qty) < ALMOST_ZERO:
            return
        
        if not fractional and 'max_retries' in kwargs:
            try:
                max_retries = int(kwargs.pop('max_retries'))
                #assert max_retries >=0
                #assert qty == int(qty)
                if max_retries < 0 or qty != int(qty):
                    raise ValueError(f'illegal retry or quantity')
            except Exception:
                msg = f"Illegal parameter for order with retries -> "
                msg += f"max retries: {kwargs['max_retries']}, "
                msg += f"quantity: {qty}, will ignore."
                self.log_warning(msg, timestamp=self.context.timestamp)
            else:
                return self._order_with_retries(
                        asset, qty, max_retries=max_retries, 
                        limit_price=limit_price, stop_price=stop_price, 
                        style=style, **kwargs)
        
        return self._order(
                asset,qty,limit_price,stop_price,False,style,**kwargs)

    @api_method
    def order_target_value(self, asset:Asset, target:float, limit_price:float=0, stop_price:float=0, 
                           style=None, **kwargs) -> str|list[str]|None:
        """
            Place a new order sized to achieve a position of a certain value
            of the asset, taking into account the current positions and 
            outstanding open orders (including one-click orders that require 
            user confirmation).

            :param asset: asset on which the order to be placed.
            :type asset: :ref:`asset<Asset>` object.
            :param float target: value to target (> 0 is buy, < 0 is sale).
            :param float limit_price: limit price for limit order
            :param float stop_price: Stop-loss price (currently ignored).
            :param kwargs: Extra keyword parameters passed on to the broker.
            :return: order ID.
            :rtype: str or list or None.
            
            .. important::
                This order method computes the required unit from the target 
                and the current market price, and does not guarantee the 
                execution price or the value after execution.
                
                See the documentation of the `order` API for other 
                important details.

            .. seealso:: :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.order_value`
        """
        if 'latency' not in kwargs:
            kwargs['latency'] = time.time_ns()
            
        if not isinstance(asset, Asset):
            msg = f'Cannot place order for {asset.symbol}, invalid type, '
            msg += 'expected an Asset, got {type(asset)}.'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            raise ValidationError(msg)
            
        try:
            float(target)
        except Exception:
            msg = f'Cannot place order for {asset.symbol}, invalid '
            msg += 'target, expected a number, got {type(target)}.'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            raise ValidationError(msg)
            
        last_price = self.context.data_portal.current(asset, "close")
        last_price = cast(float, last_price)
        if math.isnan(last_price):
            msg = f"Cannot place order for {asset.symbol}, no price data."
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            self.log_info(msg)
            return

        last_fx = asset.fx_rate(
                self.context.blotter.ccy, self.context.data_portal)
        if math.isnan(last_fx):
            msg = f"cannot place order for {asset.symbol}, no fx data."
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            self.log_info(msg)
            return
        
        # add last price to kwargs for slippage calculation
        kwargs['last_price'] = last_price
        fractional = kwargs.get('fractional',False) or self._is_fractional(asset)
        if fractional:
            target = target/(last_price*last_fx)
        else:
            target = int(target/(last_price*last_fx))
        
        return self.order_target(
                asset,target,limit_price,stop_price,style,**kwargs)

    @api_method
    def order_target_percent(self, asset:Asset, percent:float, limit_price:float=0, stop_price:float=0, 
                             style=None, **kwargs) -> str|list[str]|None:
        """
            Place a new order sized to achieve a position of a certain 
            percent of the net account value. This method applies a haircut 
            of 2% on the current portfolio value for computing the order 
            value, in case of market order.

            :param asset: asset on which the order to be placed.
            :type asset: :ref:`asset<Asset>` object.
            :param float percent: fraction of portfolio value to target (> 0 is buy, < 0 is sale).
            :param float limit_price: limit price for limit order
            :param float stop_price: Stop-loss price (currently ignored).
            :param kwargs: Extra keyword parameters passed on to the broker.
            :return: order ID.
            :rtype: str or list or None.
            
            .. important::
                See the documentation of the `order` API for other 
                important details.

            .. seealso:: :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.order_percent`
        """
        if 'latency' not in kwargs:
            kwargs['latency'] = time.time_ns()
            
        if not isinstance(asset, Asset):
            msg = f'Cannot place order for {asset.symbol}, invalid type, '
            msg += 'expected an Asset, got {type(asset)}.'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            raise ValidationError(msg)
            
        try:
            float(percent)
        except Exception:
            msg = f'Cannot place order for {asset.symbol}, invalid '
            msg += 'percent, expected a number, got {type(percent)}.'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            raise ValidationError(msg)
            
        if self.mode != MODE.BACKTEST:
            timestamp = pd.Timestamp.now(
                    tz=self.context.trading_calendar.tz)
            self.context.blotter.reconcile_all(
                    timestamp, forced=True, fetch_orders=False, exit_time=False)
            
        # apply a margin of 2% for price volatility for market orders
        MARGIN = 0.02 if limit_price==0 else 0
        net = self.current_context.blotter_account["net"]*(1-MARGIN)
        target = net*percent
            
        return self.order_target_value(
                asset,target,limit_price,stop_price,style,**kwargs)
        
    @api_method
    def order_slice(self, asset:Asset, quantity:float, size:float, delay:float=0, limit_price:float=0, 
                    stop_price:float=0, style=None, **kwargs) -> list[str]:
        """
            Slice an order of total size of `quantity` into multiple orders 
            of quantity of `size` each. An order will be placed for maximum 
            `size` amount (or less if total quantity is not fully divisible 
            by size). A list of order IDs (or None values) will be returned 
            for each slice. The slice will stop till the entire quantity is 
            covered or order for any slice fails (raises exception).

            :param asset: asset on which the order to be placed.
            :type asset: :ref:`asset<Asset>` object.
            :param float quantity: Total order quantity.
            :param float size: Size for each order slice.
            :param float delay: Delay (in minutes) between slices.
            :param float limit_price: limit price for limit order
            :param float stop_price: Stop-loss price (currently ignored).
            :param kwargs: Extra keyword parameters passed on to the broker.
            :return list: list of order ID or empty list.

            .. seealso:: :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.order`
        """
        if 'latency' not in kwargs:
            kwargs['latency'] = time.time_ns()
            
        if not self.is_TRADING_BAR():
            msg = f"can't place order for {asset.symbol},"
            msg += " market not open."
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='warning')
            self.log_warning(msg, timestamp=self.context.timestamp)
            return []
        
        if not isinstance(asset, Asset):
            msg = f'Cannot place order for {asset}, invalid type, '
            msg += f'expected an Asset, got {type(asset)}.'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            raise ValidationError(msg)
        
        if quantity != int(quantity):
            msg = f'Cannot place slicing order for {asset}, quantity must be integer.'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            raise ValidationError(msg)
            
        size = abs(size)
        if size != int(size) or size > abs(quantity):
            msg = f'Cannot place slicing order for {asset}, size must be integer '
            msg += f'and less than order quantity.'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            raise ValidationError(msg)
            
        max_retries = 0
        if 'max_retries' in kwargs and not kwargs.get('fractional',False):
            try:
                max_retries = int(kwargs.pop('max_retries'))
                #assert max_retries >=0
                if max_retries < 0:
                    raise ValueError(f'illegal retry parameter')
            except Exception:
                msg = f"Illegal parameter for order with retries -> "
                msg += f"max retries: {kwargs['max_retries']}, "
                msg += f"quantity: {quantity}, will ignore."
                self.log_warning(msg, timestamp=self.context.timestamp)
            
        try:
            delay = delay*60
        except Exception:
            msg = f'Cannot place slicing order for {asset}, delay must be a number'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            raise ValidationError(msg)
        
        def _split(x, y):
            sign = -1 if x < 0 else 1
            x = abs(x)
            r = x
            for _ in range(x//y):
                r -= y
                yield y*sign
                
            if r > 0:
                yield r*sign
        
        
        raise_exception = kwargs.get('raise_exception', True)
        oids = []
        for qty in _split(quantity, size):
            start_time = time.time_ns()
            try:
                if max_retries ==0:
                    oid = self._order(asset, qty, limit_price=limit_price, 
                               stop_price=stop_price, bypass_control=False,
                               style=style, **kwargs)
                    if not oid:
                        raise OrderError(f'No order ID returned from broker.')
                    else:
                        oids.append(oid)
                else:
                    placed = self._order_with_retries(
                            asset, qty, max_retries=max_retries, 
                            limit_price=limit_price, stop_price=stop_price, 
                            bypass_control=False, style=style, **kwargs)
                    oids.extend(placed)
            except Exception as e:
                e.value = oids # type: ignore
                if raise_exception:
                    msg = f'Failed to place a leg of slicing order for '
                    msg += f'size {qty}:{str(e)}.'
                    if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                            msg, msg_type='error')
                    raise e
                else:
                    msg = f'Failed to place a leg of slicing order for '
                    msg += f'size {qty}:{str(e)}.'
                    if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                            msg, msg_type='error')
                    self.log_error(msg)
            else:
                if delay > 0:
                    elapsed = (time.time_ns() - start_time)/NANO_SECOND
                    to_wait = max(0, delay-elapsed)
                    if to_wait > 0:
                        sleep_if_not_main_thread(to_wait)
        
        return oids
    
    
    def _multi_orders(self, orders:list[Order], wait:bool=False, retries:int|None=None, timeout:float=1, 
                      exceptions:list[Type[Exception]]|None=None, **kwargs) -> list[str]:
        """
            A high level internal function to handle the typical workflow 
            for order placement. It accepts a list of order objects and 
            places the orders in sequence. If any order placement from the 
            list fails with an exception of types `exceptions`, it is 
            retries. If it still fails or raises some other type of exception, 
            the next order is retried instead.
            
            Once all orders are placed, if wait is False, the collected 
            order IDs are returned, with None for orders which failed. If 
            wait is true, and all orders are successfull placed (i.e. no 
            None in the order ID list), then we wait for the orders to 
            finalize (i.e., complete, cancel or reject). If the orders 
            times out or completes, the order ID list is returned. Else, 
            if retries is not None, that many attempts are done for 
            partially filled (or unfilled) orders - following the above 
            sequence of steps.
        """
        if not listlike(orders):
            orders = [orders] # type: ignore
            
        valid = all([isinstance(o, Order) for o in orders])
        if not valid:
            raise OrderError(f'expected list of order objects')
            
        if retries is not None:
            try:
                retries = int(retries) + 1
            except Exception:
                raise OrderError(f'retries, if specified, must be a number')
        else:
            retries = 1
            
        if exceptions is None:
            exceptions = [PriceOutOfRange, PreTradeCheckError]
            
        if not wait:
            return self._multi_orders_no_wait(orders, retries, exceptions, **kwargs)
        else:
            return self._multi_orders_wait(orders, retries, timeout, exceptions, **kwargs)
    
    def _multi_orders_no_wait(self, orders:list[Order], retries:int, 
                              exceptions:list[Type[Exception]], **kwargs) -> list[str]:
        bypass_control = kwargs.pop('bypass_control', False)
        kwargs.pop('do_not_reject', None)
        
        order_ids = []
        for o in orders:
            kwargs.pop('reference_id', None)
            reference_id = uuid.uuid4().hex
            oid = None
            
            for i in range(retries):
                try:
                    oid = self._order(
                            o, reference_id=reference_id, 
                            bypass_control=bypass_control, do_not_reject=True, **kwargs)
                except tuple(exceptions) as e:
                    if isinstance(e, PriceOutOfRange):
                        msg = f'Order price out of range, will try again:{str(e)}'
                        self.log_error(msg)
                        sleep_if_not_main_thread(ORDER_RETRY_TIME)
                        continue
                    elif isinstance(e, PreTradeCheckError):
                        msg = f'pre-trade check failed, will try again:{str(e)}'
                        self.log_error(msg)
                        sleep_if_not_main_thread(ORDER_RETRY_TIME)
                        continue
                    else:
                        msg = f'order placement failed, will try again:{str(e)}'
                        self.log_error(msg)
                        sleep_if_not_main_thread(ORDER_RETRY_TIME)
                        continue
                except Exception as e:
                    msg = f'order placement failed:{str(e)}'
                    self.log_error(msg)
                    raise
                    break
                else:
                    break
                    
            order_ids.append(oid)
            
        return order_ids
            
    def _multi_orders_wait(self, orders:list[Order], retries:int, timeout:float, 
                           exceptions:list[Type[Exception]], **kwargs) -> list[str]:
        orders_to_execute = orders
        ret_order_ids = []
        
        # maintain the original limit and stop prices
        oids = [o.oid for o in orders_to_execute]
        limit_prices = {o.oid:o.price for o in orders_to_execute}
        stop_prices = {o.oid:o.stoploss_price for o in orders_to_execute}
        
        if self.mode in REALTIME_MODES:
            self.log_info(f'multiple orders {orders} with retries {retries} and timeout {timeout}')
        
        for i in range(retries):
            order_ids = self._multi_orders_no_wait(
                    orders_to_execute, retries, exceptions, **kwargs)
            ret_order_ids.extend(order_ids)
            
            if None in order_ids:
                # some order placement errors, we will not wait and return
                return ret_order_ids
            
            success = self._wait_for_trade(order_ids, timeout/2, exit_time=False)
            if not success:
                # try again and force an order fetch
                success = self._wait_for_trade(order_ids, timeout/2, exit_time=True)
                if not success:
                    # we are done waiting and timed out, return the list
                    return ret_order_ids
            
            # check if we are done or need to repeat for partial fills
            placed_orders = [self.get_order(oid, algo=False) for oid in order_ids]
            
            if None in placed_orders:
                # we should not be here
                return ret_order_ids
            
            placed_orders = cast(list[Order], placed_orders)
            if not all([o.is_final() for o in placed_orders]):
                # we should not be here
                return ret_order_ids
            
            if all([o.is_done() for o in placed_orders]):
                # we are all good
                return ret_order_ids
            
            # now create a new list of orders to execute
            partial_fills = {oid:o for oid,o in zip(oids, placed_orders) if not o.is_done()}
            orders_to_execute = []
            
            for oid, o in partial_fills.items():
                side = 1 if o.side == OrderSide.BUY else -1
                quantity = o.pending*side
                validity = o.order_validity
                
                new_order = self._create_order(
                        o.asset, quantity, limit_price=limit_prices[oid], 
                        stop_price=stop_prices[oid], validity=validity,
                        product_type=o.product_type, 
                        remark=o.remark, **kwargs)
                orders_to_execute.append(new_order)
            
            # else try again
            oids = [o.oid for o in orders_to_execute]
            limit_prices = {o.oid:o.price for o in orders_to_execute}
            stop_prices = {o.oid:o.stoploss_price for o in orders_to_execute}
            sleep_if_not_main_thread(ORDER_RETRY_TIME)
            continue
        
        return ret_order_ids
            
    
    @api_method
    def order_with_retry(self, asset:Asset, quantity:float, max_retries:int=3,
              limit_price:float=0, stop_price:float=0, style=None, **kwargs) -> list[str]:
        """ See `order_with_retries` """
        return self.order_with_retries(asset, quantity, max_retries=max_retries,
              limit_price=limit_price, stop_price=stop_price, style=style, 
              **kwargs)
    
    @api_method
    def order_with_retries(self, asset:Asset, quantity:float, max_retries:int=3,
              limit_price:float=0, stop_price:float=0, style=None, **kwargs) -> list[str]:
        """
            Place an order with IOC order validty, with retries 
            in case IOC orders is not filled completely. Raises 
            `OrderError` in case of failure to enter the trades, else 
            returns a (list of) order ID(s). Any `validty` keyword 
            argument will be ignored. You can provide a tuple of exception 
            classes to retry (defaults to PriceOutOfRange if not limit order 
            and PreTradeCheckError) in addition to incomplete fill. To remove
            the default behaviour, pass an empty tuple as keyword argument 
            like exceptions=tuple(). Specify an optional keyword argument 
            `timeout` to specify the timeout (in minutes) to wait for IOC 
            order completion/ cancellation for each retry. If the order 
            remains open beyond this period, the order placement will be 
            considered failed and exception will be raised. The default 
            timeout is 10s.
            
            .. important::
                This method returns a `list` of order IDs (unlike a 
                regular order function which returns just the order ID).
                This is required to capture additional orders in case 
                partial execution and cancellation of IOC order. Keep  
                this in mind when handling the returned value.

            :param asset: asset on which the order to be placed.
            :type asset: :ref:`asset<Asset>` object.
            :param float quantity: Total order quantity.
            :param int max_retries: Max number of retries.
            :param float limit_price: limit price for limit order
            :param float stop_price: Stop-loss price (currently ignored).
            :param kwargs: Extra keyword parameters passed on to the broker.
            :return list: list of order ID or empty list.
            
            .. note:: 
                Usually, it is NOT safe to retry an order as there is a 
                chance that it may result in unintended positions - for 
                e.g. cancel before retry failed. This method can retry 
                safely because the cancel before retry is guaranteed by 
                the order validity type. If you want to place IOC orders 
                with retries for a single entry, this method is suitable. 
                For waiting for multiple orders (and with more flexibility), 
                consider using the regular order method(s) and use the 
                `wait_for_trade` API method to wait for them. They are 
                almost equivalent, except this method fixes the order 
                validity to IOC and retries the entry on order cancel or 
                reject.
                
            .. note::
                In backtesting, this will attempt to fill the entire order
                in the current simulation bar, including retries if any. 
                This behaviour is different than the regular simulation 
                model, where the fill happens in the next bar (and any 
                remaining amount is attempted in the subsequent trading 
                minutes).

            .. seealso:: :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.order`
            .. seealso:: :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.wait_for_trade`
        """
        if 'latency' not in kwargs:
            kwargs['latency'] = time.time_ns()
            
        if not self.is_TRADING_BAR():
            msg = f"can't enter position for {asset.symbol},"
            msg += " market not open."
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='warning')
            self.log_warning(msg, timestamp=self.context.timestamp)
            return []
        
        if not isinstance(asset, Asset):
            msg = f'Failed to enter position for {asset}, invalid type, '
            msg += f'expected an Asset, got {type(asset)}.'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            raise ValidationError(msg)
            
        if quantity != int(quantity):
            msg = f'Cannot enter position for {asset}, quantity must be integer.'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            raise ValidationError(msg)
            
        return self._order_with_retries(
                asset, quantity, max_retries=max_retries,
                limit_price=limit_price, stop_price=stop_price, style=style, 
                **kwargs)
    
    def _order_with_retries(self, asset:Asset, quantity:float, max_retries:int=3,
              limit_price:float=0, stop_price:float=0, style=None, **kwargs) -> list[str]:
        if isinstance(asset, Order) and asset.order_validity != OrderValidity.IOC:
            msg = f'order validity must be IOC for retries.'
            raise OrderError(msg)
            
        exceptions = kwargs.pop('exceptions', None)
        if exceptions is None:
            exceptions = [PriceOutOfRange, PreTradeCheckError]
        
        if not listlike(exceptions):
            exceptions = [exceptions]

        exceptions = cast(list[Type[Exception]], exceptions)
        for exc in exceptions:
            if not issubclass(exc, Exception):
                raise OrderError(f'illegal input {exc}, expected and exception type')
        exceptions = tuple(exceptions)
        
        oids = []
        amount = quantity
        reference_id = uuid.uuid4().hex
        
        for attempt in range(max_retries):
            try:
                kwargs['reference_id'] = reference_id
                msg = f'order reference ID {reference_id} with retries for asset {asset}@{amount} '
                msg += f'with wait time {IOC_WAIT_TIME}.'
                self.log_info(msg)
                amount = self._handle_ioc_entry(
                        oids, asset, amount, limit_price=limit_price, 
                        stop_price=stop_price, style=style, **kwargs)
                if amount == 0:
                    break
                reference_id = uuid.uuid4().hex
            except exceptions as e:
                if isinstance(e, PriceOutOfRange) and limit_price==0:
                    msg = f'Order price out of range, will try again:{str(e)}'
                    self.log_error(msg)
                    sleep_if_not_main_thread(ORDER_RETRY_TIME)
                    continue
                
                if isinstance(e, PreTradeCheckError):
                    msg = f'pre-trade check failed, will try again:{str(e)}'
                    self.log_error(msg)
                    sleep_if_not_main_thread(ORDER_RETRY_TIME)
                    continue
                
                msg = 'order placement failed, will try again:{str(e)}'
                self.log_error(msg)
                sleep_if_not_main_thread(ORDER_RETRY_TIME)
                continue
            except Exception as e:
                msg = f'Failed to enter required position for asset {asset}:{str(e)}.'
                if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                        msg, msg_type='error')
                self.log_error(msg)
                e.value=oids # type: ignore
                raise e
            else:
                continue
            
        if amount != 0:
            msg = f'Failed to enter required position for asset {asset}: '
            msg += f'exhausted retries.'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            self.log_error(msg)
            e = OrderError(msg)
            e.value = oids # type: ignore
            raise e
            
        return oids
    
    def _handle_ioc_entry(self, oids:list[str], asset:Asset, qty:float, limit_price:float=0, 
                          stop_price:float=0, style=None, **kwargs) -> float:
        remark = kwargs.pop('remark', None) or 'entry(IOC)'
        kwargs.pop('validity', None)
        timeout = kwargs.pop('timeout', IOC_WAIT_TIME/60)
        
        oid = self._order(
                asset, qty, validity=OrderValidity.IOC, 
                limit_price=limit_price, stop_price=stop_price, 
                style=style, remark=remark, bypass_control=False, **kwargs)
        
        if not oid:
            # order did not go through, return 0 to give up
            # as we are not sure why the order was rejected
            sym = asset.asset.symbol if isinstance(asset, Order) else asset.symbol
            msg = f'Failed to place order for {sym}, please visit '
            msg += f'the broker order page.'
            raise OrderError(msg)
        
        oids.append(oid)
        
        order = self.get_order(oid)
        if order is None:
            # order not found, return 0 to give up
            msg = f'Could not check status for entry IOC order '
            msg += f'{oid} for asset, no order found. Please check your broker account.'
            raise OrderNotFound(msg)
        
        # wait for the order to complete
        if order.is_open():
            # timeout is in seconds, api expects in minutes
            status = self._wait_for_trade(oid, timeout=timeout/2, exit_time=False)
            if not status:
                # wait again -> the total timeout is 2*timeout
                status = self._wait_for_trade(oid, timeout=timeout/2, exit_time=True)
                if not status:
                    o = self.context.broker.get_order(oid)
                    if o is None:
                        msg = f'Entry IOC order {oid} not found.'
                        raise OrderError(msg)
                    if o.is_open():
                        msg = f'Entry IOC order {oid} still open for asset '
                        msg += f'{asset}. Please check your broker account.'
                        raise OrderError(msg)
        
        order = self.get_order(oid)
        if order is None:
            # order not found, return 0 to give up
            msg = f'Could not check order status for entry IOC order '
            msg += f'{oid} for asset, no order found. Please check your broker account.'
            raise OrderNotFound(msg)
        
        if not order.is_final():
            msg = f'Entry IOC order {oid} for asset {asset} still open, '
            msg += f'please check your broker account.'
            raise OrderError(msg)
        elif order.is_done():
            return 0
        else: # cancelled or rejected
            # check remaining amount and place another order
            pending = order.pending
            if pending > 0:
                side = 1 if order.side == OrderSide.BUY else -1
                return side*pending
            
            return 0
        
    def _handle_ioc_square_off(self, asset:Asset, qty:float, product_type:ProductType|None=None, 
                               remark:str|None=None, reference_id:str|None=None,
                               timeout:float|None=None) -> float:
        remark = remark or 'square-off(IOC)'
        
        oid = self._order(
                asset, qty, validity=OrderValidity.IOC, 
                product_type=product_type, bypass_control=True, 
                remark=remark, reference_id=reference_id)
        
        if not oid:
            # order did not go through, return 0 to give up
            msg = f'Failed to square off {asset.symbol}, please visit '
            msg += f'the broker order page'
            raise OrderError(msg)
        
        order = self.get_order(oid)
        if order is None:
            # order not found, return 0 to give up
            msg = f'Could not check status for square off IOC order '
            msg += f'{oid} for asset, no order found. Please check your broker account.'
            raise OrderNotFound(msg)
            
        if not timeout:
            is_main = threading.current_thread() is threading.main_thread()
            timeout = _WAIT_TIME_IOC_EXIT if is_main else IOC_WAIT_TIME
            timeout = timeout/60
        
        if order.is_open():
            status = self._wait_for_trade(oid, timeout=timeout, exit_time=True)
            if not status:
                msg = f'Square off IOC order {oid} still open for asset '
                msg += f'{asset}. Please check your broker account.'
                raise OrderError(msg)
        
        order = self.get_order(oid) # fetch again in case order obj is replaced
        if order is None:
            # order not found, return 0 to give up
            msg = f'Could not check order status for square off IOC order '
            msg += f'{oid} for asset, no order found. Please check your broker account.'
            raise OrderNotFound(msg)
            
        if not order.is_final():
            msg = f'Square off IOC order {oid} for asset {asset} still open, '
            msg += f'please check your broker account.'
            raise OrderError(msg)
        elif order.is_done():
            return 0
        else: # cancelled or rejected
            # check remaining amount and place another order
            pending = order.pending
            if pending > 0:
                side = 1 if order.side == OrderSide.BUY else -1
                return side*pending
            return 0
        
    def _square_off_multi(self, assets:Asset|list[Asset], positions:dict[Asset, Position], 
                          ioc:bool=False, remark:str|None=None, **kwargs) -> bool:
        if not assets:
            return True
        
        if not listlike(assets):
            assets = [assets] # type: ignore
        
        assets = cast(list[Asset], assets)
        is_main = threading.current_thread() is threading.main_thread()          
        max_iter = int(kwargs.get('max_retries', MAX_ITER_IOC_EXIT))
        max_iter=_MAX_ITER_EXIT if is_main else max_iter
        timeout = float(kwargs.get('timeout', IOC_WAIT_TIME/60))
        timeout = _WAIT_TIME_IOC_EXIT if is_main else timeout*60
        timeout = timeout/60
        
        def _is_all_done(orders, targets) -> bool:
            done = {}
            for o in orders:
                if o.asset not in done:done[o.asset] = 0
                done[o.asset] = o.filled + done[o.asset]
                
            for asset in targets:
                if asset not in done:
                    return False
                if targets[asset] != done[asset]:
                    return False
                
            return True
                   
        if self.mode in REALTIME_MODES:
            msg = f'initiating multi square-off...'
            self.log_info(msg)
        
        orders:list[Order] = []
        try:
            for asset in assets:
                qty = positions[asset].quantity
                if qty==0 or abs(qty) < ALMOST_ZERO:
                    continue
                
                fractional = self._is_fractional(asset)
                product_type = positions[asset].product_type
                validity = 'ioc' if ioc else None
                remark = remark or 'square-off (multi)'
                
                if not fractional:qty = int(qty)
                qty = - qty
                
                if validity:
                    o = self._create_order(
                            asset, qty, validity=validity, 
                            product_type=product_type, remark=remark)
                else:
                    o = self._create_order(
                            asset, qty, product_type=product_type, remark=remark)
                    
                if not o:
                    raise ValueError(f'order create control checks.')
                orders.append(o)
        except Exception as e:
            msg = f'Failed to create orders for square off, please check your '
            msg += f'broker account: {str(e)}'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            self.log_platform(msg)
            return False
            
        try:
            targets = {o.asset:o.quantity for o in orders}
            order_ids = self._multi_orders(
                    orders, wait=True, retries=max_iter, timeout=timeout,
                    bypass_control=True)
            
            if not order_ids or None in order_ids:
                raise OrderError('some square-off order placement failed.')
                
            placed:list[Order] = [self.get_order(oid, algo=False) for oid in order_ids] # type: ignore
            
            if None in placed:
                raise OrderError('no order IDs found for some square-off orders.')
            
            if not all([o.is_final() for o in placed]):
                raise OrderError('some square-off order are still open.')
                
            if not _is_all_done(placed, targets):
                raise OrderError('some square-off order failed to complete.')
        except Exception as e:
            msg = f'Failed to square off positions, please check your '
            msg += f'broker account: {str(e)}'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            self.log_platform(msg)
            return False
        else:
            return True
    
    def _square_off(self, asset:Asset, positions:dict[Asset, Position], ioc:bool=False, 
                    remark:str|None=None, **kwargs) -> bool:
        if asset not in positions:
            return True
            
        qty = positions[asset].quantity
        if qty==0 or abs(qty) < ALMOST_ZERO:
            return True
            
        fractional = self._is_fractional(asset)
        product_type = positions[asset].product_type
        
        if not fractional:qty = int(qty)
        qty = - qty
        
        max_iter = int(kwargs.get('max_retries', MAX_ITER_IOC_EXIT))
        is_main = threading.current_thread() is threading.main_thread()
        max_iter=_MAX_ITER_EXIT if is_main else max_iter
        
        if not ioc:
            reference_id = uuid.uuid4().hex
            remark = remark or 'square-off'
            
            for i in range(max_iter):
                # retry in case order fails in pre-trade check or price out
                # of range, else give up. Do not wait for order confirmation
                try:
                    oid = self._order(asset, qty, product_type=product_type, 
                               bypass_control=True, remark=remark, reference_id=reference_id)
                    if not oid:
                        msg = f'Failed to square off position for asset '
                        msg += f'{asset}:no order ID received'
                        if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                                msg, msg_type='error')
                        self.log_error(msg)
                        return False
                    return True
                except PriceOutOfRange as e:
                    msg = f'Order price out of range, will try again: {str(e)}'
                    self.log_error(msg)
                    sleep_if_not_main_thread(ORDER_RETRY_TIME)
                    continue
                except PreTradeCheckError as e:
                    msg = f'pre-trade check failed, will try again:{str(e)}'
                    self.log_error(msg)
                    sleep_if_not_main_thread(ORDER_RETRY_TIME)
                    continue
                except Exception as e:
                    msg = f'Failed to square off position for asset '
                    msg += f'{asset}:{str(e)}.'
                    if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                            msg, msg_type='error')
                    self.log_error(msg)
                    return False
            # all retries exhausted, we give up
            return False
        else:
            timeout = float(kwargs.get('timeout', IOC_WAIT_TIME/60))
            timeout = _WAIT_TIME_IOC_EXIT if is_main else timeout*60
            
            amount = qty
            msg = f'trying IOC square-off with {max_iter} retries and '
            msg += f'and timeout {timeout}s.'
            self.log_info(msg)
            reference_id = uuid.uuid4().hex
            
            for i in range(max_iter):
                if self.wants_to_quit:
                    self.log_platform(f'quitting square-off attempt as the algo exited.')
                    return False
                try:
                    msg = f'trying IOC square-off for asset {asset}@{amount} '
                    self.log_info(msg)
                    amount = self._handle_ioc_square_off(
                            asset, amount, product_type=product_type, 
                            remark=remark, reference_id=reference_id,
                            timeout=timeout/60)
                    if int(amount) == 0:
                        break
                    reference_id = uuid.uuid4().hex
                except PriceOutOfRange as e:
                    msg = f'Order price out of range, will try again: {str(e)}'
                    self.log_error(msg)
                    sleep_if_not_main_thread(ORDER_RETRY_TIME)
                    continue
                except PreTradeCheckError as e:
                    msg = f'pre-trade check failed, will try again:{str(e)}'
                    self.log_error(msg)
                    sleep_if_not_main_thread(ORDER_RETRY_TIME)
                    continue
                except Exception as e:
                    msg = f'Failed to square off position for asset '
                    msg += f'{asset}:{str(e)}.'
                    if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                            msg, msg_type='error')
                    self.log_error(msg)
                    return False
                else:
                    continue
            
            if amount != 0:
                msg = f'Failed to square off position for asset '
                msg += f'{asset}. Please check your broker account.'
                if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                        msg, msg_type='error')
                self.log_platform(msg)
                return False
            return True
                
    def _validate_and_cancel_order(self, order_id:str, order:Order):
        asset = self.get_asset_from_order(order)
        if not self._exits_handler.validate_squareoff(asset):
            msg = f'Skipping cancelling order {order_id} as '
            msg += f'square-off cool-off is on.'
            self.log_warning(msg)
        else:
            self._cancel_order(order_id)
            
    def _validate_and_cancel_notification(self, notification_id:str, order:Order):
        asset = self.get_asset_from_order(order)
        if not self._exits_handler.validate_squareoff(asset):
            msg = f'Skipping cancelling open notification for '
            msg += f'asset {asset} as square-off cool-off is on.'
            self.log_warning(msg)
        else:
            self.cancel_notification(notification_id)
        
    def _algo_order(self, algo:IAlgoOrder, **kwargs):
        """
            Function to place an algorithmic order. The argument must be 
            object of a class derived from `IAlgoOrder`. Placing an algo 
            order will call the `execute` method of the algo order object
            and schedule the `update` method to be called periodically. 
            The user strategy must track the returned algo for status. 
            To cancel a still-open algo order, call the `cancel` method 
            of the object. The `update` method will be called at the 
            same frequency of `handle_data` as well as on a trade 
            event (`on_trade`), as long as the order is `open`.
            
            :param algo: An algo order object.
            :type algo: py:attr:`blueshift.lib.execution.algo_orders.IAlgoOrder`
            
            .. important:: 
                Algo order classes are responsible for implementing the 
                logic of the order placements, as well as tracking the 
                order status. The `IAlgoOrder` class implements similar 
                properties to a regular `Order` (e.g. quantity, status, 
                filled etc.), but they are quite distinct.
                
        """
        if self.execution_mode == ExecutionMode.ONECLICK:
            msg = f'Execution algos are not supported in oneclick mode.'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            raise ValidationError(msg)
            
        if not isinstance(algo, IAlgoOrder):
            msg = f'Failed to place algo order: expected an Algo Order type.'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            raise ValidationError(msg)
        
        if algo.is_final():
            raise OrderError(f'attempt to place an algo order which is not open.')
            
        algo.user = algo.user or str(self._env.platform)
        algo.placed_by = self.current_context.algo_id or ''
        algo.tag = self.context.name
        algo.parent_order_id = self.current_context.name
        if 'remark' in kwargs:
            algo.remark = kwargs.get('remark')
        
        self._algo_order_handler.place_order(algo)
    
    def _trigger_cooloff(self, assets=None, squareoff=False, 
                                     is_global=False, skip_callback=False):            
        self._exits_handler.set_cooloff(
                assets, squareoff=squareoff, is_global=is_global)
        
        if self.mode != MODE.BACKTEST and not skip_callback:
            context = self.current_context
            
            if not assets:
                assets = [None]
            elif not listlike(assets):
                assets=[assets]
            
            for asset in assets:
                h = self._exits_handler_scheduled_resets.get((context, asset), None)
                if h:h.cancel()
                func = partial(self._exits_handler.reset_cooloff, context, asset)
                delay = self._exits_handler.get_cooloff_period()*60
                h = self.run_later(delay, func)
                self._exits_handler_scheduled_resets[(context, asset)] = h

    @api_method
    def square_off(self, assets:Asset|list[Asset]|None=None, algo:bool=False, ioc:bool=USE_IOC_SQUAREOFF, 
                   remark:str|None=None, **kwargs) -> bool:
        """
            Function to square off open positions and cancel
            open orders. Typically useful for end-of-day closure for
            intraday strategies or for risk management.
            
            If `algo` is True, only the positions held in the current active
            context will be squared off. The cool-off enforced after such 
            a square-off will be only enforced in that context.
            
            If `algo` is False, then all positions across contexts will 
            be squared off, and a global cool-off will be set, that will 
            prevent all contexts (the main strategy as well as any other 
            sub-strategies added) to enter a new trade till the cool-off 
            period is elapsed.
            
            if `ioc` is True (or made default), the orders sent for square-off 
            will be IOC and will be retried on partial fill, cancel or reject.
 
            If ``assets`` is `None`, all existing open orders will be
            cancelled, and then all existing positions will be squared off.
            If ``assets`` is a list or a single asset, only those positions
            and orders will be affected.
            
            .. note::
                Square off order for each asset will be sent in sequence.
                For global square-off (algo=False), orders for each context
                will be sent in sequence and may lead to offsetting orders
                in case contexts (main and sub-strategies) have offsetting 
                positions.
                
            .. note::
                A square-off will initiate an exit cool-off period till the 
                next clock tick during which no exits in the asset is 
                permitted - to safeguard against multiple square-off 
                attempts. It will also trigger an entry cooloff that will 
                remain in forced for a period defined by `set_cooloff_period` 
                (defaults to 30 minutes).
                
            .. important::
                This method, unless IOC, places an exit order and does not wait 
                for the order to execute. You can use the API function 
                `exit_when_done` to wait for the transactions (in case you are 
                exiting the strategy after squaring off), or use the API method 
                `wait_for_exit` (in case it is not a call before algo exit).
                Alternatively, you can also check the open positions/ orders 
                explicitly by scheduling repeated callbacks.

            .. important:: 
                This API will bypass any trading controls to make sure 
                square-off orders are not blocked by such controls. 
                Orders will be sent to the broker directly, even 
                in one-click execution mode. Also, see above the behaviour 
                differences between the cases where the underlying broker 
                does or does not support a native square-off method.

            :param list assets: A list of assets, or a single asset or None
            :param bool algo: If we should consider algo or broker positions
            :param bool ioc: If we should use IOC order instead of DAY validity.
            :return bool: True if square-off was successful, False otherwise.

            .. danger:: 
                This function only initiates square off. It does not and 
                cannot ensure actual square-off. Also, in case we attempt 
                to squareoff during a cool-off period, it will be ignored 
                (to prevent multiple squareoff attempts which may lead to 
                unintended positions), but will attempt to cancel any 
                open orders. Using squaring off function repeatedly may lead 
                to cancelling of exit orders triggered by a previous call.
                
            .. seealso:: 
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.set_cooloff_period`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.set_stoploss`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.set_takeprofit`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.set_exit_policy`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.wait_for_exit`
        """
        if not self.is_global_context:
            algo = True
            
        is_global = not algo
        self._err_on_square_off = False
        
        def _squareoff_msg(asset=None):
            if not asset:
                msg = f'Global Square off cool-off flag is on. Attempting to '
            else:
                msg = f'Square off cool-off flag is on for asset {asset}. Attempting to '
            msg += 'squaring off positions multiple time may create '
            msg += 'unexpected positions. If required, stop this '
            msg += 'algo and go to your brokers app to take necessary '
            msg += 'actions. This squareoff order will be ignored.'
                
            return msg
        
        def _square_off_in_ctx(assets):
            status = True
            open_orders = self.current_context.open_orders
            open_positions = self.current_context.blotter_portfolio
                
            if not open_orders and not open_positions:
                # nothing to square-off
                self.log_info(f'nothing to square-off')
                return status
            
            # first cancel the open orders
            if not assets:
                for order_id in open_orders:
                    try:
                        self._validate_and_cancel_order(
                                order_id, open_orders[order_id])
                    except OrderAlreadyProcessed:
                        self.log_info(f'order {order_id} is already processed.')
                    except Exception as e:
                        msg = f'Failed to cancel {order_id} in square-off:{str(e)}.'
                        if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                                msg, msg_type='error')
                        self.log_error(msg)
                        status = False
                        
                if self.execution_mode == ExecutionMode.ONECLICK:
                    for id_, obj in self.get_open_notifications().items():
                        try:
                            self._validate_and_cancel_notification(id_, obj)
                        except Exception as e:
                            msg = f'Failed to cancel notification {id_} in square-off:{str(e)}.'
                            self.log_error(msg)
                            status = False
            else:
                for order_id in open_orders:
                    try:
                        asset = self.get_asset_from_order(open_orders[order_id])
                        if asset not in assets:
                            continue
                        self._validate_and_cancel_order(
                                order_id, open_orders[order_id])
                    except OrderAlreadyProcessed:
                        self.log_info(f'order {order_id} is already processed.')
                    except Exception as e:
                        msg = f'Failed to cancel {order_id} in square-off:{str(e)}.'
                        if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                                msg, msg_type='error')
                        self.log_error(msg)
                        status = False
                   
                if self.execution_mode == ExecutionMode.ONECLICK and self.context.notifier:
                    # in live mode, asset from order will be same
                    for asset in assets:
                        for id_, obj in self.context.notifier.get_open_notifications_by_asset(
                                asset, self.current_context).items():
                            try:
                                self._validate_and_cancel_notification(id_, obj)
                            except Exception as e:
                                msg = f'Failed to cancel notification {id_} in square-off:{str(e)}.'
                                self.log_error(msg)
                                status = False
            
            if self.mode in LIVE_MODES:
                # wait for a while in live mode to affect cancellation
                # if any
                time.sleep(_WAIT_TIME_EXIT)
            
            # then run another recon in case the cancelled order was  
            # partially filled, just before we check what to square-off
            try:
                self.reconcile_all(
                        timestamp=None, forced=True, exit_time=True, 
                        fetch_orders=True, only_txns=True, simulate=True)
            except Exception as e:
                # carry on even if the recon fails
                msg = f'reconciliation in square-off failed - '
                msg += f'check your broker account to confirm:{str(e)}.'
                if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                        msg, msg_type='error')
                self.log_error(msg)
                status = False
                
            open_positions = self.current_context.blotter_portfolio
            if not open_positions:
                self.log_info(f'no open positions to square-off')
                return status
            
            if not assets:
                self.log_info('Squaring off all positions...')
                # sort to pick shorts first (quantity<0) so that buy 
                # orders are placed first
                ordered_assets = sorted(
                        open_positions, 
                        key=lambda x:open_positions[x].quantity)
                if USE_MULTI_SQUAREOFF:
                    status = self._square_off_multi(
                            ordered_assets, open_positions, ioc=ioc, remark=remark,
                            **kwargs)
                else:
                    for asset in ordered_assets:
                        if not self._exits_handler.validate_squareoff(asset):
                            msg = _squareoff_msg(asset)
                            self.log_error(msg)
                            status = False
                            continue
                        try:
                            status &= self._square_off(
                                    asset, open_positions, ioc=ioc, remark=remark,
                                    **kwargs)
                            self._trigger_cooloff(asset, squareoff=True, 
                                                  skip_callback=True)
                        except Exception as e:
                            msg = f'Failed to square off position for '
                            msg += f'asset {asset}:{str(e)}.'
                            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                                    msg, msg_type='error')
                            self.log_platform(msg)
                            status = False
                self._trigger_cooloff(None, squareoff=True)
            else:
                self.log_info(f'Squaring off positions in {assets}...')
                if USE_MULTI_SQUAREOFF:
                    status = self._square_off_multi(
                            assets, open_positions, ioc=ioc, remark=remark,
                            **kwargs)
                else:
                    for asset in assets:
                        if not self._exits_handler.validate_squareoff(asset):
                            msg = _squareoff_msg(asset)
                            self.log_error(msg)
                            status = False
                            continue
                        try:
                            status &= self._square_off(
                                    asset,open_positions, ioc=ioc, remark=remark,
                                    **kwargs)
                            self._trigger_cooloff(asset, squareoff=True)
                        except Exception as e:
                            msg = f'Failed to square off position for '
                            msg += f'asset {asset}:{str(e)}.'
                            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                                    msg, msg_type='error')
                            self.log_platform(msg)
                            status = False
            
            return status
        
        if not self._exits_handler.validate_squareoff():
            # check if we have any square off cool-off on
            msg = _squareoff_msg()
            self.log_error(msg)
            self._err_on_square_off = True
            return False
        
        if not assets:
            assets = []
        elif not listlike(assets):
            assets = [assets] # type: ignore
        
        assets = cast(list[Asset], assets)
        try:
            self.reconcile_all(
                    timestamp=None, forced=True, exit_time=True, 
                    fetch_orders=True, only_txns=True, simulate=True)
        except Exception as e:
            self._err_on_square_off = True
            # carry on even if the recon fails
            msg = f'reconciliation just before square-off failed, '
            msg += f'but will continue with square-off:{str(e)}.'
            self.log_error(msg)
            
        if is_global:
            # square off the main strategy
            success = _square_off_in_ctx(assets)
            # now square off all the substrategies
            strategies = list(self._strategies)
            
            try:
                for name in strategies:
                    with self.context.switch_context(name):
                        try:
                            success &= _square_off_in_ctx(assets)
                        except Exception as e:
                            self.log_error(f'failed to run square-off in context {name}')
                            self.handle_sub_strategy_error(e, name)
            except NoContextError:
                pass
            
            # now set the exit handler cooloff to global
            if not assets:
                self._trigger_cooloff(None, squareoff=True, is_global=is_global)
            
            if not self._err_on_square_off:
                self._err_on_square_off = not success
            return success
        
        status = _square_off_in_ctx(assets)
        if not self._err_on_square_off:
            self._err_on_square_off = not status
        
        return status
        
    
    @api_method
    @ensure_state(states=["TRADING_BAR"],
                  err_msg="can only be called during a trading session.")
    def update_order(self, order_param, *args, **kwargs):
        """
            Function to update an existing open order. The parameter 
            `order_param` can be either an Order object or an order ID.
            Use `limit_price` keyword for updating the price of a limit order. 
            Other keyword support is implementation dependent.

            .. important:: 
                This API will try to modify an existing order, but will 
                fail if the order is already executed.

            :param order_param: An :ref:`order<Order>` object, or a valid
                order ID to modify.
        """
        
        if not self.is_TRADING_BAR():
            msg = f"Can't modify order for {order_param}, market not open."
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='warning')
            raise ValidationError(msg=msg)
            
        if 'limit_price' in kwargs:
            kwargs['price'] = kwargs.pop('limit_price')
        
        kwargs['algo_name'] = self._env.name
        kwargs['algo_user'] = self._env.algo_user
        kwargs['logger'] = self.logger

        order_id = order_param.oid if isinstance(order_param, Order)\
                        else order_param
                        
        if self.mode in REALTIME_MODES:
            msg = f"sending update for order {order_id} to broker:{kwargs}."
            self.log_platform(msg, self.context.timestamp)
        
        try:
            order_param = self.current_context.broker.update_order(
                    order_id, *args, **kwargs)
            
            if order_param:
                msg = f'Update request for order {order_id} sent.'
                if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(msg)
        except Exception as e:
            msg = f'Failed to update order for {order_id}:{str(e)}.'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            raise
        
        if isinstance(order_param, Order):
            old_id = order_id
            # some broker (e.g. Alpaca) replaces the order, i.e. cancel
            # and create a new order (with new order ID) to replace the 
            # existing one. We must add it to the blotter to track
            order_id = order_param.oid
            self.current_context.blotter.add_transactions(
                        order_id, order_param, 0, 0)
            msg = f'Existing order {old_id} got replaced by {order_id}.'
            self.log_platform(msg, self.context.timestamp)
        else:
            order_id = order_param

        if self.mode in REALTIME_MODES:
            msg = f"sent update order for {order_id}"
            self.log_info(msg, self.context.timestamp)
            
        if order_id and self.mode == MODE.PAPER:
            # put an idle packet for order simulation
            self.schedule_idle(kind='possible')

        return order_id
    
    @api_method
    @ensure_state(states=["TRADING_BAR"],
                  err_msg="can only be called during a trading session.")
    def cancel_notification(self, notification_id):
        """
            Function to cancel a pending notification (not confirmed 
            or rejected by the user yet) in oneclick execution mode.

            :param str notification_id: A notification ID to cancel.
        """
        if self.execution_mode == ExecutionMode.ONECLICK and self.context.notifier:
            msg = f'Cancelling oneclick notification ID {notification_id}'
            self.log_warning(msg)
            self.context.notifier.cancel(notification_id)

    @api_method
    @ensure_state(states=["TRADING_BAR"],
                  err_msg="can only be called during a trading session.")
    def cancel_order(self, order_param, *args, **kwargs):
        """
            Function to cancel an open order not filled yet (or 
            partially filled).

            :param order_param: An :ref:`order<Order>` object, or a valid
                order ID to cancel.
            
            .. danger:: 
                This function only initiates a cancel request. It does 
                not and cannot ensure actual cancellation.
        """
        if not self.is_TRADING_BAR():
            msg = f"Can't cancel order, market not open."
            raise ValidationError(msg=msg)
            
        return self._cancel_order(order_param, *args, **kwargs)
        
    def _cancel_order(self, order_param:Order|str, *args, **kwargs):
        order_id = order_param.oid if isinstance(order_param, Order)\
                        else order_param
                        
        kwargs['algo_name'] = self._env.name
        kwargs['algo_user'] = self._env.algo_user
        kwargs['logger'] = self.logger
        
        try:
            order_id = self.current_context.broker.cancel_order(
                    order_id, *args, **kwargs)
            if order_id:
                msg = f'Cancel request for order {order_id} sent.'
                if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(msg)
        except Exception as e:
            msg = f'Failed to cancel order for {order_id}:{str(e)}.'
            if self.mode != MODE.BACKTEST:self.context.blotter.emit_notify_msg(
                    msg, msg_type='error')
            raise OrderError(f'failed to cancel order:{str(e)}') from e

        if self.mode in REALTIME_MODES:
            msg = f"sent cancel order for {order_id}"
            self.log_info(msg, self.context.timestamp)
            
        if order_id and self.mode == MODE.PAPER:
            # put an idle packet for order simulation
            self.schedule_idle(kind='possible')

        return order_id

    @api_method
    def get_order(self, order_id, algo=True) -> Order|None:
        """
            Function to retrieve an order by order id. If `algo` is 
            True, only the current context (the main or any sub 
            strategy context), from where the method is called, is used. 
            Otherwise, the order_id is searched across all contexts.
            
            :param str order_id: A valid order id to retrieve.
            :param bool algo: Whether to search the current context or all contexts.
            :return: The matching :ref:`order<Order>` object.

            .. important:: 
                Up to what history orders can be retrieved depends on 
                broker implementation. Usually for most cases, 
                only the closed orders placed in the current session (day), 
                plus all open orders are available.
        """
        if algo:
            return self.current_context.orders.get(order_id, None)
        else:
            orders = self.context.blotter.get_orders()
            return orders.get(order_id, None)
        
    @api_method
    def get_order_by_notification_id(self, notification_id):
        """
            Function to retrieve an order by notification id. This
            is only valid if the algo is running in oneclick execution
            mode. Notification ID is the ID returned on successful order 
            placement in oneclick mode.
            
            .. note::
                If the notification did not result in an actual order (
                e.g. expired, or cancelled notification), it will return
                None.
            
            :param str notification_id: A valid notification id to retrieve.
            :return: The matching :ref:`order<Order>` object.
        """
        if self.execution_mode == ExecutionMode.ONECLICK and self.context.notifier:
            oid = self.context.notifier.get_order_id_from_notification_id(
                    notification_id)
            if oid:
                return self.get_order(oid)
            
    @api_method
    def get_asset_for_order(self, asset, product_type=None):
        """
            Method to retrieve the asset for an order created from a 
            given asset and product_type. Usually they will be the same, but 
            maybe different if input asset is a rolling asset (in which case
            the equivalent dated asset will be returned), or the underlying 
            broker has a default product type.
            
            .. note:: 
                the asset as it appears in the order attribute 
                will always be dated, even if the order was created using a 
                rolling asset definition.
            
            :param order: An :ref:`asset<Asset>` object
            :return: The matching :ref:`asset<Asset>` object.
        """
        if asset.is_rolling():
            return self.symbol(asset.exchange_ticker, dt=self.context.timestamp)
        asset, _ = self._infer_product_type(asset, product_type=product_type)
        return asset
            
    @api_method
    def get_asset_from_order(self, order):
        """
            Method to retrieve the asset for a position created from a 
            given order. Usually they will be the same, but maybe different 
            if order was created using a rolling asset and the underlying 
            broker implementation tracks positions in terms of rolling 
            assets. 
            
            .. note:: 
                the asset as it appears in the order attribute 
                will always be dated, even if the order was created using a 
                rolling asset definition.
            
            :param order: An :ref:`order<Order>` object
            :return: The matching :ref:`asset<Asset>` object.
        """
        # TODO: this should ideally fetch from the blotter, not broker
        return self.current_context.broker.get_asset_from_order(order)
            
    @api_method
    def get_oneclick_status(self, notification_id, algo=True):
        """
            Function to retrieve the status of by notification id. This
            is only valid if the algo is running in oneclick execution
            mode. Notification ID is the ID returned on successful order 
            placement in oneclick mode. If `algo` is True, the current 
            context is searched, else all the running contexts (i.e. the 
            main context and any sub strategy context, if added) of the 
            current algo are searched for a match.
            
            :param str notification_id: A valid notification id to retrieve.
            :param bool algo: Whether to search the current context or all contexts.
            :return: The :ref:`state<Oneclick notification status>`.
        """
        if self.execution_mode == ExecutionMode.ONECLICK and self.context.notifier:
            ctx = self.current_context if algo else None
            return self.context.notifier.get_status(
                    notification_id, ctx)
        
    @api_method
    def get_oneclick_order(self, notification_id, algo=True):
        """
            Function to retrieve the notification order object. This may 
            be the initial order object created (with an app generated 
            order ID), or an actual order that has been sent to the broker (
            with the actual order ID). Returns None if not found. If 
            `algo` is True, order in the current context is returned, 
            else all the running contexts (i.e. the main 
            context and any sub strategy context, if added) of the 
            current algo are searched for a match.
            
            :param str notification_id: A valid notification id to retrieve.
            :param bool algo: Whether to search the current context or all contexts.
            :return: The :ref:`order<Order>` object matching the id.
        """
        if self.execution_mode == ExecutionMode.ONECLICK and self.context.notifier:
            ctx = self.current_context if algo else None
            return self.context.notifier.get_oneclick_order(
                    notification_id, ctx)
            
    @api_method
    def get_open_notifications(self, algo=True):
        """
            Get a dictionary of all open notifications, keyed by their id. The 
            return value is a dict object with notification IDs (str) as keys 
            and :ref:`order<Order>` objects as values. Applicable only 
            when running in oneclick order confirmation mode. If `algo` 
            is True, notifications in the current context is returned, 
            else all notifications in the running contexts (i.e. the main 
            context and any sub strategy context, if added) of the 
            current algo is returned.

            :param bool algo: Whether to search the current context or all contexts.
            :return: A dictionary of open notifications (yet to be confirmed or expire).
            :rtype: dict
        """
        if self.execution_mode == ExecutionMode.ONECLICK and self.context.notifier:
            ctx = self.current_context if algo else None
            return self.context.notifier.get_open_notifications(ctx)
        
        return {}

    @api_method
    def get_open_orders(self, algo=True):
        """
            Get a dictionary of all open orders, keyed by their id. The 
            return value is a dict object with order IDs (str) as keys 
            and :ref:`order<Order>` objects as values. If `algo` is 
            True, orders in the current context is returned, else all 
            orders running in the context (i.e. the main context and any 
            sub strategy context, if added) of the current algo is 
            returned.
            
            .. note::
                Order asset will be dated asset. To fetch orders by rolling
                asset (applicable only for futures in backtest), use 
                `context.get_open_orders_by_asset` instead.

            :param bool algo: Whether to search the current context or all contexts.
            :return: A dictionary of open orders.
            :rtype: dict
        """
        if algo:
            return BlueshiftOrderDict(
                    self.current_context.open_orders)
        else:
            return BlueshiftOrderDict(
                    self.context.blotter.get_open_orders())

    @api_method
    def get_open_positions(self, algo=True):
        """
            Get all open positions. The return value is a dict, keyed 
            by :ref:`assets<Asset>` and :ref:`positions<Position>` as 
            values. If `algo` is True, only the open positions in the 
            current context is returned. Else all open positions in 
            all contexts (the main and any sub strategy contexts, if 
            added), are returned.

            :param bool algo: Whether to search the current context or all contexts.
            :return: A dictionary of open orders.
            :rtype: dict
        """
        if algo:
            positions = self.current_context.blotter_portfolio
        else:
            positions = self.context.blotter.get_open_positions()

        open_positions = {}
        for asset, pos in positions.items():
            if pos.quantity != 0:
                open_positions[asset] = pos

        return BlueshiftPositionDict(open_positions)

    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def set_benchmark(self, asset):
        """
            Overwrite the default benchmark asset for algo performance 
            analysis.

            :param: :ref:`asset <Asset>` object.

            .. warning::
                This method can only be called once in the 
                :ref:`initialize<Initialize>` function. Calling it 
                anywhere else will crash the algo.
        """
        if self.mode == MODE.EXECUTION:
            msg = f'Benchmark is not supported for smart orders.'
            raise ValidationError(msg)
        
        self.current_context.blotter.set_benchmark(asset)
        
    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def set_initial_positions(self, positions):
        """
            Set starting positions for the algo. The argument `positions` 
            must be a list of positions objects or a list of dictionaries 
            that can be converted to a position objects. If the list contains
            dictionaries, each must have at least `asset`, `entry_price` and 
            `quantity` keys. The `asset` must be a valid symbol (and will 
            be converted to an Asset object using the `symbol` function 
            automatically) or an Asset object. The `entry_price` key must 
            have the price at entry for the position, and `quantity` is a 
            signed number of the position (negative for short position).

            :param list positions: List of initial positions keyed by assets.

            .. important::
                The asset must match exactly with what is intended. For 
                example, a rolling options asset may resolve to a different 
                instrument. Also cash and margin traded equities are 
                treated as different assets, even for the same symbol. For 
                the later, you can either create the asset directly, or 
                supply the `product_type` keyword (e.g. product_type=margin)
                to specify a margin equity product.
            
            .. warning::
                This method can only be called once in the 
                :ref:`initialize<Initialize>` function. Calling it 
                anywhere else will crash the algo. Also, for restarts, this 
                call will be ignored, and the initial positions will be 
                the last saved state of the algo.
        """            
        if not isinstance(positions, list):
            msg = f'Expected a positions list, got {type(positions)}.'
            raise ValidationError(msg)
            
        if self.mode == MODE.EXECUTION:
            msg = f'Initial positions are not allowed in this mode.'
            raise ValidationError(msg)
            
        if self._env.restart:
            msg = f'Skipping setting initial portfolio in restart.'
            self.log_info(msg)
            return
            
        ts = self.get_datetime()
        portfolio = {}
        asset_finder = self.context.asset_finder
        tz =  self.context.trading_calendar.tz
        for pos in positions:
            if isinstance(pos, Position):
                if pos.product_type not in self.current_context.broker.supported_products:
                    type_ = ProductType(pos.product_type).name
                    msg = f"Product type {type_} is not supported"
                    msg += " by the broker."
                    raise ValidationError(msg)
                    
                if pos.asset in portfolio:
                    portfolio[pos.asset].add_to_position(pos)
                else:
                    portfolio[pos.asset] = pos
            elif isinstance(pos, dict):
                pos['asset_finder'] = asset_finder
                pos['tz'] = tz
                
                if 'product_type' not in pos:
                    pos['product_type'] = self.current_context.broker.supported_products[0]
                    
                pos = create_position(**pos)
                if pos.product_type not in self.current_context.broker.supported_products:
                    type_ = ProductType(pos.product_type).name
                    msg = f"Product type {type_} is not supported"
                    msg += " by the broker."
                    raise ValidationError(msg)
                
                if pos.asset in portfolio:
                    portfolio[pos.asset].add_to_position(pos)
                else:
                    portfolio[pos.asset] = pos
            else:
                msg = f'Expected a dict or Position, got {type(pos)}.'
                raise ValidationError(msg)
                
        if portfolio:
            self.current_context.blotter.reset(ts, initial_positions=portfolio)

    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def set_slippage(self, model=None, *args, **kwargs):
        """
            Set the slippage model. For more details on available 
            slippage models, see :ref:`Slippage Models`. Only valid 
            for backtests.
            
            .. warning::
                This method can only be called once in the 
                :ref:`initialize<Initialize>` function. Calling it 
                anywhere else will crash the algo. This method is 
                ignored in live trading.
                
            :param model: A valid slippage model object.
            :type model: :ref:`slippage model<Slippage Models>`.
            
        """
        # for backward compatibility
        if 'fx' in kwargs:
            model = kwargs['fx']
            
        if model:
            self.current_context.broker.setup_model('slippage', model)

    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def set_commission(self, model=None, charge_model=None, *args, **kwargs):
        """
            Set the commission (cost) model. For more details see the 
            :ref:`Commissions and Cost Models` descriptions. A commission 
            model captures trading costs that are charged by the broker 
            or the trading venue. An optional charge_model can also be 
            specified to capture trading charges captured by authorities
            (e.g. taxes and levies). Only valid for backtests.

            .. warning::
                This method can only be called once in the 
                :ref:`initialize<Initialize>` function. Calling it 
                anywhere else will crash the algo. This method is ignored 
                in live trading.
                
            :param model: A valid commission model object.
            :type model: :ref:`commission model<Commissions and Cost Models>`.
        """
        # for backward compatibility
        if 'fx' in kwargs:
            model = kwargs['fx']
            
        if model:
            self.current_context.broker.setup_model(
                    'cost',model,charge_model=charge_model)

    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def set_margin(self, model):
        """
            Set the margin model. For more details on available margin
            models, see :ref:`Margin Models`. Only valid for backtests.

            .. warning::
                This method can only be called once in the 
                :ref:`initialize<Initialize>` function. Calling it 
                anywhere else will crash the algo. This method is 
                ignored in live trading.
                
            :param model: A valid margin model object.
            :type model: :ref:`margin model<Margin Models>`.
        """
        self.current_context.broker.setup_model('margin', model)
            
    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def set_account_currency(self, currency):
        """
            Set the currency of the account. Only valid for backtests.

            :param currency: A supported currency code or enumeration.
            :type currency: str or :class:`.CCY`
            
            .. note::
                for non-fx brokers, this will be ignored. All non-fx 
                brokers assume the algo performance is maintained in the
                ``LOCAL`` currency.
                
            .. warning::
                This method can only be called once in the 
                :ref:`initialize<Initialize>` function. Calling it 
                anywhere else will crash the algo. This method is 
                ignored in live trading.
                
        """
        self.current_context.broker.set_currency(currency)
            
    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def set_algo_parameters(self, param_name:str='params'):
        """
            Declare a context attribute as algo parameters. If any 
            parameters are passed (from commandline for example), those 
            will be captured and stored as a dictionary under this name.

            :param param_name: An attribute of the context variable.
            :type param_name: str
            
            .. note::
                If the attribute does not exist, a new attribute 
                is created and set to the values passed as the 
                `--parameters` flag as a dictionary of key-value pairs. 
                If it exists, it must be a dictionary, and parameter 
                key-value pairs from the `--parameters` flag are added 
                to the attribute (overwritten if exists).
                
            .. warning::
                This method can only be called once in the 
                :ref:`initialize<Initialize>` function. Calling it 
                anywhere else will crash the algo.
                
        """
        params = self._env.parameters
        current_params:dict[str, Any] = getattr(self.current_context, param_name, {})
        
        if not isinstance(current_params, dict):
            msg = 'parameter must be of dictionary type,'
            msg = ' got {type(current_params)}.'
            raise InitializationError(msg)
        
        if not current_params:
            setattr(self.current_context,param_name,params)
        else:
            for key in params:
                current_params[key] = params[key]
            setattr(self.current_context,param_name,current_params)
            
    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def set_roll_policy(self, roll_day=None, roll_time=None):
        """
            Set the roll policy on this broker. The parameter ``roll_day`` 
            is the offset from the nearest expiry when futures and 
            options are rolled to compute pricing data for continuous 
            instruments. The parameter ``roll_time`` specifies the offset 
            in terms of time in a tuple of (hours, minutes). In addition, 
            trying to trade an instrument beyond the roll date and time 
            may result in error.
            
            :param int roll_day: Set the roll day offset from the expiry date.
            :param tuple roll_time: Set (hour, minute) tuple for roll time.
            
            .. warning::
                This method can only be called once in the 
                :ref:`initialize<Initialize>` function. Calling it 
                anywhere else will crash the algo.
            
        """
        if not roll_day or not roll_time:
            msg = f'roll_day and roll_time must be specified.'
            raise ValidationError(msg)
        if not isinstance(roll_time, tuple):
            msg = f'Invalid roll_time argument, expected (hour,minute) tuple.'
            raise ValidationError(msg)
                
        self.current_context.broker.set_roll_policy(roll_day, roll_time)
    
    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def set_exit_policy(self, cancel_orders=True, square_off=False):
        """
            Set the exit policy for this algo. If `cancel_orders` is 
            True and `square_off` is False, only the open order cancellation
            is attempted. If `square_off` is True, all open orders are 
            cancelled and positions are squared off. Exit policies are only
            applicable for the global context, and cannot be set inside 
            a sub-context.
            
            :param bool cancel_orders: Cancel all open order on exit.
            :param bool square_off: Square off all positions on exit.
            
            .. important::
                The exit policy is triggered only during a graceful exit 
                of the algo. Unexpected fatal errors or a crash or an 
                explicit kill may not trigger the exit policy.
                
            .. warning::
                This method can only be called once in the 
                :ref:`initialize<Initialize>` function. Calling it 
                anywhere else will crash the algo.
                
            .. danger:: 
                This function only initiates square off and/or order 
                cancellation. It does not and cannot ensure actual 
                square-off or cancellation. Also, in case we attempt 
                to squareoff during a cool-off period, it will be ignored 
                (to prevent multiple squareoff attempts which may lead to 
                unintended positions), but will attempt to cancel any 
                open orders. If the order cancellation or square-off takes 
                too much time to get executed, the algo may exit before 
                they are confirmed and the same may not be reflected in 
                the final blotter.
                
            .. seealso:: 
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.square_off`
        """
        if not self.is_global_context:
            msg = f'Exit policy can be set in the global context only.'
            warnings.warn(msg, BlueshiftWarning)
            return
        
        self._cancel_orders_on_exit = cancel_orders
        self._squareoff_on_exit = square_off
        
    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def set_oneclick_timeout(self, timeout:float):
        """
            Overwrite the default notification timeout when running in 
            one-click execution mode.

            :param int timeout: The timeout (in minutes) for notification expiry.

            .. warning::
                This method can only be called once in the 
                :ref:`initialize<Initialize>` function. Calling it 
                anywhere else will crash the algo.
        """
        if self.execution_mode != ExecutionMode.ONECLICK or not self.context.notifier:
            return
        
        try:
            self.context.notifier.timeout = float(timeout)
        except Exception:
            raise ValidationError(f'Failed to set oneclick timeout.')

    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def set_riskfree_rate(self, rate):
        """
            Set the risk free rate of returns for algo performance analysis.
        """
        raise ValidationError(f'risk free rate not implemented.')
    
    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def set_cooloff_period(self, period=DEFAULT_COOLOFF):
        """
            Set the cool-off period following a call to the square_off 
            function, or following an exit resulting from a take profit 
            or stop loss, or an explicit call to trigger a cool-off 
            period using `set_cooloff` method.
            
            :param int period: The period (in minutes) to cool off.
            
            .. warning::
                This method can only be called once in the 
                :ref:`initialize<Initialize>` function. Calling it 
                anywhere else will crash the algo.
                
            .. note::
                In general, cooloff period should be a positive integer, 
                however, you may pass on a fraction in live trading. For 
                backtests, any fractional number will be rounded up to 
                integer automatically.
            
            .. seealso:: 
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.get_cooloff_period`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.set_cooloff`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.reset_cooloff`
        """
        try:
            if self.mode == MODE.BACKTEST:
                if period != round(period):
                    msg = f'Cooloff period not an integer in backtest, '
                    msg += f'will be rounded to an integer.'
                    warnings.warn(msg, BlueshiftWarning)
                period = round(period)
            else:
                period = float(period)
        except Exception:
            raise ValidationError(f'Cool-off period must be a number.')
            
        self._exits_handler.set_cooloff_period(period)
        
    @api_method
    def get_cooloff_period(self):
        """
            Get the current cooloff period for the algo.
            
            .. seealso:: 
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.set_cooloff_period`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.set_cooloff`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.reset_cooloff`
        """
        return self._exits_handler.get_cooloff_period()
        
    @api_method
    def set_cooloff(self, assets=None):
        """
            Trigger a cool-off period. This will result in no further 
            entry trades for the specified assets, or any assets if 
            not specified. This cool-off period will last till the 
            defined value (set by `set_cooloff_period`, defaults to 30
            minutes).
            
            :param list assets: List of assets (or None) to enable the cooloff for.
            
            .. important::
                If assets are specified, they should not be in rolling
                format for options, else they might be ignored.
            
            .. seealso:: 
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.set_cooloff_period`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.get_cooloff_period`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.reset_cooloff`
        """
        self._trigger_cooloff(assets)
        
    @api_method
    def reset_cooloff(self):
        """
            Reset all cooloffs tracking. This means any entry or square-off
            cooloff validation immediately after this reset will succeed.
            
            .. seealso:: 
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.set_cooloff_period`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.get_cooloff_period`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.set_cooloff`
        """
        self._exits_handler.reset_cooloff(self.current_context, None)
        
    @api_method
    def check_exits(self):
        """
            Utility function that triggers an explicit check for stoploss
            or take-profit exits.
            
            :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.set_stoploss`
            :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.set_takeprofit`
        """
        self._exits_handler.check_exits(reset=False)
    
    @api_method
    def set_stoploss(self, asset, method, target, trailing=False, 
                     on_stoploss=None, skip_exit=False, entry=None,
                     rolling=False):
        """
            Set the stoploss for a position, defined for an asset or at 
            the strategy level. Method can be one of `PRICE`, `MOVE`, 
            `PERCENT`, `AMOUNT` or `PNL`. 
            
            If `asset` is passed as `None`, the stoploss applies to the 
            current strategy context.  If an asset is supplied, only the 
            asset position is monitored. If `trailing` is non-zero, 
            stoploss is updated at each lookup. Stoplosses are defined 
            on the position for an asset, and NOT for an order. You can 
            have multiple orders placed on the same asset, and set the 
            stoploss just once.
            
            If a strategy has both strategy-level and asset level 
            stoploss, the strategy level stoploss is checked first. If 
            hit, all asset level stoplosses and takeprofits will be reset 
            (unless skip_exit is True). This is done to avoid calling 
            square-off multiple times.
            
            if `entry` is specified, it must be a number, and that is 
            taken as a basis computation where applicable (e.g. MOVE,
            PERCENT or trailing stoploss etc.)
            
            Stoploss will be reset each day in before trading start 
            routine (for both strategy-level and asset-level stoplosses).
            
            You can pass an order object instead of asset. This is useful 
            when using rolling option asset - which may result in a different 
            asset in positions than the asset used to place the order. It 
            is recommended to use an order object when using rolling assets.
            
            Once the target is hit, if `skip_exit` is False, the position
            will be squared off and the callable `on_stoploss`, if provided, 
            will be called. If `skip_exit` is True, only the callable 
            will be invoked and no position exit will take place. The 
            callable must have a signature of `f(context, asset)`, where 
            `asset` is the asset for which the trigger has been hit. The 
            asset argument will be `None` for a strategy level stoploss. 
            The callable is invoked AFTER the required square-off is 
            triggered (if skip_exit is True).
            
            :param asset: The asset for the stoploss check (or None).
            :type asset: :ref:`asset<Asset>` object.
            :param str method: The method for the stoploss check.
            :param float target: The stoploss target.
            :param float trailing: Trailing stoploss.
            :param callable on_stoploss: A callable invoked if target is hit.
            :param bool skip_exit: If true, no exit order will be triggered.
            :param float entry: Optional entry level.
            :param bool rolling: Should the stoploss be rolled to next day.
            
            .. warning::
                stoplosses are valid only for the day. Each day in before 
                trading start, stoplosses are reset unless `rolling` is 
                set to True. Note: setting `rolling` to True will only work 
                in backtest mode as no stoploss details will be saved. In 
                live mode, no stoploss information is restored after a 
                restart. You will need to explicitly restore any takeprofits 
                after a restart (e.g. in the before_trading_start). Also, 
                once a stoploss target is hit, (with or without 
                `skip_exit`),the entry is automatically removed and no 
                subsequent trigger will occur. If the asset has both a 
                stoploss and a takeprofit target, a trigger in any will 
                cancel (i.e. remove) both of them.
                
            .. important::
                If you are using a rolling option assets in your strategy, 
                use the order object to set the stoploss, instead of 
                the asset itself.

            .. important:: 
                If you are placing the entry order with a stoploss 
                specified (if supported), do not use this function. The 
                stoploss in the order, if supported by the broker, will 
                automatically enable stoploss exit. This function 
                will try to square off the position if the stoploss 
                is hit by placing a market order. Also, after an exit,
                this will cause a cool-off period for that asset which
                will prevent further entry trade till it resets. Cool 
                off period can be set using the `set_cooloff_period` 
                api function. Also a stoploss, once set, will be 
                valid till it is cancelled using `remove_stoploss` 
                api method, or removed by a trigger (either by a 
                stoploss or takeprofit being hit on the asset). Also 
                note, this function can only initiate the exit orders 
                and cannot ensure actual executions of those orders.
                
            .. note::
                If `skip_exit` is False, the function will initiate exit.
                These exit orders (if any) will bypass any trading controls 
                to make sure square-off orders are not blocked by such 
                controls. Orders will be sent to the broker directly, even 
                in one-click execution mode.
                
            .. seealso:: 
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.set_takeprofit`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.set_cooloff_period`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.square_off`
                :py:class:`blueshift.protocol.ExitMethod`
                
        """
        if asset is not None:
            if isinstance(asset, Order):
                asset = self.get_asset_from_order(asset)
            else:
                asset = self._maybe_get_converted_asset(asset)
                
            if not isinstance(asset, Asset):
                msg = f'Illegal argument {asset}, expected asset.' 
                raise ValidationError(msg)
            
        try:
            if isinstance(method, str):
                method = method.upper()
            method = ExitMethod(method)
        except Exception:
            try:
                method = ExitMethod[method]
            except Exception:
                methods = list(ExitMethod.__members__.keys())
                msg = f'Illegal stoploss method, must be one of {methods}.'
                raise ValidationError(msg)
        else:
            if method == ExitMethod.PNL and asset is not None:
                method = ExitMethod.AMOUNT
            elif method == ExitMethod.PRICE and asset is None:
                msg = f'Illegal stoploss method PRICE for strategy'
                msg += f' level stoploss'
                raise ValidationError(msg)
            elif method not in (ExitMethod.PNL, ExitMethod.AMOUNT) and asset is None:
                msg = f'Setting strategy level stoploss on portfolio value '
                msg += f'will ignore any capital addition or withdrawal, and '
                msg += f'may not work as expected.'
                warnings.warn(msg, BlueshiftWarning)
        
        if on_stoploss is None:
            if self.is_global_context:
                on_stoploss = self._on_stoploss
            else:
                strategy = self._strategies.get(self.current_context.name)
                if strategy:
                    on_stoploss = strategy._on_stoploss
                else:
                    on_stoploss = noop
        
        if on_stoploss not in self._callable_cache:
            self._callable_cache[on_stoploss] = wrap_with_kwarg(
                    on_stoploss, 'success')
        handler = self._callable_cache[on_stoploss] # decorated version
            
        obj = self._exits_handler.get_stoploss(
                self.current_context, asset)
        if obj and self.mode != MODE.BACKTEST:
            if asset:
                msg = f'An existing stoploss for asset {asset} is in place. '
            else:
                msg = f'An existing stoploss for strategy is in place. '
            msg += f'This will be overwritten.'
            self.log_warning(msg)
            
        try:
            trailing = float(trailing)
        except Exception:
            msg = f'Illegal input for trailing parameter, must be a '
            msg += f'boolean or a number, got {trailing}.'
            raise ValidationError(msg)
            
        if entry is None:
            entry = np.nan
        self._exits_handler.add_stoploss(
                self.current_context, asset, method, target, handler,
                trailing, do_exit=not skip_exit, entry_level=entry,
                rolling=rolling)
        
    @api_method
    def get_current_stoploss(self, asset):
        """
            Return the currently defined stoploss for the strategy, or asset, 
            if any. Returns a tuple of None's if not found. Set asset to 
            `None` for the stoploss set at the strategy level in the current 
            context.
            
            You can pass an order object instead of asset. This is useful 
            when using rolling option asset - which may result in a different 
            asset in positions than the asset used to place the order. It 
            is recommended to use an order object when using rolling assets.
            
            :param asset: The asset for the stoploss check.
            :type asset: :ref:`asset<Asset>` object.
            
            :return: Tuple of (ExitMethod, target, price at entry)
            :rtype: tuple
            
            .. seealso:: 
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.set_stoploss`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.remove_stoploss`
        """
        if asset is not None:
            if isinstance(asset, Order):
                asset = self.get_asset_from_order(asset)
            else:
                asset = self._maybe_get_converted_asset(asset)
                
            if not isinstance(asset, Asset):
                msg = f'Illegal argument {asset}, expected asset.' 
                raise ValidationError(msg)
            
        obj = self._exits_handler.get_stoploss(
                self.current_context, asset)
        
        if obj:
            return obj.method, obj.target, obj.entry
        
        return None, None, None
        
    @api_method
    def remove_stoploss(self, asset):
        """
            Remove stoploss for the strategy context or for an asset. Set 
            asset to `None` for strategy-level stoploss. You can pass an 
            order object instead of asset as well. This is useful when 
            using rolling option asset - which may result in a different 
            asset in positions than the asset used to place the order. 
            It is recommended to use an order object when using rolling 
            assets.
            
            :param asset: The asset for the stoploss check.
            :type asset: :ref:`asset<Asset>` object.
            
            .. seealso:: 
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.set_stoploss`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.get_current_stoploss`
        """
        if asset is not None:
            if isinstance(asset, Order):
                asset = self.get_asset_from_order(asset)
            else:
                asset = self._maybe_get_converted_asset(asset)
                
            if not isinstance(asset, Asset):
                msg = f'Illegal argument {asset}, expected asset.' 
                raise ValidationError(msg)
            
        self._exits_handler.reset_stoploss(
                self.current_context, asset)
        
    @api_method
    def set_takeprofit(self, asset, method, target, on_takeprofit=None,
                       skip_exit=False, entry=None, rolling=False):
        """
            Set the take-profit for a position, defined for an asset or 
            at the strategy level. Method can be one of `PRICE`, `MOVE`, 
            `PERCENT`, `AMOUNT` or `PNL`. 
            
            If `asset` is passed as `None`, the takeprofit applies to the 
            current strategy context. If an asset is supplied, only the 
            asset position is monitored. Take-profit will be reset each 
            day in before trading start routine.
            
            You can pass an order object instead of asset. This is useful 
            when using rolling option asset - which may result in a different 
            asset in positions than the asset used to place the order. It 
            is recommended to use an order object when using rolling assets.
            
            If a strategy has both strategy-level and asset level 
            takeprofit, the strategy level takeprofit is checked first. 
            If hit, all asset level stoplosses and takeprofits will be 
            reset (unless skip_exit is True). This is done to avoid 
            calling square-off multiple times.
            
            if `entry` is specified, it must be a number, and that is 
            taken as a basis computation where applicable (e.g. MOVE,
            PERCENT etc.)
            
            Once the target is hit, if `skip_exit` is False, the position
            will be squared off and the callable `on_takeprofit`, if provided, 
            will be called. If `skip_exit` is True, only the callable 
            will be invoked and no position exit will take place. The 
            callable must have a signature of `f(context, asset)`, where 
            `asset` is the asset for which the trigger has been hit. The 
            asset argument will be None for a strategy level takeprofit. 
            The callable is invoked AFTER the required square-off is 
            triggered (if skip_exit is True).
            
            :param asset: The asset for the stoploss check.
            :type asset: :ref:`asset<Asset>` object.
            :param str method: The method for the stoploss check.
            :param float target: The stoploss target.
            :param callable on_takeprofit: A callable invoked if target is hit.
            :param bool skip_exit: If true, no exit order will be triggered.
            :param float entry: Optional entry level.
            :param bool rolling: Should the takeprofit be rolled to next day.
            
            .. warning::
                take-profits are valid only for the day. Each day in before 
                trading start, take-profits are reset unless `rolling` is 
                set to True. Note: setting `rolling` to True will only work 
                in backtest mode as no takeprofit details will be saved. In 
                live mode, no takeprofit information is restored after a 
                restart. You will need to explicitly restore any takeprofits 
                after a restart (e.g. in the before_trading_start). Also, 
                once a take-profit target is hit, (with or without 
                `skip_exit`), the entry is automatically removed and no 
                subsequent trigger will occur. If the asset has both a 
                stoploss and takeprofit target, a trigger in any will 
                cancel (i.e. remove) both of them.
                
            .. important::
                If you are using a rolling option assets in your strategy, 
                use the order object to set the stoploss, instead of 
                the asset itself.

            .. important:: 
                If you are placing the entry order with a takeprofit 
                specified, do not use this function. The takeprofit 
                in the order, if supported by the broker, will 
                automatically enable takeprofit exit. Also a 
                takeprofit, once set, will be valid till it is 
                cancelled using `remove_takeprofit` api method, or removed 
                by a trigger (either by a stoploss or takeprofit being 
                hit on the asset). Also note, this function can only 
                initiate the exit orders and cannot ensure actual 
                executions of those orders.
                
            .. note::
                If `skip_exit` is False, the function will initiate exit.
                These exit orders (if any) will bypass any trading controls 
                to make sure square-off orders are not blocked by such 
                controls. Orders will be sent to the broker directly, even 
                in one-click execution mode.
                
            .. seealso:: 
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.set_stoploss`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.set_cooloff_period`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.square_off`
                :py:class:`blueshift.protocol.ExitMethod`
                
        """    
        if asset is not None:      
            if isinstance(asset, Order):
                asset = self.get_asset_from_order(asset)
            else:
                asset = self._maybe_get_converted_asset(asset)
                
            if not isinstance(asset, Asset):
                msg = f'Illegal argument {asset}, expected asset.' 
                raise ValidationError(msg)
            
        try:
            if isinstance(method, str):
                method = method.upper()
            method = ExitMethod(method)
        except Exception:
            try:
                method = ExitMethod[method]
            except Exception:
                methods = list(ExitMethod.__members__.keys())
                msg = f'Illegal take-profit method, must be one of {methods}.'
                raise ValidationError(msg)
        else:
            if method == ExitMethod.PNL and asset is not None:
                method = ExitMethod.AMOUNT
            elif method == ExitMethod.PRICE and asset is None:
                msg = f'Illegal takeprofit method PRICE for strategy'
                msg += f' level stoploss'
                raise ValidationError(msg)
            elif method not in (ExitMethod.PNL, ExitMethod.AMOUNT) and asset is None:
                msg = f'Setting strategy level take-profit on portfolio value '
                msg += f'will ignore any capital addition or withdrawal, and '
                msg += f'may not work as expected.'
                warnings.warn(msg, BlueshiftWarning)
        
        if on_takeprofit is None:
            if self.is_global_context:
                on_takeprofit = self._on_takeprofit
            else:
                strategy = self._strategies.get(self.current_context.name)
                if strategy:
                    on_takeprofit = strategy._on_takeprofit
                else:
                    on_takeprofit = noop
                    
        if on_takeprofit not in self._callable_cache:
            self._callable_cache[on_takeprofit] = wrap_with_kwarg(
                    on_takeprofit, 'success')
        handler = self._callable_cache[on_takeprofit] # decorated version
            
        obj = self._exits_handler.get_takeprofit(
                self.current_context, asset)
        if obj and self.mode != MODE.BACKTEST:
            if asset:
                msg = f'An existing take-profit for asset {asset} is in place. '
            else:
                msg = f'An existing take-profit for strategy is in place. '
            msg += f'This will be overwritten.'
            self.log_warning(msg)
            
        if entry is None:
            entry = np.nan
        self._exits_handler.add_takeprofit(
                self.current_context, asset, method, target, handler,
                do_exit=not skip_exit, entry_level=entry, rolling=rolling)
        
    @api_method
    def get_current_takeprofit(self, asset):
        """
            Return the currently defined take-profit for the strategy, or 
            asset, if any. Returns a tuple of None's if not found. Set 
            asset to `None` for the stoploss set at the strategy level in 
            the current context.
            
            You can pass an order object instead of asset. This is useful 
            when using rolling option asset - which may result in a different 
            asset in positions than the asset used to place the order. It 
            is recommended to use an order object when using rolling assets.
            
            :param asset: The asset for the takeprofit check.
            :type asset: :ref:`asset<Asset>` object.
            
            :return: Tuple of (ExitMethod, target, price at entry)
            :rtype: tuple
            
            .. seealso:: 
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.set_takeprofit`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.remove_takeprofit`
        """
        if asset is not None:
            if isinstance(asset, Order):
                asset = self.get_asset_from_order(asset)
            else:
                asset = self._maybe_get_converted_asset(asset)
                
            if not isinstance(asset, Asset):
                msg = f'Illegal argument {asset}, expected asset.' 
                raise ValidationError(msg)
            
        obj = self._exits_handler.get_takeprofit(
                self.current_context, asset)
        
        if obj:
            return obj.method, obj.target, obj.entry
        
        return None, None, None
        
    @api_method
    def remove_takeprofit(self, asset):
        """
            Remove take-profit for the strategy context or for an asset. 
            Set asset to `None` for strategy-level stoploss. You can pass 
            an order object instead of asset as well. This is useful when 
            using rolling option asset - which may result in a different 
            asset in positions than the asset used to place the order. 
            It is recommended to use an order object when using rolling 
            assets.
            
            :param asset: The asset for the stoploss check.
            :type asset: :ref:`asset<Asset>` object.
            
            .. seealso:: 
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.set_takeprofit`
                :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.get_current_takeprofit`
        """
        if asset is not None:
            if isinstance(asset, Order):
                asset = self.get_asset_from_order(asset)
            else:
                asset = self._maybe_get_converted_asset(asset)
                
            if not isinstance(asset, Asset):
                msg = f'Illegal argument {asset}, expected asset.' 
                raise ValidationError(msg)
            
        self._exits_handler.reset_takeprofit(
                self.current_context, asset)
        
    @api_method
    def fund_transfer(self, amount, *args, **kwargs):
        """
            Add (or remove) capital to the algo account. Note: this only 
            impact performance and account metrics (eg. net value or 
            algo percentage returns). In live trading, it does not actually 
            add any funds to the account and orders will fail with insufficient 
            funds errors, even if you add funds via this API method.
            
            .. important::
                Performance metrics are computed based on end of day 
                funds in the account - i.e. the starting capital plus any 
                funds added during the day is the denominator for returns 
                calculation.
            
            :param float amount: Amount to add (or withdraw if negative).
        """
        return self.current_context.blotter.fund_transfer(
                amount, *args, **kwargs)
        
    @api_method
    def is_trading_hours(self):
        """
            Function to check if the market is currently open for
            trading and the algo can place orders.
            
            :return: ``True`` if open for trading, else ``False``.
            :rtype: bool
        """
        return self.is_TRADING_BAR()
    
    @api_method
    @ensure_state(states=["INITIALIZED"],
                  err_msg="can only be called in initialize")
    def attach_pipeline(self, pipeline, name, chunks=None, eager=True):
        """
            Register a pipeline for this algorithm. Pipeline with the 
            same name, if exists, will be overwritten. The name can 
            later be used in the `pipeline_output` function to run 
            the pipeline computation.
            
            :param pipeline: The pipeline object to register.
            :type pipeline: zipline_pipeline.pipeline.Pipeline
            :param str name: Name of the pipeline to register with.
            :param iterable chunks: Iterator to specify the computation chunks.
            :param bool eager: Compute ahead the pipeline.

            :return: Return the pipeline object.
            :rtype: zipline_pipeline.pipeline.Pipeline
                
            .. seealso:: :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.pipeline_output`
        """
        try:
            from blueshift.pipeline import Pipeline # type: ignore
        except ImportError:
            msg = 'No support for pipeline APIs.'
            raise BlueshiftDataException(msg)

            
        if self._pipeline_engine is None:
            self._init_pipeline()
            
        if not self.is_global_context:
            raise ValidationError(
                    f'Cannot attach pipeline from a sub strategy.')
            
        if not isinstance(pipeline, Pipeline):
            raise ValidationError(
                    f'Cannot attach pipeline, invalid first argument, expected Pipeline.')
            
        if self._pipeline_engine is None:
            msg = 'No support for pipeline or engine not initialized.'
            raise BlueshiftDataException(msg)

        if chunks is None:
            chunks = chain([5], repeat(126))
        elif isinstance(chunks, int):
            chunks = repeat(chunks)

        self._pipelines[name] = AttachedPipeline(
                pipeline, iter(chunks), eager)

        return pipeline

    @api_method
    def pipeline_output(self, name):
        """
            Return the result of the pipeline `name` for the current 
            time. The result is a pandas DataFrame, indexed by asset 
            objects (that are trading at the current time and has 
            passed the filtering criteria in the pipeline). Each column
            in the dataframe corresponds to the factors added to the 
            pipeline definition.
            
            .. note::
                Pipeline output for a given date consider inputs till 
                the previous trading day. Also, pipeline output works 
                with daily data. Therefore running the same pipeline 
                multiple times on the same day is superfluous as it 
                will return the same data on each call.
            
            :return: Result from the pipeline.
            :rtype: pandas.DataFrame
            
            .. seealso:: :py:attr:`blueshift.core.algorithm.algorithm.TradingAlgorithm.attach_pipeline`
        """
        if self._pipeline_engine is None:
            return pd.DataFrame()
        
        today = self.get_datetime()
        if today is None:
            raise BlueshiftException(f'cannot compute pipeline output, algo is not set up yet.')
        else:
            today = today.normalize()

        if self.mode in REALTIME_MODES:
            today = self.context.trading_calendar.previous_close(today)
            today = today.normalize()
        
        try:
            pipeline, chunks, eager = self._pipelines[name]
        except KeyError:
            raise NoSuchPipeline(f'no pipeline named {name} registered.')
        else:
            try:
                data = self._pipeline_cache.get(name, today)
            except KeyError:
                try:
                    data, validity = self.run_pipeline(pipeline, today, next(chunks))
                except Exception as e:
                    msg = 'Session out of range, are you using date range within supported '
                    msg += f'range?:{str(e)}'
                    raise SessionOutofRange(msg)
                else:
                    self._pipeline_cache.set(name, data, validity)
            
            try:
                idx = data.index.get_level_values(0)
                if idx.tz: # type: ignore
                    result = data.loc[today]
                else:
                    result = data.loc[today.tz_localize(None)]
                # replace the pipeline assets index with broker assets
                assets = result.index.tolist()
                assets = [self.current_context.broker.symbol(
                        asset.exchange_ticker, logger=self.logger) for \
                              asset in assets]
                result.index = assets
                return result
            except Exception as e:
                if isinstance(e, TerminationError):
                    raise
                if self.mode in REALTIME_MODES:
                    msg = f'Error in pipeline evaluation:{str(e)}'
                    self.log_error(msg)
                return pd.DataFrame(columns=data.columns)

    def run_pipeline(self, pipeline, start_date, chunksize):
        if self._pipeline_engine is None:
            return pd.DataFrame(), start_date
        
        start_date = self.context.trading_calendar.bump_previous(start_date)
        end_date = self.context.trading_calendar.bump_forward(self._env.end_dt.normalize())
            
        if self.mode in REALTIME_MODES:
            # can't compute pipeline into the future! So we miss the 
            # cache everytime and run pipeline computation in live mode.
            end_date = pd.Timestamp.now(tz=self.context.trading_calendar.tz).normalize()
            end_date = self.context.trading_calendar.bump_forward(end_date)

        sessions = self.context.trading_calendar.all_sessions
        
        try:
            start_loc:int = cast(int, sessions.get_loc(start_date))
            end_loc:int = cast(int, sessions.get_loc(end_date))
            end_loc = min(start_loc + chunksize, end_loc)
        except KeyError:
            msg = f'pipeline evaluation start {start_date} or end {end_date} is invalid.'
            raise SessionOutofRange(msg)
        
        if start_loc > end_loc:
            raise ValueError(f'pipeline evaluation start {start_date} is later than end {end_date}.')

        start_session = sessions[start_loc]
        end_session = sessions[end_loc]
        
        data = self._pipeline_engine.run_pipeline(
                pipeline, start_session, end_session)
        
        return data, end_session
    
    @api_method
    def send_update(self, data, msg=None):
        """
            API method to share algo update. This is only available in 
            certain deployments and is a restricted method. Not available
            in backtest mode.
        """
        if self.mode ==MODE.BACKTEST:
            return
                         
        if not ALLOW_EXTERNAL_EVENT:
            raise ValidationError(f'external update event not allowed.')
        
        try:
            # data must be json serializable, msg must be none or string
            import json
            json.dumps(data)
            
            if msg:
                #assert isinstance(msg, str)
                if not isinstance(msg, str):
                    raise ValueError(f'illegal type {type(msg)} for message')
        except Exception as e:
            raise ValidationError(f'Illegal data or msg for user update: {str(e)}.')
        
        self._env.user_callback(data, msg, status=self._status.value)
        
