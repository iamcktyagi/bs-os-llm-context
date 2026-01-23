from __future__ import annotations
from typing import TYPE_CHECKING, cast
import numpy as np
from os import path as os_path
import math
import sys
import threading
import time
import logging

from blueshift.calendar.trading_calendar import TradingCalendar
from blueshift.lib.common.ctx_mgrs import TimeoutRLock
from blueshift.lib.common.constants import CCY, Currency
from blueshift.interfaces.assets._assets import MarketData, Asset
from blueshift.lib.common.enums import BlotterType, AlgoMode, AlgoCallBack
from blueshift.lib.trades._order_types import OrderType, OrderValidity
from blueshift.lib.trades._accounts import BlotterAccount
from blueshift.lib.trades._position import Position
from blueshift.lib.common.functions import listlike
from blueshift.lib.common.platform import ensure_directory, get_exception
from blueshift.lib.exceptions import (
        BlueshiftException, ExceptionHandling, DataWriteException,
        InitializationError, TerminationError, AlgoStartError,
        ReconciliationError)
from blueshift.interfaces.assets.assets import IAssetFinder
from blueshift.interfaces.trading.broker import IBroker
from blueshift.interfaces.data.data_portal import DataPortal
from blueshift.config import blueshift_saved_performance_path
from blueshift.interfaces.trading.blotter import IBlotter, register_blotter
from blueshift.config import (
        OPEN_ORDER_TIMEOUT, MARKET_ORDER_TIMEOUT, LOCK_TIMEOUT, 
        BLOTTER_UPDATE_TIMEOUT, BLOTTER_MISSING_ORDER_THRESHOLD)

from ..trackers._tracker import (TransactionsTracker,
                                        PositionTracker,
                                        AccountTracker)
from ..trackers._xtracker import (XTransactionsTracker,
                                        XPositionTracker,
                                        XAccountTracker)
from ..trackers._perfs import PerformanceTracker
from ..analytics.pertrade import (
        create_txns_frame, create_positions_frame, 
        create_round_trips_frame)

if TYPE_CHECKING:
    import pandas as pd
    from blueshift.core.utils.environment import TradingEnvironment
else:
    import blueshift.lib.common.lazy_pandas as pd


MINIMUM_UPDATE_TIME=1.5 # update requests below this will be refused
SLEEP_TIME_FOR_ORDER_FETCH=1.5

class LiveBlotter(IBlotter):
    """
        A specific blotter suitable for EOD reconciliation and save.
        Primarily meant for LIVE trading mode. This is more suitable
        for cases where we need to create and maintain virtual accounts,
        like live trading. For back-testing use a blotter that directly
        access the underlying backtesting account.

        Note:
            initialize the start positions and historical records of
            transactions. If a start position is suppplied, that will
            overwrite the saved positions, if any.

        Args:
            ``name (str)``: Unique name of the run.
            
            ``asset_finder (object)``: Asset finder object for the run.
            
            ``data_portal (object)``: Data portal object for the run.

            ``broker (object)``: Broker object for the run.

            ``env (object)``: Trading environment.

            ``logger (object)``: Logger object of the run.

            ``init_capital (float)``: Starting capital.

            ``timestamp (Timestamp)``: Timestamp at creation.

            ``ccy (object)``: Blotter currency.
            
            ``output_dir (str)``: Path to output directory.
            
            ``blotter_type (BotterType)``: Blotter type.
    """
    _OPEN_ORDER_TIMEOUT = MARKET_ORDER_TIMEOUT # timeout for market orders
    _ORDER_FETCH_TIMEOUT = OPEN_ORDER_TIMEOUT # timeout for all orders fetch
    _RECURSION_TIMEOUT = 0.1 # Hard limit to avoid recursion
    _CHECK_LIMIT_ORDERS = True

    def __init__(self, name:str, asset_finder:IAssetFinder, data_portal:DataPortal, broker:IBroker, 
                 env:TradingEnvironment ,logger:logging.Logger|None=None, init_capital:float|None=None, 
                 timestamp:pd.Timestamp|None=None, ccy:Currency=CCY.LOCAL, output_dir=None, # type: ignore
                 blotter_type:BlotterType=BlotterType.EXCLUSIVE):
        super(LiveBlotter, self).__init__(name)
        self._initialized = False
        self._create(
                asset_finder, data_portal, broker, env, logger, 
                init_capital, timestamp, ccy, output_dir, 
                blotter_type)
        self._initialized = True

    def _create(self, asset_finder, data_portal, broker, env,
                logger=None, init_capital=None, timestamp=None,
                ccy=CCY.LOCAL, output_dir=None,  # type: ignore
                blotter_type=BlotterType.EXCLUSIVE):
        """
            Create the blotter instance.
        """      
        self._asset_finder = asset_finder
        self._data_portal = data_portal
        self._broker = broker
        self._mode = AlgoMode.LIVE
        self._ccy = ccy
        self.blotter_type = blotter_type
        self._publish = False
        self._publisher = None
        self._timestamp = None
        self._daily_positions = {}
        self._benchmark_asset = None
        
        if not output_dir:
            self._output_dir = blueshift_saved_performance_path(self.name)
        else:
            self._output_dir = output_dir
        ensure_directory(self._output_dir)
        
        self._max_open_order_since = 0
        self._last_reconciled = None
        self._last_reconciled_attempt = None
        self._last_valuation_attempt = None
        self._internal_recon_counter = 0
        self._last_updated = None
        self._last_saved = None
        self._last_valuation_update = None
        self._last_risk_update = None
        self._needs_reconciliation = False
        self._last_reconciliation_status = False
        self._read_success = True
        # keep track of open order cash outlay to compute changes in recon.
        self._oo_cashflow = 0
        
        self._env = env
        if not self._env:
            msg = f'Failed to create blotter, no trading environment '
            msg += f'supplied, nor found.'
            raise InitializationError(msg)
            
        if logger:
            self.logger = logger
        else:
            self.logger = self._env.logger
            
        if not init_capital:
            init_capital = self._env.init_capital
            
        self.trading_calendar = cast(TradingCalendar, self._env.trading_calendar)
        
        if self.blotter_type == BlotterType.VIRTUAL:
            self._txns_tracker = TransactionsTracker(self.name, self._mode, None, self._ccy)
            self._pos_tracker = PositionTracker(self.name, self._mode, None, self._ccy)
            self._acct_tracker = AccountTracker(self.name, self._mode, init_capital)
        else:
            self._txns_tracker = XTransactionsTracker(self.name, self._mode, None, self._ccy)
            self._pos_tracker = XPositionTracker(self.name, self._mode, None, self._ccy)
            self._acct_tracker = XAccountTracker(self.name, self._mode, init_capital)

        tz = self.trading_calendar.tz
        self._perfs_tracker = PerformanceTracker(self.name, self._mode, tz=tz)
        
        self._publish = self._env.publish
        # this is exclusive to algo, and not expected to timeout
        self._lock = TimeoutRLock(timeout=LOCK_TIMEOUT)

        if not self._env.alert_manager:
            msg = f'No alert manager defined for the trading environment.'
            raise InitializationError(msg)
            
        self.publisher = self._env.alert_manager.publisher

        self.reset()
        self.set_benchmark()
        self._record_vars = pd.DataFrame()
        
        self._strict_check = False
        if self.blotter_type == BlotterType.EXCLUSIVE:
            self._strict_check = True
            
        self._market_orders_tracker = set()
        self._last_api_call = None

    def __str__(self):
        return "Blueshift Blotter [%s:%s,%s]" % (
                self._mode.name.lower(), self.blotter_type.name, self._broker)

    def __repr__(self):
        return self.__str__()
    
    def reset(self, timestamp:pd.Timestamp|None=None, account:BlotterAccount|float|None=None, 
              initial_positions:dict[Asset, Position]|None=None, last_saved:pd.Timestamp|None=None, 
              last_reconciled:pd.Timestamp|None=None, needs_reconciliation:bool=False, 
              last_reconciliation_status:bool=False, **kwargs) -> None:
        """
            Reset blotter. this in turns calls the `reset` functions
            of the individual trackers.
        """
        if not initial_positions:
            initial_positions = {}
            
        self._last_reconciled = last_reconciled
        self._last_reconciled_attempt = last_reconciled
        self._last_valuation_attempt = last_reconciled
        self._last_saved = last_saved
        self._needs_reconciliation = needs_reconciliation
        self._last_reconciliation_status = last_reconciliation_status

        self._txns_tracker.reset(
                last_saved, last_reconciled, needs_reconciliation,
                last_reconciliation_status,
                initial_positions=initial_positions)

        self._pos_tracker.reset(
                last_saved, last_reconciled, needs_reconciliation,
                last_reconciliation_status,
                initial_positions=initial_positions)

        if account is None:
            account = self._env.init_capital
        self._acct_tracker.reset(
                account, last_saved, last_reconciled,
                needs_reconciliation, last_reconciliation_status)
        
        # update the account with initial positions, if any, we ideally
        # should trigger performance update as well, but let's hope 
        # that is not needed before the first TRADING_BAR. Note: this 
        # is really needed for the first `reset` call, but since reet 
        # is called by the context only once, this is okay, and we do 
        # not need to check if this is the first reset call.
        if self._pos_tracker._current_pos:
            self.logger.info(f'Updating account for initial positions.')
            self._acct_tracker.update_valuation(
                    self._pos_tracker._current_pos, self._broker)
            
        self._perfs_tracker.reset(
                self._acct_tracker._account, last_saved, last_reconciled)

        self._oo_cashflow = self._txns_tracker.\
                                open_order_cash_outlay(self._data_portal)
                                
        self.sanitize()
                                
    def set_record_vars(self, record_vars):
        self._record_vars = record_vars
                                
    def _save(self, timestamp=None, *args, **kwargs):
        """
            Save the blotter analytics. this triggers saving analytics in the 
            performance directory.
        """
        if not self._read_success:
            if self.logger:
                msg = f'Skipping blotter data save as read was not successful.'
                self.logger.warning(msg)
            return
        
        if self._last_saved and self._last_saved == timestamp:
            return
        
        self._txns_tracker._save(timestamp)
        self._pos_tracker._save(timestamp)
        self._acct_tracker._save(timestamp)
        self._perfs_tracker._save(timestamp)
        
        txns_df = pd.DataFrame()
        try:
            txns_df = create_txns_frame(
                    self.transactions,tz=self.trading_calendar.tz)
            if not txns_df.empty:
                fname = 'transactions.csv'
                fname = os_path.join(self._output_dir, fname)
                txns_df.to_csv(fname)
        except Exception as e:
            if isinstance(e, TerminationError):
                raise
            
            if self.logger:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                err_msg = get_exception(exc_type, exc_value, exc_traceback)
                self.logger.info(err_msg)
                
            msg = f"failed to process transaction data in blotter:{str(e)}"
            handling = ExceptionHandling.WARN
            raise DataWriteException(msg=msg, handling=handling)
            
        pos_df = pd.DataFrame()
        try:
            pos_df = create_positions_frame(self.positions)
            if not pos_df.empty:
                fname = 'positions.csv'
                fname = os_path.join(self._output_dir, fname)
                pos_df.to_csv(fname)
        except (TypeError, KeyError, BlueshiftException) as e:
            if self.logger:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                err_msg = get_exception(exc_type, exc_value, exc_traceback)
                self.logger.info(err_msg)
                
            msg = f"failed to process positions data in blotter:{str(e)}"
            handling = ExceptionHandling.WARN
            raise DataWriteException(msg=msg, handling=handling)
            
        trip_df = pd.DataFrame()
        try:
            if self.round_trips:
                trip_df = create_round_trips_frame(
                        self.round_trips)
                if not trip_df.empty:
                    fname = 'round_trips.csv'
                    fname = os_path.join(self._output_dir, fname)
                    trip_df.to_csv(fname)
            self._perfs_tracker._save(timestamp)
        except (TypeError, OSError) as e:
            if self.logger:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                err_msg = get_exception(exc_type, exc_value, exc_traceback)
                self.logger.info(err_msg)
                
            msg = f"failed to process round trips data in blotter:{str(e)}"
            handling = ExceptionHandling.WARN
            raise DataWriteException(msg=msg, handling=handling)
            
        self._last_saved = timestamp
        if self.logger:
            self.logger.info(
                    f'successfully saved blotter data at {timestamp}.')

    def _read(self, timestamp):
        self._read_success = False
        try:
            self._txns_tracker._read(self._asset_finder, timestamp)
            self._pos_tracker._read(self._asset_finder, timestamp)
            self._acct_tracker._read(timestamp)
            self._perfs_tracker._read(timestamp)
            self._read_success = True
            if self.logger:
                self.logger.info(
                        f'successfully read saved blotter data at {timestamp}.')
        except Exception as e:
            if self.logger:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                err_msg = get_exception(exc_type, exc_value, exc_traceback)
                self.logger.info(err_msg)
            raise e
            
    def sanitize(self):
        pos = self.portfolio
        
        # asset expiry date are without tz-info
        today = pd.Timestamp.now(
                tz=self.trading_calendar.tz).normalize().tz_localize(
                        None)
        for asset in pos:
            try:
                self.asset_finder.symbol(asset.exchange_ticker, logger=self.logger)
            except Exception:
                msg = f'Cannot restart with unknown asset {asset} in '
                msg += f'open position.'
                raise AlgoStartError(msg)
                
            if hasattr(asset, 'expiry_date') and asset.expiry_date < today:
                msg = f'Cannot restart with expired asset {asset} in '
                msg += f'open position.'
                raise AlgoStartError(msg)
        
        if self.blotter_type == BlotterType.EXCLUSIVE:
            self._txns_tracker.sanitize(self._broker, self._env.restart)
            self._pos_tracker.sanitize(self._broker, self._env.restart)
            self._acct_tracker.sanitize(self._broker, self._env.restart)
            
        if self._txns_tracker._open_orders:
            # reconcile for open orders over the restart
            try:
                timestamp = pd.Timestamp.now(tz=self.trading_calendar.tz)
                self.reconcile(timestamp=timestamp, internal=True)
            except Exception as e:
                msg = f'Failed to do open order reconciliation on restart:{str(e)}.'
                raise AlgoStartError(msg)

    def _roll(self, timestamp, *args, **kwargs):
        try:
            self.reconcile(timestamp, internal=True)
        except BlueshiftException:
            # in case of exception, we save the latest
            # snapshot anyways, instead of saving nothing.
            pass
        
        # update benchmark if any
        self.update_benchmark(timestamp)
        
        # we must call update metrics here, as it may not ever be
        # called during the whole day if there were no trades.
        self._perfs_tracker.update_metrics(
                self._acct_tracker._account, self._get_all_orders(), 
                self._get_portfolio(), timestamp.value)
        
        # capture the positions data for today
        self._daily_positions[timestamp] = {}
        try:
            current_positions = self.portfolio
            for asset in current_positions:
                pos = current_positions[asset]
                self._daily_positions[timestamp][asset.exchange_ticker]=pos.to_json()
        except Exception:
            pass
        
        # now roll the trackers.
        self._txns_tracker.roll(self._asset_finder, timestamp)
        self._pos_tracker.roll(self._asset_finder, timestamp)
        self._acct_tracker.roll(timestamp)
        self._perfs_tracker.roll(timestamp)
        self._save(timestamp)

        self._last_saved = timestamp
        self._oo_cashflow = self._txns_tracker.\
                                open_order_cash_outlay(self._data_portal)
                                
        # Finally, update/ create the analytics reports
        if self._last_risk_update is None or\
                                    self._last_risk_update < timestamp:
            self.update_perf_report(timestamp)
            self._last_risk_update = timestamp

    def _finalize(self, timestamp=None, *args, **kwargs):
        if timestamp is None:
            timestamp = self._timestamp

        if timestamp is None:
            # we were probably never initialized!
            # example, algo end < algo start!
            msg = "Suspect improper initialization or a failure before "
            msg = msg + "the clock started."
            raise DataWriteException(msg = msg)
        
        exit_time = kwargs.pop('exit_time', False)
        try:
            if exit_time or self._last_saved is None or self._last_saved < timestamp:
                self._perfs_tracker.update_metrics(
                        self._acct_tracker._account, self._get_all_orders(), 
                        self._get_portfolio(), timestamp.value)
        except Exception as e:
            if isinstance(e, TerminationError):
                raise
                
            if self.logger:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                err_msg = get_exception(exc_type, exc_value, exc_traceback)
                self.logger.info(err_msg)
                
            msg = f'failed to update performance metrics, see logs for details: {str(e)}'
            self.logger.error(msg)

        try:
            self.update_perf_report(timestamp)
            self._last_risk_update = timestamp
        except Exception as e:
            if isinstance(e, TerminationError):
                raise
            
            if self.logger:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                err_msg = get_exception(exc_type, exc_value, exc_traceback)
                self.logger.info(err_msg)
                
            msg = f'failed to update performance reports: {str(e)}'
            self.logger.error(msg)

        try:
            self._save(timestamp)
        except Exception as e:
            if isinstance(e, TerminationError):
                raise
                
            if self.logger:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                err_msg = get_exception(exc_type, exc_value, exc_traceback)
                self.logger.info(err_msg)
                
            msg = f'failed to save the blotter: {str(e)}'
            raise DataWriteException(msg=msg)

    def add_transactions(self, order_id, order, commissions, charges,
                         if_market=None, **kwargs):
        """
            Add entry to blotter to be verified. This triggers the
            transaction tracker function.

            Args:
                ``order_id (str)``: Order ID to add.

                ``order (object)``: An order object with details.

                ``commissions (float)``: Ignored (obtained directly through broker API).

                ``charges (float)``: Ignored (obtained directly through broker API).

            Returns:
                None.
        """
        if listlike(order_id):
            for oid in order_id:
                self.emit_transactions_msg({order.oid:order})
                self._txns_tracker.add_transaction(oid, order)
        else:
            self.emit_transactions_msg({order.oid:order})
            self._txns_tracker.add_transaction(order_id, order)
        
        if if_market or order.order_type == OrderType.MARKET:
            self._market_orders_tracker.add(order_id)
        
        self._needs_reconciliation = True
    
    @property
    def name(self):
        return self._name
    
    @property
    def mode(self):
        return self._mode
    
    @property
    def ccy(self):
        return self._ccy
    
    @property
    def asset_finder(self):
        return self._asset_finder
    
    @property
    def data_portal(self):
        return self._data_portal
    
    @property
    def broker(self):
        return self._broker
    
    @property
    def performance_tracker(self):
        return self._perfs_tracker
    
    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, timestamp):
        self._timestamp = timestamp
        self._txns_tracker.set_timestamp(timestamp)
        self._pos_tracker.set_timestamp(timestamp)
        self._acct_tracker.set_timestamp(timestamp)
        self._perfs_tracker.set_timestamp(timestamp)
        
        for name in self.blotters:
            self.blotters[name].timestamp = timestamp

    @property
    def account(self):
        self.reconcile(timestamp=self._timestamp, internal=True)
        return self._acct_tracker._account

    @property
    def portfolio(self):
        self.reconcile(timestamp=self._timestamp, internal=True)
        return self._get_portfolio()
    
    def _get_portfolio(self):
        positions = self._pos_tracker._current_pos.copy()
        return positions
    
    @property
    def current_positions(self):
        """ this is positions as is, without reconciliation and original copy. """
        return self._pos_tracker._current_pos
    
    @property
    def round_trips(self):
        return self._txns_tracker._round_trips

    @property
    def open_orders(self):
        self.reconcile(timestamp=self._timestamp, internal=True)
        open_orders = self._txns_tracker._open_orders.copy()
        unprocessed_orders = self._txns_tracker._unprocessed_orders.copy()
        return {**open_orders, **unprocessed_orders}

    @property
    def orders(self):
        self.reconcile(timestamp=self._timestamp, internal=True)
        return self._get_all_orders()
    
    def _orders_no_reconcile(self):
        return self._get_all_orders()
    
    def _get_all_orders(self):
        closed_orders = self._txns_tracker.flatten_transactions_dict()
        open_orders = self._txns_tracker._open_orders.copy()
        unprocessed_orders = self._txns_tracker._unprocessed_orders.copy()
        return {**closed_orders, **open_orders, **unprocessed_orders}

    @property
    def performance(self):
        self.reconcile(timestamp=self._timestamp, internal=True)
        return self._perfs_tracker.current_performance.copy()
    
    @property
    def perfs_history(self):
        perfs = self._perfs_tracker.to_dataframe()
        perfs.index = pd.to_datetime(perfs.index.astype('int64'))
        perfs.index = perfs.index.tz_localize('UTC').\
                            tz_convert(self.trading_calendar.tz)
        return perfs
    
    @property
    def current_performance(self):
        """ current performance without reconciliaton and original copy. """
        return self._perfs_tracker.current_performance

    @property
    def risk_report(self):
        # we do not update reconciliation or performance report here.
        # updates supposed to run at EOD process as part of the roll.
        # also for live trading, details stats are not computed.
        return self._perfs_tracker.create_eod_report(quick_mode=True)

    @property
    def pnls(self):
        self.reconcile(timestamp=self._timestamp, internal=True)
        return self._perfs_tracker.to_dataframe_performance()
    
    @property
    def transactions(self):
        return self._txns_tracker._transactions
    
    @property
    def positions(self):
        return self._daily_positions
    
    def reconcile(self, timestamp=None, emit_log=True, forced=False,
                  exit_time=False, internal=False, fetch_orders=True,
                  only_txns=False, **kwargs):
        """
            Blotter reconciliation function. The aim is to be stateless,
            so that this function can be called on the blotter object
            any number of times. The function fetches the broker version
            of orders, does a recon with transaction tracker. This
            produces the True if matched. It also generates a list of
            freshly matched orders, and update the open orders fill
            status therein as well. These later two are then used to
            update the positions in the position tracker. After this, we
            fetch the positions from the brokers and do a recon with the
            updated position tracker. this also implicitly update the
            position pnls based on last prices from the broker positions
            data.

            Args:
                ``timestamp (Timestamp)``: Timestamp for the call.
                ``emit_log (bool)``: Emit log on reconciliation diff.
                ``forced (bool)``: Force an update.

            Returns:
                Tuple. Details about order and position reconciliations.
        """
        skippable = not (forced or exit_time)
        now = pd.Timestamp.now(tz=self.trading_calendar.tz)
        
        if not self._last_reconciled_attempt:
            self._last_reconciled_attempt = now
            
        if abs((now - self._last_reconciled_attempt).total_seconds()) < \
            self._RECURSION_TIMEOUT and skippable:
            # hard protection against unintended recursion
            return True
        
        try:
            with self._lock:
                if not self._env.algo or not self._initialized:
                    return True
                
                if not self._env.algo.is_TRADING_BAR() and skippable:
                    return True
                
                if internal:
                    self._internal_recon_counter += 1
                    
                if self._internal_recon_counter > 2:
                    # hard protection against unintended recursion
                    self._internal_recon_counter = 0
                    return False
            
                return self._reconcile(
                        timestamp=timestamp, emit_log=emit_log, 
                        forced=forced, exit_time=exit_time,
                        internal=internal, fetch_orders=fetch_orders,
                        only_txns=only_txns)
        except Exception as e:
            if isinstance(e, (TerminationError, ReconciliationError)):
                raise
                
            if self.logger:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                err_msg = get_exception(exc_type, exc_value, exc_traceback)
                self.logger.info(err_msg)
                
            self.logger.error(f'reconciliation could not be done: {str(e)}.')
            return False
        finally:
            if internal:
                self._internal_recon_counter = max(0, self._internal_recon_counter-1)
            else:
                self._internal_recon_counter = 0
        
    def _reset_market_order_tracker(self):
        open_orders = self._txns_tracker._open_orders
        market_orders = [oid for oid in self._market_orders_tracker \
                                  if oid in open_orders]
        
        self._market_orders_tracker = set(market_orders)
        
    def _fetch_orders_for_reconcile(self, exit_time=False, fetch_orders=True):
        """ 
            Check if we have any missing orders from the unprocessed 
            list. This might be due to some issues with receiving 
            order updates from the broker. Force an update.
        """
        now = pd.Timestamp.now(tz=self.trading_calendar.tz)
        orders = self._broker.get_all_orders(
                algo_name=self._env.name,
                algo_user=self._env.algo_user,
                logger=self.logger)
        
        if not fetch_orders:
            return orders
        
        unprocessed = self._txns_tracker._unprocessed_orders
        open_orders = self._txns_tracker._open_orders
        
        # check if anything left in unprocessed not appearing in the fetched
        # order list. This relies on the fact that the unprocessed conainer
        # will have the actual broker ID.
        for order_id in unprocessed:
            if order_id not in orders:
                msg = f'Updates not yet received for some new order(s), fetching '
                msg += f'order details from the broker for reconciliation.'
                self.logger.warning(msg)
                self._last_api_call = now
                return self._broker.fetch_all_orders(
                        algo_name=self._env.name,
                        algo_user=self._env.algo_user,
                        logger=self.logger, exit_time=exit_time)
        
        missing_open_orders = [oid for oid in open_orders if oid not in orders]
        if missing_open_orders:
            msg = f'Some open order(s) are missing from broker data, fetching '
            msg += f'order details from the broker for reconciliation.'
            self.logger.warning(msg)
            self._last_api_call = now
            orders = self._broker.fetch_all_orders(
                        algo_name=self._env.name,
                        algo_user=self._env.algo_user,
                        logger=self.logger, exit_time=exit_time)
            still_missing = [oid for oid in open_orders if oid not in orders]
            if still_missing:
                is_main = threading.get_ident()==threading.main_thread().ident
                if not is_main:
                    sleep_time = SLEEP_TIME_FOR_ORDER_FETCH
                    self.logger.info(f'sleeping {sleep_time}s before retrying order fetch')
                    time.sleep(sleep_time)
                        
                for oid in still_missing:    
                    try:
                        o = self._broker.get_order(oid, logger=self.logger)
                        #assert o is not None, 'order not found.'
                        if o is None:
                            raise ValueError(f'order not found.')
                    except Exception as e:
                        msg = f'Failed to fetch order details for {oid} from '
                        msg += f'broker: {str(e)}.'
                        self.logger.error(msg)
                    else:
                        orders[oid] = o
                        
            return orders
        
        # check if we have any open market order still not filled. This is 
        # unlikely and maybe our order streaming connection is broker and 
        # we are missing updates. Fetch orders explicitly in such case.
        open_market_orders = [oid for oid in open_orders \
                                  if oid in self._market_orders_tracker]
        try:
            for oid in open_market_orders:
                if not orders[oid].is_open():
                    continue
                o = open_orders[oid]
                is_ioc = o.order_validity in (OrderValidity.IOC, OrderValidity.FOK,)
                last = o.timestamp
                is_ioc = exit_time and is_ioc # fetch only for exit time
                
                if is_ioc or (last and (now-last).total_seconds() > self._OPEN_ORDER_TIMEOUT):
                    msg = f'Some order(s) updates maybe missing, fetching '
                    msg += f'latest order details from the broker for '
                    msg += f'reconciliation.'
                    self.logger.warning(msg)
                    self._last_api_call = now
                    return self._broker.fetch_all_orders(
                            algo_name=self._env.name,
                            algo_user=self._env.algo_user,
                            logger=self.logger, exit_time=exit_time)
        except Exception as e:
            self.logger.error(f'failed checking open market order timeouts:{str(e)}.')
            
        # fetch from api for exit time recon if any open orders
        if exit_time:
            for oid in orders:
                if orders[oid].is_open():
                    msg = f'Fetching order details from broker for '
                    msg += f'exit time reconciliation.'
                    self.logger.warning(msg)
                    self._last_api_call = now
                    return self._broker.fetch_all_orders(
                            algo_name=self._env.name,
                            algo_user=self._env.algo_user,
                            logger=self.logger, exit_time=exit_time)
            
        if not self._CHECK_LIMIT_ORDERS:
            return orders
        
        try:
            for oid in open_orders:
                if not orders[oid].is_open():
                    continue
                o = open_orders[oid]
                last = o.timestamp
                if last and (now-last).total_seconds() > self._OPEN_ORDER_TIMEOUT:
                    elapsed = self._ORDER_FETCH_TIMEOUT+1 if self._last_api_call is None else \
                        (now - self._last_api_call).total_seconds()
                    
                    if elapsed > self._ORDER_FETCH_TIMEOUT:
                        msg = f'Suspected missing updates for some orders, fetching '
                        msg += f'latest order details from the broker for '
                        msg += f'reconciliation.'
                        self.logger.info(msg)
                        self._last_api_call = now
                        return self._broker.fetch_all_orders(
                                algo_name=self._env.name,
                                algo_user=self._env.algo_user,
                                logger=self.logger, exit_time=exit_time)
        except Exception as e:
            self.logger.error(f'failed checking open order timeouts:{str(e)}.')
            
        return orders

    def _reconcile(self, timestamp=None, emit_log=True, forced=False,
                   exit_time=False, internal=False, fetch_orders=True,
                   only_txns=False):
        """
            underlying reconciliation to be wrapped in a thread-safe 
            context manager.
        """
        now = pd.Timestamp.now(tz=self.trading_calendar.tz)
        
        if timestamp is None:
            timestamp = now
            
        pending_check = self._txns_tracker.if_update_required(self._broker)
        skippable = not (forced or exit_time)
        
        if not self._last_reconciled_attempt:
            self._last_reconciled_attempt = now
        
        if abs((now - self._last_reconciled_attempt).total_seconds()) < \
            self._RECURSION_TIMEOUT and skippable:
            # hard protection against unintended recursion
            return True
        self._last_reconciled_attempt = now
        
        ts_check = False
        if self._last_reconciled:
            # a minimum floor of MINIMUM_UPDATE_TIME
            if abs((timestamp - self._last_reconciled).total_seconds()) < \
                MINIMUM_UPDATE_TIME and skippable:
                return True
            
            # more than minute of pending recon
            if self._last_reconciled < self._normalize_ts(timestamp):
                ts_check = True
                
        can_skip = not (pending_check or ts_check or forced or exit_time)
        if can_skip:
            return self._last_reconciliation_status
        
        net_difference = 0
        oo_cash = 0
        matched_orders = {}
        extra_orders = []
        missing_orders = []
        unexplained_pos = {}
        orders_matched = positions_matched = True
        open_orders_missing = []

        # do nothing if nothing to reconcile!
        txns_need_update = self._txns_tracker.if_update_required(
                                                    self._broker)
        if only_txns and not txns_need_update:
            # nothing to reconcile
            return True
        
        if txns_need_update:
            orders = self._fetch_orders_for_reconcile(
                    exit_time=exit_time, fetch_orders=fetch_orders)
            # reset market orders tracker as it is at this point clean
            #self._market_orders_tracker = set()
            # get the updated synthetic orders, if any
            synthetic_orders = self._broker.synthetic_orders
            # get the orders reconciliation results, this must be first.
            orders_matched, missing_orders, extra_orders, matched_orders, open_orders_missing = \
                                    self._txns_tracker.reconcile(
                                            timestamp, orders,
                                            synthetic_orders)
            # get a record of change in open order cash outlay, if any
            oo_cashflow = self._txns_tracker.\
                                open_order_cash_outlay(self._data_portal)
            oo_cash = oo_cashflow - self._oo_cashflow
            self._oo_cashflow = oo_cashflow
            open_orders = self._txns_tracker._open_orders.copy()
            if open_orders and self._publish:
                self.emit_transactions_msg(open_orders)
                
            if matched_orders and self._env.algo:
                oids = list(matched_orders.keys())
                self._env.algo.schedule_event(AlgoCallBack.RECONCILIATION, param=oids)
            
            if matched_orders:
                self.logger.info(f'Reconciliation complete for order(s) {matched_orders}.')
                if self._publish:
                    self.emit_transactions_msg(matched_orders)
            
            # now update open trades, round trips and open order positions.
            open_pos = self._txns_tracker.update_positions_from_orders(
                    matched_orders.copy(), self._broker, self._data_portal)
            open_trades = self._txns_tracker._open_trades
            # update the current position - combine open trades and pos
            self._pos_tracker.update_current_pos(open_trades, open_pos)
            
            # update the open market order tracker
            self._reset_market_order_tracker()

        # update positions data if we have some or something change!
        pos_need_update = self._pos_tracker.if_update_required(
                self._broker) or orders_matched or missing_orders
        if pos_need_update:
            if self.blotter_type == BlotterType.EXCLUSIVE:
                # not supported by multuuser broker
                positions = self._broker.positions
            else:
                # not used for virtual blotter
                positions = {}
            # match positions, update pnls from the latest data from broker.
            positions_matched, unexplained_pos = self._pos_tracker.reconcile(
                    timestamp, positions, self._data_portal)

        # updating the account is always required.
        acct_needs_update = pos_need_update or \
                            self._txns_tracker._open_orders or \
                            self._acct_tracker.if_update_required(
                                    self._broker)
        if acct_needs_update:
            # recompute pnls on open items, add to realized of close
            # also compare to reported account, but that is not crucial.
            if self.blotter_type == BlotterType.EXCLUSIVE:
                # not supported by multuuser broker
                account = self._broker.account
            else:
                # not used for virtual blotter
                account = None
            
            net_difference = self._acct_tracker.reconcile(
                    timestamp, account, self._pos_tracker._current_pos,
                    matched_orders, oo_cash, self._broker,
                    self._data_portal,
                    self._txns_tracker._round_trips_realized,
                    self._txns_tracker._cash_adj)
            
            self._perfs_tracker.update_metrics(
                    self._acct_tracker._account, self._get_all_orders(), 
                    self._get_portfolio(), timestamp.value)

        self._last_reconciled = timestamp
        self._needs_reconciliation = False
        self._last_reconciliation_status = orders_matched
        self._last_updated = timestamp

        # we are done. write to log if required. else exit.
        if emit_log:
            failed = False
            if not orders_matched:
                failed = self.emit_reconciliation_msg(
                        orders_matched, missing_orders, extra_orders,
                        positions_matched, unexplained_pos, net_difference,
                        timestamp)
            
            if open_orders_missing:
                self._last_reconciliation_status = False
                msg = "Orders reconciliation failed, missing orders are "
                msg += f"{open_orders_missing}. Will exit algo. Please "
                msg += f'check and verify your broker account to reconcile.'
                raise ReconciliationError(msg)
            
            if failed:
                self._last_reconciliation_status = False
                msg = "Orders reconciliation failed, missing orders. "
                msg += "Will exit algo. Please check your broker account "
                msg += f"to reconcile. See logs for more details."
                raise ReconciliationError(msg)
                
        return self._last_reconciliation_status

    def emit_reconciliation_msg(self, orders_matched, missing_orders,
                                extra_orders, positions_matched,
                                unexplained_pos, net_difference,
                                timestamp):
        """
            Emit a reconciliation message to the logger based.
        """
        failed = False
        order_msg = pos_msg = ""

        order_msg = "orders reconciliation failed. "
        order_msg = order_msg + \
                f"missing orders: {','.join([str(e) for e in missing_orders])}. "

        for order_id in missing_orders:
            if self._txns_tracker._missing_orders[order_id] > BLOTTER_MISSING_ORDER_THRESHOLD:
                order_msg = order_msg + f"permanently failed to match "
                order_msg = order_msg + f"order ID {order_id}. "
                self._txns_tracker.pop_missing_order(order_id)
                failed = True

        if self.blotter_type == BlotterType.EXCLUSIVE:
            if not positions_matched:
                pos_msg = "positions reconciliation failed."
                pos_msg = pos_msg + f"unexplained: {unexplained_pos}. "
    
            msg = order_msg + pos_msg +\
                f"account net difference: {round(net_difference,4)}"
        else:
            # do not show extra info for virtual accounts
            msg = order_msg
        
        if self.logger:
            self.logger.warning(msg)
            
        return failed
            
    def _update_valuation(self, timestamp, timeout=None):
        """
            Function to update the valuation of the current portfolio.
            We call the position tracker update valuation method. If it
            returns any change, we call the account tracker update method
            as well.

            Args:
                ``timestamp (Timestamp)``: timestamp of call.

                ``timeout (float)``: timeout in seconds.
        """
        now = pd.Timestamp.now(tz=self.trading_calendar.tz)
        if not self._last_valuation_attempt:
            self._last_valuation_attempt = now
            
        if abs((now - self._last_valuation_attempt).total_seconds()) < \
            self._RECURSION_TIMEOUT:
            # hard protection against unintended recursion
            return True
            
        try:
            with self._lock:
                if not self._env.algo:
                    return True
                    
                timestamp = timestamp or now
                if not self._env.algo.is_TRADING_BAR():
                    self._last_valuation_update = timestamp
                    return True
                
                self._last_valuation_attempt = now
                res = self._update_valuation_(
                        timestamp=timestamp, timeout=timeout)
                self._last_valuation_update = timestamp
                return res
        except Exception as e:
            if isinstance(e, (TerminationError, ReconciliationError)):
                raise
                
            if self.logger:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                err_msg = get_exception(exc_type, exc_value, exc_traceback)
                self.logger.info(err_msg)
                
            self.logger.info(f'valuation update failed: {str(e)}.')
            
            if self._last_valuation_update:
                late = (timestamp - self._last_valuation_update).total_seconds(
                        ) > BLOTTER_UPDATE_TIMEOUT
                if late and self._pos_tracker._current_pos:
                    msg = f'Failed to reconcile positions repeatedly, '
                    msg += f'will exit the algo. Please check and verify '
                    msg += f'your broker account to reconcile: {str(e)}.'
                    raise ReconciliationError(msg)
            
            return False

    def _update_valuation_(self, timestamp, timeout=None):
        """
            function to be wrapped in thread-safe context manager.
        """
        if self._txns_tracker._unprocessed_orders or \
                self._txns_tracker._open_orders:
            self.reconcile(timestamp, internal=True)
            return True
        
        if self._last_updated and timestamp:
            # put a minimum floor of MINIMUM_UPDATE_TIME
            if abs((timestamp - self._last_updated).total_seconds()) < MINIMUM_UPDATE_TIME:
                return False
        
        change = self._pos_tracker.update_valuation(
                self._data_portal, timeout, self._broker)

        if change:
            self._acct_tracker.update_valuation(
                    self._pos_tracker._current_pos, self._broker)

            self._perfs_tracker.update_metrics(
                    self._acct_tracker._account, self._get_all_orders(), 
                    self._get_portfolio(), timestamp.value)

        self._last_updated = timestamp
        return change
    
    def _strict_check_restart(self):
        # not supported by multuuser broker
        blotter_pos = self._pos_tracker.positions
        broker_pos = self._broker.positions
        
        for asset in blotter_pos:
            if asset not in broker_pos:
                msg = f"Expected algo positions for {asset.symbol} missing "
                msg = msg + "in broker account, will exit."
                raise AlgoStartError(msg = msg)
        
            broker_qty = broker_pos[asset].quantity
            blotter_qty = blotter_pos[asset].quantity
            
            if self.blotter_type == BlotterType.VIRTUAL:
                if np.sign(broker_qty) != np.sign(blotter_qty):
                    pos = 'long' if np.sign(blotter_qty) > 0 else 'short'
                    msg = f"Algo has a {pos} position for {asset.symbol}, "
                    msg = msg + "but in broker account, it is opposite. "
                    msg = msg + ' Will exit.'
                    raise AlgoStartError(msg = msg)
            else:
                if broker_qty != blotter_qty:
                    msg = f"Algo has {blotter_qty} position for {asset.symbol}, "
                    msg = msg + f"but in broker account, it is {broker_qty}. "
                    msg = msg + ' Will exit.'
                    raise AlgoStartError(msg = msg)

    def check_on_restart(self):
        """ 
            Check if we have all positions in the broker account as well
            as the signs (long or short) are correct. A different amount 
            of actual position is allowed. No check is done if `_strict_check`
            is False (default for Virtual accounts).
            
            Note:
                A strict check for exact position matching may be restrictive
                for dual use of the account (virtual account). Also in 
                cases like splits/ bonuses, holdings can change.
        """
        self.sanitize()
        
        if self._strict_check:
            self._strict_check_restart()
                    
    def set_benchmark(self, benchmark=None):
        """
            Set up benchmark at the algo start.
        """
        if not benchmark:
            benchmark = self._env.benchmark
            
        if benchmark is None:
            self._benchmark_asset = None
            return
        
        if isinstance(benchmark, MarketData):
            self._benchmark_asset = benchmark
        else:
            msg = f'benchmark must be a a valid asset.'
            raise InitializationError(msg)
        
        try:
            last = self.broker.current(self._benchmark_asset, 'close')
            timestamp = pd.Timestamp.now(tz=self.trading_calendar.tz)
            timestamp = timestamp.normalize() - pd.Timedelta(days=1)
            benchmark = pd.Series([last], index=[timestamp])
        except Exception:
            pass
        else:
            if not math.isnan(last):
                self._perfs_tracker.set_benchmark(benchmark)
        
                    
    def update_benchmark(self, timestamp=None, benchmark=None):
        """
            Update the benchmark for today. Should be called during 
            the algo roll at the end-of-day.
        """
        if self._benchmark_asset is None:
            return
        
        try:
            last = self.broker.current(self._benchmark_asset, 'close')
        except Exception:
            pass
        else:
            if not math.isnan(last):
                self._perfs_tracker.update_benchmark(timestamp, last)
                    
    def update_perf_report(self, timestamp):
        """
            Compute host of analytics. For backtest usually this will be
            called at the end of the algo run. For live mode, this is
            called at every bar. This includes a per-trade analytics pack,
            a transaction reports pack and a risk statistics pack.
        """
        pass
    
    def add_blotter(self, strategy, timestamp=None):
        """ 
            Add a botter.
        """        
        output_dir = None
        logger = self.logger
        output_dir = os_path.join(self._output_dir, strategy.name)
        
        blotter = LiveBlotter(
                strategy.name, 
                self.asset_finder,
                self.data_portal, 
                broker=self.broker,
                logger=logger, 
                init_capital=strategy.initial_capital, 
                timestamp=timestamp, 
                env=self._env, 
                ccy=self.ccy,
                output_dir=output_dir,
                blotter_type=self.blotter_type)
        
        self.blotters[blotter.name] = blotter
        blotter.topic = self.name
        blotter.parent = self
        
        return blotter
    
    
register_blotter('live', LiveBlotter)