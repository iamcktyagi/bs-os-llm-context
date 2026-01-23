from __future__ import annotations
from typing import TYPE_CHECKING, cast, Any
from os import path as os_path
import json
import sys
import logging

from blueshift.lib.common.constants import CCY, Currency
from blueshift.lib.common.platform import get_exception
from blueshift.lib.common.enums import (
        AlgoMessageType as MessageType, BlotterType, AlgoMode)
from blueshift.lib.common.enums import AlgoOrderStatus
from blueshift.lib.common.functions import create_message_envelope
from blueshift.lib.exceptions import (
        ExceptionHandling, DataWriteException, InitializationError,
        BlueshiftException)
from blueshift.interfaces.assets._assets import Asset
from blueshift.lib.trades._accounts import BlotterAccount
from blueshift.lib.trades._position import Position
from blueshift.interfaces.trading.blotter import register_blotter
from blueshift.lib.serialize.json import convert_nan, BlueshiftJSONEncoder
from blueshift.interfaces.assets.assets import IAssetFinder
from blueshift.interfaces.trading.broker import IBroker
from blueshift.interfaces.data.data_portal import DataPortal

from ..trackers._tracker import (
        DailyTransactionsTracker, DailyPositionTracker, DailyAccountTracker)
from ..trackers.execution_tracker import ExecutionTracker
from .live_blotter import LiveBlotter

if TYPE_CHECKING:
    import pandas as pd
    from blueshift.core.utils.environment import TradingEnvironment
else:
    import blueshift.lib.common.lazy_pandas as pd


MINIMUM_UPDATE_TIME=1.5 # update requests below this will be refused

class ExecutionBlotter(LiveBlotter):
    """
        A specific blotter suitable for running smart execution algos, 
        that are interested in optimizing order execution and tracking 
        execution performance - instead of generating signals and tracking 
        profit and loss performance.

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
    def __init__(self, name:str, asset_finder:IAssetFinder, data_portal:DataPortal, broker:IBroker, 
                 env:TradingEnvironment, logger:logging.Logger|None=None, init_capital:float|None=None, 
                 timestamp:pd.Timestamp|None=None, ccy:Currency=CCY.LOCAL, output_dir=None, # type: ignore
                 blotter_type:BlotterType=BlotterType.VIRTUAL):
        if blotter_type!=BlotterType.VIRTUAL:
            msg = f'Only virtual blotters are supported for smart orders.'
            raise InitializationError(msg)
        
        self._initialized = False
        super(ExecutionBlotter, self).__init__(
                name, asset_finder, data_portal, broker, 
                logger=logger, init_capital=init_capital, timestamp=timestamp, 
                env=env, ccy=ccy, output_dir=output_dir, 
                blotter_type=blotter_type)
        self._initialized = False
        self._rejected = False
        
        self._txns_tracker = DailyTransactionsTracker(self.name, self._mode, None, self._ccy)
        self._pos_tracker = DailyPositionTracker(self.name, self._mode, None, self._ccy)
        self._acct_tracker = DailyAccountTracker(self.name, self._mode, init_capital)
        
        tz = self.trading_calendar.tz
        self._perfs_tracker = ExecutionTracker(
                self.name, self._mode, tz=tz, env=self._env)
        
        self._mode = AlgoMode.EXECUTION
        self._initialized = True

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
        if initial_positions:
            msg = f'Initial positions not supported for smart orders.'
            raise NotImplementedError(msg)
            
        super(ExecutionBlotter, self).reset(
                account=account, last_saved=last_saved, 
                last_reconciled=last_reconciled, 
                needs_reconciliation=needs_reconciliation, 
                last_reconciliation_status=last_reconciliation_status,
                initial_positions=initial_positions)
                                
    def _save(self, timestamp=None, *args, **kwargs):
        """
            Save the blotter analytics. this triggers saving analytics in the 
            performance directory.
        """
        if self._last_saved and self._last_saved == timestamp:
            return
        
        data = {}
        
        orders = {}
        for oid in self.orders:
            orders[str(oid)] = self.orders[oid].to_json()
        
        data['performance'] = self.performance
        
        orders_write = {}
        try:
            for oid in self.orders:
                orders_write[str(oid)] = self.orders[oid].to_json()
        except (TypeError, KeyError, BlueshiftException) as e:
            msg = "corrupt orders data in blotter:"
            msg = msg + str(e)
            if self.logger:
                self.logger.error(msg)
        else:
            data['orders'] = orders_write
        
        pos_write = {}
        try:
            for pos in self.current_positions:
                pos_write[pos.exchange_ticker] = self.current_positions[pos].to_json()
        except (TypeError, KeyError, BlueshiftException) as e:
            msg = "corrupt positions data in blotter:"
            msg = msg + str(e)
            if self.logger:
                self.logger.error(msg)
        else:
            data['positions'] = pos_write
        
        try:
            fname = self.name + '.json'
            path = os_path.join(self._output_dir, fname)
            with open(path,'w') as fp:
                convert_nan(data)
                json.dump(data, fp, cls=BlueshiftJSONEncoder)
        except (TypeError, OSError) as e:
            if self.logger:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                err_msg = get_exception(exc_type, exc_value, exc_traceback)
                self.logger.info(err_msg)
                
            msg = f"failed to write blotter data to:{str(e)}"
            handling = ExceptionHandling.WARN
            raise DataWriteException(msg=msg, handling=handling)
            
        self._last_saved = timestamp
        if self.logger:
            self.logger.info(
                    f'successfully saved blotter data at {timestamp}.')

    def _read(self, timestamp):
        """ Not supported for smart orders. """
        pass

    @property
    def pnls(self):
        msg = f'Profit-loss tracking not supported for smart orders.'
        raise NotImplementedError(msg)
    
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
        if not self._env.algo or not self._initialized or self._rejected:
            return True
        
        super().reconcile(
                timestamp, emit_log=emit_log, forced=forced,
                exit_time=exit_time, internal=internal, 
                fetch_orders=fetch_orders, only_txns=only_txns, **kwargs)
        
        perf = self.current_performance.copy()
        statuses = [o.status for oid,o in perf.items()]
        
        if not statuses:
            return
        elif AlgoOrderStatus.ERRORED in statuses:
            # find the first order with status error
            msg = ''
            for oid in perf:
                if perf[oid].status == AlgoOrderStatus.ERRORED:
                    msg = str(perf[oid].status_message)
            msg = msg or 'Smart order errored, see logs for more.'
            self._rejected = True
            self._env.algo.stop(msg=msg)
        elif AlgoOrderStatus.REJECTED in statuses:
            msg = ''
            for oid in perf:
                if perf[oid].status == AlgoOrderStatus.REJECTED:
                    msg = str(perf[oid].status_message)
            msg = msg or 'Smart order rejected, see logs for more.'
            self._rejected = True
            self._env.algo.reject(msg=msg)
        elif AlgoOrderStatus.CANCELLED in statuses:
            msg = ''
            for oid in perf:
                if perf[oid].status == AlgoOrderStatus.CANCELLED:
                    msg = str(perf[oid].status_message)
            msg = msg or 'Smart order cancelled.'
            self._rejected = True
            self._env.algo.cancel(msg=msg)
        
    def emit_transactions_msg(self, orders):
        super().emit_transactions_msg(orders)
        # this self-reference reconcile, schedule it later to avoid
        # an infinite recursion
        func = self.emit_perf_msg
        self._env.algo.run_soon(func)
            
    def emit_perf_msg(self, timestamp):
        if self.publisher:
            perf = self.current_performance.copy()
            perf = {k:v.to_json() for k,v in perf.items()}
            perf:dict[str, Any] = {"orders":perf}
            perf['order_id'] = self.name
            packet = create_message_envelope(self.topic, MessageType.SMART_ORDER, data=perf)
            self.publisher.send_to_topic(self.topic, packet)
            
    
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
        msg = f'Restart is not supported for smart orders.'
        raise NotImplementedError(msg)
                    
    def set_benchmark(self, benchmark=None):
        """
            Set up benchmark at the algo start.
        """
        pass
                    
    def update_benchmark(self, timestamp=None, benchmark=None):
        """
            Update the benchmark for today. Should be called during 
            the algo roll at the end-of-day.
        """
        pass
                    
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
        raise NotImplementedError(f'Sub blotters are not supported.')
        
    def _on_error(self, error):
        self._perfs_tracker.on_error(error)
    
    def _on_cancel(self):
        self._perfs_tracker.on_cancel()
        

register_blotter('execution', ExecutionBlotter)