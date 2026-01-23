from __future__ import annotations
from typing import TYPE_CHECKING
import asyncio
from abc import ABC, abstractmethod
import pandas as pd
import weakref
import sys
import time

from blueshift.lib.common.platform import get_exception
from blueshift.lib.common.functions import create_message_envelope
from blueshift.lib.common.decorators import asyncify
from blueshift.lib.exceptions import (
        TerminationError, InitializationError, BlueshiftException)
from blueshift.lib.common.enums import (
        AlgoMode as MODE, AlgoMessageType as MessageType, AlgoCallBack,
        LIVE_MODES, REALTIME_MODES, SIMULATION_MODES)
from blueshift.config import (
        BLUESHIFT_DISABLE_KILL_SWITCH, LOCK_TIMEOUT, STREAMING_TIMEOUT)

if TYPE_CHECKING:
    import pandas as pd
    from blueshift.core.algorithm.algorithm import TradingAlgorithm
    from blueshift.core.alerts.alert import BlueshiftAlertManager
else:
    import blueshift.lib.common.lazy_pandas as pd

_MAX_ERROR = 50

class Monitor(ABC):
    """
        Abstract base class for monitoring. These are objects that can
        be wrapped in a co-routine (``watch`` method), that periodically
        (``_sleep_interval``) runs a check on something and calls another
        method (``respond``) to act on the outcome.
    """
    def __init__(self, *args, **kwargs):
        self._watchlist = None
        self._sleep_interval = 0.2
        
    @abstractmethod
    def start(self):
        raise NotImplementedError
        
    @abstractmethod
    def stop(self):
        raise NotImplementedError
        
    @abstractmethod
    def add_event(self, event, *args, **kwargs):
        raise NotImplementedError
    
    @abstractmethod
    async def watch(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    async def respond(self, *args, **kwargs):
        raise NotImplementedError


class RiskMonitor(Monitor):
    """
        The position and account monitor. It monitors the values and if
        enabled, publishes them as well.

        Args:
            ``alert_manager (object)``: The associated alert manager.
    """
    def __init__(self, algo:TradingAlgorithm, alert_manager:BlueshiftAlertManager, 
                 risk_management:dict|None=None):
        self._algo = weakref.ref(algo)
        self._sleep_interval = 5
        self._wants_to_quit = False
        self._is_finalized = False
        self._valuation_timeout = 10
        self._publish = False
        self._risk_management = {}
        self._task = None

        self._send_valuation = True
        self._send_positions = True
        self._send_transactions = False
        self._send_smart_orders = True
        
        self._err_count = 0
        self.queue = asyncio.Queue()
        
        self._disconnect_ts = 0
        self._streaming_timeout = STREAMING_TIMEOUT

        self._create(alert_manager, risk_management)
        
    def __str__(self):
        name = self._alert_manager.name
        return "monitor-"+name
    
    def __repr__(self):
        return self.__str__()

    def _create(self, alert_manager:BlueshiftAlertManager, risk_management:dict|None=None):
        algo = self._algo()

        if not alert_manager or not algo:
            msg = f'Cannot create risk monitor, no alert monitor found or not a running algo.'
            raise InitializationError(msg)
        
        if risk_management is None:
            risk_management = {}
            
        self._alert_manager = alert_manager
        self._publish = alert_manager.publish
        
        init_cap = algo.env.init_capital
            
        if BLUESHIFT_DISABLE_KILL_SWITCH:
            msg = f"risk management: kill switch is disabled. "
            msg += f'Algo kill switch will be off.'
            algo.log_warning(msg)
        elif not init_cap:
            msg = f"risk management: algo running with no initial capital "
            msg += f'specified. Algo kill switch will be off.'
            algo.log_warning(msg)
        elif 'kill' in risk_management:
            threshold = round(100*float(risk_management['kill']),1)
            msg = f"risk management: enabled kill switch at {threshold}%."
            algo.log_warning(msg)
        else:
            msg = f"risk management: kill switch threshold not defined. "
            msg += f'Algo kill switch will be off.'
            algo.log_warning(msg)
            risk_management = None
            
        if risk_management:
            self._risk_management = risk_management

        if self._publish:
            self._publisher = alert_manager.publisher
            
        self._async_enforce_risk_management = self._asyncify(
                self._enforce_risk_management)
        self._async_emit_valuation_packets = self._asyncify(
                self._emit_valuation_packets)
        self._async_emit_positions_packets = self._asyncify(
                self._emit_positions_packets)
        self._async_emit_txns_packets = self._asyncify(
                self._emit_txns_packets)
        self._async_update_valuation = self._asyncify(
                algo.context.blotter.update_valuation)
        self._async_run_recon_orders = self._asyncify(
                self._run_recon_orders)
        self._async_run_recon_data = self._asyncify(
                self._run_recon_data)
            
    def _start(self):
        if self._task and not self._task.done():
            # we are already running this task
            return
        
        loop = self._alert_manager.loop
        self._task = loop.create_task(self.watch(), name=str(self))
        
    def _stop(self):
        self._wants_to_quit = True
        if self._task and not self._task.done():
            self._task.cancel()
        
    def start(self):
        self._alert_manager.loop.call_soon_threadsafe(self._start)
            
    def stop(self):
        if self._alert_manager.loop.is_running():
            self._alert_manager.loop.call_soon_threadsafe(self._stop)
            
    def add_event(self, event):
        """ add AlgoCallBack events. """
        if event not in (AlgoCallBack.TRADE, AlgoCallBack.DATA):
            return
        
        if event not in self.queue._queue: # type: ignore
            self.queue.put_nowait(event)
            
    async def _get_event(self):
        try:
            return await asyncio.wait_for(self.queue.get(), self._sleep_interval)
        except asyncio.TimeoutError:
            return
        
    def _asyncify(self, func):
        algo = self._algo()
        if not algo:
            raise BlueshiftException('algo is destroyed already')

        return asyncify(
                loop=algo.loop, executor=self._alert_manager.executor)(func)

    async def watch(self):
        """
            Watch the resource: the trackers in the blotter. In case we
            are in ``BACKTEST`` mode, return. End the watch if the algo
            is stopped.
        """
        if self._alert_manager.mode == MODE.BACKTEST:
            self._alert_manager.logger.warn(
                    "real-time updates are not available for backtesting")
            return
        
        self._wants_to_quit = False
        while not self._wants_to_quit and not self._is_finalized:
            # ensure the alert manager is connected in server mode
            algo = self._algo()
            if not algo:
                self._alert_manager.logger.warn(
                    "the algo is already destroyed, quitting risk monitor.")
                break
            
            self._alert_manager.ensure_connected()
            
            try:
                # in case algo terminates naturally, we exit quitely
                if algo.is_STOPPED(): # type: ignore
                    await asyncio.sleep(0)
                    continue
                # else continue monitoring the risk metrics
                event = await self._get_event()
                ts = pd.Timestamp.now(
                        tz=algo.context.trading_calendar.tz)
                
                # this can potentially block for a long time
                # mostly in the position tracker updates. This cannot be
                # handled by asyncio, so pos tracker must be cooperative.
                await asyncio.wait_for(self.respond(ts, event=event), LOCK_TIMEOUT)
                self._err_count = 0
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                msg = f'Risk monitor channel shutting down...'
                algo.log_info(msg)
                self._wants_to_quit = True
                break
            except asyncio.TimeoutError:
                msg = f'Risk monitor watch timed out.'
                algo.log_error(msg)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                err_msg = get_exception(exc_type, exc_value, exc_traceback)
                algo.log_info(err_msg)
            except TerminationError:
                raise
            except BlueshiftException as e:
                msg = await self._alert_manager.handle_blueshift_async_exception(
                        algo, e)
                msg = f'Algo {algo.name} encountered an error in risk monitor, but recovered to continue.'
                algo.log_platform(msg)
            except Exception as e:
                # do not reset alert manager error tracker here
                msg = await self._alert_manager.handle_async_exception(
                        algo, e, source='monitor')
                msg = f'Algo {algo.name} encountered an exception in risk monitor, but recovered to continue.'
                algo.log_platform(msg)
                    
                
    async def respond(self, timestamp, incremental=False, event=None):
        """
            Respond to command. We publish three packets - one each for
            performance and position and transactions, and sleep a while
            in between to give other co-routines a chance.

            Note:
                No publishing during heartbeat (market closed hours) or
                at start-up. Also silent returns if the algo is stopped.

            Args:
                ``change (bool)``: If there is any change in portfolio.

                ``timestamp (Timestamp)``: Current timestamp.

                ``incremental (bool)``: Update only if there is a change.
        """
        if self._publish and not self._publisher.is_connected:
            # this socket bind is called in the algo main loop
            # this co-coutine got called before the bind happened
            # so we simply return. this is safe, if the algo main
            # loop has not yet started, nothing to worry on risks.
            return await asyncio.sleep(self._sleep_interval)
            
        algo = self._algo()

        if not algo or algo.is_STOPPED(): # type: ignore
            await asyncio.sleep(0)
            return
        
        # send a health heartbeat
        if self._publish and self._publisher.is_connected:
            packet = create_message_envelope(algo.name, MessageType.HEALTH_HEARTBEAT)
            self._publisher.send_to_topic(algo.name, packet)
            await asyncio.sleep(0)

        if algo.is_HEARTBEAT() or algo.is_STARTUP(): # type: ignore
            # no need to emit any package or monitor any risks
            # simply return after a while of sleeping
            return await asyncio.sleep(self._sleep_interval)
        
        if algo.context.is_disconnected:
            # wake up event waiters in case we are disconnected
            algo.wakeup_order_waiters()
            algo.wakeup_data_waiters()
            
        if event and algo.mode in REALTIME_MODES:
            change = await self._handle_event(timestamp, event)
        else:
            f = self._async_update_valuation
            change = False
            
            try:
                change = await asyncio.wait_for(
                        f(timestamp, self._valuation_timeout), 2*self._valuation_timeout)
            except asyncio.TimeoutError:
                algo.log_error(f'timed out while updating blotter for valuation.')
            except Exception as e:
                algo.log_error(f'error while updating blotter for valuation:{str(e)}')
            
        change = change or not incremental
        await asyncio.sleep(0)
        
        if algo.mode in (MODE.LIVE, MODE.PAPER):
            await self._async_enforce_risk_management()
            await self._async_emit_valuation_packets(change, timestamp)
            await self._async_emit_positions_packets(change, timestamp, True)
            await self._async_emit_txns_packets(change, timestamp)
            await self._enforce_disconnect_check()
        elif algo.mode == MODE.EXECUTION:
            self._emit_smart_order_packets(change, timestamp)
            await self._enforce_disconnect_check()
        
            
    def _run_recon_orders(self, algo):
        try:
            if algo.mode in LIVE_MODES:
                algo.reconcile_all(
                        timestamp=None, forced=True, exit_time=False, 
                        fetch_orders=False, only_txns=True, 
                        simulate=False)
        except Exception:
            algo.log_error(f'failed to run recon on order streaming updates.')
            
    def _run_recon_data(self, algo):
        try:
            if algo.mode in SIMULATION_MODES:
                algo.reconcile_all(
                        timestamp=None, forced=True, exit_time=False, 
                        fetch_orders=False, only_txns=True, 
                        simulate=True)
        except Exception:
            algo.log_error(f'failed to run recon on data streaming updates.')
        
    async def _handle_event(self, timestamp, event):
        algo = self._algo()
        if not algo:
            return
        
        change = True
        
        if event == AlgoCallBack.TRADE:
            f = self._async_run_recon_orders
            try:
                try:
                    change = await asyncio.wait_for(f(algo), LOCK_TIMEOUT)
                except asyncio.TimeoutError:
                    algo.log_error(f'timed out while running recon on order streaming update.')
            finally:
                # wake up the waiters
                algo.wakeup_order_waiters()
        elif event == AlgoCallBack.DATA:
            try:
                f = self._async_run_recon_data
                try:
                    change = await asyncio.wait_for(f(algo), LOCK_TIMEOUT)
                except asyncio.TimeoutError:
                    algo.log_error(f'timed out while running recon on data streaming update.')
            finally:
                # wake up the waiters
                algo.wakeup_data_waiters()
            
        return change
    
    async def _enforce_disconnect_check(self):
        if not self._streaming_timeout:
            return
        
        algo = self._algo()
        if not algo:
            return
        
        if not algo.context.is_disconnected:
            self._disconnect_ts = 0
            return
        
        if not self._disconnect_ts:
            self._disconnect_ts = time.time()
        elif time.time() - self._disconnect_ts > self._streaming_timeout:
            if algo.is_stopped:
                return
            
            try:
                msg = f"streaming disconnected for more than {self._streaming_timeout}s, will exit algo."
                algo.stop(msg=msg)
                return
            except Exception as e:
                if isinstance(e, TerminationError):
                    raise
                algo.log_error(
                        f'failed to kill algo {algo.name} on streaming disconnect: {str(e)}')
            
        
    def _enforce_risk_management(self):
        if not self._risk_management or BLUESHIFT_DISABLE_KILL_SWITCH:
            return
        
        algo = self._algo()
        if not algo:
            return
        
        kill_ratio = self._risk_management.get("kill", None)
        if kill_ratio is None:
            return
        
        try:
            loss_ratio = algo.context.blotter.get_loss_ratios()
        except (KeyError, AttributeError, TypeError, ZeroDivisionError) as e:
            msg = str(e) + ": failed to compute algo loss ratio, "
            msg = msg + "cannot enforce risk management."
            algo.log_error(msg)
            return
            
        if loss_ratio < kill_ratio:
            try:
                msg = "Loss threshold breached, will exit - "
                msg = msg + f'loss:{loss_ratio}, threshold:{kill_ratio}'
                algo.log_error(msg)
                acct = algo.context.blotter.get_account()
                info_msg = f'Algo kill details: valuation {acct.to_dict()}'
                algo.log_info(info_msg)
                positions = algo.context.blotter.get_open_positions()
                positions = {k.symbol:v.to_json() for k,v in positions.items()}
                info_msg = f'Algo kill details: positions {positions}'
                algo.log_info(info_msg)
                algo.stop(msg=msg)
                return
            except Exception as e:
                if isinstance(e, TerminationError):
                    raise
                algo.log_error(
                        f'failed to kill algo {algo.name} on risk threshold breach: {str(e)}')
        
    def _emit_valuation_packets(self, change, timestamp):
        algo = self._algo()
        if not algo:
            return
        
        # error in algo can make the socket closed.
        if self._publish and change and self._publisher.is_connected \
                            and self._send_valuation:
            algo.context.blotter.emit_valuation_packets(timestamp)
        
    def _emit_positions_packets(self, change, timestamp, send=False):
        algo = self._algo()
        if not algo:
            return
        
        # error in algo can make the socket closed.
        if self._publish and change and self._publisher.is_connected \
                            and self._send_positions:
            algo.context.blotter.emit_positions_packets(timestamp, send)
        
    def _emit_txns_packets(self, change, timestamp):
        algo = self._algo()
        if not algo:
            return
        
        # error in algo can make the socket closed.
        if self._publish and change and self._publisher.is_connected \
                            and self._send_transactions:
            algo.context.blotter.emit_txns_packets(timestamp, change)
            
    def _emit_smart_order_packets(self, change, timestamp):
        algo = self._algo()
        if not algo:
            return
        
        # error in algo can make the socket closed.
        if self._publish and change and self._publisher.is_connected \
                            and self._send_smart_orders:
            algo.context.blotter.emit_perf_msg(timestamp)
            
    def finalize(self):
        algo = self._algo()
        if not algo:
            return
        
        timestamp = pd.Timestamp.now(tz=algo.context.trading_calendar.tz)
            
        try:
            if algo.mode in (MODE.LIVE, MODE.PAPER):
                self._emit_valuation_packets(True, timestamp)
                self._emit_positions_packets(True, timestamp, send=True)
                self._emit_txns_packets(True, timestamp)
                
                msg = f'Successfully updated algo positions and valuation '
                msg += f'on exit for algo {algo.name}.'
                algo.log_info(msg)
        except Exception as e:
            msg = f'Failed to update algo valuations in risk monitor'
            msg += f'for algo {algo.name}:{str(e)}.'
            try:
                algo.log_error(msg)
            except Exception:
                self._alert_manager.logger.error(msg)
        finally:
            self._is_finalized = True
                

