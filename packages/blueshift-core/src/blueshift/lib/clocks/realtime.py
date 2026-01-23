from __future__ import annotations
from typing import TYPE_CHECKING
import asyncio
from enum import Enum
from collections import namedtuple

from blueshift.calendar.trading_calendar import (
        TradingCalendar,END_DATE)

from ..common.ctx_mgrs import TimeoutRLock
from ..common.enums import AlgoCallBack, AlgoTerminateEvent, AlgoTimeoutEvent
from ._clock import TradingClock, BARS
from ..exceptions import ClockError
from ..common.constants import NANO_SECOND as NANO

if TYPE_CHECKING:
    import pandas as pd
else:
    import blueshift.lib.common.lazy_pandas as pd

class ClockState(Enum):
    '''
        State of the realtime clock
    '''
    START_ALGO = 0
    BEFORE_SESSION = 1
    IN_SESSION = 2
    AFTER_SESSION = 3
    IN_RECESS = 4
    ALGO_END = 5

STATE_BARS_DICT = {ClockState.START_ALGO: BARS.ALGO_START,
                   ClockState.BEFORE_SESSION: BARS.BEFORE_TRADING_START,
                   ClockState.IN_SESSION: BARS.TRADING_BAR,
                   ClockState.AFTER_SESSION: BARS.AFTER_TRADING_HOURS,
                   ClockState.IN_RECESS: BARS.HEART_BEAT,
                   ClockState.ALGO_END: BARS.ALGO_END}

_idempotent_events = []
for kind in (BARS, AlgoTerminateEvent, AlgoCallBack):
    _idempotent_events.extend(list(kind.__members__.values()))
    
_idempotent_events = set(_idempotent_events)
_idempotent_events.remove(AlgoCallBack.TRADE)
_idempotent_events.remove(AlgoCallBack.ONECLICK)
_idempotent_events.remove(AlgoCallBack.EXTERNAL)
_idempotent_events.remove(AlgoCallBack.RECONCILIATION)

class ClockItem(namedtuple('ClockItem', ['data', 'event'])):
    def __eq__(self, other):
        if self.event != other.event:
            return False
        
        if self.event in _idempotent_events and self.event == other.event:
            return True
        
        if str(self.data) == str(other.data):
            return True
        
        # special handling for too many recursive schedule later or schedule soon
        if self.event == AlgoTimeoutEvent.TIMEOUT and self.event==other.event and len(self.data)>3:
            # if the callable, the context and the delay all same, then events are same
            if other.data[0] == self.data[0] and other.data[1] == self.data[1]:
                if other.data[2] is None and self.data[2] is None:
                    return True
                elif other.data[2] is not None and self.data[2] is not None \
                    and round(other.data[2], 6) == round(self.data[2], 6):
                    return True
        
        return False

class ClockQueue:
    '''
        Extend the asyncio Queue to add a method to pop the last
        item and clear the queue properly
    '''
    _skippable_bars = set([BARS.HEART_BEAT, BARS.TRADING_BAR])
    _discardable_bars = set([BARS.HEART_BEAT,AlgoCallBack.DATA, AlgoCallBack.IDLE])
    _priority_bars = set([AlgoTerminateEvent.CANCEL,
                          AlgoTerminateEvent.STOP,
                          AlgoTerminateEvent.KILL,
                          AlgoTerminateEvent.REJECT,])
    _max_len = 200
    _optional_threshold = 100
    _idle_len = 5
    
    def __init__(self, maxsize=0, *args, **kwargs):
        if maxsize <= 0:
            maxsize = self._max_len
        else:
            self._max_len = maxsize
            
        self.logger = kwargs.pop('logger', None)
        self._lock = TimeoutRLock(timeout=30) # clock tick is 60s
        self._priority_queue = asyncio.Queue(self._max_len, *args, **kwargs)
        self._event_queue = asyncio.Queue(self._max_len, *args, **kwargs)
        self._optional_queue = asyncio.Queue(self._optional_threshold, *args, **kwargs)
        self._idle_queue = asyncio.Queue(self._idle_len, *args, **kwargs)
        
        self._ok_to_fail = self._skippable_bars.union(
                self._discardable_bars)
        
        self._waiters = set([])
        
    def qsize(self):
        return self._event_queue.qsize()
    
        
    def put_if_idle(self, item, algo=None):
        """
            Put an item if idle queue is empty, discard if full.
        """
        if not isinstance(item, ClockItem):
            item = ClockItem(*item)
            
        if item in self._idle_queue._queue: # type: ignore
            return
        
        try:
            with self._lock:
                self._idle_queue.put_nowait(item)
            # copy waiters set to avoid change of size in iteration
            for fut in self._waiters.copy():
                if not fut.done():
                    fut.set_result(None)
        except TimeoutError:
            pass
        except asyncio.QueueFull:
            pass
        except Exception as e:
            e.algo = algo # type: ignore
            raise e
        
    def put_if_possible(self, item, algo=None):
        """
            Discard item on backpressure built-up instead of blocking.
        """
        if not isinstance(item, ClockItem):
            item = ClockItem(*item)
            
        if item.event == AlgoCallBack.IDLE:
            return self.put_if_idle(item)
            
        if item in self._optional_queue._queue: # type: ignore
            return
        
        try:
            with self._lock:
                self._optional_queue.put_nowait(item)
            for fut in self._waiters.copy():
                if not fut.done():
                    fut.set_result(None)
        except TimeoutError:
            pass
        except asyncio.QueueFull:
            pass
        except Exception as e:
            e.algo = algo # type: ignore
            raise e
        
    def put_no_repeat(self, item, algo=None):
        """ put an item only if it is already not there. """
        if not isinstance(item, ClockItem):
            item = ClockItem(*item)
            
        queue = self._priority_queue if item.event in self._priority_bars \
            else self._event_queue
            
        if item in queue._queue: # type: ignore
            return
        
        try:
            with self._lock:
                queue.put_nowait(item)
            for fut in self._waiters.copy():
                if not fut.done():
                    fut.set_result(None)
        except TimeoutError:
            if item.event in self._ok_to_fail:
                return
            if self.logger:
                self.logger.error(f'Error in clock putting {item}, timed out.')
            msg = f'Timed out while waiting adding clock event, '
            msg += f'Suspect too many event handlers, or '
            msg += f'long running event handler(s).'
            e = ClockError(msg)
            e.algo = algo # type: ignore
            raise e
        except asyncio.QueueFull:
            if item.event in self._ok_to_fail:
                return
            if self.logger:
                self.logger.error(f'Error in clock putting {item}, too many events {queue}')
            msg = f'Too many events to handle in the event loop, will '
            msg += f'terminate. Suspect too many event handlers, or '
            msg += f'long running event handler(s).'
            e = ClockError(msg)
            e.algo = algo # type: ignore
            raise e
            
    def put_clock_event(self, item):
        self.put_no_repeat(item)
        
    async def wait_for_event(self, fut):
        if self._priority_queue.qsize() > 0 or \
            self._event_queue.qsize() > 0 or \
            self._optional_queue.qsize() > 0 or \
            self._idle_queue.qsize():
            return
        
        await fut
    
    async def get_last(self):
        # this must always be run in the main thread
        # scan the underlying dqueue for algo termination event
        # https://github.com/python/cpython/blob/main/Lib/asyncio/queues.py
        try:
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            self._waiters.add(fut)
            await asyncio.wait_for(self.wait_for_event(fut), timeout=5)
        except asyncio.CancelledError:
            return (None, None)
        except asyncio.TimeoutError:
            return (None, None)
        finally:
            try:
                if not fut.done(): # type: ignore
                    fut.cancel() # type: ignore
            except Exception:
                pass
            finally:
                if fut in self._waiters: # type: ignore
                    self._waiters.remove(fut) # type: ignore
        
        try:
            if self._priority_queue.qsize()>0:
                return self._priority_queue.get_nowait()
            
            if self._event_queue.qsize()>0:
                return self._event_queue.get_nowait()
            
            if self._optional_queue.qsize()>0:
                return self._optional_queue.get_nowait()
            
            if self._idle_queue.qsize()>0:
                return self._idle_queue.get_nowait()
            
            return (None, None)
        except asyncio.QueueEmpty:
            return (None, None)

class RealtimeClock(TradingClock):
    '''
        Realtime clock to generate clock events to control the algo
        process. It is modelled as a state machine with states in
        ClockState enum and transition defined in a class method.
    '''

    def __init__(self, trading_calendar: TradingCalendar,
                 emit_frequency: int, queue: ClockQueue| None = None,
                 start_dt = None, end_dt = None, synchronized=False,
                 logger=None):
        super(RealtimeClock, self).__init__(trading_calendar,
                                            emit_frequency)

        self.start_dt = start_dt
        self.end_dt = end_dt
        self.clock_state = None
        self.last_emitted = None
        self._set_start_end()
        self.reset(queue)
        self.synchronized = synchronized
        self.logger = logger

    def __str__(self):
        return f"Blueshift Realtime Clock [tick:{self.emit_frequency}, \
                    tz:{self.trading_calendar.tz}]"

    def __repr__(self):
        return self.__str__()
    
    def _set_start_end(self, start_dt=None, end_dt=None):
        if not start_dt:
            if not self.start_dt:
                start_dt = pd.Timestamp.now(
                        tz=self.trading_calendar.tz).normalize()
            else:
                start_dt = self.start_dt

        if not end_dt:
            if not self.end_dt:
                end_dt = END_DATE
            else:
                end_dt = self.end_dt
        
        if hasattr(start_dt,'tz') and start_dt.tz:
            self.start_dt = pd.Timestamp(
                    start_dt).tz_convert(self.trading_calendar.tz)
        else:
            self.start_dt = pd.Timestamp(
                    start_dt, tz=self.trading_calendar.tz)
            
        if hasattr(end_dt,'tz') and end_dt.tz:
            self.end_dt = pd.Timestamp(
                    end_dt).tz_convert(self.trading_calendar.tz)
        else:
            self.end_dt = pd.Timestamp(
                    end_dt, tz=self.trading_calendar.tz)
        
        self.end_dt = pd.Timestamp(
                self.end_dt.value+self.after_trading_hours_nano,
                tz=self.trading_calendar.tz)
        
        self.start_nano = self.start_dt.value
        self.end_nano = self.end_dt.value
        
    @property
    def size(self):
        try:
            return self.queue.qsize() # type: ignore
        except Exception:
            return 0

    def reset(self, queue: ClockQueue|None = None, start_dt=None, 
              end_dt=None):
        '''
            Reset the clock, updating emit freq, queue etc.
        '''
        # TODO: make sure the queue is not unbound
        if queue and not isinstance(queue, ClockQueue):
            msg = "live clock queue object must be of type ClockQueue."
            raise ClockError(msg)
        
        self.queue = queue
        self.clock_state = ClockState.START_ALGO
        self.last_emitted = None
        self._offset = self.emit_frequency*60*NANO

    async def synchronize(self):
        '''
            At the start-up we must wait (blocking wait) to sync
            with the nearest (next) match of emission rate. For
            example, with 5 mins frequency and getting created at
            10:28:00 AM, we wait for 2 minutes before emitting the
            first event.
        '''
        if not self.synchronized:
            return
        
        t = pd.Timestamp.now()
        minutes = t.minute + t.second/60
        miniutes_to_sync = self.emit_frequency-(minutes % self.emit_frequency)
        await asyncio.sleep(miniutes_to_sync*60)

    async def tick(self):
        '''
            The tick generator. It runs in the same thread as the
            algo and put the bars in a queue in async manner. This
            enables us to ensure a deterministic sleep time
            irrespective of how long the algo takes to complete a
            single processing loop. If clock runs faster than the
            algo can process, the clock events will get piled up
            in the async queue. It is the responsibility of the
            algo to handle that.
        '''
        if self.queue is None:
            raise ClockError("missing queue object for live clock")
            
        if not isinstance(self.queue, ClockQueue):
            msg = "live clock queue object must be of type ClockQueue."
            raise ClockError(msg)
        
        await self.synchronize()

        try:
            while True:
                # emit the tuple
                t1 = pd.Timestamp.now(tz=self.trading_calendar.tz)
                await self.sleep_till_start(t1)

                bar = self.emit(self.clock_state)
                t = pd.Timestamp(int(t1.value/NANO)*NANO,
                                 tz=self.trading_calendar.tz)
                
                self.queue.put_clock_event((t, bar))

                # just in case we are now at END state, we can quit
                if self.clock_state == ClockState.ALGO_END:
                    break

                # update the current state
                elapsed = t1.value - t1.normalize().value
                self.update_state(elapsed, t1)
                
                # in case we are in market hours, and just emitted 
                # before trading or initialize, we do not want to 
                # wait for a whole minute to start trading
                if self._if_market_hours(elapsed) and \
                    self.last_emitted in (
                            ClockState.BEFORE_SESSION,
                            ClockState.START_ALGO):
                    timeleft = 0
                else:
                    # prepare to sleep
                    t2 = pd.Timestamp.now(tz=self.trading_calendar.tz)
                    timeleft = max(0,self.emit_frequency*60 - \
                                   (t2 - t1).total_seconds())

                await asyncio.sleep(timeleft)
        except asyncio.CancelledError:
            if self.logger:
                self.logger.info('Clock routine is cancelled.')
        except Exception as e:
            import sys
            from blueshift.lib.common.platform import get_exception
            exc_type, exc_value, exc_traceback = sys.exc_info()
            err_msg = get_exception(exc_type, exc_value, exc_traceback)
            
            if self.logger:
                self.logger.info(err_msg)
                self.logger.error(f'Error in clock routine: {str(e)}.')
            raise ClockError(msg='unexpected error {}'.format(str(e)))
            
        if self.logger:
            self.logger.info('Clock routine is exited.')

    def update_state(self, elapsed_time, timestamp):
        '''
            The state transition logic. This determines the BAR
            that is sent to the algorithm.
        '''
        close_follows_open = self.close_nano > self.open_nano

        # time to quite now
        if self.time_to_quit(timestamp):
            self.clock_state = ClockState.ALGO_END
            return

        # it is a holiday!
        if not self.trading_calendar.is_session(pd.Timestamp.now(
                tz=self.trading_calendar.tz).normalize()):
            self.clock_state = ClockState.IN_RECESS
            return

        market_hours = self._if_market_hours(elapsed_time)
        offset = self._offset

        if not market_hours:
            if close_follows_open:
                if elapsed_time < self.before_trading_start_nano-offset:
                    self.clock_state = ClockState.IN_RECESS
                    return
                elif elapsed_time < self.open_nano-offset:
                    if self.last_emitted != ClockState.BEFORE_SESSION:
                        self.clock_state = ClockState.BEFORE_SESSION
                        return
                    else:
                        self.clock_state = ClockState.IN_RECESS
                        return
                elif elapsed_time < self.after_trading_hours_nano-offset:
                    self.clock_state = ClockState.IN_RECESS
                else:
                    if self.last_emitted != ClockState.AFTER_SESSION:
                        self.clock_state = ClockState.AFTER_SESSION
                        return
                    else:
                        self.clock_state = ClockState.IN_RECESS
                        return
            else:
                if elapsed_time < self.after_trading_hours_nano-offset:
                    self.clock_state = ClockState.IN_RECESS
                    return
                elif elapsed_time < self.before_trading_start_nano-offset:
                    if self.last_emitted != ClockState.AFTER_SESSION:
                        self.clock_state = ClockState.AFTER_SESSION
                        return
                    else:
                        self.clock_state = ClockState.IN_RECESS
                        return
                else:
                    if self.last_emitted != ClockState.BEFORE_SESSION:
                        self.clock_state = ClockState.BEFORE_SESSION
                        return
                    else:
                        self.clock_state = ClockState.IN_RECESS
                        return

        if market_hours:
            if self.clock_state == ClockState.START_ALGO:
                self.clock_state = ClockState.BEFORE_SESSION
                return
            else:
                self.clock_state = ClockState.IN_SESSION
                return

    def emit(self, tick):
        '''
            Emit a clock signal. Also keep track of the last non
            hearbeat signal send to coordinate start and end of
            sessions signals.
        '''
        if tick != ClockState.IN_RECESS:
            self.last_emitted = tick
        return STATE_BARS_DICT[tick]


    async def sleep_till_start(self, ts):
        """ check if it is ok to start ticking, given the start
        date. """
        if self.start_dt:
            dt = self.start_dt - ts
            if dt.value > 0:
                await asyncio.sleep(dt.seconds)

    def time_to_quit(self, ts):
        """ check if we have to quit - past the end date. """
        if self.is_terminated:
            return True
        
        if self.end_dt:
            dt = ts - self.end_dt
            if dt.value > 0:
                return True

        return False

    def _if_market_hours(self, elapsed_time):
        if self.close_nano < self.open_nano:
            if elapsed_time > self.close_nano - 2*self._offset \
                    and elapsed_time < self.open_nano + self._offset:
                return False
            else:
                return True
        else:
            if elapsed_time > self.open_nano - self._offset \
                    and elapsed_time < self.close_nano - self._offset:
                return True
            else:
                return False

