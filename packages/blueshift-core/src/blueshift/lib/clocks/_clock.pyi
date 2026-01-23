import numpy as np
from typing import TYPE_CHECKING
import numpy.typing as npt
from enum import Enum

from blueshift.calendar.trading_calendar import TradingCalendar

if TYPE_CHECKING:
    import pandas as pd

class BARS(Enum):
    ALGO_START:int
    BEFORE_TRADING_START:int
    TRADING_BAR:int
    DAY_END:int
    AFTER_TRADING_HOURS:int
    ALGO_END:int
    HEART_BEAT:int

class TradingClock:
    trading_calendar:TradingCalendar
    emit_frequency:int
    is_terminated:bool
    open_nano:np.int64
    close_nano:np.int64
    before_trading_start_nano:np.int64
    after_trading_hours_nano:np.int64
    intraday_nanos:npt.NDArray[np.int64]
    def __init__(self, trading_calendar:TradingCalendar, emit_frequency:int):...
    def generate_intraday_nanos(self):...
    def terminate(self):...
    def reset(self, *args, **kwargs):...
    
class SimulationClock(TradingClock):
    start_nano:np.int64
    end_nano:np.int64
    session_nanos:npt.NDArray[np.int64]
    def __init__(self, trading_calendar:TradingCalendar, emit_frequency:int,
                 start_dt:pd.Timestamp, end_dt:pd.Timestamp):...
    def __iter__(self):...
    ...