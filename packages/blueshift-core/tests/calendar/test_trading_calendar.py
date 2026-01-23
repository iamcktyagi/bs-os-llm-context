import pytest
import pandas as pd
from blueshift.calendar import get_calendar, TradingCalendar

def test_get_calendar_nyse():
    cal = get_calendar('NYSE')
    assert isinstance(cal, TradingCalendar)
    assert cal.name == 'NYSE'
    assert cal.tz == 'US/Eastern'

def test_is_session():
    cal = get_calendar('NYSE')
    # Monday
    monday = pd.Timestamp('2023-10-23', tz='US/Eastern')
    assert cal.is_session(monday) == True
    
    # Sunday
    sunday = pd.Timestamp('2023-10-22', tz='US/Eastern')
    assert cal.is_session(sunday) == False

def test_open_close_times():
    cal = get_calendar('NYSE')
    open_time = cal.open_time
    close_time = cal.close_time
    
    assert open_time.hour == 9
    assert open_time.minute == 30
    assert close_time.hour == 16
    assert close_time.minute == 0
