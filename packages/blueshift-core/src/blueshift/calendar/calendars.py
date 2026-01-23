from __future__ import annotations
from typing import TYPE_CHECKING
import os
import json

from blueshift.config.config import BLUESHIFT_DIR as BLUESHIFT_HOME
from .trading_calendar import TradingCalendar
from .holidays import NYSE_HOLIDAYS, FX_HOLIDAYS

if TYPE_CHECKING:
    import pandas as pd
else:
    import blueshift.lib.common.lazy_pandas as pd

def _fetch_holidays(path):
    if not os.path.exists(path):
        return
    
    holidays = pd.read_csv(path)
    holidays = pd.to_datetime(holidays.iloc[:,0])
    
    today = pd.Timestamp.now()
    if not holidays.empty and holidays.iloc[-1].year >= today.year:
        return holidays


def _make_nyse():
    NYSE = TradingCalendar('NYSE', tz='US/Eastern', opens=(9,30,0), 
                           closes=(16,0,0), start='2005-01-01 00:00:00',
                           holidays=NYSE_HOLIDAYS)
    return NYSE

def _make_nse():
    path = os.path.join(os.path.expanduser(BLUESHIFT_HOME),'nse_holidays.csv')
    holidays = _fetch_holidays(path)
    
    try:
        specials = os.getenv('NSE_SPECIAL_SESSIONS')
        specials = json.loads(specials) # type: ignore
    except Exception:
        specials = None
        
    NSE = TradingCalendar('NSE', tz='Asia/Calcutta', opens=(9,15,0), 
                           closes=(15,30,0), start='2013-01-01 00:00:00', 
                           holidays=None, special_sessions=specials)
    NSE.add_holidays(holidays)
    
    return NSE

def _make_bse():
    path = os.path.join(os.path.expanduser(BLUESHIFT_HOME),'bse_holidays.csv')
    holidays = _fetch_holidays(path)
    
    try:
        specials = os.getenv('BSE_SPECIAL_SESSIONS')
        specials = json.loads(specials) # type: ignore
    except Exception:
        specials = None
        
    BSE = TradingCalendar('BSE', tz='Asia/Calcutta', opens=(9,15,0), 
                           closes=(15,30,0), start='2013-01-01 00:00:00', 
                           holidays=None, special_sessions=specials)
    BSE.add_holidays(holidays)
    
    return BSE

def _make_nse_uat():
    path = os.path.join(os.path.expanduser(BLUESHIFT_HOME),'nse_holidays.csv')
    holidays = _fetch_holidays(path)
        
    NSE = TradingCalendar('NSE_UAT', tz='Asia/Calcutta', opens=(9,15,0), 
                           closes=(23,30,0), start='2013-01-01 00:00:00', 
                           weekends=[], holidays=None)
    NSE.add_holidays(holidays)
    
    return NSE

def _make_nfo():
    path = os.path.join(os.path.expanduser(BLUESHIFT_HOME),'nse_holidays.csv')
    holidays = _fetch_holidays(path)
    
    try:
        specials = os.getenv('NSE_SPECIAL_SESSIONS')
        specials = json.loads(specials) # type: ignore
    except Exception:
        specials = None
        
    NFO = TradingCalendar('NFO', tz='Asia/Calcutta', opens=(9,15,0), 
                           closes=(15,30,0), start='2013-01-01 00:00:00', 
                           holidays=None, special_sessions=specials)
    NFO.add_holidays(holidays)
    
    return NFO

def _make_bfo():
    path = os.path.join(os.path.expanduser(BLUESHIFT_HOME),'bse_holidays.csv')
    holidays = _fetch_holidays(path)
    
    try:
        specials = os.getenv('BSE_SPECIAL_SESSIONS')
        specials = json.loads(specials) # type: ignore
    except Exception:
        specials = None
        
    BFO = TradingCalendar('BFO', tz='Asia/Calcutta', opens=(9,15,0), 
                           closes=(15,30,0), start='2013-01-01 00:00:00', 
                           holidays=None, special_sessions=specials)
    BFO.add_holidays(holidays)
    
    return BFO

def _make_fx():
    FX = TradingCalendar('FX', tz='US/Eastern', opens=(18,0,0), 
                           closes=(17,0,0), start='2000-01-01 00:00:00', 
                           holidays=FX_HOLIDAYS)
    return FX

def _make_crypto():
    CRYPTO = TradingCalendar('crypto', tz='Etc/UTC', opens=(0,30,0), 
                           closes=(23,30,0), start='2010-01-01 00:00:00', 
                           weekends=[], holidays=None)
    return CRYPTO

def _make_default():
    DEFAULT = TradingCalendar('twenty-four-seven', tz='Etc/UTC', opens=(0,0,0), 
                           closes=(23,59,0), start='2010-01-01 00:00:00', 
                           weekends=[], holidays=None)
    return DEFAULT

_BUILT_IN_CALS = {
        'NYSE':_make_nyse,
        'NSE':_make_nse,
        'BSE':_make_bse,
        'NSE_UAT':_make_nse_uat,
        'NFO':_make_nfo,
        'BFO':_make_bfo,
        'FX':_make_fx,
        'FOREX':_make_fx,
        'CRYPTO':_make_crypto,
        'DEFAULT':_make_default,
     }

_BUILT_IN_CALS = {**_BUILT_IN_CALS, **{k.lower():v for k,v in _BUILT_IN_CALS.items()}}

def _make_calendar(name):
    if name in _BUILT_IN_CALS:
        return _BUILT_IN_CALS[name]()
    
def _make_calendars():
    return {k:v() for k,v in _BUILT_IN_CALS.items()}
        
    