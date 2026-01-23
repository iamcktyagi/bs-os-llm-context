from __future__ import annotations
from typing import TYPE_CHECKING
import os
import json
import copy

from blueshift.config.config import BLUESHIFT_DIR as BLUESHIFT_HOME
from blueshift.lib.exceptions import BlueshiftCalendarException
from blueshift.lib.common.enums import ExpiryType
from .chunker import ExpiryDispatch
from .calendar_dispatch import get_calendar

if TYPE_CHECKING:
    from .chunker import ExpiryChunker
    from .trading_calendar import TradingCalendar

def _fetch_dispatch(path):
    if not os.path.exists(path):
        return {}
    
    try:
        with open(path) as fp:
            return json.load(fp)
    except Exception:
        raise BlueshiftCalendarException(f'failed to read expiry dispatch from path {path}')
    
class ExchangeExpiry:
    def __init__(self, futures_dispatch:ExpiryDispatch|None, options_dispatch:ExpiryDispatch|None):
        self._fut_dispatch = futures_dispatch
        self._opt_dispatch = options_dispatch

    @property
    def futures_dispatch(self):
        return self._fut_dispatch
    
    @property
    def opt_dispatch(self):
        return self._opt_dispatch
    
    @property
    def options_dispatch(self):
        return self._opt_dispatch

    def get_futures_chunker(self, underlying:str, expiry_type:ExpiryType|str) -> ExpiryChunker|None:
        if isinstance(expiry_type, ExpiryType):
            expiry_type = expiry_type.value.lower()

        if self._fut_dispatch:
            return self._fut_dispatch.get(underlying, expiry_type)
        
    def get_options_chunker(self, underlying:str, expiry_type:ExpiryType|str) -> ExpiryChunker|None:
        if isinstance(expiry_type, ExpiryType):
            expiry_type = expiry_type.value.lower()

        if self._opt_dispatch:
            return self._opt_dispatch.get(underlying, expiry_type)

def _make_nfo():
    path = os.path.join(os.path.expanduser(BLUESHIFT_HOME),'nse_expiries.json')
    dispatch = _fetch_dispatch(path)
    
    fut_dispatch = opt_dispatch = None
    try:
        if 'futures' in dispatch:
            fut_dispatch = ExpiryDispatch.from_dict(dispatch['futures'])
            opt_dispatch = ExpiryDispatch.from_dict(dispatch['options'])
    except Exception:
        specials = None
        
    NFO = ExchangeExpiry(fut_dispatch, opt_dispatch)
    
    return NFO

def _make_bfo():
    path = os.path.join(os.path.expanduser(BLUESHIFT_HOME),'bse_expiries.json')
    dispatch = _fetch_dispatch(path)
    
    fut_dispatch = opt_dispatch = None
    try:
        if 'futures' in dispatch:
            fut_dispatch = ExpiryDispatch.from_dict(dispatch['futures'])
            opt_dispatch = ExpiryDispatch.from_dict(dispatch['options'])
    except Exception:
        specials = None
        
    BFO = ExchangeExpiry(fut_dispatch, opt_dispatch)
    
    return BFO

_BUILT_IN_DISPATCH = {
    'NFO':_make_nfo,
    'BFO':_make_bfo,
}

_expiry_registry:dict[str,ExchangeExpiry] = {}

def register_expiry_dispatch(name, expiry:ExchangeExpiry):
    _expiry_registry[name] = expiry

def get_expiry_dispatch(name):
    if name in _expiry_registry:
        return _expiry_registry[name]
    
    raise NotImplementedError(f'expiry dispatch for {name} not implemented.')

def list_expiry_dispatch() -> list[str]:
    return list(_expiry_registry)

for name in _BUILT_IN_DISPATCH:
    dispatch = _BUILT_IN_DISPATCH[name]()
    register_expiry_dispatch(name, dispatch)