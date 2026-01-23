from __future__ import annotations
from blueshift.lib.exceptions import NotValidCalendar
from .trading_calendar import TradingCalendar
from .calendars import _make_calendar, _make_calendars


class CalendarDispatch:
    '''
        The global calendar registry and dispatch. Instantiate a calendar
        on the fly. Registering is adding an entry and an object
        to a global dict.
    '''
    
    def __init__(self, calendars:dict[str, TradingCalendar]):
        self._create(calendars)
        
    def _create(self, calendars):
        self._calendars = calendars
        self._aliases:dict[str, str] = {}
        
    def resolve_alias(self, name:str) -> str:
        '''
            Resolve aliases
        '''
        if name in self._calendars:
            return name
        
        if name in self._aliases:
            return self._aliases[name]
        
        return name
        
    def get_calendar(self, name:str, no_cache:bool=False) -> TradingCalendar:
        '''
            Get an instance if already there, else raise error.
        '''
        name = self.resolve_alias(name)
        
        if no_cache:
            try:
                cal = _make_calendar(name)
                if cal:
                    self._calendars[name] = cal
                    return cal
                else:
                    raise NotValidCalendar(f"calendar {name} not registered.")
            except Exception:
                raise NotValidCalendar(f"calendar {name} not registered.")
        else:
            try:
                return self._calendars[name]
            except KeyError:
                # not registed, search known calendars
                cal = _make_calendar(name)
                if cal:
                    self._calendars[name] = cal
                    return cal
                
                raise NotValidCalendar(f"calendar {name} not registered.")
    
    def register_alias(self, name:str, alias:str) -> None:
        '''
            Register a calendar name alias.
        '''
        if alias in self._aliases:
            return
        
        if name not in self._calendars:
            raise NotValidCalendar(f"{name} not a registered calendar")
        
        self._aliases[alias] = name
        
    def deregister_alias(self, alias:str) -> None:
        '''
            Register a calendar name alias.
        '''        
        if alias in self._aliases:
            self._aliases.pop(alias, None)
        
    def register_calendar(self, name:str, *args, **kwargs) -> None:
        '''
            Register a calendar.
        '''
        if len(args)>0 and isinstance(args[0], TradingCalendar):
            self._calendars[name] = args[0]
        elif 'calendar' in kwargs and isinstance(kwargs['calendar'], TradingCalendar):
            self._calendars[name] = kwargs['calendar']
        elif 'trading_calendar' in kwargs and isinstance(kwargs['trading_calendar'], TradingCalendar):
            self._calendars[name] = kwargs['trading_calendar']
        else:
            self._calendars[name] = TradingCalendar(name, *args, **kwargs)
        
    def deregister_calendar(self, name:str) -> None:
        '''
            Remove a constructor from the dict.
        '''
        if name in self._calendars:
            self._calendars.pop(name, None)
            
    def list_calendars(self) -> list[str]:
        return list(self._calendars)
            
# the global calendar registry
global_cal_dispatch = CalendarDispatch({})

get_calendar = global_cal_dispatch.get_calendar
register_calendar = global_cal_dispatch.register_calendar
deregister_calendar = global_cal_dispatch.deregister_calendar
register_calendar_alias = global_cal_dispatch.register_alias
deregister_calendar_alias = global_cal_dispatch.deregister_alias
list_calendars = global_cal_dispatch.list_calendars

def register_builtin_calendars():
    cals = _make_calendars()
    
    for name in cals:
        register_calendar(name, cals[name])