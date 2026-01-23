from __future__ import annotations
from typing import Callable
from datetime import date

from .date_utils import (
        month_first_day, nth_weekday, last_weekday, bump_sundays, bump_weekends)

class Holiday():
    """ A class for computing annual holiday on the go. """
    
    def __init__(self, name:str, rule_func:Callable):
        """ 
            The rule function must accept a year (integer) and return 
            a date object.
        """
        self._name = name
        self._rule_func = rule_func
        
    def rule_func(self, year:int) -> date:
        return self._rule_func(year)
        
    @property
    def name(self) -> str:
        return self._name
        
    def __call__(self, year) -> date:
        return self.rule_func(year)
    
def holidays_range(start_year:int, end_year:int, holidays:list[Holiday]=[], 
                   inclusive:bool=True) -> list[date]:
    """ 
        Return a list of holidays between start and end years, given a list 
        of annual holiday rules.
    """
    start = start_year
    end = end_year + 1 if inclusive else end_year
    def f():
        for year in range(start, end):
            yield [holiday(year) for holiday in holidays]
            
    return [dt for dts in f() for dt in dts]

#######################################################################
#  COMMON US HOLIDAYS AND HOLIDAY LISTS.
#######################################################################

def ny_day(year):
    return bump_sundays(month_first_day(year))
NYD = Holiday("New Year's Day", ny_day)
    
def mlk_day(year):
    return nth_weekday(year, month=1, weekday=0, n=3).date()
MLK = Holiday("Martin Luther King, Jr. Day", mlk_day)

def gw_birthday(year):
    return nth_weekday(year, month=2, weekday=0, n=3).date()
GWB = Holiday("Washington's Birthday", gw_birthday)

def g_friday(year):
    from pandas.tseries.holiday import GoodFriday
    return GoodFriday.dates(
            date(year,1,1),date(year,12,31))[0].date()
GOODFRIDAY = Holiday("Good Friday	", g_friday)

def mem_day(year):
    return last_weekday(year, month=5, weekday=0).date()
MEMORIAL = Holiday("Memorial Day", mem_day)

def juneteenth(year):
    return bump_weekends(date(year, month=6, day=19))
JUNETEENTH = Holiday("Juneteenth", juneteenth)

def i_day(year):
    return bump_weekends(date(year=year, month=7, day=4))
IDAY = Holiday("Independence Day", i_day)

def labor_day(year):
    return nth_weekday(year, month=9, weekday=0, n=1).date()
LDAY = Holiday("Labor Day", labor_day)

def columbus_day(year):
    return nth_weekday(year, month=10, weekday=0, n=2).date()
CDAY = Holiday("Columbus Day", columbus_day)

def veterans_day(year):
    return date(year=year, month=11, day=11)
VDAY = Holiday("Veterans Day", veterans_day)

def thanksgiving_day(year):
    return nth_weekday(year, month=11, weekday=3, n=4).date()
TDAY = Holiday("Thanksgiving Day", thanksgiving_day)

def xmas(year):
    return bump_weekends(date(year=year, month=12, day=25))
XMAS = Holiday("Christmas Day", xmas)

NYSE_HOLIDAYS = [NYD, MLK, GWB, GOODFRIDAY, MEMORIAL, JUNETEENTH, IDAY, LDAY, TDAY, XMAS]
FX_HOLIDAYS = [NYD, XMAS]
COMMON_US_HOLIDAYS = [NYD, MLK, GWB, GOODFRIDAY, MEMORIAL, JUNETEENTH, IDAY, LDAY, CDAY, VDAY, TDAY, XMAS]
