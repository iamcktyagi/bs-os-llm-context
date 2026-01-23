import math
import asyncio

""" a fake function that takes any arguments and does nothing. """
def noops(*args, **kwargs) -> None:
    # pylint: disable=unused-argument
    pass

""" alias of the fake function. """
def noop(*args, **kwargs) -> None:
    # pylint: disable=unused-argument
    pass

""" the asyncio version of the fake function. """
async def async_noop(*args, **kwargs) -> None:
    # pylint: disable=unused-argument
    return await asyncio.sleep(0)

""" a fake function that takes any arguments and returns NaN. """
def nan_op(*args, **kwargs) -> float:
    # pylint: disable=unused-argument
    return math.nan

""" A function that always returns True. """
def truth_func(*args, **kwargs) -> bool:
    # pylint: disable=unused-argument
    return True

""" A function that always returns False. """
def false_func(*args, **kwargs) -> bool:
    # pylint: disable=unused-argument
    return False

""" a fake module for sentinel purpose """
def get_no_module():
    from types import ModuleType
    no_module = ModuleType("no_module")
    no_module.__file__ = ''
    no_module.__path__ = ['']
    return no_module

class Sentinel:
    """
        Singleton sentinel object to mimic `None` without matching with built-in None.
    """
    __instances = {}
    
    def __new__(cls, val) -> "Sentinel":
       if val in Sentinel.__instances:
           return Sentinel.__instances[val]
       return super(Sentinel, cls).__new__(cls)
    
    def __init__(self, val):
        super(Sentinel, self).__init__()
        self.__val = val
        self.__instances[val] = self
        
    def __str__(self):
        return str(self.__val)
    
    def __repr__(self):
        return self.__str__()
    
    def __call__(self, *args, **kwargs):
        if self.__val.upper() == 'NOPS':
            return None
        
        raise ValueError('not a function.')
        
NOPS = Sentinel("NOPS")
NOPSType = type(NOPS)
NULL = Sentinel("NULL")
NULLType = type(NULL)
NOTHING = Sentinel("NOTHING")
NOTHINGType = type(NOTHING)
NotSpecified = Sentinel('NotSpecified')
NotSpecifiedType = type(NotSpecified)
MissingSymbol = Sentinel("MissingSymbol")
MissingSymbolType = type(MissingSymbol)