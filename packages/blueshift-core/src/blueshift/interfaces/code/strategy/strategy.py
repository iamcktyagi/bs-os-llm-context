from __future__ import annotations

import json
import uuid

from blueshift.lib.exceptions import NotValidStrategy
from blueshift.lib.common.types import AttrDict

from ...plugin_manager import load_plugins
from .schema import StrategyValidator, StrategyType, _VERSION

class BlueshiftStrategy:
    """
        Blueshift strategy abstract interface.
    """
    def __init__(self, data:str|dict, **kwargs):
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception:
                name = kwargs.get('name',f'strategy_{uuid.uuid4().hex[:5]}')
                type_ = kwargs.get('type','python')
                data = {'version':_VERSION, 
                        'name':name,
                        'capital': {'type':'free'},
                        'type':type_,
                        'code':data,
                        }
                
        if not isinstance(data, dict):
            msg = f'invalid strategy - expected a dictionary, got {type(data)}'
            raise NotValidStrategy(msg)
            
        validator = StrategyValidator()
        is_valid, error = validator.validate_safe(data)
        
        if error:
            err_msg = ''
            errors = validator.get_validation_errors(data)
            for i, error in enumerate(errors, 0):
                err_msg = err_msg + str(error) + ' \n'
                
            err_msg = f'invalid strategy - see errors below:\n' + err_msg
            raise NotValidStrategy(err_msg)
        
        self._strategy = json.loads(
                json.dumps(data), object_hook=lambda d: AttrDict(d))
        
        try:
            self._strategy['type'] = StrategyType('python')
        except Exception:
            raise NotValidStrategy(f'illegal strategy type')
        
    def __str__(self) -> str:
        return f'{self._strategy.name}({self._strategy.version})'
    
    def __repr__(self) -> str:
        return self.__str__()
        
    def __getattribute__(self, attr):
        if attr in ('to_python','_strategy'):
            return object.__getattribute__(self, attr)
        try:
            return getattr(object.__getattribute__(self, '_strategy'),attr)
        except KeyError:
            raise AttributeError(attr)
        
    def to_python(self):
        """ Get the python strategy code. """
        return self._strategy.code
        
        
class PythonStrategy(BlueshiftStrategy):
    """
        Blueshift Python strategy.
    """
    def __init__(self, data, **kwargs):
        super().__init__(data, **kwargs)
        if self.type != StrategyType.PYTHON:
            msg = f'illegal strategy type'
            raise NotValidStrategy(msg)
        
class WizardStrategy(BlueshiftStrategy):
    """
        Blueshift Python strategy.
    """
    def __init__(self, data, **kwargs):
        super().__init__(data, **kwargs)
        if self.type != StrategyType.WIZARD:
            msg = f'illegal strategy type'
            raise NotValidStrategy(msg)
            
class ExecutionStrategy(BlueshiftStrategy):
    """
        Blueshift Python strategy.
    """
    def __init__(self, data, **kwargs):
        super().__init__(data, **kwargs)
        if self.type != StrategyType.EXECUTION:
            msg = f'illegal strategy type'
            raise NotValidStrategy(msg)
        
        
_strategy_register:dict[str, BlueshiftStrategy] = {}

def register_strategy(name:str, strategy:BlueshiftStrategy):
    """ register a strategy by name. """
    if not isinstance(strategy, BlueshiftStrategy):
        raise NotValidStrategy()
        
    _strategy_register[name] = strategy

def get_strategy(name:str) -> BlueshiftStrategy|None:
    """ get a strategy by name. """
    if name not in _strategy_register:
        try:
            load_plugins('blueshift.plugins.strategy')
        except Exception:
            pass

    return _strategy_register.get(name)

def list_strategies() -> list[str]:
    """ list all registered strategies."""
    try:
        load_plugins('blueshift.plugins.strategy')
    except Exception:
        pass
    return list(_strategy_register)

__all__ = [
    'BlueshiftStrategy',
    'PythonStrategy',
    'WizardStrategy',
    'ExecutionStrategy',
    'register_strategy',
    'get_strategy',
    'list_strategies',
    ]