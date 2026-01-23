from __future__ import annotations
from typing import Callable
import datetime
from collections import namedtuple

from blueshift.lib.exceptions import ValidationError

Order = namedtuple('Order',['symbol','quantity'])
Target = namedtuple('Target',['type','value'])

def get_time_from_algo_param(name:str, param:dict, before:datetime.time|None=None, 
                             after:datetime.time|None=None, on_or_before:datetime.time|None=None, 
                             on_or_after:datetime.time|None=None, 
                             validation:Callable|None=None) -> datetime.time:
    """ get datetime time from time parameter input. """
    try:
        h, m = int(param['hours']), int(param['minutes'])
        s = int(param.get('seconds', 0))
        dt = datetime.time(h,m,s)
    except Exception:
        raise ValidationError(f'Invalid {name}, input must be hh:mm format')
    
    try:
        if before and dt >= before:
            raise ValueError(f'{name} must be before {before}')
            
        if after and dt <= after:
            raise ValueError(f'{name} must be after {before}')
            
        if on_or_before and dt > on_or_before:
            raise ValueError(f'{name} must be on or before {on_or_before}')
            
        if on_or_after and dt < on_or_after:
            raise ValueError(f'{name} must be on or after {on_or_after}')
            
        if validation and callable(validation):
            validation(dt)
    except Exception as e:
        raise ValidationError(f'Invalid {name}: {str(e)}')
        
    return dt
    
def get_number_from_algo_param(name:str, param:float, is_integer:bool=False, lt:float|None=None, 
                               gt:float|None=None, lte:float|None=None, gte:float|None=None, 
                               validation:Callable|None=None) -> float|int:
    """ get number from number input parameter input. """
    try:
        if is_integer:
            num = int(param)
        else:
            num = float(param)
    except Exception:
        if is_integer:
            msg = f'Invalid {name}, input must be an integer'
        else:
            msg = f'Invalid {name}, input must be an number'
        raise ValidationError(msg)
    
    try:
        if lt and num >= lt:
            raise ValueError(f'{name} must be less than {lt}')
            
        if gt and num <= gt:
            raise ValueError(f'{name} must be greater than {gt}')
            
        if lte and num > lte:
            raise ValueError(f'{name} must be less than or equal to {lte}')
            
        if gte and num < gte:
            raise ValueError(f'{name} must be greater than or equal to {gte}')
            
        if validation and callable(validation):
            validation(num)
    except Exception as e:
        raise ValidationError(f'Invalid {name}: {str(e)}')
        
    return num
    
def get_orders_from_algo_param(name:str, param:list[dict], fractional:bool=False, 
                               validation:Callable|None=None) -> list[Order]:
    orders:list[Order] = []
    
    try:
        for entry in param:
            if entry['side'] == 'sell':
                entry['side'] = -1
            elif entry['side'] == 'buy':
                entry['side'] = 1
            else:
                raise ValueError(f'illegal side for input {entry}')
                
            qty = entry['side']*float(entry['mult'])*float(entry['lots'])
            if fractional and qty != int(qty):
                raise ValueError(f'fractional quantity not allowed')
                qty = int(qty)
            if qty==0:
                raise ValueError(f'quantity selected is 0')
            
            order = Order(**{'symbol':entry['symbol'], 'quantity':qty})
            orders.append(order)
    except Exception as e:
        raise ValidationError(f'invalid {name} parameter {param}:{str(e)}')
        
    if not orders:
        raise ValidationError(f'Invalid {name}, no order details found.')
      
    try:
        if validation and callable(validation):
            validation(orders)
    except Exception as e:
        raise ValidationError(f'Invalid {name}: {str(e)}')
        
    return orders

def get_target_from_algo_param(name:str, param:dict, validation:Callable|None=None) -> Target:
    try:
        param = {param[key] for key in klass._fields} # type: ignore
        target = Target(**param)
    except Exception:
        raise ValidationError(f'invalid parameter {param}')
      
    try:
        if validation and callable(validation):
            validation(target)
    except Exception as e:
        raise ValidationError(f'Invalid {name}: {str(e)}')
        
    return target


