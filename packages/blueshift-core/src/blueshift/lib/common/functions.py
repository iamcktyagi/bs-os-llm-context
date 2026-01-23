from __future__ import annotations
from typing import Any, TYPE_CHECKING, Literal
from collections.abc import Iterable
import time
import asyncio
from enum import Enum, EnumMeta
import datetime

from blueshift.lib.exceptions import BlueshiftException
from blueshift.lib.common.enums import AlgoMessageType


""" flatten a list of lists. """
flatten = lambda s: [item for sublist in s for item in sublist]

def get_func_caller():
    """ return the immedeate caller for a function. """
    import inspect
    try:
        return inspect.stack()[2][3]
    except Exception:
        return 'not found'
    
def get_func_caller_chain():
    """ return the whole call chain for a function. """
    import inspect
    chain = ''
    try:
        for frame in reversed(inspect.stack()):
            chain += f"->{frame[3]}"
        return chain
    except Exception:
        if not chain:
            return 'not found'
    return chain

def sleep_if_not_main_thread(sleep):
    import threading
    import time
    
    is_main = threading.current_thread() is threading.main_thread()
    if not is_main:
        time.sleep(sleep)
    
async def cancel_all_tasks(sleep=10):
    tasks = [task for task in asyncio.all_tasks() if not task.done()]
    for t in tasks:
        t.cancel()
    
    await asyncio.sleep(sleep)
    
def to_int_or_float(x):
    x = float(x)
    int_x = int(x)
    
    if int_x == x:
        return int_x
    return x
    
def str2bool(value):
    """ Convert string input to boolean. """
    if isinstance(value, str):
        if value.lower() in ('true','yes','y'):
            return True
        return False
    if value:
        return True
    return False

def to_enum(x, enum:EnumMeta) -> Enum:
    try:
        return enum(x) # type: ignore
    except Exception:
        return enum[x] # type: ignore

def listlike(x):
    """ check if argument is iterable but not a string. """
    if isinstance(x, str):
        return False
    if isinstance(x, Iterable):
        return True
    return False

def list_index(array, item, default=None):
    """ find the item at the index from a list. """
    if not isinstance(array, list):
        array = list(array)
        
    if not default:
        default = len(array) + 999
        
    if item in array:
        return array.index(item)
    
    return default

def as_numeric(x):
    """ Convert to numeric, using int or if fails, float type casting. """
    try:
        return int(x)
    except ValueError:
        return float(x)
    
def split(x, n):
    """ split an iterable in `n` segments, equal if possible."""
    def _split(x, n):
        k=0
        segments = int(len(x)/n)
        for s in range(segments):
            yield [x[i] for i in range(k, k+n)]
            k = k + n
        if x[k:]:
            yield x[k:]
    
    x = list(x)
    segments = []
    for segment in _split(x, n):
        segments.append(segment)
    
    return segments

def merge_dicts(x, y, *args):
    """ merge two or more dictionaries and return a new dict with merged key/value pairs. """
    d = x.copy()
    d.update(y)
    for e in args:
        d.update(e)
    return d

def filter_dict(keywords, kwargs):
    """ filter a dict `kwargs` based on a list of keywords. """
    new_kwargs={}
    for key in kwargs:
        if key in keywords:
            new_kwargs[key] = kwargs[key]
            
    return new_kwargs

def merge_lists(x, y, *args):
    """ merge all given list in to a single new list. """
    merged = x + y
    for e in args:
        merged = merged + e
        
    return merged

def map_dict(data, mapping):
    """ 
        create a dict from input an input data dict and a mapping
        dict. The mapping dict maps a keyword to a tuple. The tuple
        first item specifies the keyword entry in the return dict. 
        The second item of the tuple is a function that takes the 
        data item as inputs and returns a value for the returned dict.
        If the second item is `None`, there must be a third item in 
        the tuple and this will be put in the returned dict as value.
    """
    d = {}
    try:
        for key in data:
            if key in mapping:
                entries = mapping[key]
                for entry in entries:
                    if data[key] is None and len(entry) < 3:
                        # no default value defined, raise error
                        msg = f'Error mapping data: Missing value for {key} without any default.'
                        raise ValueError(msg)
                    d[entry[0]] = entry[1](data[key]) if data[key] is not None else entry[1](entry[2])
        return d
    except Exception:
        raise

def dict_diff(d1, d2):
    '''
        take difference of two dictions, treating the value as integer. it
        will list all keys (union of keys) in a new dict with values as the
        difference between the first and the second dict values, skipping
        zeros and non-numbers.
    '''
    diff = {}
    keys = set(d1.keys()).union(set(d2.keys()))
    for key in keys:
        try:
            value = int(d1.get(key,0)) - int(d2.get(key,0))
        except (ValueError, TypeError):
            pass
        else:
            if value !=0:
                diff[key] = value
    
    return diff

def exec_user_module(source, module, path):
    '''
        function to load multi-file user code in to Blueshift. This
        execs the source_file, which may contain full (NOT relative)
        import of other resources from the module `module_name`. The
        path is usually the local source dir under the Blueshift
        root which contains the `module_name`, which in turn contains
        the `source_file`.
    '''
    from os import path as os_path
    from blueshift.lib.common.ctx_mgrs import AddPythonPath
    namespace = {}
    
    if os_path.isfile(source):
        source_file = os_path.basename(source)
        if module==None:
            with open(source) as algofile:
                algo_text = algofile.read()
            code = compile(algo_text,source_file,'exec')
            exec(code, namespace) #nosec
        else:
            module_path = os_path.expanduser(path)
            with AddPythonPath(module_path):
                path = os_path.join(module_path, module, source)
                with open(path) as fp:
                    algo_text = fp.read()
                code = compile(algo_text,source_file,'exec')
                exec(code, namespace) #nosec
    elif isinstance(source, str):
            source_file = "<string>"
            algo_text = source
            code = compile(algo_text,source_file,'exec')
            exec(code, namespace) #nosec
    else:
        raise 
    
    return namespace

def staggered_execution(iterator, func, delay=1, stop_on_error=False,
                        *args, **kwargs):
    """
        Takes an iterable and a function and applies the function to the
        items in the iterables with a delay determined by the `delay` 
        parameter.
    """
    if not callable(delay):
        def do_delay(item, *args, **kwargs):
            time.sleep(delay)
    else:
        do_delay = delay
    
    t1 = time.time()
    for item in iterator:
        try:
            func(item)
        except Exception:
            if stop_on_error:
                raise
        do_delay(item, *args, **kwargs)
    t2 = time.time()
    return t2-t1

def _merge_json_recursive(first, second):
    results = {}
    first = first.copy()
    second = second.copy()
    keys = list(set(list(first.keys()) + list(second.keys())))
    
    for key in keys:
        if key in first and key not in second:
            results[key] = first[key]
        elif key in second and key not in first:
            results[key] = second[key]
        else:
            val1 = first[key]
            val2 = second[key]
            if isinstance(val1, dict) and isinstance(val2, dict):
                results[key] =_merge_json_recursive(val1, val2)
            else:
                results[key] = val2
                
    return results
            
def merge_json_recursive(data:dict[str, Any], *args) -> dict[str, Any]:
    result:dict[str, Any] = data.copy()
    for arg in args:
        result = _merge_json_recursive(result, arg)
        
    return result

def list_to_args_kwargs(opt_list:list[str]) -> tuple[list[str],dict[str,str]]:
    '''
        Utility to convert extra arguments passed from command 
        processors (click) in to args and kwargs. Keyword arguments
        are extracted if there is an `=` in the option string, else 
        it is assumed to be args element.
    '''
    args = []
    kwargs = {}
    
    for opt in opt_list:
        if "=" in opt:
            params = opt.split("=")
            kwargs[params[0]] = opt[len(params[0])+1:]
        else:
            args.append(opt)
            
    return args, kwargs

def generate_args(strargs:str, separator:str=";") -> list[str]:
    """ generate arg from input string separated by ';'. """
    if not strargs:
        return []
    
    strargs = strargs.replace('-','_')
    return strargs.split(separator)
    
def generate_kwargs(strkwargs:str, separator:str=";") -> dict[str,str]:
    """ generate key=value pair from input string of key=values separated by ';'. """
    kwargs = {}
    if not strkwargs:
        return kwargs
    
    strkwargs = strkwargs.replace('-','_')
    pairs = strkwargs.split(separator)
    for pair in pairs:
        pair = pair.split('=')
        if len(pair)<2:
            key, value = pair[0], None
        else:
            key, value = tuple(pair[:2])
        kwargs[key] = value
        
    return kwargs

def process_extra_args(**kwargs):
    """ 
        Function called with extra arguments from blueshift ingest 
        command line. Add all processing here that is necessary.
    """
    new_kwargs = {}
    for key in kwargs:
        new_kwargs[key.lstrip('-').replace('-','_')] = kwargs[key]
        
    return new_kwargs

def get_kwargs_from_click_ctx(ctx):
    """ get kwargs from a click context. """
    kwargs = dict()
    for key in ctx.args:
        key = key.lstrip('-')
        parts = key.split('=')
        
        if len(parts) > 2:
            raise BlueshiftException(f'illegal argument {key}')
            
        param = parts[0].replace('-','_')
        
        if len(parts) == 1:
            parts = {param:True}
        else:
            parts = {param:parts[1]}
        
        kwargs.update(parts)
    
    return kwargs

def to_camel_case(snake_str):
    """ convert snake case to camel case. """
    snake_str = snake_str.replace('-','_')
    components = snake_str.split('_')
    return components[0] + ''.join(x.capitalize() for x in components[1:])

def to_title_case(snake_str):
    """ convert snake case to title case. """
    snake_str = snake_str.replace('-','_')
    components = snake_str.split('_')
    return ''.join(x.capitalize() for x in components)

def to_snake_case(camel_str):
    """ convert title or camel case to snake case. """
    import re
    s = re.sub('([a-z0-9])([A-Z])', r'\1_\2', camel_str)
    return s.lower()

def create_message_envelope(topic:str, message_type:AlgoMessageType, msg:str|None=None, 
                            data:dict[str, Any]|None=None, 
                            level:Literal['info','error','warning','success','update']|None=None,
                            context:Literal['performance','state','status','log','trades','positions','monitor','action']|None=None,
                            source:Literal['algo', 'service','platform']|None=None,
                            display:Literal['none','alert','no']|None=None,
                            ) -> dict[str, Any]:
    """ create msg envelop. """
    timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
    if level:
        level = str(level).lower() # type: ignore

    packet:dict[str, Any] = {
        'topic':str(topic),
        'timestamp':timestamp.isoformat(),
        'event':message_type.name.lower(), 
        'message':msg or '',
        'data':data or {},
        'context':context or '',
        'level':level or 'update',
        'source':source or 'algo',
        'display':display or 'none',
        }

    if message_type in (
        AlgoMessageType.INIT, AlgoMessageType.SOB, AlgoMessageType.HEARTBEAT):
        packet['context']= context or 'state'
        packet['display'] = display or 'no'
    if message_type in (
        AlgoMessageType.PAUSED, AlgoMessageType.RESUMED, AlgoMessageType.ALGO_END):
        packet['context']= context or 'state'
    if message_type in (
        AlgoMessageType.BAR, AlgoMessageType.DAILY, AlgoMessageType.COMPLETE):
        packet['context']=context or 'performance'
    elif message_type in (AlgoMessageType.HEALTH_HEARTBEAT,AlgoMessageType.DEBUG):
        packet['context']=context or 'monitor'
        packet['display'] = display or 'no'
    elif message_type == AlgoMessageType.LOG:
        if level is None:
            raise ValueError(f'level must be specified')
        packet['context']=context or 'log'
        packet['level'] = level
    elif message_type in (AlgoMessageType.PLATFORM, AlgoMessageType.STATUS_MSG):
        packet['context']=context or 'status'
    elif message_type in (AlgoMessageType.TRADES, AlgoMessageType.TRANSACTIONS):
        packet['context']='orders'
    elif message_type == AlgoMessageType.POSITIONS:
        packet['context']=context or 'positions'
    elif message_type == AlgoMessageType.VALUATION:
        packet['context']='performance'
    elif message_type == AlgoMessageType.SMART_ORDER:
        packet['context']=context or 'performance'
    elif message_type == AlgoMessageType.ONECLICK:
        packet['context']=context or 'action'
    elif message_type == AlgoMessageType.COMMAND:
        packet['context']=context or 'action'
    elif message_type == AlgoMessageType.STDOUT:
        packet['context']=context or 'output'
    elif message_type == AlgoMessageType.NOTIFY:
        packet['context']=context or 'platform'
        packet['display'] = display or 'alert'
        packet['level'] = level or 'info'
    elif message_type == AlgoMessageType.CALLBACK:
        packet['context']=context or 'callback'

    return packet