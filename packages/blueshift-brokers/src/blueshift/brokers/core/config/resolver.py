from __future__ import annotations
from typing import Callable, Any, TYPE_CHECKING, Type
from types import CodeType
import builtins
import ast
from enum import Enum, EnumMeta
from dataclasses import dataclass, field
import importlib
import math

from blueshift.lib.exceptions import UnsafeCodeError, ValidationError, APIError, BlueshiftException
from blueshift.lib.common.sentinels import NULL
from blueshift.lib.common.functions import to_enum
from blueshift.interfaces.logger import get_logger

logger = get_logger()

if TYPE_CHECKING:
    import pandas as pd
else:
    import blueshift.lib.common.lazy_pandas as pd

_SAFE_BUILTINS = [
    'ArithmeticError', 'AssertionError', 'AttributeError', 'BaseException', 'BlockingIOError', 
    'BrokenPipeError', 'BufferError', 'BytesWarning', 'ChildProcessError', 'ConnectionAbortedError', 
    'ConnectionError', 'ConnectionRefusedError', 'ConnectionResetError', 'DeprecationWarning', 
    'EOFError', 'Ellipsis', 'EncodingWarning', 'EnvironmentError', 'Exception', 'False', 
    'FileExistsError', 'FileNotFoundError', 'FloatingPointError', 'FutureWarning', 'GeneratorExit', 
    'IOError', 'ImportError', 'ImportWarning', 'IndentationError', 'IndexError', 'InterruptedError', 
    'IsADirectoryError', 'KeyError', 'KeyboardInterrupt', 'LookupError', 'MemoryError', 
    'ModuleNotFoundError', 'NameError', 'None', 'NotADirectoryError', 'NotImplemented', 
    'NotImplementedError', 'OSError', 'OverflowError', 'PendingDeprecationWarning', 'PermissionError', 
    'ProcessLookupError', 'RecursionError', 'ReferenceError', 'ResourceWarning', 'RuntimeError', 
    'RuntimeWarning', 'StopAsyncIteration', 'StopIteration', 'SyntaxError', 'SyntaxWarning', 
    'SystemError', 'SystemExit', 'TabError', 'TimeoutError', 'True', 'TypeError', 'UnboundLocalError', 
    'UnicodeDecodeError', 'UnicodeEncodeError', 'UnicodeError', 'UnicodeTranslateError', 
    'UnicodeWarning', 'UserWarning', 'ValueError', 'Warning', 'WindowsError', 'ZeroDivisionError', 
    '__build_class__', '__debug__', '__doc__', '__import__', '__loader__', '__name__', '__package__', 
    '__spec__', 'abs', 'aiter', 'all', 'anext', 'any', 'ascii', 'bin', 'bool', 'bytearray', 'bytes', 
    'callable', 'chr', 'classmethod', 'complex', 'delattr', 'dict', 'dir', 'divmod', 'enumerate', 
    'filter', 'float', 'format', 'frozenset', 'getattr', 'hasattr', 'hash', 'hex', 'id', 'int', 
    'isinstance', 'issubclass', 'iter', 'len', 'list', 'map', 'max', 'memoryview', 'min', 'next', 
    'object', 'oct', 'ord', 'property', 'range', 'repr', 'reversed', 'round', 'set', 'setattr', 
    'slice', 'sorted', 'staticmethod', 'str', 'sum', 'super', 'tuple', 'type', 'vars', 'zip'
    ]

_globals = {k: v for k, v in vars(builtins).items() if k in _SAFE_BUILTINS}
_globals = {'__builtins__':_globals}

class ConfigRegistry:
    def __init__(self, registry:dict[str, Any]|None=None):
        if registry is None:
            registry = {}
        else:
            registry = registry.copy()

        for k in ('__name__','__doc__','__package__','__loader__','__spec__','__file__','__cached__',
                  '__builtins__','annotations'):
            registry.pop(k, None)

        registry['__builtins__'] = {}
        self.registry:dict[str, Any] = registry

    def __setitem__(self, name:str, value: Any) -> None:
        self.registry[name] = value

    def __getitem__(self, name:str):
        return self.registry[name]
    
    def __contains__(self, item):
        return item in self.registry
    
    def get_registry(self):
        return self.registry
    
    def get(self, item, default=None) -> Callable|None:
        return self.registry.get(item, default)
    
    def register(self, name:str|None=None) -> Callable:
        def _register(fn):
            _name = name if name else fn.__name__
            self[str(_name)] = fn
            return fn
        return _register

class CompiledExpr:
    """ compute dynamic expression - implementation for `expr` schema def. """
    BANNED_AST_NODES = (
        ast.AnnAssign, ast.Assert, ast.AsyncFor, ast.AsyncFunctionDef, ast.AsyncWith, ast.Await, 
        ast.Break, ast.ClassDef, ast.Continue, ast.Del, ast.Delete, ast.ExceptHandler, ast.For, 
        ast.FunctionDef, ast.FunctionType, ast.GeneratorExp, ast.Global, ast.Import, ast.ImportFrom, 
        ast.Interactive, ast.MatchClass, ast.Module, ast.Nonlocal, ast.While, ast.With, ast.Yield, 
        ast.YieldFrom, ast.Return, ast.Try, ast.TypeIgnore, ast.Raise
    )

    def __init__(self, expr:str, registry:ConfigRegistry) -> None:
        self.expr = expr
        self.registry = registry
        self.code:CodeType|None = None
        self._compile(expr)

    def _compile(self, expr:str) -> None:
        tree = ast.parse(expr, mode="eval")

        for node in ast.walk(tree):
            if isinstance(node, self.BANNED_AST_NODES):
                raise UnsafeCodeError(f"Disallowed syntax: {type(node).__name__}")
            if isinstance(node, ast.Attribute) and node.attr.startswith('_'):
                raise UnsafeCodeError("private attribute access is not allowed")
            if isinstance(node, ast.Name) and node.id.startswith('_'):
                raise UnsafeCodeError(f"illegal name: {node.id}")
            
        self.code = compile(expr, "<Expression>", mode="eval") # nosec

    def resolve(self, **context) -> Any:
        if self.code is not None:
            # use registry as base, but allow context (data) to override
            ctx = self.registry.registry.copy()
            ctx.update(context)
            try:
                return eval(self.code, _globals, ctx) # nosec
            except Exception as e:
                raise ValidationError(f"Error evaluating expression '{self.expr}': {str(e)}") from e

        raise ValidationError(f'invalid or illegal expression.')
    
@dataclass(slots=True)
class CustomCallable:
    """ A func or an expression that returns value - implementation for `callable_ref` schema def. """
    fn:Callable

    def __post_init__(self):
        if isinstance(self.fn, str):
            if not callable(self.fn):
                raise ValidationError(f'function {self.fn} not registered.')

    def resolve(self, *args, **context) -> Any:
        if isinstance(self.fn, str):
            raise ValidationError(f'function {self.fn} not registered.')
        return self.fn(*args, **context)
    
@dataclass(frozen=True, slots=True)
class CompiledCondition:
    """ A func or an expression that returns bool. """
    kind: str
    fn: CustomCallable|None = None
    expr: CompiledExpr|None = None

    def resolve(self, **context) -> bool:
        if self.kind == "fn" and self.fn:
            return self.fn.resolve(**context)
        elif self.expr:
            return self.expr.resolve(**context)
        raise ValidationError(f'invalid expression or custom function.')
    
@dataclass(frozen=True, slots=True)
class CompiledValue:
    """ A func or an expression that returns value. """
    kind: str
    fn: CustomCallable|None = None
    expr: CompiledExpr|None = None

    def resolve(self, **context) -> Any:
        if self.kind == "fn" and self.fn:
            return self.fn.resolve(**context)
        elif self.expr:
            return self.expr.resolve(**context)
        raise ValidationError(f'invalid expression or custom function.')
    
@dataclass(frozen=True, slots=True)
class ComputedField:
    """ computed field - implementation for `field_spec` schema def"""
    source: CompiledExpr|None = None
    condition: CompiledCondition|None = None
    transform: CompiledValue|None = None
    custom: CompiledValue|None = None
    default_value: Any = NULL

    def resolve(self, **context) -> Any:
        ctx = context.copy()

        if self.source:
            try:
                value = self.source.resolve(**context)
                ctx['value'] = value
            except Exception as e:
                if self.default_value is not NULL:
                    logger.debug(f"Field resolution failed, using default: {e}")
                    return self.default_value
                raise
        
        if self.condition:
            if not self.condition.resolve(**ctx):
                return NULL
        
        if self.custom:
            return self.custom.resolve(**context) # side effect allowed
        
        if self.source:
            value = ctx['value']
            if self.transform:
                return self.transform.resolve(value=value, **ctx) # side effect not allowed
            return value
        
        raise ValidationError(f'illegal computed field - no source or custom function defined.')
    
@dataclass(frozen=True, slots=True)
class CompiledMsg:
    error_message:str
    def resolve(self, **context) -> str:
        return self.error_message.format(**context)

@dataclass(frozen=True, slots=True)
class ErrorSpec:
    """ computed error - implementation for `error_rule` schema def"""
    error_condition: CompiledCondition
    error_class: type[Exception]
    error_message: CompiledMsg

    def resolve(self, **context) -> None:
        if self.error_condition.resolve(**context):
            raise self.error_class(self.error_message)

@dataclass(frozen=True, slots=True)
class ErrorHandler:
    """ computed error - implementation for `errors` schema - collection of error rules to check. """
    error_specs:list[ErrorSpec] = field(default_factory=list)

    def resolve(self, **context) -> None:
        for entry in self.error_specs:
            entry.resolve(**context)

@dataclass(frozen=True, slots=True)
class ComputedValues:
    values:dict[str,CompiledValue]

    def resolve(self, **context) -> dict[str, Any]:
        computed = {}
        for name, entry in self.values.items():
            computed[name] = entry.resolve(**context)

        return computed
    
@dataclass(frozen=True, slots=True)
class ComputedFields:
    """ computed fields dict - implementation for `fields_object` schema def"""
    fields:dict[str, ComputedField]
    custom:CustomCallable|None=None

    def resolve(self, **context) -> dict[str, Any]:
        ctx = context.copy()
        computed = {}
        for name, entry in self.fields.items():
            try:
                res = entry.resolve(**ctx)
                if res is not NULL:
                    computed[name] = res
            except Exception as e:
                raise ValidationError(f"Failed to resolve field '{name}': {str(e)}") from e

        if self.custom:
            ctx['values'] = computed
            return self.custom.resolve(**ctx)
        
        return computed

@dataclass(frozen=True, slots=True)
class ComputedOHLCFields:
    """ computed OHLC data - implementation for `ohlc_fields_group` schema def"""
    symbol:str|None = None
    frame: str|None = None
    rename: dict[str,str]|None = None
    timestamp: dict[str,str]|None = None
    custom: CustomCallable|None = None

    def resolve(self, data:Any, **context) -> Any:
        if self.symbol is None:
            return self._resolve(data, **context)
        
        if self.symbol == 'key':
            if not isinstance(data, dict):
                raise ValidationError(
                    f'for symbol in data as key, expected data to be a dict, got {type(data)}')
            
            res = {}
            for k in data:
                part = data[k]
                res[k] = self._resolve(data[k], **context)

            return res
        
        raise NotImplementedError(f'only symbol in key for multi asset OHLC data is implemented.')


    def _resolve(self, data:Any, **context) -> Any:
        if self.custom:
            return self.custom.resolve(data, **context)
        
        if not self.frame:
            raise ValidationError(f'missing frame type for OHLC data parsing')

        if self.frame == 'records':
            """
            [
                {"t": 1700000000, "o": 100, "h": 105, "l": 99, "c": 102, "v": 1200},
                {"t": 1700000060, "o": 102, "h": 106, "l": 101, "c": 104, "v": 900}
            ]
            """
            df = pd.DataFrame(data)
        elif self.frame == 'dict_of_arrays':
            """
            {
                "t": [1700000000, 1700000060],
                "o": [100, 102],
                "h": [105, 106],
                "l": [99, 101],
                "c": [102, 104],
                "v": [1200, 900]
            }
            """
            df = pd.DataFrame(data)
        elif self.frame == 'values':
            """
            [
                [1700000000, 100, 105, 99, 102, 1200],
                [1700000060, 102, 106, 101, 104, 900]
            ]
            """
            df = pd.DataFrame(data)
        else:
            raise ValidationError(f'unknown dataframe type {self.frame}')
            
        if self.rename:
            # rename is Target(standard) -> Source(input)
            # we need Source -> Target mapping for pd.rename
            keep_cols = list(self.rename.keys())
            if 'timestamp' not in keep_cols and 'timestamp' in df.columns:
                keep_cols = ['timestamp'] + keep_cols

            rename_map = {}
            for target, source in self.rename.items():
                if self.frame == 'values' and isinstance(source, str) and source.isdigit():
                    source = int(source)
                rename_map[source] = target
            df.rename(columns=rename_map, inplace=True)
            df = df[keep_cols]

        if 'timestamp' not in df.columns:
            raise ValidationError(f'missing timestamp column from data.')
        
        if self.timestamp:
            unit = self.timestamp.get('unit')
            fmt = self.timestamp.get('format')
            # handle to_datetime args
            kwargs = {}
            if unit: kwargs['unit'] = unit
            if fmt: kwargs['format'] = fmt
            
            df['timestamp'] = pd.to_datetime(df['timestamp'], **kwargs)
            df.set_index('timestamp', inplace=True)
        
        return df
    
@dataclass(frozen=True, slots=True)
class ComputedQuoteFields:
    """ computed quote data - implementation for `quote_fields_group` schema def"""
    fields: dict[str, ComputedField]
    market_depth: dict[str,list[tuple[ComputedField,ComputedField]]]
    custom:CustomCallable|None=None

    def resolve(self, **context) -> dict[str, Any]:
        if self.custom:
            return self.custom.resolve(**context)
        
        computed = {}
        for name, entry in self.fields.items():
            try:
                res = entry.resolve(**context)
                if res != NULL:
                    computed[name] = res
                else:
                    if name in ('bid_volume','ask_volume'):
                        computed[name] = 0
                    else:
                        computed[name] = math.nan
            except Exception as e:
                raise ValidationError(f"Failed to resolve quote field '{name}': {str(e)}") from e

        md = {'bids':[], 'asks':[]}
        bids = []
        asks = []

        for i, entry in enumerate(self.market_depth.get('bids',[])):
            try:
                if len(entry) < 2:
                    continue
                px = entry[0].resolve(**context)
                size = entry[1].resolve(**context)
                if px == NULL or size == NULL:
                    continue
            except Exception as e:
                 raise ValidationError(f"Failed to resolve market depth bids'{i}': {str(e)}") from e
            else:
                md['bids'].append((px, size))

        for i, entry in enumerate(self.market_depth.get('asks',[])):
            try:
                if len(entry) < 2:
                    continue
                px = entry[0].resolve(**context)
                size = entry[1].resolve(**context)
                if px == NULL or size == NULL:
                    continue
            except Exception as e:
                 raise ValidationError(f"Failed to resolve market depth bids'{i}': {str(e)}") from e
            else:
                md['asks'].append((px, size))

        if md:
            computed['market_depth'] = md

        return computed

@dataclass(frozen=True, slots=True)
class StreamingBackend:
    """ backend description for streaming source. """
    type:str
    options:dict[str, Any]

@dataclass(frozen=True, slots=True)
class ComputedStreamingDataFields:
    """ computed streaming data - implementation for `streaming_data_fields_group` schema def"""
    asset: ComputedField
    timestamp: ComputedField
    data: ComputedFields
    custom:CustomCallable|None = None

    def resolve(self, **context) -> tuple[Any, Any, dict[str, Any]]:
        if self.custom:
            return self.custom.resolve(**context)
        
        asset = self.asset.resolve(**context)
        timestamp = self.timestamp.resolve(**context)
        data = self.data.resolve(**context)
        return asset, timestamp, data

@dataclass(frozen=True, slots=True)
class ToObject:
    """ object conversion - implementation for `object_conversion` schema def"""
    target:Any|Callable|None = None
    condition: CompiledCondition|None = None
    custom: CustomCallable|None = None
    fields: ComputedFields|None = None

    def resolve(self, **context):
        if self.condition and not self.condition.resolve(**context):
            return NULL
        
        if self.custom:
            return self.custom.resolve(**context)
        elif self.fields:
            return self.fields.resolve(**context)
        else:
            msg = f'invalid conversion definition, either fields + target class or a custom function must be defined.'
            raise ValidationError(msg)

@dataclass(frozen=True, slots=True)
class EnumMap:
    """ Map between BlueShift Enums/Strings and Broker Strings """
    registry:ConfigRegistry
    mapping: dict[str, str|list[str]]
    default: str|None = None # default is the blueshift value/enum
    _reverse: dict[str, str] = field(default_factory=dict, init=False)
    _cls: Type|None = field(init=False)

    def __post_init__(self):
        _cls = None
        # normalize keys if they are Enums
        normalized_mapping = {}
        for k, v in self.mapping.items():
            key_str = str(k)
            if isinstance(k, Enum):
                self.registry[key_str] = k
                _cls = k.__class__
            
            if isinstance(v, list):
                if not v:
                    raise ValidationError(f"Empty mapping list for key {key_str}")
                normalized_mapping[key_str] = v
            else:
                normalized_mapping[key_str] = v

        # Bypass frozen dataclass constraint to update mapping
        object.__setattr__(self, 'mapping', normalized_mapping)

        reverse = {}
        for k, v in normalized_mapping.items():
            if isinstance(v, list):
                for i in v:
                    reverse[i] = k
            else:
                reverse[v] = k
        object.__setattr__(self, '_reverse', reverse)
        object.__setattr__(self, 'mapping', normalized_mapping)
        object.__setattr__(self, '_cls', _cls)

    def from_blueshift(self, value: Any, **kwargs) -> str:
        # handle Enums -> auto-convert if not in EnumName.VALUE format
        convert_to_enum = not isinstance(value, str) or '.' not in value
        if self._cls is not None and convert_to_enum:
            value = to_enum(value, self._cls)
        
        key = str(value)
        if key in self.mapping:
            val = self.mapping[key]
            return val[0] if isinstance(val, list) else val
        
        if self.default is not None:
            key = str(self.default)
            if key in self.mapping:
                val = self.mapping[key]
                return val[0] if isinstance(val, list) else val
            
        raise ValidationError(f"Mapping not found for {key}")

    def to_blueshift(self, value: str, **kwargs) -> Any:
        if value in self._reverse:
            res = self._reverse[value]
            return self.registry.get(res, res)
        
        if self.default is not None:
            return self.default
        
        raise ValidationError(f"Reverse mapping not found for {value}")
    
def get_condition(cond, registry:ConfigRegistry):
    if callable(cond):
        cond = CompiledCondition('fn',CustomCallable(cond), None)
    elif isinstance(cond, str):
        if cond in registry and callable(registry[cond]):
            cond = CompiledCondition('fn', CustomCallable(registry[cond]), None)
        else:
            cond = CompiledCondition('expr',None,CompiledExpr(cond, registry))
    else:
        raise ValidationError(f'invalid condition {cond}')
    
    return cond


def get_value(name, value, registry:ConfigRegistry) -> CompiledValue:
    if callable(value):
        return CompiledValue('fn',CustomCallable(value), None)
    elif isinstance(value, str):
        if value in registry and callable(registry[value]):
            return CompiledValue('fn', CustomCallable(registry[value]), None)
        else:
            return CompiledValue('expr',None,CompiledExpr(value, registry))
    else:
        raise ValidationError(f'invalid value {value} for field {name}')
    
def get_values(spec, registry:ConfigRegistry) -> dict[str, CompiledValue]:
    values:dict[str,CompiledValue] = {}

    for name, value in spec.items():
        values[str(name)] = get_value(name, value, registry)

    return values

def get_custom_callable(spec, registry:ConfigRegistry) -> CustomCallable|None:
    if spec is None:
        return
    
    if callable(spec):
        return CustomCallable(spec)
    elif isinstance(spec, str):
        if spec in registry and callable(registry[spec]):
            return CustomCallable(registry[spec])
        else:
            raise ValidationError(f'custom function {spec} not registerd.')
    else:
        raise ValidationError(f'illegal custom function {spec} of type {type(spec)}')

def _get_field(name:str, details:dict[str, Any], registry:ConfigRegistry) -> ComputedField:
    source = details.get('source')

    if source:
        source = CompiledExpr(str(details['source']),registry)

    custom = details.get('custom')
    if custom:
        custom = get_value(name, custom, registry=registry)

    if source is None and custom is None:
        raise ValidationError(f'missing source and custom attributes for field {name}')
    
    cond = details.get('condition')
    if cond:
        if callable(cond):
            cond = CompiledCondition('fn',CustomCallable(cond), None)
        elif isinstance(cond, str):
            if cond in registry and callable(registry[cond]):
                cond = CompiledCondition('fn', CustomCallable(registry[cond]), None)
            else:
                cond = CompiledCondition('expr',None,CompiledExpr(cond,registry))
        else:
            raise ValidationError(f'invalid condition {cond} for field {name}')
    else:
        cond = None

    transform = details.get('transform')
    if transform:
        if callable(transform):
            transform = CompiledValue('fn',CustomCallable(transform), None)
        elif isinstance(transform, str):
            if transform in registry and callable(registry[transform]):
                transform = CompiledValue('fn', CustomCallable(registry[transform]), None)
            else:
                transform = CompiledValue('expr',None,CompiledExpr(transform, registry))
        else:
            raise ValidationError(f'invalid condition {cond} for field {name}')
    else:
        transform = None
    
    default_value = details.get('default_value', NULL)
    return ComputedField(source, cond, transform, custom, default_value=default_value)


def get_fields(spec, registry:ConfigRegistry) -> dict[str, ComputedField]:
    fields:dict[str, ComputedField] = {}
    
    for name, details in spec.items():
        fields[name] = _get_field(name, details, registry)

    return fields

def get_ohlc_fields(spec, registry:ConfigRegistry) -> ComputedOHLCFields:
    registry = registry
    spec = spec.get('data', spec)
    
    custom = get_custom_callable(spec.get('custom'), registry)
    symbol = spec.get('symbol')
    frame = spec.get('frame')
    rename = spec.get('rename')
    timestamp = spec.get('timestamp')
    
    return ComputedOHLCFields(symbol, frame, rename, timestamp, custom)

def get_quote_fields(spec, registry:ConfigRegistry) -> ComputedQuoteFields:
    spec = spec.copy()

    custom = get_custom_callable(spec.get('custom', None), registry)
    md_spec:dict[str,Any] = spec.pop('market_depth', {})
    md_bids:list[tuple[dict[str,Any],dict[str,Any]]] = md_spec.get('bids', {})
    md_asks:list[tuple[dict[str,Any],dict[str,Any]]] = md_spec.get('asks', {})
    
    fields = get_fields(spec, registry)
    market_depth:dict[str,list[tuple[ComputedField,ComputedField]]] = {}
    bids:list[tuple[ComputedField,ComputedField]] = []
    asks:list[tuple[ComputedField,ComputedField]] = []
    
    if md_bids:
        for i, details in enumerate(md_bids):
            name = f"bids[{i}]"
            if len(details) < 2:
                continue
            px = _get_field(name+' quote', details[0], registry)
            size = _get_field(name + ' size', details[1], registry)
            bids.append((px,size))

    if md_asks:
        for i, details in enumerate(md_asks):
            name = f"asks[{i}]"
            if len(details) < 2:
                continue
            px = _get_field(name+' quote', details[0], registry)
            size = _get_field(name + ' size', details[1], registry)
            asks.append((px,size))

    market_depth['bids'] = bids
    market_depth['asks'] = asks

    return ComputedQuoteFields(fields, market_depth, custom)

def get_streaming_data_fields(spec, registry: ConfigRegistry) -> ComputedStreamingDataFields:
    spec = spec.copy()

    custom = get_custom_callable(spec.get('custom', None), registry)
    asset_spec = spec.pop('asset')
    timestamp_spec = spec.pop('timestamp')
    data_spec = spec.pop('data')
    
    asset = get_fields({'asset': asset_spec}, registry)['asset']
    timestamp = get_fields({'timestamp': timestamp_spec}, registry)['timestamp']
    data = ComputedFields(get_fields(data_spec, registry))
    
    return ComputedStreamingDataFields(asset, timestamp, data, custom)

def _get_err_spec(spec, registry) -> ErrorSpec|None:
    cond = msg = klass = None

    error_condition = spec.get('condition')
    if not error_condition:
        return

    if callable(error_condition):
        cond = CompiledCondition("fn", CustomCallable(error_condition), None)
    elif isinstance(error_condition, str):
        if error_condition in registry and callable(registry[error_condition]):
            cond = CompiledCondition('fn', CustomCallable(registry[error_condition]), None)
        else:
            cond = CompiledCondition("expr", None, CompiledExpr(str(error_condition),registry))
    else:
        raise ValidationError(f"illegal error condition {error_condition} of type {type(error_condition)}")
        
    error_class = spec.get('exception')
    if error_class:
        if issubclass(error_class, Exception):
            klass = error_class
        elif isinstance(error_class, str):
            cls = getattr(builtins, error_class, None)
            if cls and issubclass(cls, Exception):klass = cls
            else:
                blueshift_errors = importlib.import_module("blueshift.lib.exceptions")
                cls = getattr(blueshift_errors, error_class, None)
                if cls and issubclass(cls, Exception):klass=cls

    error_msg = spec.get('message')
    if error_msg:
        if isinstance(error_msg, str):
            msg = CompiledMsg(error_msg)
        else:
            raise ValidationError(f'illegal error message {error_msg}')
    
    if not klass:
        klass = APIError
    if not msg:
        msg = CompiledMsg('something went wrong')
        
    return ErrorSpec(cond, klass, msg)

def get_error_specs(spec, registry:ConfigRegistry):
    registry = registry
    err_specs:list[ErrorSpec] = []

    for entry in spec:
        err_spec = _get_err_spec(entry, registry)
        if err_spec:err_specs.append(err_spec)

    return err_specs

def get_hooks(spec, registry:ConfigRegistry):
    hooks:dict[str, CustomCallable] = {}

    for hook, func in spec.items():
        custom = get_custom_callable(func, registry)
        if custom:
            hooks[hook] = custom

    return hooks

def get_enum_map(spec: dict, registry:ConfigRegistry, enum_type:EnumMeta|None = None) -> EnumMap:
    mapping = spec.get('map', {})
    default = spec.get('default_value')
    from_name = spec.get('from_blueshift')
    to_name = spec.get('to_blueshift')

    if enum_type:
        mapping = {to_enum(k,enum_type ):v for k,v in mapping.items()}

    if not mapping and enum_type:
        mapping = {v:v.name for k,v in enum_type.__members__.items()} # type: ignore
    
    enum_map = EnumMap(registry, mapping, default) # type: ignore
    
    if registry is not None:
        if from_name:
            registry[from_name] = enum_map.from_blueshift
        if to_name:
            registry[to_name] = enum_map.to_blueshift
            
    return enum_map