from __future__ import annotations
from typing import Any
from dataclasses import dataclass
import json

from blueshift.lib.exceptions import ValidationError

from .resolver import (
    ConfigRegistry, CompiledExpr, CustomCallable, ComputedFields, ComputedStreamingDataFields,
    StreamingBackend, get_fields, get_condition, CompiledCondition, get_hooks, get_custom_callable,
    get_streaming_data_fields, ToObject, get_quote_fields, ComputedQuoteFields
)
from .schema import apply_defaults, STREAMING_SCHEMA
from .objects import get_pipeline as get_objects_pipeline
from blueshift.lib.common.sentinels import NULL
from blueshift.lib.trades._order import Order
from blueshift.lib.trades._position import Position
from blueshift.lib.trades._accounts import Account


@dataclass(frozen=True, slots=True)
class MessageDef:
    format: str
    json: ComputedFields | None
    text: str | None
    tokens: ComputedFields | None
    encoder: CustomCallable | None

    def resolve(self, **context) -> Any:
        if self.format == 'json' and self.json:
            return self.json.resolve(**context)
        elif self.format == 'text' and self.text:
            text = self.text
            if self.tokens:
                tokens = self.tokens.resolve(**context)
                try:
                    text = text.format(**tokens)
                except Exception as e:
                    raise ValidationError(f"Failed to format text message: {e}")
            return text
        elif self.format == 'binary' and self.encoder:
            return self.encoder.resolve(**context)
        
        raise ValidationError(f"illegal streaming message type {self.format}")

@dataclass(frozen=True, slots=True)
class StreamingAuth:
    mode: str
    url_query: ComputedFields | None
    url_format: dict | None
    headers: ComputedFields | None
    first_message: MessageDef | None

@dataclass(frozen=True, slots=True)
class Parser:
    format: str
    preprocess: CustomCallable | None
    decode: CustomCallable | None
    payload_path: CompiledExpr | None

    def resolve(self, raw, **context) -> str|dict[str,Any]|list:
        parsed = raw
        ctx = context.copy()
        
        if self.preprocess:
            parsed = self.preprocess.resolve(parsed, **ctx)
        
        if self.decode:
            parsed = self.decode.resolve(parsed, **ctx)
        elif self.format=='json' and isinstance(parsed, (str, bytes, bytearray)):
            # auto-decode for json
            parsed = json.loads(parsed)
        elif isinstance(parsed, (bytes, bytearray)):
            # auto-decode to string for bytes like
            parsed = bytes(parsed).decode('utf-8')
        
        if not isinstance(parsed, (str, dict, list)):
            raise ValidationError(f'expected a parsed output of a string, dict or list')
        
        if self.payload_path:
            if not isinstance(parsed, dict):
                raise ValidationError(f'expected a parsed output of a dict')
            ctx['data'] = parsed
            parsed = self.payload_path.resolve(**ctx)

        return parsed

@dataclass(frozen=True, slots=True)
class RouterRule:
    channel: str
    match: CompiledExpr

    def resolve(self, msg, **context) -> str|None:
        ctx = context.copy()
        ctx['data'] = msg
        if self.match.resolve(**ctx):
            return self.channel
        return None

@dataclass(frozen=True, slots=True)
class Router:
    enabled: bool
    rules: list[RouterRule]
    default_channel: str | None

    def resolve(self, msg, **context) -> str:
        if not self.enabled:
            return self.default_channel or 'unknown'
        
        for rule in self.rules:
            channel = rule.resolve(msg, **context)
            if channel:
                return channel
            
        return self.default_channel or 'unknown'


@dataclass(frozen=True, slots=True)
class ToStreamingData:
    condition: CompiledCondition | None
    custom: CustomCallable | None
    fields: ComputedStreamingDataFields | None

    def resolve(self, **context):
        if self.condition and not self.condition.resolve(**context):
            return NULL
        
        if self.custom:
            return self.custom.resolve(**context)
        elif self.fields:
            return self.fields.resolve(**context)
        else:
             raise ValidationError("Invalid streaming data conversion rule")
        
@dataclass(frozen=True, slots=True)
class ToStreamingQuote:
    condition: CompiledCondition | None
    custom: CustomCallable | None
    fields: ComputedQuoteFields | None

    def resolve(self, **context):
        if self.condition and not self.condition.resolve(**context):
            return NULL
        
        if self.custom:
            return self.custom.resolve(**context)
        elif self.fields:
            return self.fields.resolve(**context)
        else:
             raise ValidationError("Invalid streaming quote conversion rule")

@dataclass(frozen=True, slots=True)
class StreamConnection:
    url: str
    backend: StreamingBackend
    streams: list[str]
    frame: str
    events:dict[str,str]
    auth: StreamingAuth | None
    subscribe: dict[str, dict[str,MessageDef]] | None
    parser: Parser | None
    router: Router | None
    converters: dict[str, list[ToStreamingData | ToObject]] | None
    hooks: dict[str, CustomCallable]

@dataclass(frozen=True, slots=True)
class StreamingConfig:
    connections: dict[str, StreamConnection]

    def resolve(self, **context):
        pass

def get_message_def(spec: dict[str, Any], registry: ConfigRegistry) -> MessageDef:
    fmt = spec['format']
    json_fields = None
    text = None
    tokens = None

    if 'encoder' in spec:
        encoder = get_custom_callable(spec['encoder'], registry)
    else:
        encoder = None

    if fmt == 'json' and 'json' in spec:
        json_fields = ComputedFields(
            get_fields(spec['json'].get('fields', {}), registry),
            get_custom_callable(spec['json'].get('custom', None), registry)
            )
    elif fmt == 'text' and 'text' in spec:
        text = spec['text']
        if 'tokens' in spec:
            tokens = ComputedFields(
                get_fields(spec['tokens'].get('fields', {}), registry),
                get_custom_callable(spec['tokens'].get('custom', None), registry)
                )
    
    return MessageDef(fmt, json_fields, text, tokens, encoder)

def get_auth(spec: dict[str, Any], registry: ConfigRegistry) -> StreamingAuth:
    mode = spec['mode']
    url_query = None
    url_format = None
    headers = None
    first_message = None

    if mode == 'url':
        if 'query' in spec.get('url', {}):
            url_query = ComputedFields(
                get_fields(spec['url']['query'].get('fields',{}), registry),
                get_custom_callable(spec['url']['query'].get('custom',None), registry)
                )
        url_format = spec.get('url', {}).get('format')
    elif mode == 'headers' and 'headers' in spec:
        headers = ComputedFields(
            get_fields(spec['headers'].get('fields', {}), registry),
            get_custom_callable(spec['headers'].get('custom', None), registry),
            )
    elif mode == 'first_message' and 'first_message' in spec:
        first_message = get_message_def(spec['first_message'], registry)
    
    return StreamingAuth(mode, url_query, url_format, headers, first_message)

def get_parser(spec: dict[str, Any], registry: ConfigRegistry) -> Parser:
    fmt = spec['format']
    preprocess = None
    decode = None
    payload_path = None

    if 'preprocess' in spec:
        preprocess = CustomCallable(registry[spec['preprocess']]) if isinstance(spec['preprocess'], str) else CustomCallable(spec['preprocess'])
    
    if 'decode' in spec:
        decode = CustomCallable(registry[spec['decode']]) if isinstance(spec['decode'], str) else CustomCallable(spec['decode'])
    
    if 'payload_path' in spec:
        payload_path = CompiledExpr(spec['payload_path'], registry)
    
    return Parser(fmt, preprocess, decode, payload_path)

def get_router(spec: dict[str, Any], registry: ConfigRegistry) -> Router:
    enabled = spec.get('enabled', True)
    default_channel = spec.get('default_channel')
    rules = []

    for r in spec.get('rules', []):
        rules.append(RouterRule(r['channel'], CompiledExpr(r['match'], registry)))
    
    return Router(enabled, rules, default_channel)

def get_pipeline(spec: list[dict[str, Any]], registry: ConfigRegistry) -> list[ToStreamingData]:
    rules = []
    for rule_spec in spec:
        condition = None
        custom = None
        fields = None
        
        if 'condition' in rule_spec:
            condition = get_condition(rule_spec['condition'], registry)
        
        if 'custom' in rule_spec:
            c = rule_spec['custom']
            if callable(c):
                custom = CustomCallable(c)
            elif isinstance(c, str):
                if c in registry and callable(registry[c]):
                    custom = CustomCallable(registry[c])
                else:
                    raise ValidationError(f"Custom function {c} not found in registry")
        
        if 'fields' in rule_spec:
            fields = get_streaming_data_fields(rule_spec['fields'], registry)
            
        rules.append(ToStreamingData(condition, custom, fields))
        
    return rules

def get_quotes_pipeline(spec: list[dict[str, Any]], registry: ConfigRegistry) -> list[ToStreamingQuote]:
    rules = []
    for rule_spec in spec:
        condition = None
        custom = None
        fields = None
        
        if 'condition' in rule_spec:
            condition = get_condition(rule_spec['condition'], registry)
        
        if 'custom' in rule_spec:
            c = rule_spec['custom']
            if callable(c):
                custom = CustomCallable(c)
            elif isinstance(c, str):
                if c in registry and callable(registry[c]):
                    custom = CustomCallable(registry[c])
                else:
                    raise ValidationError(f"Custom function {c} not found in registry")
        
        if 'fields' in rule_spec:
            fields = get_quote_fields(rule_spec['fields'], registry)
            
        rules.append(ToStreamingQuote(condition, custom, fields))
        
    return rules

def get_converters(spec: dict[str, Any], registry: ConfigRegistry) -> dict[str, list[ToStreamingData | ToObject]]:
    converters = {}
    for channel, pipeline_spec in spec.items():
        if channel in ('data'):
             converters[channel] = get_pipeline(pipeline_spec, registry)
        elif channel == 'quote':
             converters[channel] = get_quotes_pipeline(pipeline_spec, registry)
        elif channel == 'order':
             converters[channel] = get_objects_pipeline(pipeline_spec, Order, registry)
        elif channel == 'position':
             converters[channel] = get_objects_pipeline(pipeline_spec, Position, registry)
        elif channel == 'account':
             converters[channel] = get_objects_pipeline(pipeline_spec, Account, registry)
        else:
             converters[channel] = get_objects_pipeline(pipeline_spec, dict, registry) 

    return converters

def get_connection(spec: dict[str, Any], registry: ConfigRegistry) -> StreamConnection:
    url = spec['url']
    backend = spec['backend']
    frame = spec.get('frame','record')
    if not isinstance(backend, dict) or 'type' not in backend:
        raise ValidationError(f'invalid streaming backend specification')
    backend_opts = backend.get('options', {})
    backend = StreamingBackend(backend['type'], backend_opts)

    streams:list[str] = spec['streams']
    events:dict[str,str] = spec.get('events', {})
    auth = get_auth(spec['auth'], registry) if 'auth' in spec else None
    
    subscribe:dict[str, dict[str,MessageDef]]|None = None
    if 'subscribe' in spec:
        subscribe = {}
        if 'subscribe' in spec['subscribe']:
            subscribe['subscribe'] = {}
            subscriptions = spec['subscribe']['subscribe']
            for channel in subscriptions:
                subscribe['subscribe'][channel] = get_message_def(subscriptions[channel], registry)
            #subscribe['subscribe'] = get_message_def(spec['subscribe']['subscribe'], registry)
        if 'unsubscribe' in spec['subscribe']:
            subscribe['unsubscribe'] = {}
            unsubscriptions = spec['subscribe']['unsubscribe']
            for channel in unsubscriptions:
                subscribe['unsubscribe'][channel] = get_message_def(unsubscriptions[channel], registry)
            #subscribe['unsubscribe'] = get_message_def(spec['subscribe']['unsubscribe'], registry)
    
    parser = get_parser(spec['parser'], registry) if 'parser' in spec else None
    router = get_router(spec['router'], registry) if 'router' in spec else None

    if len(streams) == 0:
        raise ValidationError(f'connection must define a stream.')
    
    if not router and len(streams) > 1:
        raise ValidationError(f'multi-channel connection must define a router.')
    
    converters = None
    if 'converters' in spec:
        converters = get_converters(spec['converters'], registry)
        
    hooks = get_hooks(spec.get('hooks', {}), registry)

    return StreamConnection(url, backend, streams, frame, events, auth, subscribe, parser, router, converters, hooks)

def get_streaming_config(spec: dict[str, Any], registry: ConfigRegistry) -> StreamingConfig:
    spec = apply_defaults(STREAMING_SCHEMA, spec, registry)
    connections = {}
    for name, conn_spec in spec.get('connections', {}).items():
        connections[name] = get_connection(conn_spec, registry)
    
    return StreamingConfig(connections)
