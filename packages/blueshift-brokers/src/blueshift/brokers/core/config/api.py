from __future__ import annotations
from typing import Any
from dataclasses import dataclass

from blueshift.lib.exceptions import ValidationError

from .resolver import (
    CompiledExpr, CompiledMsg, ComputedFields, ComputedOHLCFields, ComputedQuoteFields, ErrorHandler, 
    CustomCallable, ConfigRegistry, get_fields, get_ohlc_fields, get_quote_fields, get_error_specs, 
    get_hooks, get_custom_callable)
from .instruments import AssetDef
from .schema import apply_defaults, API_SCHEMA

def get_endpoint(ep_spec, global_spec) -> str:
    """ infer URL from schema. """
    endpoint = ep_spec.get("endpoint", "")
    if not endpoint:
        raise ValidationError(f'no endpoint defined.')
    
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        return endpoint
    
    base_url = global_spec.get('base_url')
    
    if not base_url:
        raise ValidationError(f'no base url defined for APIs')
    
    return f"{base_url}/{endpoint.lstrip('/')}"
        
@dataclass(frozen=True, slots=True)
class TokenPlacement:
    parameter: str
    location: str

@dataclass(frozen=True, slots=True)
class APIRequest:
    url:CompiledMsg
    verb:str|None = None
    custom:CustomCallable|None = None
    headers:ComputedFields|None = None
    params:ComputedFields|None = None
    query:ComputedFields|None = None
    body:ComputedFields|None = None
    json:ComputedFields|None = None
    next_page_token:TokenPlacement|None = None

    @classmethod
    def from_config(cls, ep_spec, global_spec, registry:ConfigRegistry) -> APIRequest:
        url = get_endpoint(ep_spec, global_spec)
        req_spec = ep_spec.get('request', {})

        custom = None
        if 'custom' in req_spec:
            if callable(req_spec['custom']):
                custom = CustomCallable(req_spec['custom'])
            elif req_spec['custom'] in registry and callable(registry[req_spec['custom']]):
                custom = CustomCallable(registry[req_spec['custom']])
            else:
                raise ValidationError(f'custom callable {req_spec["custom"]} is not defined or registered.')

        method = str(ep_spec['method']).upper()

        use_global_headers = ep_spec.get('use_global_headers', True)
        custom_headers = None
        if use_global_headers:
            global_headers = global_spec.get('headers', {})
            ep_headers = ep_spec.get('headers', {})
            custom_headers = get_custom_callable(ep_headers.get('custom'), registry)
            if custom_headers is None:
                custom_headers = get_custom_callable(global_headers.get('custom'), registry)
            h_dict = get_fields(global_headers.get('fields', {}), registry)
            h_dict.update(get_fields(ep_headers.get('fields', {}),registry))
        else:
            ep_headers = ep_spec.get('headers', {})
            custom_headers = get_custom_callable(ep_headers.get('custom'), registry)
            h_dict = get_fields(ep_headers.get('fields', {}),registry)

        headers = ComputedFields(h_dict, custom_headers)

        
        params = ComputedFields(
            get_fields(req_spec.get('path', {}).get('fields', {}),registry),
            get_custom_callable(req_spec.get('path', {}).get('custom', None),registry)
            )
        query = ComputedFields(
            get_fields(req_spec.get('query', {}).get('fields', {}),registry),
            get_custom_callable(req_spec.get('query', {}).get('custom', None),registry)
            )
        body = ComputedFields(
            get_fields(req_spec.get('body', {}).get('fields', {}),registry),
            get_custom_callable(req_spec.get('body', {}).get('custom', None),registry)
            )
        json = ComputedFields(
            get_fields(req_spec.get('json', {}).get('fields', {}),registry),
            get_custom_callable(req_spec.get('json', {}).get('custom', None),registry)
            )

        next_page_token = req_spec.get('next_page_token')
        if next_page_token:
            next_page_token = TokenPlacement(next_page_token['parameter'], next_page_token.get('location', 'query'))

        return cls(CompiledMsg(url), method, custom, headers, params, query, body, json, next_page_token)


    def resolve(self, **context
                ) -> tuple[str, CustomCallable|None, str|None, dict[str,str],dict[str,Any],dict[str,Any],dict[str,Any]]:
        verb = self.verb
        url_params = self.params.resolve(**context) if self.params else {}
        params = self.query.resolve(**context) if self.query else {}
        body = self.body.resolve(**context) if self.body else {}
        json_params = self.json.resolve(**context) if self.json else {}
        
        context['path'] = url_params
        context['query'] = params
        context['body'] = body
        context['json'] = json_params
        headers = self.headers.resolve(**context) if self.headers else {}
        
        # inject next page token if available in context
        token = context.get('next_page_token')
        if token and self.next_page_token:
            if self.next_page_token.location == 'query':
                params[self.next_page_token.parameter] = token
            elif self.next_page_token.location == 'body':
                # if json_params is used (likely), inject there, else body
                if json_params is not None:
                    json_params[self.next_page_token.parameter] = token
                else:
                    body[self.next_page_token.parameter] = token
        
        url = self.url.resolve(**url_params)

        return url, self.custom, verb, headers, params, body, json_params

@dataclass(frozen=True, slots=True)
class APIResponse:
    payload_type:str
    custom:CustomCallable|None = None
    payload_path:CompiledExpr|None=None
    result:ComputedFields|None=None
    items:ComputedFields|None=None
    data:ComputedOHLCFields|None=None
    quote:ComputedQuoteFields|None=None
    next_page_token:CompiledExpr|None=None

    @classmethod
    def from_config(cls, ep_spec, global_spec, registry:ConfigRegistry) -> APIResponse:
        resp_spec = ep_spec.get('response')
        if not resp_spec:
            raise ValidationError(f'no response block defined.')
        
        custom = None
        if 'custom' in resp_spec:
            if callable(resp_spec['custom']):
                custom = CustomCallable(resp_spec['custom'])
            elif resp_spec['custom'] in registry and callable(registry[resp_spec['custom']]):
                custom = CustomCallable(registry[resp_spec['custom']])
            else:
                raise ValidationError(f'custom callable {resp_spec["custom"]} is not defined or registered.')
        
        if custom:
            payload_type = 'custom'
        else:
            payload_type = resp_spec.get('payload_type', 'object')
        
        payload_path = resp_spec.get('payload_path')
        if payload_path:
            payload_path = CompiledExpr(payload_path, registry)
        
        next_page_token = resp_spec.get('next_page_token')
        if next_page_token:
            next_page_token = CompiledExpr(next_page_token, registry)
            
        result = None
        items = None
        data = None
        quote = None
        
        if payload_type == 'data':
            if 'data' in resp_spec:
                data = get_ohlc_fields(resp_spec['data'], registry)
            else:
                 raise ValidationError(f'payload_type is data but no data block defined.')
        elif payload_type == 'quote':
            if 'quote' in resp_spec:
                quote = get_quote_fields(resp_spec['quote'], registry)
            else:
                 raise ValidationError(f'payload_type is quote but no quote block defined.')
        elif payload_type == 'object':
             if 'result' in resp_spec:
                result = ComputedFields(
                    get_fields(resp_spec.get('result',{}).get('fields',{}),registry),
                    get_custom_callable(resp_spec.get('result',{}).get('custom',None),registry),
                    )
        elif payload_type == 'array':
            if 'items' in resp_spec:
                items = ComputedFields(
                    get_fields(resp_spec.get('items',{}).get('fields',{}),registry),
                    get_custom_callable(resp_spec.get('items',{}).get('custom',None),registry)
                    )

        return cls(payload_type, custom, payload_path, result, items, data, quote, next_page_token)

    def resolve(self, response:Any, **context) -> tuple[Any, Any]:
        ctx = context.copy()
        ctx['response'] = response # add raw response to the context

        if self.custom:
            parsed = self.custom.resolve(ctx)
            return parsed, None

        data = response
        
        if self.payload_path:
            # pass response_data as 'response' to the context for extraction
            data = self.payload_path.resolve(**ctx)
        
        token = None
        if self.next_page_token:
            try:
                # pass response as 'response' so we can extract token from anywhere in the response
                token = self.next_page_token.resolve(**ctx)
            except Exception:
                # token extraction might fail if not present (end of pagination), assume None
                token = None

        parsed = data
        if self.payload_type == 'data':
            if self.data:
                parsed = self.data.resolve(data=data, **ctx)
            else:
                parsed = data
        elif self.payload_type == 'quote':
            if self.quote:
                parsed = self.quote.resolve(quote=data, result=data, **ctx)
            else:
                parsed = data
        elif self.payload_type == 'array':
            if self.items and isinstance(data, list):
                parsed = []
                for item in data:
                    parsed.append(self.items.resolve(item=item, **ctx))
            elif isinstance(data, list):
                parsed = data
            else:
                parsed = []
        elif self.payload_type == 'object':
            if self.result:
                parsed = self.result.resolve(result=data, **ctx)
            else:
                parsed = data
        else:
            raise ValidationError(f'unknown payload type {self.payload_type}')
            
        return parsed, token
    
@dataclass(frozen=True, slots=True)
class APIEndPoint:
    """ API Endpoint specification. """
    name:str
    registry:ConfigRegistry
    request:APIRequest
    response:APIResponse
    hooks:dict[str,CustomCallable]
    error_handler:ErrorHandler
    timeout:int
    max_retries:int
    rate_limit:int
    rate_limit_period:int
    api_delay_protection:float

@dataclass(frozen=True, slots=True)
class APIConfig:
    verify_cert:bool
    proxies:dict
    timeout:int
    max_retries:int
    rate_limit:int
    rate_limit_period:int
    api_delay_protection:float
    endpoints:dict[str, APIEndPoint]
    extras:dict[str, Any]

    def resolve(self, **context):
        pass

@dataclass(frozen=True, slots=True)
class MasterDataConfig:
    """ Configuration for fetching and parsing instrument master data """
    preload:bool
    endpoint: APIEndPoint
    mode: str
    format: str
    compression: str
    csv_options: dict[str, Any]
    assets: list[AssetDef]

def get_api_endpoint(name:str, ep_spec:dict[str, Any], global_spec:dict[str, Any], 
                     registry:ConfigRegistry):
    try:
        request = APIRequest.from_config(ep_spec, global_spec, registry)
        response = APIResponse.from_config(ep_spec, global_spec, registry)
    except Exception as e:
        raise ValidationError(f'error in endpoint definition for {name}:{str}') from e

    errors = get_error_specs(ep_spec.get('errors',[]),registry)
    errors.extend(get_error_specs(global_spec.get('errors',[]),registry))
    error_handler = ErrorHandler(errors)

    hooks = get_hooks(global_spec.get('hooks', {}), registry)
    hooks = {**hooks, **get_hooks(ep_spec.get('hooks', {}),registry)}

    timeout = ep_spec.get('timeout') or global_spec['timeout']
    max_retries = ep_spec.get('max_retries') or global_spec['max_retries']
    rate_limit = ep_spec.get('rate_limit') or global_spec['rate_limit']
    rate_limit_period = ep_spec.get('rate_limit_period') or global_spec['rate_limit_period']
    api_delay_protection = ep_spec.get('api_delay_protection') or global_spec['api_delay_protection']

    return APIEndPoint(str(name), registry, request, response, hooks, error_handler, 
                       timeout, max_retries, rate_limit, rate_limit_period, 
                       api_delay_protection)

def get_master_data_config(specs: list[dict[str, Any]], global_spec: dict[str, Any], registry:ConfigRegistry) -> list[MasterDataConfig]:
    """ parse master data config and generate MasterData definition. """
    # No apply_defaults here, assuming it's part of the larger API_SCHEMA validation
    # passed spec corresponds to `master_data` block
    
    if not specs:
        raise ValidationError("master_data configuration is missing")

    configs = []

    for i, spec in enumerate(specs):
        preload = spec.get('preload', True)
        endpoint_spec = spec.get('endpoint')
        if not endpoint_spec:
            raise ValidationError(f"endpoint is required in master_data[{i}]")
            
        endpoint = get_api_endpoint(f"master_data_{i}", endpoint_spec, global_spec, registry)
        
        mode = spec.get('mode', 'list')
        fmt = spec.get('format', 'json')
        compression = spec.get('compression', 'none')
        csv_options = spec.get('csv_options', {})
        
        assets_spec = spec.get('assets', [])
        if not assets_spec:
            raise ValidationError(f"assets list is required in master_data[{i}]")
            
        assets = [AssetDef.from_config(asset_spec, registry) for asset_spec in assets_spec]
        
        configs.append(MasterDataConfig(preload, endpoint, mode, fmt, compression, csv_options, assets))
    
    return configs

def get_api_config(spec:dict[str, Any], registry:ConfigRegistry) -> tuple[APIConfig, list[MasterDataConfig]]:
    """ parse API config and generate API definition, including all endpointts. """
    spec = apply_defaults(API_SCHEMA, spec, registry)
    master_config = spec.pop('master_data', [])
    config = spec.copy()

    verify_cert = bool(config.pop('verify_cert'))
    proxies = dict(config.pop('proxies'))
    timeout = int(config.pop('timeout'))
    max_retries = int(config.pop('max_retries'))
    rate_limit = int(config.pop('rate_limit'))
    rate_limit_period = int(config.pop('rate_limit_period'))
    api_delay_protection = float(config.pop('api_delay_protection'))
    extras:dict[str, Any] = {}
    endpoints:dict[str, APIEndPoint] = {}
    # strip of anything that is not an endpoint or unknown prop
    config.pop('base_url', None)
    config.pop('headers', None)
    config.pop('errors', None)
    config.pop('hooks', None)

    extras = config.pop('extras', {})
    for key in extras:
        extras[key] = config[key]

    endpoints_spec = config.pop('endpoints', {})
    for key, val in endpoints_spec.items():
        endpoint = get_api_endpoint(key, val, spec, registry)
        endpoints[endpoint.name] = endpoint

    api = APIConfig(verify_cert, proxies, timeout, max_retries, rate_limit, rate_limit_period, 
                     api_delay_protection, endpoints, extras)
    master = get_master_data_config(master_config, spec, registry)

    return api, master


__all__ = ['get_api_config',
           'APIConfig',
           'MasterDataConfig',
           ]