from __future__ import annotations
from typing import Any, Type
from dataclasses import dataclass

from blueshift.lib.common.enums import BlotterType, AlgoMode, ExecutionMode
from blueshift.interfaces.trading.rms import IRMS, rms_factory
from blueshift.interfaces.assets.assets import get_asset_type, MarketData
from blueshift.lib.exceptions import ValidationError
from blueshift.lib.common.functions import str2bool
from blueshift.lib.common.constants import CCY, Currency
from blueshift.calendar import get_calendar, TradingCalendar
from blueshift.lib.trades._order_types import (
    OrderType, OrderValidity, ProductType, OrderSide, OrderStatus)
from blueshift.interfaces.assets._assets import OptionType
from blueshift.interfaces.assets.opt_chains import ExpiryType
from blueshift.lib.common.constants import Frequency
from .resolver import (
    EnumMap, get_enum_map, get_fields, get_condition, ComputedFields, CompiledCondition,
    ConfigRegistry)

from .schema import apply_defaults, BROKER_SCHEMA

@dataclass(frozen=True, slots=True)
class APICredentials:
    """ API credentials store. """
    credentials:dict[str,str|None]|dict[str,None]
    setters:ComputedFields|None
    validator:CompiledCondition|None

    @classmethod
    def from_config(cls, spec, registry):
        fields = spec['fields']
        if not isinstance(fields, list) or len(fields) == 0:
            raise ValidationError(f'expected a non-empty credentials fields list')
        
        credentials = {str(f):None for f in fields}

        validator = setters = None

        if 'setters' in spec:
            setters = ComputedFields(get_fields(spec['setters'], registry))

        if 'validator' in spec:
            validator = get_condition(spec['validator'], registry)
            
        return cls(credentials, setters, validator)

    def __getattr__(self, name):
        if name in self.credentials:
            return self.credentials[name]
        
        raise AttributeError(f'attribute {name} is not available')
    
    def update(self, **kwargs):
        self.resolve(**kwargs)
    
    def resolve(self, **context):
        generated = {}
        
        for key in self.credentials:
            if key in context and key not in generated:
                generated[key] = context[key]

        if self.setters:
            ctx = context.copy()
            ctx['credentials'] = self
            generated = self.setters.resolve(**ctx)

        for k in generated:
            self.credentials[k] = generated[k]
    
    def validate(self, **context) -> bool:
        if not self.validator:
            return True
        
        ctx = context.copy()
        ctx['credentials'] = self
        res = self.validator.resolve(**ctx)
        
        if res:
            return True
        
        return False

@dataclass(frozen=True, slots=True)
class BrokerOptions:
    agent: str
    interafce:str
    timeout: float
    fractional_trading: bool
    lazy_session:bool
    check_host_before_trade: bool
    unwind_before_trade: bool
    intraday_cutoff: list[int]
    ccy: Currency
    blotter_type: BlotterType
    data_columns: list[str]
    expiry_dispatch:dict[str,str]
    rms: IRMS|dict[str, Any]
    stale_data_tolerance: float
    supported_modes: list[AlgoMode]
    execution_modes: list[ExecutionMode]
    max_tickers: int
    max_subscription: int
    multi_assets_data_query: bool
    multi_assets_data_subscribe: bool
    max_nbars: int
    max_data_fetch: int
    max_page_fetch: int

    def resolve(self, **context):
        if isinstance(self.rms, dict):
            # the only late evaluation required is for instantiating the RMS
            broker = context['broker']
            rms_config=self.rms.copy()
            rms = rms_config.pop('rms')
            rms_config['broker'] = broker
            rms = rms_factory(rms, **rms_config)
            object.__setattr__(self, 'rms', rms)

@dataclass(frozen=True, slots=True)
class Mappings:
    order_type:EnumMap
    order_validity:EnumMap
    order_side:EnumMap
    order_status:EnumMap
    option_type:EnumMap
    expiry_type:EnumMap
    product_type:EnumMap
    data_frequency:EnumMap

@dataclass(frozen=True, slots=True)
class BrokerConfig:
    name: str
    variant:str
    display_name: str
    version: str
    description: str
    calendar: TradingCalendar
    credentials: APICredentials
    options: BrokerOptions
    assets: list[Type[MarketData]]
    mappings:Mappings

    def resolve(self, **context):
        self.options.resolve(**context)
        self.credentials.resolve(**context)

def get_broker_options(spec: dict[str, Any]) -> BrokerOptions:
    """ parse broker options from spec. """
    supported_modes=spec['supported_modes']
    supported_modes = [AlgoMode(m.upper()) if isinstance(m,str) 
                       else AlgoMode(m) for m in supported_modes]
    
    execution_modes=spec['execution_modes']
    execution_modes = [ExecutionMode(m.upper()) if isinstance(m,str) 
                       else ExecutionMode(m) for m in execution_modes]

    return BrokerOptions(
        agent=str(spec['agent']),
        interafce=str(spec.get('interface',"broker")),
        timeout=int(spec['timeout']),
        fractional_trading=str2bool(spec['fractional_trading']),
        lazy_session=str2bool(spec['lazy_session']),
        check_host_before_trade=str2bool(spec['check_host_before_trade']),
        unwind_before_trade=str2bool(spec['unwind_before_trade']),
        intraday_cutoff=spec['intraday_cutoff'],
        ccy=CCY(spec['ccy']), # type: ignore
        blotter_type=BlotterType(spec['blotter_type']),
        data_columns=spec['data_columns'],
        expiry_dispatch=spec['expiry_dispatch'],
        rms=spec['rms'],
        stale_data_tolerance=int(spec['stale_data_tolerance']),
        supported_modes=supported_modes,
        execution_modes=execution_modes,
        max_tickers=int(spec['max_tickers']),
        max_subscription=int(spec['max_subscription']),
        multi_assets_data_query=str2bool(spec['multi_assets_data_query']),
        multi_assets_data_subscribe=str2bool(spec['multi_assets_data_subscribe']),
        max_nbars=int(spec['max_nbars']),
        max_data_fetch=int(spec['max_data_fetch']),
        max_page_fetch=int(spec['max_page_fetch']),
    )

def get_broker_config(spec: dict[str, Any], registry:ConfigRegistry) -> BrokerConfig:
    """ parse broker config and generate Broker definition. """
    spec = apply_defaults(BROKER_SCHEMA, spec, registry)
    config = spec.copy()

    name = str(config.pop('name'))
    variant = str(config.pop('variant',name))
    display_name = str(config.pop('display_name', name))
    version = str(config.pop('version','0.0.0'))
    description = str(config.pop('description', f'Blueshift Broker implementation for {name}'))

    freqs = config.pop('data_frequencies')
    if 'map' in freqs:
        for k,v in freqs['map'].copy().items():
            freqs['map'][Frequency(k)] = v


    assets = config.pop('assets')
    assets = [get_asset_type(asset) for asset in assets]
    options = get_broker_options(config.pop('options'))
    data_frequency_map = get_enum_map(freqs, registry)
    order_type_map = get_enum_map(config.pop('order_type', {}), registry, OrderType)
    order_validity_map = get_enum_map(config.pop('order_validity', {}), registry, OrderValidity)
    order_side_map = get_enum_map(config.pop('order_side', {}), registry, OrderSide)
    order_status_map = get_enum_map(config.pop('order_status', {}), registry, OrderStatus)
    product_type_map = get_enum_map(config.pop('product_type', {}), registry, ProductType)
    option_type_map = get_enum_map(config.pop('option_type', {}), registry, OptionType)
    expiry_type_map = get_enum_map(config.pop('expiry_type', {}), registry, ExpiryType)
    calendar = get_calendar(config.pop('calendar'))
    credentials = APICredentials.from_config(config.pop('credentials'), registry)

    mappings = Mappings(
        order_type=order_type_map, 
        order_validity=order_validity_map, 
        order_side=order_side_map,
        order_status=order_status_map,
        option_type=option_type_map,
        product_type=product_type_map,
        expiry_type=expiry_type_map,
        data_frequency=data_frequency_map)

    return BrokerConfig(
        name, variant, display_name, version, description, calendar, credentials,
        options, assets, mappings)

__all__ = ['get_broker_config', 'BrokerConfig', 'BrokerOptions', 'APICredentials']
