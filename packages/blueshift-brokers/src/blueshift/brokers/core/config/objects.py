from __future__ import annotations
from typing import Any, Callable
from dataclasses import dataclass

from blueshift.interfaces.assets.assets import asset_factory
from blueshift.lib.trades._order import Order
from blueshift.lib.trades._position import Position
from blueshift.lib.trades._accounts import Account

from .resolver import (
    ConfigRegistry, ToObject, CompiledCondition, CustomCallable, ComputedFields,
    get_condition, get_fields, CompiledExpr, ToObject
)
from .schema import apply_defaults, OBJECTS_SCHEMA
from blueshift.lib.exceptions import ValidationError

@dataclass(frozen=True, slots=True)
class ObjectConfig:
    asset: list[ToObject]
    order: list[ToObject]
    position: list[ToObject]
    account: list[ToObject]

    def resolve(self, **context):
        # Pipelines don't need global resolution, they are invoked per-item
        pass

def get_pipeline(spec: list[dict[str, Any]], target: Any|Callable, registry: ConfigRegistry) -> list[ToObject]:
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
            else:
                 raise ValidationError(f"Invalid custom function definition: {c}")

        if 'fields' in rule_spec:
            fields = ComputedFields(get_fields(rule_spec['fields'], registry))
            
        rules.append(ToObject(target, condition, custom, fields))
        
    return list(rules)

def get_objects_config(spec: dict[str, Any], registry: ConfigRegistry) -> ObjectConfig:
    spec = apply_defaults(OBJECTS_SCHEMA, spec, registry)
    
    asset = get_pipeline(spec.get('asset', []), asset_factory, registry) if 'asset' in spec else []
    order = get_pipeline(spec.get('order', []), Order, registry) if 'order' in spec else []
    position = get_pipeline(spec.get('position', []), Position, registry) if 'position' in spec else []
    account = get_pipeline(spec.get('account', []), Account, registry) if 'account' in spec else []
    
    return ObjectConfig(asset, order, position, account)

__all__ = ['get_objects_config', 'ObjectConfig']