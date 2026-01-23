"""
[Broker Name] REST API broker integration for Blueshift.
"""
from __future__ import annotations
import pandas as pd
import json
import hashlib
import hmac
import base64

# Blueshift Imports
from blueshift.interfaces.assets._assets import InstrumentType, OptionType
from blueshift.lib.common.constants import Frequency
from blueshift.lib.common.functions import merge_json_recursive, to_title_case
from blueshift.lib.trades._order_types import (
    OrderSide, ProductType, OrderType, OrderValidity, OrderStatus)
from blueshift.calendar import get_calendar

from blueshift.brokers.core.config.config import APIBrokerConfig
from blueshift.brokers.core.broker import RestAPIBroker
from blueshift.brokers.core.config.resolver import ConfigRegistry
from blueshift.brokers.factory import broker_class_factory

# -------------------------------------------------------------------------
# Registry Initialization
# -------------------------------------------------------------------------
registry = ConfigRegistry(globals())
cal = get_calendar('NYSE') # Change to appropriate calendar

# -------------------------------------------------------------------------
# Helper Functions (Registered)
# -------------------------------------------------------------------------
@registry.register()
def my_broker_auth_header(api_key, api_secret, **kwargs):
    """ Example auth header generator """
    return f"Basic {api_key}:{api_secret}"

@registry.register()
def my_broker_timeframe(freq, **kwargs):
    """ Map Frequency to Broker Interval String """
    mapping = {
        Frequency('1m'): '1min',
        Frequency('1d'): 'day'
    }
    return mapping.get(freq, '1min')

# -------------------------------------------------------------------------
# Configuration Specifications
# -------------------------------------------------------------------------
BROKER_SPEC = {
    "calendar": "NYSE",
    "credentials": {
        "fields": ["api_key", "api_secret"],
        "validator": "credentials.api_key and credentials.api_secret",
    },
    "options": {
        "timeout": 10,
        "rate_limit": 10,
        "rate_limit_period": 1,
    },
    "assets": ["equity"],
    
    # ENUM MAPPINGS
    "order_side": {
        "map": {
            OrderSide.BUY: "buy",
            OrderSide.SELL: "sell",
        }
    },
    "order_type": {
        "map": {
            OrderType.MARKET: "market",
            OrderType.LIMIT: "limit",
        },
        "default_value": OrderType.MARKET,
    },
    # Add other mappings: order_validity, order_status, product_type
}

API_SPEC = {
    "base_url": "https://api.broker.com",
    "headers": {
        "fields": {
            "Authorization": {"source": "my_broker_auth_header(credentials.api_key, credentials.api_secret)"}
        }
    },
    "endpoints": {
        "get_account": {
            "endpoint": "/account",
            "method": "GET",
            "response": {
                "payload_type": "object",
                "result": {
                    "fields": {
                        "name": {"source": "result['id']"},
                        "cash": {"source": "float(result['balance'])"},
                        "currency": {"source": "'USD'"}
                    }
                }
            }
        },
        # Add get_orders, place_order, cancel_order, get_positions, get_history, get_quote
    }
}

STREAMING_SPEC = {
    # Optional: WebSocket configuration
}

OBJECTS_SPEC = {
    # Optional: Override default object conversion if needed
}

MASTER_DATA_SPEC = [
    # Optional: Asset universe configuration
]

# -------------------------------------------------------------------------
# Registration Logic
# -------------------------------------------------------------------------
BROKER_VARIANTS = {
    'live': {
        "broker": {"name": "mybroker-live", "variant": "live"},
        "api": {"base_url": "https://api.broker.com"}
    },
    'paper': {
        "broker": {"name": "mybroker-paper", "variant": "paper"},
        "api": {"base_url": "https://paper-api.broker.com"}
    }
}

def create_config(variant) -> APIBrokerConfig:
    broker_spec = dict(BROKER_SPEC)
    api_spec = dict(API_SPEC)
    obj_spec = dict(OBJECTS_SPEC)
    stream_spec = dict(STREAMING_SPEC)
    api_spec["master_data"] = MASTER_DATA_SPEC.copy()

    variant_specs = dict(BROKER_VARIANTS).get(variant)
    if variant_specs:
        broker_spec = merge_json_recursive(broker_spec, variant_specs.get("broker", {}))
        api_spec = merge_json_recursive(api_spec, variant_specs.get("api", {}))
        obj_spec = merge_json_recursive(obj_spec, variant_specs.get("objects", {}))
        stream_spec = merge_json_recursive(stream_spec, variant_specs.get("streaming", {}))
        broker_spec["variant"] = variant

    return APIBrokerConfig(broker_spec, api_spec, obj_spec, stream_spec, registry=registry)

def register_brokers():
    for variant in BROKER_VARIANTS:
        config = create_config(variant)
        cls_name = to_title_case(config.broker.name)
        broker_class_factory(cls_name, config)
