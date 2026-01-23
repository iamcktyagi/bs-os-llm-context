from __future__ import annotations
import pandas as pd

from blueshift.lib.common.constants import Frequency
from blueshift.lib.common.functions import merge_json_recursive, to_title_case
from blueshift.lib.trades._order_types import (
    OrderSide, ProductType, OrderType, OrderValidity, OrderStatus)
from blueshift.calendar import get_calendar

from blueshift.brokers.core.config.config import APIBrokerConfig
from blueshift.brokers.core.broker import RestAPIBroker
from blueshift.brokers.core.config.resolver import ConfigRegistry
from blueshift.brokers.factory import broker_class_factory

registry = ConfigRegistry(globals())
cal = get_calendar("NYSE")

@registry.register()
def alpaca_timeframe(freq, **kwargs):
    mapping = {
        Frequency("1m"): "1Min",
        Frequency("1d"): "1Day",
    }
    return mapping.get(freq, "1Min")

BROKER_SPEC = {
    "calendar": "NYSE",
    "credentials": {
        "fields": ["api_key", "api_secret"],
        "validator": "credentials.api_key and credentials.api_secret",
    },
    "options": {
        "timeout": 10,
        "rate_limit": 180,
        "rate_limit_period": 60,
        "supported_modes": ["LIVE", "PAPER"],
    },
    "assets": ["equity"],
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
    "order_validity": {
        "map": {
            OrderValidity.DAY: "day",
            OrderValidity.IOC: "ioc",
            OrderValidity.FOK: "fok",
        },
        "default_value": OrderValidity.DAY,
    },
    "order_status": {
        "map": {
            OrderStatus.OPEN: ["new", "accepted", "partially_filled"],
            OrderStatus.COMPLETE: "filled",
            OrderStatus.CANCELLED: ["canceled", "expired"],
            OrderStatus.REJECTED: "rejected",
        },
        "default_value": OrderStatus.OPEN,
    },
    "product_type": {
        "map": {
            ProductType.MARGIN: "margin",
            ProductType.DELIVERY: "cash",
        },
        "default_value": ProductType.MARGIN,
    },
}

API_SPEC = {
    "base_url": "https://paper-api.alpaca.markets",
    "headers": {
        "fields": {
            "APCA-API-KEY-ID": {"source": "credentials.api_key"},
            "APCA-API-SECRET-KEY": {"source": "credentials.api_secret"},
            "Accept": {"source": "'application/json'"},
            "Content-Type": {"source": "'application/json'"},
        }
    },
    "endpoints": {
        "get_account": {
            "endpoint": "/v2/account",
            "method": "GET",
            "response": {
                "payload_type": "object",
                "result": {
                    "fields": {
                        "account_number": {"source": "result.get('account_number')"},
                        "cash": {"source": "result.get('cash')"},
                        "currency": {"source": "result.get('currency', 'USD')"},
                    }
                },
            },
        },
        "get_orders": {
            "endpoint": "/v2/orders",
            "method": "GET",
            "response": {
                "payload_type": "array",
                "items": {
                    "fields": {
                        "oid": {"source": "item['id']"},
                        "symbol": {"source": "item.get('symbol')"},
                        "status": {"source": "mappings.order_status.to_blueshift(item.get('status'))"},
                    }
                },
            },
        },
        "place_order": {
            "endpoint": "/v2/orders",
            "method": "POST",
            "request": {
                "json": {
                    "fields": {
                        "symbol": {"source": "order.asset.broker_symbol or order.asset.symbol"},
                        "qty": {"source": "int(order.quantity)"},
                        "side": {"source": "mappings.order_side.from_blueshift(order.side)"},
                        "type": {"source": "mappings.order_type.from_blueshift(order.order_type)"},
                        "time_in_force": {"source": "mappings.order_validity.from_blueshift(order.order_validity)"},
                    }
                }
            },
            "response": {
                "payload_type": "object",
                "result": {
                    "fields": {
                        "order_id": {"source": "result['id']"},
                    }
                },
            },
        },
        "cancel_order": {
            "endpoint": "/v2/orders/{order_id}",
            "method": "DELETE",
            "request": {
                "path": {
                    "fields": {
                        "order_id": {"source": "order.broker_order_id or order.oid"},
                    }
                }
            },
            "response": {
                "payload_type": "object",
                "result": {
                    "fields": {
                        "order_id": {"source": "order.broker_order_id or order.oid"},
                    }
                },
            },
        },
        "get_history": {
            "endpoint": "https://data.alpaca.markets/v2/stocks/{symbol}/bars",
            "method": "GET",
            "request": {
                "path": {"fields": {"symbol": {"source": "asset.symbol"}}},
                "query": {
                    "fields": {
                        "timeframe": {"source": "alpaca_timeframe(freq)"},
                        "start": {"source": "from_dt.tz_convert('Etc/UTC').isoformat()"},
                        "end": {"source": "to_dt.tz_convert('Etc/UTC').isoformat()"},
                    }
                },
            },
            "response": {
                "payload_type": "data",
                "payload_path": "response.get('bars', [])",
                "data": {
                    "frame": "records",
                    "rename": {
                        "timestamp": "t",
                        "open": "o",
                        "high": "h",
                        "low": "l",
                        "close": "c",
                        "volume": "v",
                    },
                    "timestamp": {"unit": "s"},
                },
            },
        },
    },
}

STREAMING_SPEC = {
    "connections": {
        "market_data": {
            "url": "wss://stream.data.alpaca.markets/v2/iex",
            "backend": {"type": "websocket"},
            "streams": ["data", "quote"],
            "auth": {
                "mode": "first_message",
                "first_message": {
                    "format": "json",
                    "json": {
                        "fields": {
                            "action": {"source": "'auth'"},
                            "key": {"source": "credentials.api_key"},
                            "secret": {"source": "credentials.api_secret"},
                        }
                    },
                },
            },
        }
    }
}

OBJECTS_SPEC: dict = {}
MASTER_DATA_SPEC: list = []

BROKER_VARIANTS = {
    "paper": {
        "broker": {"name": "alpaca-paper", "variant": "paper"},
        "api": {"base_url": "https://paper-api.alpaca.markets"},
    },
    "live": {
        "broker": {"name": "alpaca-live", "variant": "live"},
        "api": {"base_url": "https://api.alpaca.markets"},
    },
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
