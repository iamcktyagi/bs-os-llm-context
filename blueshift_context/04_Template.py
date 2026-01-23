"""
[Broker Name] REST API broker integration for Blueshift.

Implements:
- Broker spec (credentials, options, enum mappings)
- API spec (trading + market data endpoints)
- Streaming spec (WebSocket/SocketIO real-time data)
- Objects spec (order/position/account conversions)
- Master data spec (asset universe)
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
cal = get_calendar('NYSE')  # Change to appropriate calendar (NYSE, NSE, etc.)

# -------------------------------------------------------------------------
# Helper Functions (Registered)
# -------------------------------------------------------------------------
@registry.register()
def my_broker_timeframe(freq, **kwargs):
    """Map Blueshift Frequency -> Broker interval string."""
    mapping = {
        Frequency('1m'): '1Min',
        Frequency('5m'): '5Min',
        Frequency('15m'): '15Min',
        Frequency('1h'): '1Hour',
        Frequency('1d'): '1Day',
    }
    return mapping.get(freq, '1Min')

# Example: custom auth header signing (for brokers requiring HMAC/signing)
@registry.register()
def sign_request(url, verb, headers, query, body, json, credentials, **kwargs):
    """
    Hook: Sign request with HMAC before sending.
    Must return: (url, verb, headers, query, body, json)
    """
    payload = json.dumps(body) if body else ""
    signature = hmac.new(
        credentials.api_secret.encode(), payload.encode(), hashlib.sha256
    ).hexdigest()
    headers['X-Signature'] = signature
    headers['X-Timestamp'] = str(pd.Timestamp.now(tz='UTC'))
    return url, verb, headers, query, body, json

# Example: session token storage after login
@registry.register()
def store_session_token(response, credentials, **kwargs):
    """Hook: Store access token from login response into credentials."""
    token = response.get('access_token') or response.get('token')
    if token:
        credentials.update(access_token=token)

# Example: custom session initialization for complex auth flows
@registry.register()
def custom_session_init(**context):
    """Custom: Handle multi-step auth (TOTP, OAuth, encryption)."""
    credentials = context['credentials']
    api = context['api']
    url = context['url']
    verb = context['verb']

    # Build login payload
    body = json.dumps({"key": credentials.api_key, "secret": credentials.api_secret})
    headers = {"Content-Type": "application/json"}

    success, status_code, resp = api.raw_request(url, verb, headers, {}, body, {})
    if not success:
        from blueshift.lib.exceptions import AuthenticationError
        raise AuthenticationError(f"Login failed: {status_code}")

    credentials.update(access_token=resp.get('token'))

# -------------------------------------------------------------------------
# BROKER_SPEC
# -------------------------------------------------------------------------
BROKER_SPEC = {
    "name": "mybroker",
    "calendar": "NYSE",
    "credentials": {
        "fields": ["api_key", "api_secret", "access_token"],
        "validator": "credentials.api_key and credentials.api_secret",
    },
    "options": {
        "timeout": 10,
        "rate_limit": 10,
        "rate_limit_period": 1,
        "max_tickers": 200,
        "supported_modes": ["LIVE", "PAPER"],
        "fractional_trading": False,
        "max_page_fetch": 10,
    },
    "assets": ["equity"],

    # ENUM MAPPINGS
    "order_side": {
        "map": {OrderSide.BUY: "buy", OrderSide.SELL: "sell"}
    },
    "order_type": {
        "map": {OrderType.MARKET: "market", OrderType.LIMIT: "limit", OrderType.STOP: "stop"},
        "default_value": OrderType.MARKET,
    },
    "order_validity": {
        "map": {OrderValidity.DAY: "day", OrderValidity.IOC: "ioc", OrderValidity.GTC: "gtc"},
        "default_value": OrderValidity.DAY,
    },
    "order_status": {
        "map": {
            OrderStatus.OPEN: ["new", "partially_filled", "pending_new"],
            OrderStatus.COMPLETE: "filled",
            OrderStatus.CANCELLED: ["canceled", "expired"],
            OrderStatus.REJECTED: "rejected",
        },
        "default_value": OrderStatus.OPEN,
    },
    "product_type": {
        "map": {ProductType.DELIVERY: "delivery", ProductType.MARGIN: "margin"},
        "default_value": ProductType.DELIVERY,
    },
    "data_frequencies": {
        "map": {
            Frequency('1m'): "1Min",
            Frequency('1d'): "1Day",
        }
    },
}

# -------------------------------------------------------------------------
# API_SPEC
# -------------------------------------------------------------------------
COMMON_HEADERS = {
    "fields": {
        "Authorization": {"source": "f'Bearer {credentials.access_token}'"},
        "Content-Type": {"source": "'application/json'"},
    }
}

API_SPEC = {
    "base_url": "https://api.broker.com/v1",
    "headers": COMMON_HEADERS,
    # Global error rules (applied to all endpoints unless overridden)
    "errors": [
        {
            "condition": "status_code == 401",
            "exception": "AuthenticationError",
            "message": "Authentication failed or token expired"
        },
        {
            "condition": "status_code == 403",
            "exception": "ValidationError",
            "message": "Permission denied"
        },
        {
            "condition": "status_code == 429",
            "exception": "APIException",
            "message": "Rate limit exceeded"
        },
        {
            "condition": "not success and status_code >= 500",
            "exception": "ServerError",
            "message": "Broker server error"
        },
    ],
    "endpoints": {
        # --- Authentication (Optional) ---
        "session": {
            "endpoint": "/auth/token",
            "method": "POST",
            "request": {
                "custom": "custom_session_init",  # Complex auth -> use custom
            },
            "response": {
                "payload_type": "custom",
                "custom": "noop",  # custom request already handled everything
            },
        },

        # --- Account ---
        "get_account": {
            "endpoint": "/account",
            "method": "GET",
            "response": {
                "payload_type": "object",
                "result": {
                    "fields": {
                        "name": {"source": "result.get('account_number', 'default')"},
                        "cash": {"source": "float(result.get('cash', 0))"},
                        "currency": {"source": "'USD'"},
                    }
                }
            }
        },

        # --- Orders ---
        "get_orders": {
            "endpoint": "/orders",
            "method": "GET",
            "request": {
                "query": {
                    "fields": {
                        "status": {"source": "'all'"},
                        "limit": {"source": "100"},
                    }
                },
                # Pagination: inject token as query param on subsequent pages
                "next_page_token": {
                    "parameter": "after",
                    "location": "query"
                }
            },
            "response": {
                "payload_type": "array",
                "payload_path": "response.get('orders', response)",
                # Pagination: extract next page token from response
                "next_page_token": "response.get('next_page_token')",
                "items": {
                    "fields": {
                        "oid": {"source": "item['id']"},
                        "broker_order_id": {"source": "item['id']"},
                        "symbol": {"source": "item['symbol']"},
                        "quantity": {"source": "float(item['qty'])"},
                        "filled": {"source": "float(item.get('filled_qty', 0))"},
                        "price": {"source": "float(item.get('limit_price') or 0)"},
                        "average_price": {"source": "float(item.get('filled_avg_price') or 0)"},
                        "side": {"source": "mappings.order_side.to_blueshift(item['side'])"},
                        "order_type": {"source": "mappings.order_type.to_blueshift(item['type'])"},
                        "status": {"source": "mappings.order_status.to_blueshift(item['status'])"},
                        "timestamp": {"source": "pd.Timestamp(item['created_at'])"},
                        "exchange_timestamp": {"source": "pd.Timestamp(item.get('filled_at') or item['created_at'])"},
                    }
                }
            }
        },

        "place_order": {
            "endpoint": "/orders",
            "method": "POST",
            "request": {
                "json": {
                    "fields": {
                        "symbol": {"source": "order.asset.broker_symbol or order.asset.symbol"},
                        "qty": {"source": "str(int(order.quantity))"},
                        "side": {"source": "mappings.order_side.from_blueshift(order.side)"},
                        "type": {"source": "mappings.order_type.from_blueshift(order.order_type)"},
                        "time_in_force": {"source": "mappings.order_validity.from_blueshift(order.order_validity)"},
                        # Conditional: only include limit_price for limit orders
                        "limit_price": {
                            "source": "str(order.price)",
                            "condition": "order.order_type == OrderType.LIMIT"
                        },
                    }
                }
            },
            # Per-endpoint error rules (checked before global)
            "errors": [
                {
                    "condition": "not success and 'insufficient' in str(response.get('message','')).lower()",
                    "exception": "OrderError",
                    "message": "Insufficient funds"
                }
            ],
            "response": {
                "payload_type": "object",
                "result": {
                    "fields": {
                        "order_id": {"source": "str(result['id'])"}
                    }
                }
            }
        },

        "cancel_order": {
            "endpoint": "/orders/{order_id}",
            "method": "DELETE",
            "request": {
                "path": {
                    "fields": {
                        "order_id": {"source": "order.broker_order_id or order.oid"}
                    }
                }
            },
            "response": {
                "payload_type": "object",
                "result": {
                    "fields": {
                        "order_id": {"source": "str(result.get('id', order.oid))"}
                    }
                }
            }
        },

        "update_order": {
            "endpoint": "/orders/{order_id}",
            "method": "PATCH",
            "request": {
                "path": {
                    "fields": {
                        "order_id": {"source": "order.broker_order_id or order.oid"}
                    }
                },
                "json": {
                    "fields": {
                        "qty": {"source": "str(int(quantity))", "condition": "quantity is not None"},
                        "limit_price": {"source": "str(price)", "condition": "price is not None"},
                    }
                }
            },
            "response": {
                "payload_type": "object",
                "result": {
                    "fields": {
                        "order_id": {"source": "str(result['id'])"}
                    }
                }
            }
        },

        # --- Positions ---
        "get_positions": {
            "endpoint": "/positions",
            "method": "GET",
            "response": {
                "payload_type": "array",
                "payload_path": "response.get('positions', response) if isinstance(response, list) else response.get('positions', [])",
                "items": {
                    "fields": {
                        "symbol": {"source": "item['symbol']"},
                        "quantity": {"source": "float(item['qty'])"},
                        "average_price": {"source": "float(item['avg_entry_price'])"},
                        "last_price": {"source": "float(item['current_price'])"},
                        "product_type": {"source": "ProductType.DELIVERY"},
                    }
                }
            }
        },

        # --- Market Data ---
        "get_history": {
            "endpoint": "/bars/{symbol}",
            "method": "GET",
            "request": {
                "path": {
                    "fields": {
                        "symbol": {"source": "asset.broker_symbol or asset.symbol"}
                    }
                },
                "query": {
                    "fields": {
                        "timeframe": {"source": "my_broker_timeframe(freq)"},
                        "start": {"source": "from_dt.isoformat()"},
                        "end": {"source": "to_dt.isoformat()"},
                        "limit": {"source": "nbars"},
                    }
                }
            },
            "response": {
                "payload_type": "data",
                "payload_path": "response.get('bars', [])",
                "data": {
                    "frame": "records",
                    "timestamp": {"field": "t", "format": "%Y-%m-%dT%H:%M:%SZ"},
                    # rename: {Target_Standard: Source_API}
                    "rename": {"open": "o", "high": "h", "low": "l", "close": "c", "volume": "v"}
                }
            }
        },

        "get_quote": {
            "endpoint": "/quotes/{symbol}",
            "method": "GET",
            "request": {
                "path": {
                    "fields": {
                        "symbol": {"source": "asset.broker_symbol or asset.symbol"}
                    }
                }
            },
            "response": {
                "payload_type": "quote",
                "payload_path": "response.get('quote', response)",
                "quote": {
                    "last": {"source": "float(quote.get('last_price', 0))"},
                    "bid": {"source": "float(quote.get('bid', 0))"},
                    "bid_volume": {"source": "float(quote.get('bid_size', 0))"},
                    "ask": {"source": "float(quote.get('ask', 0))"},
                    "ask_volume": {"source": "float(quote.get('ask_size', 0))"},
                }
            }
        },
    }
}

# -------------------------------------------------------------------------
# STREAMING_SPEC
# -------------------------------------------------------------------------
STREAMING_SPEC = {
    "connections": {
        "market_data": {
            "url": "wss://stream.broker.com/v1/stream",
            "backend": {"type": "websocket", "options": {}},
            "streams": ["data", "quote"],

            # Authentication
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
                    }
                }
            },

            # Subscribe/Unsubscribe (per-channel)
            "subscribe": {
                "subscribe": {
                    "data": {
                        "format": "json",
                        "json": {
                            "fields": {
                                "action": {"source": "'subscribe'"},
                                "trades": {"source": "[a.broker_symbol or a.symbol for a in subscribe_assets]"},
                            }
                        }
                    },
                    "quote": {
                        "format": "json",
                        "json": {
                            "fields": {
                                "action": {"source": "'subscribe'"},
                                "quotes": {"source": "[a.broker_symbol or a.symbol for a in subscribe_assets]"},
                            }
                        }
                    }
                },
                "unsubscribe": {
                    "data": {
                        "format": "json",
                        "json": {
                            "fields": {
                                "action": {"source": "'unsubscribe'"},
                                "trades": {"source": "[a.broker_symbol or a.symbol for a in subscribe_assets]"},
                            }
                        }
                    }
                }
            },

            # Message Parsing
            "parser": {
                "format": "json",
                "payload_path": "data.get('data', data)",  # Extract inner payload if wrapped
            },

            # Message Routing (required for multi-stream connections)
            "router": {
                "enabled": True,
                "rules": [
                    {"channel": "data", "match": "data.get('type') == 'trade'"},
                    {"channel": "quote", "match": "data.get('type') == 'quote'"},
                ],
                "default_channel": "data"
            },

            # Converters: Raw message -> Blueshift objects
            "converters": {
                "data": [
                    {
                        "fields": {
                            "asset": {"source": "broker.infer_asset(symbol=data['symbol'])"},
                            "timestamp": {"source": "pd.Timestamp(data['timestamp'])"},
                            "data": {
                                "close": {"source": "float(data['price'])"},
                                "volume": {"source": "float(data.get('size', 0))"},
                            }
                        }
                    }
                ],
                "quote": [
                    {
                        "fields": {
                            "asset": {"source": "broker.infer_asset(symbol=data['symbol'])"},
                            "timestamp": {"source": "pd.Timestamp(data['timestamp'])"},
                            "last": {"source": "float(data.get('last', 0))"},
                            "bid": {"source": "float(data.get('bid', 0))"},
                            "ask": {"source": "float(data.get('ask', 0))"},
                        }
                    }
                ]
            }
        }
    }
}

# -------------------------------------------------------------------------
# OBJECTS_SPEC
# -------------------------------------------------------------------------
OBJECTS_SPEC = {
    "order": [
        {
            "fields": {
                "oid": {"source": "str(data['id'])"},
                "broker_order_id": {"source": "str(data['id'])"},
                "quantity": {"source": "float(data['qty'])"},
                "filled": {"source": "float(data.get('filled_qty', 0))"},
                "price": {"source": "float(data.get('limit_price') or 0)"},
                "average_price": {"source": "float(data.get('filled_avg_price') or 0)"},
                "side": {"source": "mappings.order_side.to_blueshift(data['side'])"},
                "order_type": {"source": "mappings.order_type.to_blueshift(data['type'])"},
                "status": {"source": "mappings.order_status.to_blueshift(data['status'])"},
                "timestamp": {"source": "pd.Timestamp(data['created_at'])"},
            }
        }
    ],
    "position": [
        {
            "fields": {
                "quantity": {"source": "float(data['qty'])"},
                "average_price": {"source": "float(data['avg_entry_price'])"},
                "last_price": {"source": "float(data['current_price'])"},
            }
        }
    ],
}

# -------------------------------------------------------------------------
# MASTER_DATA_SPEC (Optional but recommended)
# -------------------------------------------------------------------------
MASTER_DATA_SPEC = [
    # Example: Fetch instrument list from broker API
    # {
    #     "endpoint": {
    #         "endpoint": "/assets",
    #         "method": "GET",
    #         "response": {"payload_type": "array", "items": {"fields": {}}}
    #     },
    #     "mode": "api",     # "api" or "file"
    #     "format": "json",  # "json" or "csv"
    #     "assets": [
    #         {
    #             "asset_class": "equity",
    #             "filter": "item.get('status') == 'active' and item.get('tradable')",
    #             "mapping": {
    #                 "symbol": {"source": "item['symbol']"},
    #                 "security_id": {"source": "item.get('id', item['symbol'])"},
    #                 "name": {"source": "item.get('name', item['symbol'])"},
    #                 "exchange_name": {"source": "item.get('exchange', 'NYSE')"},
    #             }
    #         }
    #     ]
    # }
]

# -------------------------------------------------------------------------
# Registration Logic
# -------------------------------------------------------------------------
BROKER_VARIANTS = {
    'live': {
        "broker": {"name": "mybroker-live", "variant": "live", "display_name": "My Broker Live"},
    },
    'paper': {
        "broker": {"name": "mybroker-paper", "variant": "paper", "display_name": "My Broker Paper"},
        "api": {"base_url": "https://paper-api.broker.com/v1"}
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
