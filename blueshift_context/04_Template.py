"""
[REPLACE: Broker Name] REST API broker integration for Blueshift.

Implements:
- Broker spec (credentials, options, enum mappings)
- API spec (trading + market data endpoints)
- Streaming spec (WebSocket/SocketIO real-time data)
- Objects spec (order/position/account conversions)
- Master data spec (asset universe)

INSTRUCTIONS:
1. Search for "REPLACE:" comments and update all placeholder values
2. Search for "<...>" placeholders and replace with actual broker values
3. Remove example code that doesn't apply to your broker
4. Update enum mappings to match your broker's API strings
5. Test each endpoint individually before full integration
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

# REPLACE: Use the appropriate calendar for your broker's market
# Common calendars: "NYSE" (US), "NSE" (India), "LSE" (UK), "XETR" (Germany), etc.
cal = get_calendar('<CALENDAR_NAME>')  # e.g., 'NYSE', 'NSE', 'LSE'

# -------------------------------------------------------------------------
# Helper Functions (Registered)
# -------------------------------------------------------------------------
@registry.register()
def broker_timeframe(freq, **kwargs):
    """
    Map Blueshift Frequency -> Broker interval string.
    REPLACE: Update the mapping values to match your broker's API timeframe strings.
    """
    mapping = {
        Frequency('1m'): '<1_MINUTE>',   # e.g., '1Min', '1m', '1minute'
        Frequency('5m'): '<5_MINUTE>',   # e.g., '5Min', '5m', '5minute'
        Frequency('15m'): '<15_MINUTE>', # e.g., '15Min', '15m', '15minute'
        Frequency('1h'): '<1_HOUR>',     # e.g., '1Hour', '1h', '60minute'
        Frequency('1d'): '<1_DAY>',      # e.g., '1Day', '1d', 'day'
    }
    return mapping.get(freq, '<DEFAULT_TIMEFRAME>')

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
    # REPLACE: Unique identifier for your broker (lowercase, alphanumeric + hyphens)
    "name": "<broker_name>",

    # REPLACE: Trading calendar for this broker's market
    "calendar": "<CALENDAR_NAME>",  # e.g., "NYSE", "NSE", "LSE"

    "credentials": {
        # REPLACE: List all credential fields your broker requires
        "fields": ["api_key", "api_secret", "access_token"],  # Add/remove as needed
        "validator": "credentials.api_key and credentials.api_secret",
    },
    "options": {
        "timeout": 10,
        "rate_limit": 10,        # REPLACE: Requests per period (check broker docs)
        "rate_limit_period": 1,  # REPLACE: Period in seconds
        "max_tickers": 200,      # REPLACE: Max symbols per request
        "supported_modes": ["LIVE", "PAPER"],  # REPLACE: Remove "PAPER" if not supported
        "fractional_trading": False,  # REPLACE: True if broker supports fractional shares
        "max_page_fetch": 10,
    },

    # REPLACE: Asset classes supported by this broker
    "assets": ["equity"],  # Options: "equity", "equity-futures", "equity-options", "crypto", "fx"

    # REPLACE: Map Blueshift enums to broker's string values
    # These mappings depend entirely on what strings your broker's API uses
    "order_side": {
        "map": {
            OrderSide.BUY: "<BUY_STRING>",    # e.g., "buy", "BUY", "B"
            OrderSide.SELL: "<SELL_STRING>",  # e.g., "sell", "SELL", "S"
        }
    },
    "order_type": {
        "map": {
            OrderType.MARKET: "<MARKET_STRING>",  # e.g., "market", "MARKET", "MKT"
            OrderType.LIMIT: "<LIMIT_STRING>",    # e.g., "limit", "LIMIT", "LMT"
            OrderType.STOP: "<STOP_STRING>",      # e.g., "stop", "STOP", "STP"
        },
        "default_value": OrderType.MARKET,
    },
    "order_validity": {
        "map": {
            OrderValidity.DAY: "<DAY_STRING>",  # e.g., "day", "DAY"
            OrderValidity.IOC: "<IOC_STRING>",  # e.g., "ioc", "IOC"
            OrderValidity.GTC: "<GTC_STRING>",  # e.g., "gtc", "GTC"
        },
        "default_value": OrderValidity.DAY,
    },
    "order_status": {
        "map": {
            # REPLACE: Map all possible status strings from your broker
            OrderStatus.OPEN: ["<OPEN_STRING>", "<PARTIAL_STRING>", "<PENDING_STRING>"],
            OrderStatus.COMPLETE: "<FILLED_STRING>",
            OrderStatus.CANCELLED: ["<CANCELLED_STRING>", "<EXPIRED_STRING>"],
            OrderStatus.REJECTED: "<REJECTED_STRING>",
        },
        "default_value": OrderStatus.OPEN,
    },
    "product_type": {
        "map": {
            ProductType.DELIVERY: "<DELIVERY_STRING>",  # e.g., "delivery", "cash", "CNC"
            ProductType.MARGIN: "<MARGIN_STRING>",      # e.g., "margin", "MIS"
        },
        "default_value": ProductType.DELIVERY,
    },
    "data_frequencies": {
        "map": {
            Frequency('1m'): "<1_MINUTE>",
            Frequency('1d'): "<1_DAY>",
        }
    },
}

# -------------------------------------------------------------------------
# API_SPEC
# -------------------------------------------------------------------------
# REPLACE: Configure authentication headers for your broker
COMMON_HEADERS = {
    "fields": {
        # Common patterns:
        # - Bearer token: "Authorization": {"source": "f'Bearer {credentials.access_token}'"}
        # - API key header: "X-API-KEY": {"source": "credentials.api_key"}
        # - Multiple headers: add multiple entries
        "Authorization": {"source": "f'Bearer {credentials.access_token}'"},
        "Content-Type": {"source": "'application/json'"},
    }
}

API_SPEC = {
    # REPLACE: Your broker's base URL
    "base_url": "https://api.<broker>.com/<VERSION>",
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
        # REPLACE: Configure if your broker requires session initialization
        "session": {
            "endpoint": "/<AUTH_ENDPOINT>",  # e.g., "/auth/token", "/login"
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
            # REPLACE: Account endpoint path
            "endpoint": "/<ACCOUNT_ENDPOINT>",  # e.g., "/account", "/user/account"
            "method": "GET",
            "response": {
                "payload_type": "object",
                "result": {
                    "fields": {
                        # REPLACE: Field names from your broker's response
                        "name": {"source": "result.get('<ACCOUNT_ID_FIELD>', 'default')"},
                        "cash": {"source": "float(result.get('<CASH_FIELD>', 0))"},
                        "currency": {"source": "'<DEFAULT_CURRENCY>'"},  # e.g., 'USD', 'INR', 'EUR'
                    }
                }
            }
        },

        # --- Orders ---
        "get_orders": {
            # REPLACE: Orders endpoint path
            "endpoint": "/<ORDERS_ENDPOINT>",  # e.g., "/orders", "/v1/orders"
            "method": "GET",
            "request": {
                "query": {
                    "fields": {
                        # REPLACE: Query parameter names
                        "<STATUS_PARAM>": {"source": "'all'"},
                        "<LIMIT_PARAM>": {"source": "100"},
                    }
                },
                # REPLACE: Pagination configuration (if supported)
                "next_page_token": {
                    "parameter": "<PAGE_TOKEN_PARAM>",  # e.g., "after", "page_token", "cursor"
                    "location": "query"
                }
            },
            "response": {
                "payload_type": "array",
                # REPLACE: Path to orders array in response (if nested)
                "payload_path": "response.get('<ORDERS_FIELD>', response)",
                # REPLACE: Expression to extract next page token
                "next_page_token": "response.get('<NEXT_PAGE_TOKEN_FIELD>')",
                "items": {
                    "fields": {
                        # REPLACE: All field names to match your broker's response
                        "oid": {"source": "item['<ORDER_ID_FIELD>']"},
                        "broker_order_id": {"source": "item['<ORDER_ID_FIELD>']"},
                        "symbol": {"source": "item['<SYMBOL_FIELD>']"},
                        "quantity": {"source": "float(item['<QTY_FIELD>'])"},
                        "filled": {"source": "float(item.get('<FILLED_QTY_FIELD>', 0))"},
                        "price": {"source": "float(item.get('<LIMIT_PRICE_FIELD>') or 0)"},
                        "average_price": {"source": "float(item.get('<AVG_PRICE_FIELD>') or 0)"},
                        "side": {"source": "mappings.order_side.to_blueshift(item['<SIDE_FIELD>'])"},
                        "order_type": {"source": "mappings.order_type.to_blueshift(item['<TYPE_FIELD>'])"},
                        "status": {"source": "mappings.order_status.to_blueshift(item['<STATUS_FIELD>'])"},
                        "timestamp": {"source": "pd.Timestamp(item['<CREATED_AT_FIELD>'])"},
                        "exchange_timestamp": {"source": "pd.Timestamp(item.get('<FILLED_AT_FIELD>') or item['<CREATED_AT_FIELD>'])"},
                    }
                }
            }
        },

        "place_order": {
            # REPLACE: Place order endpoint path
            "endpoint": "/<ORDERS_ENDPOINT>",  # e.g., "/orders"
            "method": "POST",
            "request": {
                "json": {
                    "fields": {
                        # REPLACE: Request body field names
                        "<SYMBOL_FIELD>": {"source": "order.asset.broker_symbol or order.asset.symbol"},
                        "<QTY_FIELD>": {"source": "str(int(order.quantity))"},
                        "<SIDE_FIELD>": {"source": "mappings.order_side.from_blueshift(order.side)"},
                        "<TYPE_FIELD>": {"source": "mappings.order_type.from_blueshift(order.order_type)"},
                        "<VALIDITY_FIELD>": {"source": "mappings.order_validity.from_blueshift(order.order_validity)"},
                        # Conditional: only include limit_price for limit orders
                        "<LIMIT_PRICE_FIELD>": {
                            "source": "str(order.price)",
                            "condition": "order.order_type == OrderType.LIMIT"
                        },
                    }
                }
            },
            # REPLACE: Per-endpoint error rules (checked before global)
            "errors": [
                {
                    "condition": "not success and '<INSUFFICIENT_FUNDS_KEYWORD>' in str(response.get('message','')).lower()",
                    "exception": "OrderError",
                    "message": "Insufficient funds"
                }
            ],
            "response": {
                "payload_type": "object",
                "result": {
                    "fields": {
                        "order_id": {"source": "str(result['<ORDER_ID_FIELD>'])"}
                    }
                }
            }
        },

        "cancel_order": {
            # REPLACE: Cancel order endpoint path
            "endpoint": "/<ORDERS_ENDPOINT>/{order_id}",
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
                        "order_id": {"source": "str(result.get('<ORDER_ID_FIELD>', order.oid))"}
                    }
                }
            }
        },

        "update_order": {
            # REPLACE: Update order endpoint path
            "endpoint": "/<ORDERS_ENDPOINT>/{order_id}",
            "method": "PATCH",  # or "PUT" depending on broker
            "request": {
                "path": {
                    "fields": {
                        "order_id": {"source": "order.broker_order_id or order.oid"}
                    }
                },
                "json": {
                    "fields": {
                        "<QTY_FIELD>": {"source": "str(int(quantity))", "condition": "quantity is not None"},
                        "<LIMIT_PRICE_FIELD>": {"source": "str(price)", "condition": "price is not None"},
                    }
                }
            },
            "response": {
                "payload_type": "object",
                "result": {
                    "fields": {
                        "order_id": {"source": "str(result['<ORDER_ID_FIELD>'])"}
                    }
                }
            }
        },

        # --- Positions ---
        "get_positions": {
            # REPLACE: Positions endpoint path
            "endpoint": "/<POSITIONS_ENDPOINT>",  # e.g., "/positions", "/portfolio/positions"
            "method": "GET",
            "response": {
                "payload_type": "array",
                # REPLACE: Path to positions array in response (if nested)
                "payload_path": "response.get('<POSITIONS_FIELD>', response) if isinstance(response, list) else response.get('<POSITIONS_FIELD>', [])",
                "items": {
                    "fields": {
                        # REPLACE: Field names from your broker's response
                        "symbol": {"source": "item['<SYMBOL_FIELD>']"},
                        "quantity": {"source": "float(item['<QTY_FIELD>'])"},
                        "average_price": {"source": "float(item['<AVG_ENTRY_PRICE_FIELD>'])"},
                        "last_price": {"source": "float(item['<CURRENT_PRICE_FIELD>'])"},
                        "product_type": {"source": "ProductType.DELIVERY"},
                    }
                }
            }
        },

        # --- Market Data ---
        "get_history": {
            # REPLACE: Historical data endpoint (may be different base URL)
            "endpoint": "/<BARS_ENDPOINT>/{symbol}",  # e.g., "/bars/{symbol}", "/history/{symbol}"
            "method": "GET",
            "request": {
                "path": {
                    "fields": {
                        "symbol": {"source": "asset.broker_symbol or asset.symbol"}
                    }
                },
                "query": {
                    "fields": {
                        # REPLACE: Query parameter names
                        "<TIMEFRAME_PARAM>": {"source": "broker_timeframe(freq)"},
                        "<START_PARAM>": {"source": "from_dt.isoformat()"},
                        "<END_PARAM>": {"source": "to_dt.isoformat()"},
                        "<LIMIT_PARAM>": {"source": "nbars"},
                    }
                }
            },
            "response": {
                "payload_type": "data",
                # REPLACE: Path to bars array in response
                "payload_path": "response.get('<BARS_FIELD>', [])",
                "data": {
                    "frame": "records",
                    # REPLACE: Timestamp field name and format
                    "timestamp": {"field": "<TIMESTAMP_FIELD>", "format": "%Y-%m-%dT%H:%M:%SZ"},
                    # REPLACE: Map standard OHLCV names to broker's field names
                    # Keys = standard names, Values = broker's field names
                    "rename": {
                        "open": "<OPEN_FIELD>",
                        "high": "<HIGH_FIELD>",
                        "low": "<LOW_FIELD>",
                        "close": "<CLOSE_FIELD>",
                        "volume": "<VOLUME_FIELD>"
                    }
                }
            }
        },

        "get_quote": {
            # REPLACE: Quote endpoint path
            "endpoint": "/<QUOTES_ENDPOINT>/{symbol}",  # e.g., "/quotes/{symbol}"
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
                # REPLACE: Path to quote data in response
                "payload_path": "response.get('<QUOTE_FIELD>', response)",
                "quote": {
                    # REPLACE: Field names from your broker's quote response
                    "last": {"source": "float(quote.get('<LAST_FIELD>', 0))"},
                    "bid": {"source": "float(quote.get('<BID_FIELD>', 0))"},
                    "bid_volume": {"source": "float(quote.get('<BID_SIZE_FIELD>', 0))"},
                    "ask": {"source": "float(quote.get('<ASK_FIELD>', 0))"},
                    "ask_volume": {"source": "float(quote.get('<ASK_SIZE_FIELD>', 0))"},
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
            # REPLACE: WebSocket URL for streaming data
            "url": "wss://stream.<broker>.com/<VERSION>/stream",
            "backend": {"type": "websocket", "options": {}},  # Options: "websocket", "socketio", "mqtt"
            "streams": ["data", "quote"],

            # REPLACE: Authentication method and message format
            "auth": {
                "mode": "first_message",  # Options: "first_message", "headers", "url", "none"
                "first_message": {
                    "format": "json",
                    "json": {
                        "fields": {
                            # REPLACE: Auth message fields
                            "<ACTION_FIELD>": {"source": "'auth'"},
                            "<KEY_FIELD>": {"source": "credentials.api_key"},
                            "<SECRET_FIELD>": {"source": "credentials.api_secret"},
                        }
                    }
                }
            },

            # REPLACE: Subscribe/Unsubscribe message format (per-channel)
            "subscribe": {
                "subscribe": {
                    "data": {
                        "format": "json",
                        "json": {
                            "fields": {
                                "<ACTION_FIELD>": {"source": "'subscribe'"},
                                "<SYMBOLS_FIELD>": {"source": "[a.broker_symbol or a.symbol for a in subscribe_assets]"},
                            }
                        }
                    },
                    "quote": {
                        "format": "json",
                        "json": {
                            "fields": {
                                "<ACTION_FIELD>": {"source": "'subscribe'"},
                                "<SYMBOLS_FIELD>": {"source": "[a.broker_symbol or a.symbol for a in subscribe_assets]"},
                            }
                        }
                    }
                },
                "unsubscribe": {
                    "data": {
                        "format": "json",
                        "json": {
                            "fields": {
                                "<ACTION_FIELD>": {"source": "'unsubscribe'"},
                                "<SYMBOLS_FIELD>": {"source": "[a.broker_symbol or a.symbol for a in subscribe_assets]"},
                            }
                        }
                    }
                }
            },

            # Message Parsing
            "parser": {
                "format": "json",
                # REPLACE: If messages are wrapped, extract the inner payload
                "payload_path": "data.get('<PAYLOAD_FIELD>', data)",
            },

            # REPLACE: Router rules to distinguish message types
            "router": {
                "enabled": True,
                "rules": [
                    # REPLACE: Conditions to identify message types
                    {"channel": "data", "match": "data.get('<TYPE_FIELD>') == '<TRADE_TYPE>'"},
                    {"channel": "quote", "match": "data.get('<TYPE_FIELD>') == '<QUOTE_TYPE>'"},
                ],
                "default_channel": "data"
            },

            # REPLACE: Convert raw messages to Blueshift objects
            "converters": {
                "data": [
                    {
                        "fields": {
                            # REPLACE: Field names from streaming messages
                            "asset": {"source": "broker.infer_asset(symbol=data['<SYMBOL_FIELD>'])"},
                            "timestamp": {"source": "pd.Timestamp(data['<TIMESTAMP_FIELD>'])"},
                            "data": {
                                "close": {"source": "float(data['<PRICE_FIELD>'])"},
                                "volume": {"source": "float(data.get('<SIZE_FIELD>', 0))"},
                            }
                        }
                    }
                ],
                "quote": [
                    {
                        "fields": {
                            "asset": {"source": "broker.infer_asset(symbol=data['<SYMBOL_FIELD>'])"},
                            "timestamp": {"source": "pd.Timestamp(data['<TIMESTAMP_FIELD>'])"},
                            "last": {"source": "float(data.get('<LAST_FIELD>', 0))"},
                            "bid": {"source": "float(data.get('<BID_FIELD>', 0))"},
                            "ask": {"source": "float(data.get('<ASK_FIELD>', 0))"},
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
                # REPLACE: Field names to match your broker's order object
                "oid": {"source": "str(data['<ORDER_ID_FIELD>'])"},
                "broker_order_id": {"source": "str(data['<ORDER_ID_FIELD>'])"},
                "quantity": {"source": "float(data['<QTY_FIELD>'])"},
                "filled": {"source": "float(data.get('<FILLED_QTY_FIELD>', 0))"},
                "price": {"source": "float(data.get('<LIMIT_PRICE_FIELD>') or 0)"},
                "average_price": {"source": "float(data.get('<AVG_PRICE_FIELD>') or 0)"},
                "side": {"source": "mappings.order_side.to_blueshift(data['<SIDE_FIELD>'])"},
                "order_type": {"source": "mappings.order_type.to_blueshift(data['<TYPE_FIELD>'])"},
                "status": {"source": "mappings.order_status.to_blueshift(data['<STATUS_FIELD>'])"},
                "timestamp": {"source": "pd.Timestamp(data['<CREATED_AT_FIELD>'])"},
            }
        }
    ],
    "position": [
        {
            "fields": {
                # REPLACE: Field names to match your broker's position object
                "quantity": {"source": "float(data['<QTY_FIELD>'])"},
                "average_price": {"source": "float(data['<AVG_ENTRY_PRICE_FIELD>'])"},
                "last_price": {"source": "float(data['<CURRENT_PRICE_FIELD>'])"},
            }
        }
    ],
}

# -------------------------------------------------------------------------
# MASTER_DATA_SPEC (Optional but recommended for production)
# -------------------------------------------------------------------------
# Master data provides the asset universe for symbol/id lookup and streaming routing.
# Configure this to fetch tradeable instruments from your broker.
MASTER_DATA_SPEC = [
    # REPLACE: Uncomment and configure for your broker
    # {
    #     "endpoint": {
    #         "endpoint": "/<ASSETS_ENDPOINT>",  # e.g., "/assets", "/instruments"
    #         "method": "GET",
    #         "response": {"payload_type": "array", "items": {"fields": {}}}
    #     },
    #     "mode": "api",     # "api" for REST endpoint, "file" for CSV/JSON file download
    #     "format": "json",  # "json" or "csv"
    #     "assets": [
    #         {
    #             "asset_class": "equity",
    #             # REPLACE: Filter expression for tradeable assets
    #             "filter": "item.get('<STATUS_FIELD>') == 'active' and item.get('<TRADABLE_FIELD>')",
    #             "mapping": {
    #                 # REPLACE: Field names from your broker's instrument data
    #                 "symbol": {"source": "item['<SYMBOL_FIELD>']"},
    #                 "security_id": {"source": "item.get('<ID_FIELD>', item['<SYMBOL_FIELD>'])"},
    #                 "name": {"source": "item.get('<NAME_FIELD>', item['<SYMBOL_FIELD>'])"},
    #                 "exchange_name": {"source": "item.get('<EXCHANGE_FIELD>', '<DEFAULT_EXCHANGE>')"},
    #                 "calendar_name": {"source": "'<CALENDAR_NAME>'"},
    #             }
    #         }
    #     ]
    # }
]

# -------------------------------------------------------------------------
# Registration Logic
# -------------------------------------------------------------------------
# REPLACE: Define variants for your broker (live, paper, sandbox, etc.)
BROKER_VARIANTS = {
    'live': {
        "broker": {
            "name": "<broker_name>-live",
            "variant": "live",
            "display_name": "<Broker Name> Live"
        },
        "api": {"base_url": "https://api.<broker>.com/<VERSION>"}
    },
    'paper': {
        "broker": {
            "name": "<broker_name>-paper",
            "variant": "paper",
            "display_name": "<Broker Name> Paper"
        },
        "api": {"base_url": "https://sandbox.<broker>.com/<VERSION>"}
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
