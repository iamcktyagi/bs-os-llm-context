"""
Alpaca Markets REST API broker integration for Blueshift (reference implementation).

This is a COMPLETE working example demonstrating:
- All required endpoints (get_orders, place_order, cancel_order, get_account, get_history)
- Recommended endpoints (get_positions, get_quote, update_order)
- Error handling (global + per-endpoint)
- Pagination (get_orders with page tokens)
- Complete streaming (auth, subscribe, parser, router, converters for data + quote)
- OBJECTS_SPEC (order + position conversion pipelines)
- Master data (asset universe from Alpaca assets API)
- Conditional fields (limit_price only for limit orders)
"""
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

# -------------------------------------------------------------------------
# Registry & Helpers
# -------------------------------------------------------------------------
registry = ConfigRegistry(globals())
cal = get_calendar("NYSE")

@registry.register()
def alpaca_timeframe(freq, **kwargs):
    """Map Blueshift Frequency -> Alpaca timeframe string."""
    mapping = {
        Frequency("1m"): "1Min",
        Frequency("5m"): "5Min",
        Frequency("15m"): "15Min",
        Frequency("1h"): "1Hour",
        Frequency("1d"): "1Day",
    }
    return mapping.get(freq, "1Min")

# -------------------------------------------------------------------------
# BROKER_SPEC
# -------------------------------------------------------------------------
BROKER_SPEC = {
    "name": "alpaca",
    "calendar": "NYSE",
    "credentials": {
        "fields": ["api_key", "api_secret"],
        "validator": "credentials.api_key and credentials.api_secret",
    },
    "options": {
        "timeout": 10,
        "rate_limit": 180,
        "rate_limit_period": 60,
        "max_tickers": 200,
        "supported_modes": ["LIVE", "PAPER"],
        "fractional_trading": True,
        "max_page_fetch": 10,
    },
    "assets": ["equity"],

    # Enum Mappings
    "order_side": {
        "map": {OrderSide.BUY: "buy", OrderSide.SELL: "sell"}
    },
    "order_type": {
        "map": {
            OrderType.MARKET: "market",
            OrderType.LIMIT: "limit",
            OrderType.STOP: "stop",
            OrderType.STOP_LIMIT: "stop_limit",
        },
        "default_value": OrderType.MARKET,
    },
    "order_validity": {
        "map": {
            OrderValidity.DAY: "day",
            OrderValidity.IOC: "ioc",
            OrderValidity.FOK: "fok",
            OrderValidity.GTC: "gtc",
        },
        "default_value": OrderValidity.DAY,
    },
    "order_status": {
        "map": {
            OrderStatus.OPEN: ["new", "accepted", "partially_filled", "pending_new",
                               "accepted_for_bidding", "pending_replace"],
            OrderStatus.COMPLETE: "filled",
            OrderStatus.CANCELLED: ["canceled", "expired", "pending_cancel"],
            OrderStatus.REJECTED: ["rejected", "suspended", "stopped"],
        },
        "default_value": OrderStatus.OPEN,
    },
    "product_type": {
        "map": {ProductType.MARGIN: "margin", ProductType.DELIVERY: "cash"},
        "default_value": ProductType.MARGIN,
    },
    "data_frequencies": {
        "map": {
            Frequency("1m"): "1Min",
            Frequency("5m"): "5Min",
            Frequency("15m"): "15Min",
            Frequency("1h"): "1Hour",
            Frequency("1d"): "1Day",
        }
    },
}

# -------------------------------------------------------------------------
# API_SPEC
# -------------------------------------------------------------------------
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

    # Global error handling
    "errors": [
        {
            "condition": "status_code == 401",
            "exception": "AuthenticationError",
            "message": "Invalid API credentials"
        },
        {
            "condition": "status_code == 403",
            "exception": "ValidationError",
            "message": "Forbidden: check account permissions"
        },
        {
            "condition": "status_code == 429",
            "exception": "APIException",
            "message": "Rate limit exceeded"
        },
        {
            "condition": "not success and status_code >= 500",
            "exception": "ServerError",
            "message": "Alpaca server error"
        },
    ],

    "endpoints": {
        # --- Account ---
        "get_account": {
            "endpoint": "/v2/account",
            "method": "GET",
            "response": {
                "payload_type": "object",
                "result": {
                    "fields": {
                        "name": {"source": "result.get('account_number', 'default')"},
                        "cash": {"source": "float(result.get('cash', 0))"},
                        "currency": {"source": "result.get('currency', 'USD')"},
                    }
                },
            },
        },

        # --- Orders (with pagination) ---
        "get_orders": {
            "endpoint": "/v2/orders",
            "method": "GET",
            "request": {
                "query": {
                    "fields": {
                        "status": {"source": "'all'"},
                        "limit": {"source": "100"},
                        "direction": {"source": "'desc'"},
                    }
                },
                # Pagination: send the 'after' token as a query param
                "next_page_token": {
                    "parameter": "after",
                    "location": "query"
                }
            },
            "response": {
                "payload_type": "array",
                # No payload_path: Alpaca returns array directly
                # Pagination: extract last order ID as next page token
                "next_page_token": "response[-1]['id'] if len(response) == 100 else None",
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
                        "qty": {"source": "str(int(order.quantity))"},
                        "side": {"source": "mappings.order_side.from_blueshift(order.side)"},
                        "type": {"source": "mappings.order_type.from_blueshift(order.order_type)"},
                        "time_in_force": {"source": "mappings.order_validity.from_blueshift(order.order_validity)"},
                        # Conditional: only include for limit/stop_limit orders
                        "limit_price": {
                            "source": "str(order.price)",
                            "condition": "order.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT)"
                        },
                        "stop_price": {
                            "source": "str(order.trigger_price)",
                            "condition": "order.order_type in (OrderType.STOP, OrderType.STOP_LIMIT)"
                        },
                    }
                }
            },
            # Per-endpoint error: insufficient buying power
            "errors": [
                {
                    "condition": "not success and 'buying power' in str(response.get('message','')).lower()",
                    "exception": "OrderError",
                    "message": "Insufficient buying power"
                }
            ],
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

        "update_order": {
            "endpoint": "/v2/orders/{order_id}",
            "method": "PATCH",
            "request": {
                "path": {
                    "fields": {
                        "order_id": {"source": "order.broker_order_id or order.oid"},
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
                        "order_id": {"source": "result['id']"},
                    }
                },
            },
        },

        # --- Positions ---
        "get_positions": {
            "endpoint": "/v2/positions",
            "method": "GET",
            "response": {
                "payload_type": "array",
                # Alpaca returns positions as a flat array
                "items": {
                    "fields": {
                        "symbol": {"source": "item['symbol']"},
                        "quantity": {"source": "float(item['qty'])"},
                        "average_price": {"source": "float(item['avg_entry_price'])"},
                        "last_price": {"source": "float(item['current_price'])"},
                        "product_type": {"source": "ProductType.MARGIN"},
                    }
                },
            },
        },

        # --- Market Data ---
        "get_history": {
            "endpoint": "https://data.alpaca.markets/v2/stocks/{symbol}/bars",
            "method": "GET",
            "use_global_headers": True,  # Still use Alpaca auth headers
            "request": {
                "path": {
                    "fields": {"symbol": {"source": "asset.broker_symbol or asset.symbol"}}
                },
                "query": {
                    "fields": {
                        "timeframe": {"source": "alpaca_timeframe(freq)"},
                        "start": {"source": "from_dt.tz_convert('Etc/UTC').isoformat()"},
                        "end": {"source": "to_dt.tz_convert('Etc/UTC').isoformat()"},
                        "limit": {"source": "min(nbars, 10000)"},
                        "adjustment": {"source": "'split'"},
                    }
                },
            },
            "response": {
                "payload_type": "data",
                "payload_path": "response.get('bars', [])",
                "data": {
                    "frame": "records",
                    "timestamp": {"field": "t", "format": "%Y-%m-%dT%H:%M:%S%z"},
                    # rename: {Target_Standard: Source_API}
                    "rename": {"open": "o", "high": "h", "low": "l", "close": "c", "volume": "v"},
                },
            },
        },

        "get_quote": {
            "endpoint": "https://data.alpaca.markets/v2/stocks/{symbol}/quotes/latest",
            "method": "GET",
            "use_global_headers": True,
            "request": {
                "path": {
                    "fields": {"symbol": {"source": "asset.broker_symbol or asset.symbol"}}
                }
            },
            "response": {
                "payload_type": "quote",
                "payload_path": "response.get('quote', response)",
                "quote": {
                    "bid": {"source": "float(quote.get('bp', 0))"},
                    "bid_volume": {"source": "float(quote.get('bs', 0))"},
                    "ask": {"source": "float(quote.get('ap', 0))"},
                    "ask_volume": {"source": "float(quote.get('as', 0))"},
                    "timestamp": {"source": "pd.Timestamp(quote.get('t'))"},
                }
            }
        },
    },
}

# -------------------------------------------------------------------------
# STREAMING_SPEC (Complete: auth + subscribe + parser + router + converters)
# -------------------------------------------------------------------------
STREAMING_SPEC = {
    "connections": {
        "market_data": {
            "url": "wss://stream.data.alpaca.markets/v2/iex",
            "backend": {"type": "websocket", "options": {}},
            "streams": ["data", "quote"],

            # Auth: Alpaca uses first_message auth
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

            # Subscribe/Unsubscribe: per-channel messages
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
                    },
                    "quote": {
                        "format": "json",
                        "json": {
                            "fields": {
                                "action": {"source": "'unsubscribe'"},
                                "quotes": {"source": "[a.broker_symbol or a.symbol for a in subscribe_assets]"},
                            }
                        }
                    }
                }
            },

            # Parser: Alpaca sends JSON arrays, each element has a 'T' type field
            "parser": {
                "format": "json",
                # Alpaca wraps messages as: [{"T":"t","S":"AAPL","p":150.5,...}]
                # Extract the first element (or iterate if list)
            },

            # Router: route by message type field 'T'
            "router": {
                "enabled": True,
                "rules": [
                    {"channel": "data", "match": "data.get('T') == 't'"},     # trades
                    {"channel": "quote", "match": "data.get('T') == 'q'"},    # quotes
                ],
                "default_channel": "data"
            },

            # Converters: transform raw messages into Blueshift objects
            "converters": {
                "data": [
                    {
                        "fields": {
                            "asset": {"source": "broker.infer_asset(symbol=data['S'])"},
                            "timestamp": {"source": "pd.Timestamp(data['t'])"},
                            "data": {
                                "close": {"source": "float(data['p'])"},
                                "volume": {"source": "float(data.get('s', 0))"},
                            }
                        }
                    }
                ],
                "quote": [
                    {
                        "fields": {
                            "asset": {"source": "broker.infer_asset(symbol=data['S'])"},
                            "timestamp": {"source": "pd.Timestamp(data['t'])"},
                            "last": {"source": "float(data.get('p', 0))"},
                            "bid": {"source": "float(data.get('bp', 0))"},
                            "bid_volume": {"source": "float(data.get('bs', 0))"},
                            "ask": {"source": "float(data.get('ap', 0))"},
                            "ask_volume": {"source": "float(data.get('as', 0))"},
                        }
                    }
                ]
            }
        }
    }
}

# -------------------------------------------------------------------------
# OBJECTS_SPEC (Order + Position conversion pipelines)
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
                "exchange_timestamp": {"source": "pd.Timestamp(data.get('filled_at') or data['created_at'])"},
            }
        }
    ],
    "position": [
        {
            "fields": {
                "quantity": {"source": "float(data['qty'])"},
                "average_price": {"source": "float(data['avg_entry_price'])"},
                "last_price": {"source": "float(data['current_price'])"},
                "product_type": {"source": "ProductType.MARGIN"},
            }
        }
    ],
}

# -------------------------------------------------------------------------
# MASTER_DATA_SPEC (Asset universe from Alpaca)
# -------------------------------------------------------------------------
MASTER_DATA_SPEC = [
    {
        "endpoint": {
            "endpoint": "/v2/assets",
            "method": "GET",
            "request": {
                "query": {
                    "fields": {
                        "status": {"source": "'active'"},
                        "asset_class": {"source": "'us_equity'"},
                    }
                }
            },
            "response": {"payload_type": "array", "items": {"fields": {}}}
        },
        "mode": "api",
        "format": "json",
        "assets": [
            {
                "asset_class": "equity",
                "filter": "item.get('tradable') and item.get('status') == 'active'",
                "mapping": {
                    "symbol": {"source": "item['symbol']"},
                    "security_id": {"source": "item['id']"},
                    "name": {"source": "item.get('name', item['symbol'])"},
                    "exchange_name": {"source": "item.get('exchange', 'NYSE')"},
                    "calendar_name": {"source": "'NYSE'"},
                }
            }
        ]
    }
]

# -------------------------------------------------------------------------
# Registration
# -------------------------------------------------------------------------
BROKER_VARIANTS = {
    "paper": {
        "broker": {"name": "alpaca-paper", "variant": "paper", "display_name": "Alpaca Paper"},
        "api": {"base_url": "https://paper-api.alpaca.markets"},
    },
    "live": {
        "broker": {"name": "alpaca-live", "variant": "live", "display_name": "Alpaca Live"},
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
