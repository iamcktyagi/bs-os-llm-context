"""
Upstox API v2 broker integration for Blueshift (declarative config).

Implements:
- Broker spec (credentials, options, enum mappings)
- API spec (trading + market data endpoints)
- Master data spec (assets universe from Upstox JSON instruments)
- Objects spec (order/position/account conversions)
- No streaming (Upstox v3 WebSocket uses binary protobuf — not supported declaratively)

Auth: Bearer token (access_token provided externally, no login flow).
Two base URLs:
  - api-hft.upstox.com for order placement/modification/cancellation (HFT)
  - api.upstox.com for everything else (positions, funds, history, quotes)
"""
from __future__ import annotations
import json
import pandas as pd
from urllib.parse import quote

from blueshift.lib.common.constants import Frequency
from blueshift.lib.common.functions import merge_json_recursive, to_title_case
from blueshift.lib.trades._order_types import (
    OrderSide, ProductType, OrderType, OrderValidity, OrderStatus)
from blueshift.calendar import get_calendar

from blueshift.brokers.core.config.config import APIBrokerConfig
from blueshift.brokers.core.config.resolver import ConfigRegistry
from blueshift.brokers.factory import broker_class_factory

registry = ConfigRegistry(globals())
cal = get_calendar('NSE')

# -------------------------------------------------------------------------
# Custom Functions
# -------------------------------------------------------------------------

@registry.register()
def upstox_timeframe(freq, **kwargs) -> str:
    """Map Blueshift Frequency -> Upstox interval string."""
    mapping = {
        Frequency('1m'): "1minute",
        Frequency('5m'): "5minute",
        Frequency('15m'): "15minute",
        Frequency('30m'): "30minute",
        Frequency('1h'): "60minute",
        Frequency('1d'): "day",
    }
    return mapping.get(freq, "1minute")


@registry.register()
def upstox_history_endpoint(asset, freq, from_dt, to_dt, **kwargs) -> str:
    """
    Build the full historical candle URL with path parameters.
    Upstox uses: /v2/historical-candle/{instrument_key}/{interval}/{to_date}/{from_date}
    instrument_key contains '|' which must be URL-encoded.
    """
    instrument_key = asset.broker_symbol or asset.symbol
    encoded_key = quote(instrument_key, safe='')
    interval = upstox_timeframe(freq)
    to_date = to_dt.strftime('%Y-%m-%d')
    from_date = from_dt.strftime('%Y-%m-%d')
    return f"https://api.upstox.com/v2/historical-candle/{encoded_key}/{interval}/{to_date}/{from_date}"


@registry.register()
def upstox_intraday_endpoint(asset, freq, **kwargs) -> str:
    """
    Build the intraday candle URL.
    Upstox uses: /v2/historical-candle/intraday/{instrument_key}/{interval}
    """
    instrument_key = asset.broker_symbol or asset.symbol
    encoded_key = quote(instrument_key, safe='')
    interval = upstox_timeframe(freq)
    return f"https://api.upstox.com/v2/historical-candle/intraday/{encoded_key}/{interval}"


@registry.register()
def upstox_history_request(**context):
    """
    Custom request for get_history.
    Determines whether to use historical or intraday endpoint based on frequency,
    then makes the request.
    """
    asset = context['asset']
    freq = context['freq']
    from_dt = context['from_dt']
    to_dt = context['to_dt']
    credentials = context['credentials']
    api = context['api']

    # Use intraday endpoint for intraday frequencies on current day
    today = pd.Timestamp.now(tz='Asia/Kolkata').normalize()
    is_intraday = (freq <= Frequency('1h')) and (from_dt.normalize() >= today)

    if is_intraday:
        url = upstox_intraday_endpoint(asset, freq)
    else:
        url = upstox_history_endpoint(asset, freq, from_dt, to_dt)

    headers = {
        "Authorization": f"Bearer {credentials.access_token}",
        "Accept": "application/json",
    }

    success, status_code, response = api.raw_request(url, "GET", headers, {}, {}, {})

    if not success:
        return []

    # Response: {"status":"success","data":{"candles":[[ts,O,H,L,C,V,OI],...]}}
    candles = []
    if isinstance(response, dict):
        data = response.get('data', {})
        candles = data.get('candles', [])

    # Convert to list of dicts for the framework
    records = []
    for c in candles:
        if len(c) >= 6:
            records.append({
                'timestamp': c[0],
                'open': float(c[1]),
                'high': float(c[2]),
                'low': float(c[3]),
                'close': float(c[4]),
                'volume': float(c[5]),
            })

    return records


@registry.register()
def upstox_product_type(order, **kwargs) -> str:
    """Map Blueshift ProductType to Upstox product string."""
    if order.product_type == ProductType.INTRADAY:
        return "I"
    elif order.product_type == ProductType.DELIVERY:
        return "D"
    elif order.product_type == ProductType.MARGIN:
        return "MTF"
    return "D"


@registry.register()
def upstox_parse_positions(response, broker, **kwargs) -> list:
    """
    Parse positions from Upstox response.
    Upstox returns net quantity as 'quantity' (positive=long, negative=short).
    Also provides day_buy_quantity, day_sell_quantity, etc.
    """
    if not isinstance(response, dict):
        return []

    data = response.get('data', [])
    if not data:
        return []

    positions = []
    for item in data:
        qty = int(item.get('quantity', 0) or 0)
        if qty == 0:
            continue
        positions.append(item)

    return positions


# -------------------------------------------------------------------------
# Broker Config
# -------------------------------------------------------------------------
BROKER_SPEC = {
    "calendar": "NSE",
    "credentials": {
        "fields": ["api_key", "api_secret", "access_token"],
        "validator": "credentials.access_token",
    },
    "options": {
        "timeout": 10,
        "lazy_session": True,
        "rate_limit": 25,
        "rate_limit_period": 1,
        "max_tickers": 200,
        "supported_modes": ["LIVE"],
        "fractional_trading": False,
        "ccy": "INR",
    },
    "assets": ["equity"],

    "order_side": {
        "map": {
            OrderSide.BUY: "BUY",
            OrderSide.SELL: "SELL",
        }
    },
    "order_type": {
        "map": {
            OrderType.MARKET: "MARKET",
            OrderType.LIMIT: "LIMIT",
            OrderType.STOP: "SL",
            OrderType.STOPLOSS_MARKET: "SL-M",
        },
        "default_value": OrderType.MARKET,
    },
    "order_validity": {
        "map": {
            OrderValidity.DAY: "DAY",
            OrderValidity.IOC: "IOC",
        },
        "default_value": OrderValidity.DAY,
    },
    "order_status": {
        "map": {
            OrderStatus.OPEN: ["open", "pending", "trigger pending", "not modified",
                               "modify pending", "modify trigger pending",
                               "cancel pending", "partially filled"],
            OrderStatus.COMPLETE: "complete",
            OrderStatus.CANCELLED: ["cancelled", "not cancelled"],
            OrderStatus.REJECTED: "rejected",
        },
        "default_value": OrderStatus.OPEN,
    },
    "product_type": {
        "map": {
            ProductType.INTRADAY: "I",
            ProductType.DELIVERY: "D",
            ProductType.MARGIN: "MTF",
        },
        "default_value": ProductType.DELIVERY,
    },
}

# -------------------------------------------------------------------------
# API Config
# -------------------------------------------------------------------------

COMMON_HEADERS = {
    "fields": {
        "Authorization": {"source": "f'Bearer {credentials.access_token}'"},
        "Accept": {"source": "'application/json'"},
    }
}

TRADING_ENDPOINTS = {
    "session": {
        # No login needed — access_token is pre-provided
        "endpoint": "/v2/user/profile",
        "method": "GET",
        "request": {},
        "response": {
            "payload_type": "object",
            "result": {
                "fields": {
                    "user_id": {"source": "result.get('data', {}).get('user_id', '')"},
                }
            },
        },
    },

    "get_account": {
        "endpoint": "/v2/user/get-funds-and-margin?segment=SEC",
        "method": "GET",
        "request": {},
        "response": {
            "payload_type": "object",
            "result": {
                "fields": {
                    "name": {"source": "credentials.api_key"},
                    "cash": {"source": "float(result.get('data', {}).get('equity', {}).get('available_margin', 0) or 0)"},
                    "currency": {"source": "'INR'"},
                }
            },
        },
    },

    "get_orders": {
        "endpoint": "/v2/order/retrieve-all",
        "method": "GET",
        "request": {},
        "response": {
            "payload_type": "array",
            "payload_path": "response.get('data', []) if response.get('status') == 'success' else []",
            "items": {
                "fields": {
                    "oid": {"source": "item['order_id']"},
                    "broker_order_id": {"source": "item['order_id']"},
                    "symbol": {"source": "item.get('trading_symbol', '')"},
                    "security_id": {"source": "item.get('instrument_token', '')"},
                    "quantity": {"source": "float(item.get('quantity', 0))"},
                    "filled": {"source": "float(item.get('filled_quantity', 0) or 0)"},
                    "side": {"source": "mappings.order_side.to_blueshift(item.get('transaction_type', 'BUY'))"},
                    "order_type": {"source": "mappings.order_type.to_blueshift(item.get('order_type', 'MARKET'))"},
                    "price": {"source": "float(item.get('price', 0) or 0)"},
                    "trigger_price": {"source": "float(item.get('trigger_price', 0) or 0)"},
                    "status": {"source": "mappings.order_status.to_blueshift(item.get('status', 'open'))"},
                    "average_price": {"source": "float(item.get('average_price', 0) or 0)"},
                    "timestamp": {"source": "pd.Timestamp(item['order_timestamp']).tz_convert(broker.tz) if item.get('order_timestamp') else pd.Timestamp.now(tz=broker.tz)"},
                    "exchange_timestamp": {"source": "pd.Timestamp(item['exchange_timestamp']).tz_convert(broker.tz) if item.get('exchange_timestamp') else None"},
                }
            },
        },
    },

    "place_order": {
        "endpoint": "https://api-hft.upstox.com/v2/order/place",
        "method": "POST",
        "request": {
            "json": {
                "fields": {
                    "quantity": {"source": "int(order.quantity)"},
                    "product": {"source": "upstox_product_type(order)"},
                    "validity": {"source": "mappings.order_validity.from_blueshift(order.order_validity)"},
                    "price": {"source": "float(order.price) if order.order_type in (OrderType.LIMIT, OrderType.STOP) else 0"},
                    "order_type": {"source": "mappings.order_type.from_blueshift(order.order_type)"},
                    "transaction_type": {"source": "mappings.order_side.from_blueshift(order.side)"},
                    "instrument_token": {"source": "order.asset.broker_symbol or order.asset.symbol"},
                    "disclosed_quantity": {"source": "0"},
                    "trigger_price": {
                        "source": "float(order.trigger_price)",
                        "condition": "order.order_type in (OrderType.STOP, OrderType.STOPLOSS_MARKET)",
                    },
                    "is_amo": {"source": "False"},
                }
            }
        },
        "response": {
            "payload_type": "object",
            "result": {
                "fields": {
                    "order_id": {"source": "result.get('data', {}).get('order_id', '')"},
                }
            },
        },
    },

    "update_order": {
        "endpoint": "https://api-hft.upstox.com/v2/order/modify",
        "method": "PUT",
        "request": {
            "json": {
                "fields": {
                    "order_id": {"source": "order.broker_order_id or order.oid"},
                    "quantity": {"source": "int(order.quantity)"},
                    "validity": {"source": "mappings.order_validity.from_blueshift(order.order_validity)"},
                    "price": {"source": "float(order.price) if order.order_type in (OrderType.LIMIT, OrderType.STOP) else 0"},
                    "order_type": {"source": "mappings.order_type.from_blueshift(order.order_type)"},
                    "disclosed_quantity": {"source": "0"},
                    "trigger_price": {
                        "source": "float(order.trigger_price)",
                        "condition": "order.order_type in (OrderType.STOP, OrderType.STOPLOSS_MARKET)",
                    },
                }
            }
        },
        "response": {
            "payload_type": "object",
            "result": {
                "fields": {
                    "order_id": {"source": "result.get('data', {}).get('order_id', '')"},
                }
            },
        },
    },

    "cancel_order": {
        "endpoint": "https://api-hft.upstox.com/v2/order/cancel",
        "method": "DELETE",
        "request": {
            "query": {
                "fields": {
                    "order_id": {"source": "order.broker_order_id or order.oid"},
                }
            }
        },
        "response": {
            "payload_type": "object",
            "result": {
                "fields": {
                    "order_id": {"source": "result.get('data', {}).get('order_id', '')"},
                }
            },
        },
    },

    "get_positions": {
        "endpoint": "/v2/portfolio/short-term-positions",
        "method": "GET",
        "request": {},
        "response": {
            "payload_type": "array",
            "payload_path": "response.get('data', []) if response.get('status') == 'success' else []",
            "items": {
                "fields": {
                    "security_id": {"source": "item.get('instrument_token', '')"},
                    "symbol": {"source": "item.get('trading_symbol', '')"},
                    "quantity": {"source": "float(item.get('quantity', 0))"},
                    "average_price": {"source": "float(item.get('average_price', 0) or 0)"},
                    "last_price": {"source": "float(item.get('last_price', 0) or 0)"},
                    "product_type": {"source": "mappings.product_type.to_blueshift(item.get('product', 'D'))"},
                }
            },
        },
    },
}

MARKET_DATA_ENDPOINTS = {
    "get_history": {
        "endpoint": "/v2/historical-candle",
        "method": "GET",
        "request": {
            "custom": "upstox_history_request",
        },
        "response": {
            "payload_type": "custom",
            "custom": "upstox_parse_history_response",
        },
    },

    "get_quote": {
        "endpoint": "/v2/market-quote/quotes",
        "method": "GET",
        "request": {
            "query": {
                "fields": {
                    "instrument_key": {"source": "asset.broker_symbol or asset.symbol"},
                }
            }
        },
        "response": {
            "payload_type": "quote",
            "payload_path": "list(response.get('data', {}).values())[0] if response.get('data') else {}",
            "quote": {
                "last": {"source": "float(quote.get('last_price', 0) or 0)"},
                "open": {"source": "float(quote.get('ohlc', {}).get('open', 0) or 0)"},
                "high": {"source": "float(quote.get('ohlc', {}).get('high', 0) or 0)"},
                "low": {"source": "float(quote.get('ohlc', {}).get('low', 0) or 0)"},
                "close": {"source": "float(quote.get('ohlc', {}).get('close', 0) or 0)"},
                "volume": {"source": "float(quote.get('volume', 0) or 0)"},
            }
        },
    },
}


@registry.register()
def upstox_parse_history_response(response, **kwargs):
    """
    Parse the history response stored by upstox_history_request.
    The custom request already returns the parsed records list.
    """
    # When custom request is used, the response is the return value
    if isinstance(response, list):
        if not response:
            return pd.DataFrame()
        df = pd.DataFrame(response)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp').sort_index()
        return df
    return pd.DataFrame()


API_SPEC = {
    'base_url': 'https://api.upstox.com',
    'headers': COMMON_HEADERS,
    'endpoints': {**TRADING_ENDPOINTS, **MARKET_DATA_ENDPOINTS},
}

# -------------------------------------------------------------------------
# Master Data Config
# -------------------------------------------------------------------------
MASTER_DATA_SPEC = [
    {
        "endpoint": {
            "endpoint": "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz",
            "method": "GET",
            "use_global_headers": False,
            "response": {"payload_type": "object", "result": {"fields": {}}}
        },
        "mode": "file",
        "format": "json",
        "compression": "gzip",
        "assets": [
            {
                # NSE Equity
                "filter": "item.get('instrument_type') == 'EQUITY' and item.get('segment') == 'NSE_EQ'",
                "asset_class": "equity",
                "mapping": {
                    "symbol": {"source": "item['trading_symbol']"},
                    "broker_symbol": {"source": "item['instrument_key']"},
                    "security_id": {"source": "str(item.get('exchange_token', ''))"},
                    "name": {"source": "item.get('name', item['trading_symbol'])"},
                    "exchange_name": {"source": "'NSE'"},
                    "calendar_name": {"source": "'NSE'"},
                },
                "details": {
                    "tick_size": {"source": "float(item.get('tick_size', 0.05))"},
                    "mult": {"source": "int(item.get('lot_size', 1))"},
                }
            },
        ]
    },
]

# -------------------------------------------------------------------------
# Object Conversions
# -------------------------------------------------------------------------
OBJECTS_SPEC = {}

# -------------------------------------------------------------------------
# Streaming (not implemented — Upstox v3 uses binary protobuf)
# -------------------------------------------------------------------------
STREAMING_SPEC = {
    "connections": {}
}

# -------------------------------------------------------------------------
# Variants and Registration
# -------------------------------------------------------------------------
BROKER_VARIANTS = {
    'live': {
        "broker": {
            "name": "upstox-live",
            "variant": "live",
            "display_name": "Upstox Live",
        }
    }
}


def create_config(variant) -> APIBrokerConfig:
    broker_spec = dict(BROKER_SPEC)
    api_spec = dict(API_SPEC)
    stream_spec = dict(STREAMING_SPEC)
    obj_spec = dict(OBJECTS_SPEC)
    api_spec["master_data"] = MASTER_DATA_SPEC.copy()

    variant_specs = dict(BROKER_VARIANTS).get(variant)
    if variant_specs:
        broker_variant = variant_specs.get('broker', {})
        api_variant = variant_specs.get('api', {})
        obj_variant = variant_specs.get('objects', {})
        stream_variant = variant_specs.get('streaming', {})

        broker_spec = merge_json_recursive(broker_spec, broker_variant)
        stream_spec = merge_json_recursive(stream_spec, stream_variant)
        api_spec = merge_json_recursive(api_spec, api_variant)
        obj_spec = merge_json_recursive(obj_spec, obj_variant)

        broker_spec['variant'] = variant

    return APIBrokerConfig(broker_spec, api_spec, obj_spec, stream_spec, registry=registry)


def register_brokers():
    for variant in BROKER_VARIANTS:
        config = create_config(variant)
        cls_name = to_title_case(config.broker.name)
        broker_class_factory(cls_name, config)
