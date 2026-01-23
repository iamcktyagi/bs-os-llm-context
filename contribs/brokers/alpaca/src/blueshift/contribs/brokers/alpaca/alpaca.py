"""
Alpaca REST API broker integration for Blueshift (declarative config).

Implements:
- Broker spec (credentials, options, enum mappings)
- API spec (trading + market data endpoints)
- Master data spec (assets universe)
- Objects spec (order/position/account conversions)

Notes:
- Trading endpoints use Alpaca Trading API (v2).
- Market data endpoints use Alpaca Market Data API (v2).
- The market-data endpoints are defined with an absolute URL so they can coexist with the
  trading base_url.
"""
from __future__ import annotations
import pandas as pd
import math

from blueshift.interfaces.assets._assets import InstrumentType
from blueshift.lib.common.constants import Frequency
from blueshift.lib.common.functions import merge_json_recursive, to_title_case
from blueshift.lib.trades._order_types import (
    OrderSide, ProductType, OrderType, OrderValidity, OrderStatus)
from blueshift.calendar.date_utils import get_aligned_timestamp
from blueshift.calendar import get_calendar

from blueshift.brokers.core.config.config import APIBrokerConfig
from blueshift.brokers.core.broker import RestAPIBroker
from blueshift.brokers.core.config.resolver import ConfigRegistry
from blueshift.brokers.factory import broker_class_factory


# -------------------------------------------------------------------------
# Instantiate the registry for expression evaluation, if required, capture 
# the current global so that imports and other variables become avaiable
# -------------------------------------------------------------------------
registry = ConfigRegistry(globals())
cal = get_calendar('NYSE')

@registry.register()
def alpaca_timeframe(freq, **kwargs) -> str:
    """
    Map Blueshift Frequency -> Alpaca timeframe.

    Alpaca expects: 1Min, 5Min, 15Min, 1Hour, 1Day, 1Week, 1Month (etc).
    We keep this mapping conservative; extend as needed based on the Frequency enum in use.
    """
    mapping = {
        Frequency('1m'): "1Min",
        Frequency('1d'): "1Day",
    }
    return mapping.get(freq, "1Min")


@registry.register()
def alpaca_symbol_list(assets, **kwargs) -> str:
    """Convert list[Asset/MarketData] -> comma separated symbols for Alpaca multi-symbol endpoints."""
    return ",".join([a.symbol for a in assets])

@registry.register()
def alpaca_symbol_array(subscribe_assets, **kwargs) -> list[str]:
    """Convert list[Asset/MarketData] -> list of symbols for Alpaca subscribe."""
    if not isinstance(subscribe_assets, list):
        subscribe_assets = [subscribe_assets]
    return [a.symbol for a in subscribe_assets]

@registry.register()
def alpaca_delay_start(start:pd.Timestamp, end:pd.Timestamp) -> str:
    """ for no subscription, delay data by at least 15 min. """
    end = cal.last_trading_minute(end)
    now = get_aligned_timestamp(pd.Timestamp.now(tz='US/Eastern'), Frequency('1m'))
    delay = round((now - end).total_seconds()/60) # total minute delay for end from now
    offset = max(0, 15+1 - delay)
    offset = int(max(0, max(offset, 15+1)))
    start = (start-pd.Timedelta(minutes=offset))
    return start.tz_convert('Etc/UTC').isoformat()

@registry.register()
def alpaca_delay_end(end:pd.Timestamp) -> str:
    """ for no subscription, delay data by at least 15 min. """
    now = get_aligned_timestamp(pd.Timestamp.now(tz='US/Eastern'), Frequency('1m'))
    delay = round((now - end).total_seconds()/60) # total minute delay for end from now
    offset = max(0, 15+1 - delay)
    offset = int(max(0, max(offset, 15+1)))
    end = (end-pd.Timedelta(minutes=offset))
    return end.tz_convert('Etc/UTC').isoformat()



# -------------------------------------------------------------------------
# Broker config -> generic config which is specialized with variants later
# -------------------------------------------------------------------------
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
        "max_tickers": 200,
        "supported_modes": ["LIVE", "PAPER"],
        "fractional_trading": True,
        # Prefer multi-asset market data endpoints where possible
        "multi_assets_data_query": True,
    },
    "assets": ["equity"],

    # Enum mappings used in request construction and (optionally) object conversions
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
        # Alpaca uses time_in_force
        "map": {
            OrderValidity.DAY: "day",
            OrderValidity.IOC: "ioc",
            OrderValidity.FOK: "fok",
        },
        "default_value": OrderValidity.DAY,
    },
    "order_status": {
        # Alpaca uses time_in_force
        "map": {
            OrderStatus.OPEN:["new","partially_filled","done_for_day","pending_new",
                              "accepted_for_bidding","stopped", "calculated"],
            OrderStatus.COMPLETE:"filled",
            OrderStatus.CANCELLED:["canceled","expired", "replaced","pending_replace"],
            OrderStatus.REJECTED:["rejected","suspended"]
        },
        "default_value": OrderStatus.OPEN,
    },
    "product_type": {
        # Alpaca is cash/margin at account level; we keep a trivial mapping
        "map": {
            ProductType.MARGIN: "margin",
            ProductType.DELIVERY: "cash",
        },
        "default_value": ProductType.MARGIN,
    },
}


# ---------------------------------------------------------------------
# API config
# ---------------------------------------------------------------------
# NOTE:
# - Trading API base_url differs for PAPER vs LIVE.
#   We specialize a generic API config with the variants later, supplying the correct base URLs
# - Market data uses https://data.alpaca.markets. Those endpoints are defined as absolute URLs
#   so they can coexist with the trading base_url.
COMMON_HEADERS = {
    "fields":{
        "APCA-API-KEY-ID": {"source": "credentials.api_key"},
        "APCA-API-SECRET-KEY": {"source": "credentials.api_secret"},
        "Accept": {"source": "'application/json'"},
        "Content-Type": {"source": "'application/json'"},
    }
}

# --- Trading endpoints (v2)
TRADING_ENDPOINTS = {
    "get_account": {
        "endpoint": "/v2/account",
        "method": "GET",
        "response": {
            "payload_type": "object",
            # Keep raw keys needed by account conversion pipeline
            "result": {
                "fields":{
                    "account_number": {"source": "result.get('account_number')"},
                    "cash": {"source": "result.get('cash')"},
                    "buying_power": {"source": "result.get('buying_power')"},
                    "equity": {"source": "result.get('equity')"},
                    "currency": {"source": "result.get('currency', 'USD')"},
                    "status": {"source": "result.get('status')"},
                }
            },
        },
    },

    "get_orders": {
        "endpoint": "/v2/orders",
        "method": "GET",
        "request": {
            "query": {
                "fields": {
                    # default to open orders (matches example guide)
                    "status": {"source": "'all'"},
                    "limit": {"source": "500"},
                    "direction": {"source": "'desc'"},
                    "after":{"source":"str(pd.Timestamp.now() - pd.Timedelta(days=1))"},
                }
            }
        },
        "response": {
            "payload_type": "array",
            "items": {
                "fields":{
                    # Keep raw fields. Asset can be inferred from 'symbol' during object conversion.
                    "oid": {"source": "item['id']"},
                    "security_id": {"source": "item.get('asset_id')"},
                    "symbol": {"source": "item.get('symbol')"},
                    "quantity": {"source": "float(item.get('qty'))"},
                    "filled": {"source": "float(item.get('filled_qty')) if item.get('filled_qty') else 0"},
                    "average_price":{"source": "float(item.get('filled_avg_price')) if item.get('filled_avg_price') else 0"},
                    "side": {"source": "mappings.order_side.to_blueshift(item.get('side'))"},
                    "order_type": {"source": "mappings.order_type.to_blueshift(item.get('type'))"},
                    "order_validity": {"source": "mappings.order_validity.to_blueshift(item.get('time_in_force'))"},
                    "price": {"source": "float(item.get('limit_price')) if item.get('limit_price') else 0"},
                    "status": {"source": "mappings.order_status.to_blueshift(item.get('status'))"},
                    "timestamp": {"source": "pd.Timestamp(item.get('created_at')).tz_convert(broker.tz)"},
                    "exchange_timestamp": {"source": "pd.Timestamp(item.get('updated_at')).tz_convert(broker.tz)"},
                    "remark":{"source":"item.get('subtag') if item.get('subtag') else ''"},
                }
            },
        },
    },

    "get_order": {
        "endpoint": "/v2/orders/{order_id}",
        "method": "GET",
        "request": {
            "path": {
                "fields": {
                    "order_id": {"source": "order_id"},
                }
            }
        },
        "response": {
            "payload_type": "object",
            "result": {
                "fields":{
                    "oid": {"source": "result['id']"},
                    "security_id": {"source": "result.get('asset_id')"},
                    "symbol": {"source": "result.get('symbol')"},
                    "quantity": {"source": "float(result.get('qty'))"},
                    "filled": {"source": "float(result.get('filled_qty')) if result.get('filled_qty') else 0"},
                    "average_price":{"source": "float(result.get('filled_avg_price')) if result.get('filled_avg_price') else 0"},
                    "side": {"source": "mappings.order_side.to_blueshift(result.get('side'))"},
                    "order_type": {"source": "mappings.order_type.to_blueshift(result.get('type'))"},
                    "order_validity": {"source": "result.get('time_in_force')"},
                    "price": {"source": "float(result.get('limit_price')) if result.get('limit_price') else 0"},
                    "status": {"source": "mappings.order_status.to_blueshift(result.get('status'))"},
                    "timestamp": {"source": "pd.Timestamp(result.get('created_at')).tz_convert(broker.tz)"},
                    "exchange_timestamp": {"source": "pd.Timestamp(result.get('updated_at')).tz_convert(broker.tz)"},
                    "remark":{"source":"result.get('subtag') if 'subtag' in result else ''"},
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
                    "symbol": {"source": "order.asset.symbol"},
                    # Alpaca allows fractional qty for market/day; represent qty as a string per API schema.
                    "qty": {"source": "str(order.quantity)"},
                    "side": {"source": "mappings.order_side.from_blueshift(order.side)"},
                    "type": {"source": "mappings.order_type.from_blueshift(order.order_type)"},
                    "time_in_force": {"source": "mappings.order_validity.from_blueshift(order.order_validity)"},
                    # price only for limit-like orders
                    "limit_price": {
                        "source": "str(order.price)",
                        "condition": "order.price and float(order.price) > 0 and mappings.order_type.from_blueshift(order.order_type) in ['limit','stop_limit']",
                    },
                }
            }
        },
        "response": {
            "payload_type": "object",
            "result": {"fields":{"order_id": {"source": "result['id']"}}},
        },
    },

    "cancel_order": {
        "endpoint": "/v2/orders/{order_id}",
        "method": "DELETE",
        "request": {"path": {"fields": {"order_id": {"source": "order.broker_order_id or order.oid or order.order_id"}}}},
        "response": {"payload_type": "object", "result": {"fields":{"order_id": {"source": "order.oid"}}}},
    },

    "update_order": {
        "endpoint": "/v2/orders/{order_id}",
        "method": "PATCH",
        "request": {
            "path": {"fields": {"order_id": {"source": "order.broker_order_id or order.oid or order.order_id"}}},
            "json": {
                "fields": {
                    "qty": {"source": "str(quantity)", "condition": "quantity is not None"},
                    "limit_price": {"source": "str(price)", "condition": "price is not None"},
                }
            },
        },
        "response": {
            "payload_type": "object",
            "result": {"fields":{"order_id": {"source": "order.oid"}}},
        },
    },

    "get_positions": {
        "endpoint": "/v2/positions",
        "method": "GET",
        "response": {
            "payload_type": "array",
            "items": {
                "fields":{
                    "security_id": {"source": "item.get('asset_id')"},
                    "symbol": {"source": "item.get('symbol')"},
                    "instrument_id": {"source": "item.get('asset_id')"},
                    "side":{"source": "OrderSide.BUY if item.get('side')=='long' else OrderSide.SELL"},
                    "quantity": {"source": "item.get('qty')"},
                    "average_price": {"source": "item.get('avg_entry_price')"},
                    "last_price": {"source": "item.get('current_price')"},
                    "exchange": {"source": "item.get('exchange')"},
                }
            },
        },
    },
}

# --- Market data endpoints (v2; absolute URLs)
MARKET_DATA_ENDPOINTS = {
    "get_history_multi": {
        "endpoint": "https://data.alpaca.markets/v2/stocks/bars",
        "method": "GET",
        "request": {
            "query": {
                "fields": {
                    "symbols": {"source": "alpaca_symbol_list(asset)"},
                    "timeframe": {"source": "alpaca_timeframe(freq)"},
                    "start": {"source": "alpaca_delay_start(from_dt, to_dt)"},
                    "end": {"source": "alpaca_delay_end(to_dt)"},
                }
            }
        },
        "response": {
            "payload_type": "data",
            # In multi mode Alpaca returns: {"bars": {"AAPL":[...], "MSFT":[...]}, ...}
            # The data portal may expect per-asset frames; if your core expects a flat list,
            # customize this payload_path or split inside a custom function.
            "payload_path": "response.get('bars', {})",
            "data": {
                "symbol":"key",
                "frame": "records", 
                "rename":{"timestamp":"t","open":"o", "high":"h", "low":"l", "close":"c", "volume":"v"},
                "timestamp": {"field": "t", "unit": "ns"}
            },
        },
    },

    "get_quote": {
        "endpoint": "https://data.alpaca.markets/v2/stocks/{symbol}/quotes/latest",
        "method": "GET",
        "request": {"path": {"fields": {"symbol": {"source": "asset.symbol"}}}},
        "response": {
            "payload_type": "quote",
            "payload_path": "response.get('quote', {})",
            "quote": {
                "timestamp": {"source": "pd.Timestamp(quote.get('t')).tz_convert(broker.tz)"},
                "market_depth":{
                    "bids":[
                        [{"source": "float(quote.get('bp', 0) or 0)"},
                         {"source": "int(quote.get('bs', 0) or 0)"},
                        ]
                    ],
                    "asks":[
                        [{"source": "float(quote.get('ap', 0) or 0)"},
                         {"source": "int(quote.get('as', 0) or 0)"},
                        ]
                    ],
                },
            },
        },
    },
}

API_SPEC = {
    'base_url':'',
    'headers':COMMON_HEADERS,
    'endpoints':{**TRADING_ENDPOINTS, **MARKET_DATA_ENDPOINTS}
}


# ---------------------------------------------------------------------
# Streaming config
# ---------------------------------------------------------------------
STREAMING_SPEC = {
    "connections": {
        "market_data": {
            "url": "wss://stream.data.alpaca.markets/v2/iex",
            "backend": {"type": "websocket", "options": {}},
            "streams": ["data","quote"],
            "auth": {
                "mode": "first_message",
                "first_message": {
                    "format": "json",
                    "json": {
                        "fields": {
                            "action": {"source": "'auth'"},
                            "key": {"source": "credentials.api_key"},
                            "secret": {"source": "credentials.api_secret"}
                        }
                    }
                }
            },
            "subscribe": {
                "subscribe": {
                    "data":{
                        "format": "json",
                        "json": {
                            "fields": {
                                "action": {"source": "'subscribe'"},
                                "trades": {"source": "alpaca_symbol_array(subscribe_assets)"}
                            }
                        }
                    },
                    "quote":{
                        "format": "json",
                        "json": {
                            "fields": {
                                "action": {"source": "'subscribe'"},
                                "quotes": {"source": "alpaca_symbol_array(subscribe_assets)"}
                            }
                        }
                    }
                },
                "unsubscribe": {
                    "data":{
                        "format": "json",
                        "json": {
                            "fields": {
                                "action": {"source": "'unsubscribe'"},
                                "trades": {"source": "alpaca_symbol_array(subscribe_assets)"}
                            }
                        }
                    },
                    "quote":{
                        "format": "json",
                        "json": {
                            "fields": {
                                "action": {"source": "'unsubscribe'"},
                                "quotes": {"source": "alpaca_symbol_array(subscribe_assets)"}
                            }
                        }
                    },
                },
            },
            "parser": {"format": "json"},
            "router": {
                "rules": [
                    {"channel": "data", "match": "data.get('T') == 't'"},
                    {"channel": "quote", "match": "data.get('T') == 'q'"}
                ]
            },
            "converters": {
                "data": [
                    {
                        "condition": "data.get('T') == 't'",
                        "fields": {
                            "asset": {"source": "broker.symbol(data['S'])"},
                            "timestamp": {"source": "pd.Timestamp(data['t']).tz_convert(broker.tz)"},
                            "data": {
                                    "price": {"source": "float(data['p'])"},
                                    "volume": {"source": "int(data['s'])"}
                            }
                        }
                    }
                ],
                "quote": [
                    {
                        "condition": "data.get('T') == 'q'",
                        "fields": {
                            "timestamp": {"source": "pd.Timestamp(data.get('t')).tz_convert(broker.tz)"},
                            "symbol":{"source":"data['S']"},
                            "market_depth":{
                                "bids":[
                                    [{"source": "float(data.get('bp', 0) or 0)"},
                                    {"source": "int(data.get('bs', 0) or 0)"},
                                    ]
                                ],
                                "asks":[
                                    [{"source": "float(data.get('ap', 0) or 0)"},
                                    {"source": "int(data.get('as', 0) or 0)"},
                                    ]
                                ],
                            }
                        }
                    }
                ]
            }
        },
        "trading": {
            "url": "wss://paper-api.alpaca.markets/stream",
            "backend": {"type": "websocket", "options": {}},
            "streams": ["order", "account"],
            "auth": {
                "mode": "first_message",
                "first_message": {
                    "format": "json",
                    "json": {
                        "fields": {
                            "action": {"source": "'auth'"},
                            "key": {"source": "credentials.api_key"},
                            "secret": {"source": "credentials.api_secret"}
                        }
                    }
                }
            },
            "subscribe": {
                "subscribe": {
                    "all":{
                        "format": "json",
                        "json": {
                            "fields": {
                                "action": {"source": "'listen'"},
                                "data": {"source": "{'streams': ['trade_updates', 'account_updates']}"}
                            }
                        }
                    }
                }
            },
            "parser": {"format": "json"},
            "router": {
                 "rules": [
                    {"channel": "order", "match": "data.get('stream') == 'trade_updates'"},
                    {"channel": "account", "match": "data.get('stream') == 'account_updates'"}
                ]
            },
            "converters": {
                "order": [
                    {
                        "condition": "data.get('stream') == 'trade_updates' and 'data' in data and 'order' in data['data']",
                        "fields": {
                            "order_id": {"source": "data['data']['order']['id']"},
                             "broker_order_id": {"source": "data['data']['order']['id']"},
                             "security_id": {"source": "data['data']['order']['asset_id']"},
                             "symbol": {"source": "data['data']['order']['symbol']"},
                             "quantity": {"source": "float(data['data']['order']['qty'])"},
                             "filled": {"source": "float(data['data']['order']['filled_qty']) if data['data']['order']['filled_qty'] else 0"},
                             "average_price":{"source": "float(data['data']['order']['filled_avg_price']) if data['data']['order']['filled_avg_price'] else 0"},
                             "side": {"source": "mappings.order_side.to_blueshift(data['data']['order']['side'])"},
                             "order_type": {"source": "mappings.order_type.to_blueshift(data['data']['order']['type'])"},
                             "order_validity": {"source": "mappings.order_validity.to_blueshift(data['data']['order']['time_in_force'])"},
                             "price": {"source": "float(data['data']['order']['limit_price']) if data['data']['order']['limit_price'] else 0"},
                             "status": {"source": "mappings.order_status.to_blueshift(data['data']['order']['status'])"},
                             "timestamp": {"source": "pd.Timestamp(data['data']['timestamp']).tz_convert(broker.tz)"},
                        }
                    }
                ]
            }
        }
    }
}


# ---------------------------------------------------------------------
# Master data config (asset universe)
# ---------------------------------------------------------------------
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
            "response": {
                "payload_type": "array",
                "items": {
                    "fields":{
                        "sec_id": {"source": "item['id']"},
                        "symbol": {"source": "item.get('symbol')"},
                        "name": {"source": "item.get('name', '')"},
                        "status": {"source": "item.get('status', 'inactive')"},
                        "exchange": {"source": "item.get('exchange', '')"},
                        "shortable": {"source": "item.get('shortable',False)"},
                        "can_trade": {"source": "item.get('tradable', False)"},
                        "fractional": {"source": "item.get('fractionable', False)"},
                    }
                },
            },
        },
        "mode": "list",
        "assets": [
            {
                "filter": [
                    "row.get('status') == 'active'", 
                    "row.get('can_trade') == True",
                    "row.get('exchange') != 'OTC'"
                ],
                "asset_class":"equity",
                "mapping": {
                    "symbol": {"source": "row['symbol']"},
                    "security_id": {"source": "row['sec_id']"},
                    "asset_type": {"source": "'equity'"},
                    "exchange_name": {"source": "row.get('exchange', 'NYSE')"},
                    "calendar_name":{"source":"'NYSE'"},
                    "can_trade": {"source": "row.get('can_trade')"},
                    "shortable": {"source": "row.get('shortable')"},
                    "fractional": {"source": "row.get('fractional')"},
                    "name": {"source": "row.get('name', '')"},
                    "instrument_type":{
                        "source":"InstrumentType.FUNDS",
                        "condition":"' ETF ' in row.get('name', '') or ' ETF' in row.get('name', '')"
                    }
                },
            }
        ],
    }
]

# ---------------------------------------------------------------------
# Objects conversion config
# ---------------------------------------------------------------------
OBJECTS_SPEC = {
    "position": [
        {
            "fields": {
                "quantity": {"source": "float(data.get('quantity', 0) or 0)"},
                "average_price": {"source": "float(data.get('average_price', 0) or 0)"},
                "last_price": {"source": "float(data.get('last_price', 0) or 0)"},
            }
        }
    ],
    "account": [
        {
            "fields": {
                "name": {"source": "data.get('account_number', '')"},
                "cash": {"source": "float(data.get('cash', 0) or 0)"},
                "currency": {"source": "data.get('currency', 'USD')"},
            }
        }
    ],
}

# -------------------------------------------------------------------------
# Broker variants - replace only the required keys (at any nesting levels)
# -------------------------------------------------------------------------

BROKER_VARIANTS = {
    'paper': {
        "broker":{
            "name": "alpaca-paper",
            "variant": "paper",
            "display_name": "Alpaca Paper Broker",
        },
        "api":{
            "base_url":"https://paper-api.alpaca.markets",
        },
        "streaming":{
            "connections": {
                "trading": {"url": "wss://paper-api.alpaca.markets/stream"},
            }
        }
    },
    'paper-trader': {
        "broker":{
            "name": "alpaca-paper-trader",
            "variant": "paper",
            "display_name": "Alpaca Paper Broker",
            "options": {"interface":"trader"},
        },
        "api":{
            "base_url":"https://paper-api.alpaca.markets",
        },
        "streaming":{
            "connections": {
                "trading": {"url": "wss://paper-api.alpaca.markets/stream"},
            }
        }
    },
    'paper-data-portal': {
        "broker":{
            "name": "alpaca-paper-data-portal",
            "variant": "paper",
            "display_name": "Alpaca Paper Broker",
            "options": {"interface":"data-portal"},
        },
        "api":{
            "base_url":"https://paper-api.alpaca.markets",
        },
    },
    'live':{
        "broker":{
            "name": "alpaca-live",
            "variant": "live",
            "display_name": "Alpaca Live Broker",
        },
        "api":{
            "base_url":"https://api.alpaca.markets",
        },
        "streaming":{
            "connections": {
                "trading": {"url": "wss://api.alpaca.markets/stream"},
                "market_data": {"url": "wss://stream.data.alpaca.markets/v2/sip"},
            }
        }
    },
}


def create_config(variant) -> APIBrokerConfig:
    """ create the config merging the variant specs with base specs. """
    broker_spec = dict(BROKER_SPEC)
    api_spec = dict(API_SPEC)
    obj_spec = dict(OBJECTS_SPEC)
    stream_spec = dict(STREAMING_SPEC)
    api_spec["master_data"] = MASTER_DATA_SPEC.copy()

    variant_specs = dict(BROKER_VARIANTS).get(variant)
    if variant_specs:
        broker_variant = variant_specs.get('broker', {})
        api_variant = variant_specs.get('api',{})
        obj_variant = variant_specs.get('objects',{})
        stream_variant = variant_specs.get('streaming', {})

        broker_spec = merge_json_recursive(broker_spec, broker_variant)
        api_spec = merge_json_recursive(api_spec, api_variant)
        obj_spec = merge_json_recursive(obj_spec, obj_variant)
        stream_spec = merge_json_recursive(stream_spec, stream_variant)
        broker_spec['variant'] = variant

    return APIBrokerConfig(broker_spec, api_spec, obj_spec, stream_spec, registry=registry)

def register_brokers():
    """ register broker class for each variant. """
    for variant in BROKER_VARIANTS:
        config = create_config(variant)
        cls_name = to_title_case(config.broker.name)
        broker_class_factory(cls_name, config)