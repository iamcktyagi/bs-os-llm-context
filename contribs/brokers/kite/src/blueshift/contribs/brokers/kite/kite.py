"""
Zerodha Kite Connect v3 REST API broker integration for Blueshift (declarative config).

Implements:
- Broker spec (credentials, options, enum mappings)
- API spec (trading + market data endpoints)
- Master data spec (assets universe from CSV)
- Objects spec (order/position/account conversions)
- Streaming spec (WebSocket real-time data)

Based on Kite Connect v3 API documentation:
https://kite.trade/docs/connect/v3/
"""
from __future__ import annotations
import pandas as pd
import hashlib
import struct
import json

from blueshift.assets import Asset
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
import struct


# -------------------------------------------------------------------------
# Registry and Helper Functions
# -------------------------------------------------------------------------
registry = ConfigRegistry(globals())
cal = get_calendar('NSE')

_FULL_HEAD_STRUCT = struct.Struct(">16i")
_DEPTH_ENTRY_STRUCT = struct.Struct(">iih2x")  # qty, price, orders, padding
_DEPTH_ENTRIES = 10
_DEPTH_START = 64
_DEPTH_END = 184
_INT16 = struct.Struct(">h")  # big-endian short

@registry.register()
def kite_checksum(api_key, request_token, api_secret, **kwargs) -> str:
    """
    Generate SHA-256 checksum for access token request.
    Checksum = SHA256(api_key + request_token + api_secret)
    """
    if not all([api_key, request_token, api_secret]):
        return ""
    checksum_string = f"{api_key}{request_token}{api_secret}"
    return hashlib.sha256(checksum_string.encode()).hexdigest()


@registry.register()
def kite_timeframe(freq, **kwargs) -> str:
    """
    Map Blueshift Frequency -> Kite interval.
    Supported: minute, 3minute, 5minute, 10minute, 15minute, 30minute, 60minute, day
    """
    mapping = {
        Frequency('1m'): "minute",
        # Frequency('3m'): "3minute",
        Frequency('5m'): "5minute",
        Frequency('10m'): "10minute",
        Frequency('15m'): "15minute",
        Frequency('30m'): "30minute",
        Frequency('1h'): "60minute",
        Frequency('1d'): "day",
    }
    return mapping.get(freq, "minute")


@registry.register()
def kite_format_datetime(dt, **kwargs) -> str:
    """
    Format datetime for Kite API (YYYY-MM-DD HH:MM:SS).
    Kite expects: 2023-01-01 09:15:00
    """
    if not dt:
        return ""
    if isinstance(dt, str):
        return dt
    # Convert to IST timezone if needed, Kite expects IST
    if hasattr(dt, 'tz_convert'):
        dt = dt.tz_convert('Asia/Kolkata')
    # Use f-string instead of strftime to avoid __builtins__ access
    return f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d} {dt.hour:02d}:{dt.minute:02d}:{dt.second:02d}"


@registry.register()
def kite_exchange_symbol(asset, **kwargs) -> str:
    """
    Format asset as exchange:tradingsymbol for Kite API.
    Example: NSE:INFY
    """
    exchange = getattr(asset, 'exchange_name', 'NSE')
    symbol = getattr(asset, 'symbol', '')
    return f"{exchange}:{symbol}"



@registry.register()
def kite_symbol_format(data:pd.Series, **kwargs) -> pd.Series:
    """
    Format asset as exchange:tradingsymbol for Kite API.
    Example: NSE:INFY
    """
    major_mapper = {
        "NIFTY50": "NIFTY",
        "NIFTYBANK" : "BANKNIFTY",
        "NIFTY MID SELECT" : "MIDCPNIFTY",
    }
    data = data.str.replace(' ','')
    data  = data.replace(major_mapper)
    # print(data)
    return  data


@registry.register()
def kite_subscriber_json(subscribe_assets, **kwargs):
    to_sub = [int(x.security_id) if isinstance(subscribe_assets, (list, tuple)) else int(subscribe_assets.security_id) for x in subscribe_assets]
    return ['full', to_sub]



@registry.register()
def kite_unsubscriber_json(subscribe_assets, **kwargs):
    to_sub = [int(x.security_id) if isinstance(subscribe_assets, (list, tuple)) else int(subscribe_assets.security_id) for x in subscribe_assets]
    return ['full', to_sub]


@registry.register()
def kite_data_converter(data,*args, **kwargs) -> tuple:
    broker = kwargs.get('broker')
    asset = broker.sid((data['security_id']))
    ts = pd.Timestamp(data['timestamp'])
    returning = (asset, ts, data)
    return returning

@registry.register()
def kite_quote_converter(data ,*args, **kwargs) -> list[dict]:
    return data

@registry.register()
def kite_product_type(asset: Asset, product_type, **kwargs) -> str:
    if asset.is_opt() or asset.is_futures():
        if product_type == ProductType.DELIVERY:
            return "NRML"
        return "MIS"
    if product_type == ProductType.DELIVERY:
        return "CNC"
    elif product_type in [ProductType.INTRADAY, ProductType.MARGIN]:
        return "MIS"
    return "CNC"


def to_dict(**kwargs):
    return kwargs

def parse_market_depth(buf: bytes):
    bids = []
    asks = []
    offset = _DEPTH_START
    for i in range(_DEPTH_ENTRIES):
        qty, price, orders = _DEPTH_ENTRY_STRUCT.unpack_from(buf, offset)
        # print(f"qty: {qty}, price: {price/100}")
        entry = (price/100, qty)
        if i < 5:
            bids.append(entry)
        else:
            asks.append(entry)

        offset += 12

    return to_dict(bids=bids, asks = asks)


def parse_full(buf: bytes, tz_info = None):
    h = _FULL_HEAD_STRUCT.unpack_from(buf, 0)
    security_id = h[0]
    ltp = h[1]/100
    ts = pd.Timestamp(h[11], unit='s',tz='UTC')
    if tz_info:
        ts = ts.tz_convert(tz_info)
    # for attr in ('last', 'close', 'open', 'high', 'low', 'upper_circuit', 'lower_circuit'):
    quote = to_dict(security_id=security_id , type = "quote" ,
                    last = ltp, timestamp = ts,
                    market_depth=parse_market_depth(buf))
    data = to_dict(
        security_id=h[0],
        price=ltp,
        volume=h[2]/100,
        # average_traded_price=h[3]/100,
        # total_volume=h[4],
        # total_buy_quantity=h[5],
        # total_sell_quantity=h[6],
        # open=h[7]/100,
        # high=h[8]/100,
        # low=h[9]/100,
        # close=h[10]/100,
        timestamp=ts,
        # open_interest=h[12],
        # oi_day_high=h[13],
        # oi_day_low=h[14],
        # exchange_timestamp=h[15],
        # market_depth=parse_market_depth(buf),
        type = 'data'
    )
    return [data , quote]


@registry.register()
def kite_parse_binary(data, *args, **kwargs) -> list[dict]:
    """
    Parse Kite WebSocket binary tick data.

    Binary structure:
    - Bytes 0-2: Number of packets (int16)
    - For each packet:
      - 2 bytes: Packet length
      - N bytes: Packet data based on mode (LTP: 8, Quote: 44, Full: 184)

    Packet structure:
    - 0-4: Instrument token (int32)
    - 4-8: Last price (int32, divide by 100)
    - For Quote/Full mode, additional fields follow

    Returns list of dicts with parsed tick data.
    """
    # print(f"Recived to parse : {type(data)} |data={data}")
    broker_tz = kwargs.get('broker').tz
    if isinstance(data, str):
        data = json.loads(data)
        if data.get("type", "") == "order":
            return data
        return []

    if not isinstance(data, (bytes, bytearray)):
        # print(f"Recived data is not bytes: {type(data)} |\ndata={data}")
        return []

    data = bytes(data)
    if len(data) < 2:
        return []

    packets = []
    offset = 0
    packet_count = _INT16.unpack_from(data, offset)[0]
    offset += 2
    for _ in range(packet_count):
        pkt_len = _INT16.unpack_from(data, offset)[0]
        if pkt_len == 184:
            offset += 2
            pkt = data[offset: offset + pkt_len]
            packets.extend(parse_full(pkt, broker_tz))
            offset += pkt_len
        else:
            print(f"Packet length {pkt_len} not supported yet.")
    return packets




# -------------------------------------------------------------------------
# BROKER_SPEC - Broker Configuration and Enum Mappings
# -------------------------------------------------------------------------
BROKER_SPEC = {
    "calendar": "NSE",
    "credentials": {
        "fields": ["api_key", "api_secret", "access_token"],
        # Validator: We need api_key and access_token for trading
        # access_token can be generated from api_key + request_token + api_secret
        "validator": "credentials.api_key and credentials.access_token",
    },
    "options": {
        "timeout": 10,
        "rate_limit": 3,  # Kite: ~3 requests/second
        "rate_limit_period": 1,
        "max_tickers": 500,  # Kite supports up to 500 instruments in quote endpoint
        "max_subscription": 3000,  # Kite supports up to 3000 instruments per WebSocket connection
        "max_nbars": 1000,  # Maximum number of bars to fetch
        "max_data_fetch": 1000,  # Maximum days of historical data to fetch
        "multi_assets_data_query": False,  # Kite doesn't support multi-asset queries in single call
        "multi_assets_data_subscribe": True,  # WebSocket supports subscribing to multiple assets
        "supported_modes": ["LIVE", "PAPER"],
        "fractional_trading": False,
        "ccy": "INR",
    },
    "assets": ["equity"],
    # "assets": ["equity", "equity-futures", "equity-options"],

    # Enum Mappings - Map Blueshift enums to Kite API values
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
            OrderType.STOPLOSS: "SL",
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
            OrderStatus.OPEN: ["OPEN", "TRIGGER PENDING", "MODIFY PENDING", "CANCEL PENDING", "UPDATE"],
            OrderStatus.COMPLETE: "COMPLETE",
            OrderStatus.CANCELLED: "CANCELLED",
            OrderStatus.REJECTED: "REJECTED",
        },
        "default_value": OrderStatus.OPEN,
    },
    "product_type": {
        "map": {
            ProductType.DELIVERY: "NRML",
            ProductType.INTRADAY: "MIS",
            ProductType.MARGIN: "MIS",  # Margin Intraday Squareoff
        },
        "default_value": ProductType.DELIVERY,
    },
}


# -------------------------------------------------------------------------
# API_SPEC - API Configuration and Endpoints
# -------------------------------------------------------------------------
COMMON_HEADERS = {
    "fields":
        {    "X-Kite-Version": {"source": "'3'"},
    "Authorization": {"source": "f'token {credentials.api_key}:{credentials.access_token}'"},
    "Content-Type": {"source": "'application/x-www-form-urlencoded'"},
}}

# Trading Endpoints
TRADING_ENDPOINTS = {
    "get_account": {
        "endpoint": "/user/margins",
        "method": "GET",
        "response": {
            "payload_type": "object",
            "payload_path": "response.get('data', {})",
            "result": {
                "fields":{
                "name": {"source": "credentials.api_key"},
                "cash": {"source": "float(result.get('equity', {}).get('available', {}).get('cash', 0))"},
                "margin_used": {"source": "float(result.get('equity', {}).get('utilised', {}).get('debits', 0))"},
                "margin_available": {"source": "float(result.get('equity', {}).get('available', {}).get('live_balance', 0))"},
                "currency": {"source": "'INR'"}
                }
            },
        },
    },

    "get_orders": {
        "endpoint": "/orders",
        "method": "GET",
        "response": {
            "payload_type": "array",
            "payload_path": "response.get('data', [])",
            "items": {
                "oid": {"source": "item['order_id']"},
                "broker_order_id": {"source": "item['order_id']"},
                "exchange_order_id": {"source": "item.get('exchange_order_id', '')"},
                "symbol": {"source": "item.get('tradingsymbol', '')"},
                "security_id": {"source": "str(item.get('instrument_token', ''))"},
                "quantity": {"source": "float(item.get('quantity'))"},
                "filled": {"source": "float(item.get('filled_quantity', 0))"},
                "pending": {"source": "float(item.get('pending_quantity', 0))"},
                "average_price": {"source": "float(item.get('average_price', 0))"},
                "price": {"source": "float(item.get('price', 0))"},
                "trigger_price": {"source": "float(item.get('trigger_price', 0))"},
                "side": {"source": "mappings.order_side.to_blueshift(item.get('transaction_type'))"},
                "order_type": {"source": "mappings.order_type.to_blueshift(item.get('order_type'))"},
                "product_type": {"source": "mappings.product_type.to_blueshift(item.get('product'))"},
                "order_validity": {"source": "mappings.order_validity.to_blueshift(item.get('validity'))"},
                "status": {"source": "mappings.order_status.to_blueshift(item.get('status'))"},
                "timestamp": {"source": "pd.Timestamp(item.get('order_timestamp', '')).tz_localize('Asia/Kolkata').tz_convert(broker.tz) if item.get('order_timestamp') else pd.Timestamp.now(tz=broker.tz)"},
                "exchange_timestamp": {"source": "pd.Timestamp(item.get('exchange_timestamp', item.get('order_timestamp', ''))).tz_localize('Asia/Kolkata').tz_convert(broker.tz) if item.get('exchange_timestamp') or item.get('order_timestamp') else pd.Timestamp.now(tz=broker.tz)"},
                "remark": {"source": "item.get('tag', '')"},
                # "exchange": {"source": "item.get('exchange', '')"},
            },
        },
    },

    "get_order": {
        "endpoint": "/orders/{order_id}",
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
            "payload_path": "list(response.get('data', []))[-1] if response.get('data') else {}",
            "result": {"fields":{
                "oid": {"source": "item['order_id']"},
                "broker_order_id": {"source": "item['order_id']"},
                "exchange_order_id": {"source": "item.get('exchange_order_id', '')"},
                "symbol": {"source": "item.get('tradingsymbol', '')"},
                "security_id": {"source": "str(item.get('instrument_token', ''))"},
                "quantity": {"source": "float(item.get('quantity', 0))"},
                "filled": {"source": "float(item.get('filled_quantity', 0))"},
                "average_price": {"source": "float(item.get('average_price', 0))"},
                "price": {"source": "float(item.get('price', 0))"},
                "side": {"source": "mappings.order_side.to_blueshift(item.get('transaction_type', 'BUY'))"},
                "order_type": {"source": "mappings.order_type.to_blueshift(item.get('order_type', 'MARKET'))"},
                "status": {"source": "mappings.order_status.to_blueshift(item.get('status', 'OPEN'))"},
                "timestamp": {"source": "pd.Timestamp(item.get('order_timestamp', '')).tz_localize('Asia/Kolkata').tz_convert(broker.tz) if item.get('order_timestamp') else pd.Timestamp.now(tz=broker.tz)"},
            },}
        },
    },

    "place_order": {
        "endpoint": "/orders/{variety}",
        "method": "POST",
        "request": {
            "path": {
                "fields": {
                    "variety": {"source": "'regular'"},  # Can be: regular, amo, co, iceberg
                }
            },
            "body": {
                "fields": {
                    "tradingsymbol": {"source": "order.asset.broker_symbol"},
                    "exchange": {"source": "order.asset.exchange_name"},
                    "transaction_type": {"source": "mappings.order_side.from_blueshift(order.side)"},
                    "order_type": {"source": "mappings.order_type.from_blueshift(order.order_type)"},
                    "quantity": {"source": "int(order.quantity)"},
                    # "product": {"source": "mappings.product_type.from_blueshift(order.product_type)"},
                    "product": {"source": "kite_product_type(order.asset, order.product_type)"},
                    "validity": {"source": "mappings.order_validity.from_blueshift(order.order_validity)"},
                    "price": {
                        "source": "float(order.price)",
                        "condition": "order.price and float(order.price) > 0 and order.order_type in [OrderType.LIMIT, OrderType.STOPLOSS]",
                    },
                    "trigger_price": {
                        "source": "float(order.trigger_price)",
                        "condition": "float(order.trigger_price) > 0 and order.order_type in [OrderType.STOPLOSS, OrderType.STOPLOSS_MARKET]",
                    },
                    "tag": {
                        "source": "order.placed_by or 'blueshift'",
                    },
                }
            },
        },
        "response": {
            "payload_type": "object",
            "result": {"fields":{
                "order_id": {"source": "result.get('data', {}).get('order_id', None)"},
            },}
        },
        "errors":[
                {
                    "condition": "isinstance(response, dict) and response.get('status') =='error' and isinstance(response['message'], str) "
                                 "and ('lower circuit limit' in response['message'].lower() or 'upper circuit limit' in response['message'].lower() )",
                    "exception": "PriceOutOfRange",
                    "message": "response",
                },
            ],
    },

    "cancel_order": {
        "endpoint": "/orders/{variety}/{order_id}",
        "method": "DELETE",
        "errors":[
                {
                    "condition": "isinstance(response, dict) and response.get('status') =='error' and isinstance(response['message'], str) "
                                 "and 'invalid `order_id`' in response['message'].lower()",
                    "exception": "OrderNotFound",
                    "message": "response",
                },
            ],
        "request": {
            "path": {
                "fields": {
                    "variety": {"source": "'regular'"},
                    "order_id": {"source": "order.broker_order_id or order.oid"},
                }
            }
        },
        "response": {
            "payload_type": "object",
            "result": {
                "fields":{"order_id": {"source": "result.get('data', {}).get('order_id', order.oid)"}, }
            },
        },
    },

    "update_order": {
        "endpoint": "/orders/{variety}/{order_id}",
        "method": "PUT",
        "request": {
            "path": {
                "fields": {
                    "variety": {"source": "'regular'"},
                    "order_id": {"source": "order.broker_order_id or order.oid"},
                }
            },
            "body": {
                "fields": {
                    "quantity": {
                        "source": "int(quantity) if quantity is not None else None",
                        "condition": "quantity is not None and quantity > 0",
                    },
                    "price": {
                        "source": "float(price)",
                        "condition": "price is not None and price > 0",
                    },
                    "order_type": {"source": "mappings.order_type.from_blueshift(order.order_type)"},
                    "validity": {"source": "mappings.order_validity.from_blueshift(order.order_validity)"},
                }
            },
        },
        "response": {
            "payload_type": "object",
            "result": {"fields":{
                "order_id": {"source": "result.get('data', {}).get('order_id', order.oid)"},
            },}
        },
    },

    "get_positions": {
        "endpoint": "/portfolio/positions",
        "method": "GET",
        "response": {
            "payload_type": "array",
            "payload_path": "response.get('data', {}).get('net', [])",
            "items": {
                "symbol": {"source": "item.get('tradingsymbol', '')"},
                "security_id": {"source": "str(item.get('instrument_token', ''))"},
                "exchange": {"source": "item.get('exchange', '')"},
                "product_type": {"source": "mappings.product_type.to_blueshift(item.get('product', 'CNC'))"},
                "quantity": {"source": "float(item.get('quantity', 0))"},
                "overnight_quantity": {"source": "float(item.get('overnight_quantity', 0))"},
                "average_price": {"source": "float(item.get('average_price', 0))"},
                "last_price": {"source": "float(item.get('last_price', 0))"},
                "close_price": {"source": "float(item.get('close_price', 0))"},
                "pnl": {"source": "float(item.get('pnl', 0))"},
                "m2m": {"source": "float(item.get('m2m', 0))"},
                "unrealised": {"source": "float(item.get('unrealised', 0))"},
                "realised": {"source": "float(item.get('realised', 0))"},
            },
        },
    },
}

# Market Data Endpoints
MARKET_DATA_ENDPOINTS = {
    "get_quote": {
        "endpoint": "/quote",
        "method": "GET",
        "request": {
            "query": {
                "fields": {
                    "i": {"source": "kite_exchange_symbol(asset)"},
                }
            }
        },
        "response": {
            "payload_type": "quote",
            "payload_path": "tuple(response.get('data', {}).values())[0] if response.get('data') else {}",
            "quote":
                {"timestamp": {
                    "source": "pd.Timestamp(quote.get('timestamp', '')).tz_localize('Asia/Kolkata').tz_convert(broker.tz) if quote.get('timestamp') else pd.Timestamp.now(tz=broker.tz)"
                },
                "last": {"source": "float(quote.get('last_price', 0))"},
                "open": {"source": "float(quote.get('ohlc', {}).get('open', 0))"},
                "high": {"source": "float(quote.get('ohlc', {}).get('high', 0))"},
                "low": {"source": "float(quote.get('ohlc', {}).get('low', 0))"},
                "close": {"source": "float(quote.get('ohlc', {}).get('close', 0))"},
                },
        },
    },
    #
    "get_history": {
        "endpoint": "/instruments/historical/{instrument_token}/{interval}",
        "method": "GET",
        "request": {
            "path": {
                "fields": {
                    "instrument_token": {"source": "asset.security_id"},
                    "interval": {"source": "kite_timeframe(freq)"},
                }
            },
            "query": {
                "fields": {
                    "from": {"source": "kite_format_datetime(from_dt)"},
                    "to": {"source": "kite_format_datetime(to_dt)"},
                    "continuous": {"source": "0"},  # Set to 1 for continuous futures
                    "oi": {"source": "0"},  # Set to 1 to include open interest
                }
            },
        },
        "response": {
            "payload_type": "data",
            "payload_path": "response.get('data', {}).get('candles', [])",
            "data": {
                "frame": "values",  # Kite returns array of arrays: [[timestamp, o, h, l, c, v], ...]
                "rename": {
                    "timestamp": "0",
                    "open": "1",
                    "high": "2",
                    "low": "3",
                    "close": "4",
                    "volume": "5"
                },
                "timestamp": {"format": "ISO8601"},
            },
        },
    },

}

API_SPEC = {
    'base_url': 'https://api.kite.trade',
    'headers': COMMON_HEADERS,
    'endpoints': {**TRADING_ENDPOINTS, **MARKET_DATA_ENDPOINTS}
}


# -------------------------------------------------------------------------
# MASTER_DATA_SPEC - Instruments Universe
# -------------------------------------------------------------------------

MASTER_DATA_SPEC = [
    {
        "endpoint": {
            "endpoint": "/instruments",
            "method": "GET",
            "response": {  "result": {}},
        },
        "mode": "file",
        "format": "csv",
        "compression":'gzip',
        "assets": [
            # Indices
            {
                "filter": "data['segment'] == 'INDICES'",
                "asset_class": "mktdata",
                "vectorized": True,
                "mapping": {
                    "symbol": {"source": "kite_symbol_format(data['tradingsymbol'])"},
                    # "symbol": {"source": "data['tradingsymbol'].str.replace(' ','')"},  # Short Name as symbol
                    "broker_symbol": {"source": "kite_symbol_format(data['tradingsymbol'])"},  # Short Name as symbol
                    "security_id": {"source": "data['instrument_token']"},
                    "name": {"source": "data['name']"},
                    "exchange_name": {"source": "data['exchange']"},
                    "calendar_name": {"source": "data['exchange']"},
                },
            },
            # Equities
            {
                "filter":  "(data['instrument_type'] == 'EQ') & (data['segment'].isin(['BSE', 'NSE']))",
                "vectorized": True,
                "asset_class": "equity",
                "mapping": {
                    "symbol": {"source": "data['tradingsymbol'].str.replace(' ','')"},  # Short Name as symbol
                    "broker_symbol": {"source": "data['tradingsymbol'].str.replace(' ','')"},  # Short Name as symbol
                    "security_id": {"source": "data['instrument_token'].astype(str)"},
                    "name": {"source": "data['name']"},
                    "exchange_name": {"source": "data['exchange']"},
                    "calendar_name": {"source": "data['exchange']"},
                    "tick_size": {"source": "data['tick_size']"},  # Tick size
                },
            },
            # Futures
            {
            "filter": "data['segment'].isin(['NFO-FUT', 'BFO-FUT'])",
            "asset_class": "equity-futures",
            "vectorized":True,
            "details": {
                "underlying_exchange": {"source": "data['exchange']"},
                "expiry_types": {"source": "['monthly']"},
            },
            "mapping": {
                "symbol": {"source": "''"},
                "broker_symbol": {"source": "data['tradingsymbol']"},
                "security_id": {"source": "data['instrument_token']"},
                "name": {"source": "data['name']"},
                "tick_size":{"source": "10/data['tick_size'].astype(float)"}, # Tick size
                "exchange_name":{"source": "data['exchange']"},
                "calendar_name": {"source": "data['exchange']"},

                "underlying": {"source": "data['name'].str.replace(' ','')"},
                "root": {"source": "data['name'].str.replace(' ','')"},
                "mult": {"source": "data['lot_size'].astype(int)"},
                "expiry_date": {"source": "pd.to_datetime(data['expiry'])"},
            }
            },
            # Options
            {
            "filter": "data['segment'].isin(['NFO-OPT', 'BFO-OPT'])",
            "asset_class": "equity-options",
            "vectorized":True,
            "details": {
                "underlying_exchange": {"source": "data['exchange']"},
                # "expiry_types": {"source": "['monthly']"},
            },
            "mapping": {
                "symbol": {"source": "''"},
                "broker_symbol": {"source": "data['tradingsymbol']"},
                "security_id": {"source": "data['instrument_token']"},
                "name": {"source": "data['name']"},
                "tick_size":{"source": "100/data['tick_size'].astype(float)"}, # Tick size
                "exchange_name":  {"source": "data['exchange']"},
                "calendar_name":  {"source": "data['exchange']"},

                "underlying": {"source": "data['name'].str.replace(' ','')"},
                "root": {"source": "data['name'].str.replace(' ','')"},
                "mult": {"source": "data['lot_size'].astype(int)"},
                "expiry_date": {"source": "pd.to_datetime(data['expiry'])"},
                "option_type": {"source": "data['instrument_type']"},
                "strike": {"source": "data['strike']"},
            }
            },



        ],
    }
]


# -------------------------------------------------------------------------
# OBJECTS_SPEC - Object Conversion Pipelines
# -------------------------------------------------------------------------
OBJECTS_SPEC = {
    "position": [
        {
            "fields": {
                "quantity": {"source": "float(data.get('quantity', 0))"},
                "average_price": {"source": "float(data.get('average_price', 0))"},
                "last_price": {"source": "float(data.get('last_price', 0))"},
            }
        }
    ],
    "account": [
        {
            "fields": {
                "name": {"source": "data.get('name', '')"},
                "cash": {"source": "float(data.get('cash', 0))"},
                "currency": {"source": "data.get('currency', 'INR')"},
            }
        }
    ],
}


# -------------------------------------------------------------------------
# STREAMING_SPEC - WebSocket Configuration
# -------------------------------------------------------------------------
STREAMING_SPEC = {
    "connections": {
        "market_data": {
            "url": "wss://ws.kite.trade",  # Will be formatted with credentials via auth
            "backend": {"type": "websocket"},
            "streams": ["data", "order", "quote"], # tood: pass data and orders
            # "streams": ["data"], # todo: pass data and orders
            "auth": {
                "mode": "url",
                "url": {
                    "query": {"fields": {
                        "api_key": {"source": "credentials.api_key"},
                        "access_token": {"source": "credentials.access_token"}
                    }}
                }
            },
            "subscribe": {
                "subscribe":{
                    "data":{
                        "format": "json",
                        "json": {
                            "fields": {
                                "a": {"source": "'mode'"},
                                "v": {"source": "kite_subscriber_json(subscribe_assets)"}
                            }
                        }
                    },
                },
                "unsubscribe": {"data":{
                    "format": "json",
                    "json": {
                        "fields": {
                            "a": {"source": "'unsubscribe'"},
                            "v": {"source": "kite_subscriber_json(subscribe_assets)"}  # todo
                        }
                    }
                }}
            },
            "parser": {
                "format": "binary",
                "decode": "kite_parse_binary"
            },
            "router": {
                "rules": [
                    {"channel": "data", "match": "isinstance(data, dict) and 'data' == data.get('type', '')"},
                    {"channel": "order", "match": "isinstance(data, dict) and 'order' == data.get('type', '')"},
                    {"channel": "quote", "match": "isinstance(data, dict) and 'quote' == data.get('type', '')"},
                ]
            },
            "converters": {
                "data": [
                        {
                        "condition": "isinstance(data, dict) and 'data' == data.get('type', '')",
                        "fields": {
                            "asset": {"source": "broker.sid(data['security_id'])"},
                            "timestamp": {"source": "pd.Timestamp(data['timestamp']).tz_convert(broker.tz)"},
                            "data": {
                                    "price": {"source": "float(data['price'])"},
                                    "volume": {"source": "int(data['volume'])"}
                            }
                        }
                    }
                ],
                "order": [
                    {
                        "condition": "isinstance(data, dict) and 'order' == data.get('type', '')",
                        "fields": {
                            "order_id": {"source": "data['data']['order_id']"},
                             "broker_order_id": {"source": "data['data']['order_id']"},
                             "security_id": {"source": "data['data']['instrument_token']"},
                             "symbol": {"source": "data['data']['tradingsymbol']"},
                             "quantity": {"source": "float(data['data'].get('quantity', 0))"},
                             "filled": {"source": "float(data['data'].get('filled_quantity', 0 ))"},
                             "average_price":{"source": "float(data['data'].get('average_price', 0 ))"},
                             "side": {"source": "mappings.order_side.to_blueshift(data['data']['transaction_type'])"},
                             "order_type": {"source": "mappings.order_type.to_blueshift(data['data']['order_type'])"},
                             "order_validity": {"source": "mappings.order_validity.to_blueshift(data['data']['validity'])"},
                             "price": {"source": "float(data['data'].get('price', 0 ))"},
                             "status": {"source": "mappings.order_status.to_blueshift(data['data']['status'])"},
                             "timestamp": {"source": "pd.Timestamp(data['data']['order_timestamp'])"},
                        }
                    }
                ],
                "quote":  [
                    {
                        "condition": "isinstance(data, dict) and 'quote' == data.get('type', '')",
                        "fields": {
                            "timestamp": {"source": "pd.Timestamp(data.get('timestamp')).tz_convert(broker.tz)"},
                            "symbol":{"source":"broker.sid(data['security_id']).symbol"},
                            "bids": {"source": "data.get('bids', [])"},
                            "asks": {"source": "data.get('asks', [])"},
                        }
                    }
                ]
            }
    }
}}


# -------------------------------------------------------------------------
# Broker Variants
# -------------------------------------------------------------------------
BROKER_VARIANTS = {
    'live': {
        "broker": {
            "name": "kite-live",
            "variant": "live",
            "display_name": "Zerodha Kite Live",
        },
    },
    'paper': {
        "broker": {
            "name": "kite-paper",
            "variant": "paper",
            "display_name": "Zerodha Kite Paper",
        },
    },
}


# -------------------------------------------------------------------------
# Config Creation and Registration
# -------------------------------------------------------------------------
def create_config(variant='live') -> APIBrokerConfig:
    """Create the broker config by merging variant specs with base specs."""
    broker_spec = dict(BROKER_SPEC)
    api_spec = dict(API_SPEC)
    obj_spec = dict(OBJECTS_SPEC)
    streaming_spec = dict(STREAMING_SPEC)

    # Add master data to API spec
    api_spec["master_data"] = MASTER_DATA_SPEC.copy()

    # Apply variant-specific overrides
    variant_specs = BROKER_VARIANTS.get(variant, {})
    if variant_specs:
        broker_variant = variant_specs.get('broker', {})
        api_variant = variant_specs.get('api', {})
        obj_variant = variant_specs.get('objects', {})

        broker_spec = merge_json_recursive(broker_spec, broker_variant)
        api_spec = merge_json_recursive(api_spec, api_variant)
        obj_spec = merge_json_recursive(obj_spec, obj_variant)
        broker_spec['variant'] = variant

    # Pass streaming_spec separately to APIBrokerConfig
    return APIBrokerConfig(broker_spec, api_spec, obj_spec, streaming_spec, registry=registry)


def register_brokers():
    """Register broker class for each variant."""
    for variant in BROKER_VARIANTS:
        config = create_config(variant)
        cls_name = to_title_case(config.broker.name)
        broker_class_factory(cls_name, config)
        # print(f"registered: {cls_name}")

print(f"Using kite from: {__file__}")