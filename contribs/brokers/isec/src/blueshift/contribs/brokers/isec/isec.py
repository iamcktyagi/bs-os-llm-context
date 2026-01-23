"""
ICICIDirect Breeze API broker integration for Blueshift (declarative config).

Implements:
- Broker spec (credentials, options, enum mappings)
- API spec (trading + market data endpoints)
- Master data spec (assets universe)
- Objects spec (order/position/account conversions)
"""
from __future__ import annotations
from typing import cast
import hashlib
import base64
import json as json_lib
import pandas as pd
from datetime import datetime
import datetime as datetimemod

from blueshift.interfaces.assets._assets import InstrumentType, Option, Asset, OptionType
from blueshift.lib.common.sentinels import noop
from blueshift.lib.common.constants import Frequency
from blueshift.lib.common.functions import merge_json_recursive, to_title_case
from blueshift.lib.trades._order_types import (
    OrderSide, ProductType, OrderType, OrderValidity, OrderStatus)
from blueshift.calendar import get_calendar
from blueshift.lib.exceptions import ValidationError, AuthenticationError

from blueshift.brokers.core.config.config import APIBrokerConfig
from blueshift.brokers.core.broker import RestAPIBroker
from blueshift.brokers.core.config.resolver import ConfigRegistry
from blueshift.brokers.factory import broker_class_factory

registry = ConfigRegistry(globals())
cal = get_calendar('NSE')

@registry.register()
def isec_jsonify(values, **kwargs):
    return json_lib.dumps(values, separators=(',', ':'))

@registry.register()
def isec_headers(**kwargs):
    """Create I-Sec API authorization header."""
    # only one of params, json or body can be valid at a time
    params = kwargs.get('params',{})
    json = params or kwargs.get('json',{})
    body = json or kwargs.get('body',{})

    if not isinstance(body, str):
        body = isec_jsonify(body)
    
    credentials = kwargs.get('credentials', None)

    if not credentials:
        return {}

    current_date = isec_timestamp()
    checksum = hashlib.sha256((current_date+body+credentials.secret_key).encode("utf-8")).hexdigest()
    
    headers = {
        "Content-Type": "application/json",
        'X-Checksum': "token "+checksum,
        'X-Timestamp': current_date,
        'X-AppKey': credentials.api_key,
        'X-SessionToken': credentials.api_token
    }
    
    return headers

@registry.register()
def isec_init_session(**context):
    credentials = context['credentials']
    if credentials.api_token:
        return
    
    api = context.get('api')
    url = context.get('url')
    verb = context.get('verb')

    if not api or not url:
        raise ValidationError(f'no API or url found for token generation')

    headers = {
        "Content-Type": "application/json"
    }

    body = {
        "SessionToken": credentials.session_token,
        "AppKey": credentials.api_key
    }
    
    body = json_lib.dumps(body, separators=(',', ':'))
    success, status_code, resp = api.raw_request(url, verb, headers, {}, body, {})
    
    if not success or status_code != 200:
        raise AuthenticationError(f'failed to get session token: {status_code}, got respponse {resp}')
    
    api_token = None
    try:
        if 'Success' in resp and resp['Success']!=None:
            api_token = resp['Success']['session_token']
            decoded = base64.b64decode(api_token.encode('ascii')).decode('ascii')
            user_id, session_key = decoded.split(":")[:2]
        elif 'Error' in resp:
            reason = resp['Error']
            msg = f"Could not authenticate credential, server reported error: {reason}"
            raise AuthenticationError(msg)
        else:
            msg = "Could not authenticate credentials, "
            msg += "please check token and keys"
            raise AuthenticationError(msg)
    except Exception as e:
        msg = "Error parsing response: {str(e)}"
        raise AuthenticationError(msg)
    else:
        credentials.update(api_token=api_token, user_id=user_id, session_key=session_key)

@registry.register()
def isec_timestamp(**kwargs) -> str:
    """
    Return current timestamp in ISO8601 format with 0 milliseconds.
    Format: YYYY-MM-DDTHH:MM:SS.000Z
    """
    now = datetime.now(datetimemod.timezone.utc)
    return now.strftime('%Y-%m-%dT%H:%M:%S.000Z')

@registry.register()
def isec_timeframe(freq, **kwargs) -> str:
    """ Map Blueshift Frequency -> Breeze interval. """
    mapping = {
        Frequency('1m'): "1minute",
        Frequency('5m'): "5minute",
        Frequency('30m'): "30minute",
        Frequency('1d'): "1day",
    }
    return mapping.get(freq, "1minute")

@registry.register()
def isec_exchange_code(asset:Asset, **kwargs) -> str:
    """ Map asset exchange to Breeze exchange code. """
    if not asset:
        return "NSE"
    
    return asset.exchange_name

@registry.register()
def isec_product_type(asset:Asset, product_type, **kwargs) -> str:
    """ Map Blueshift product type to Breeze product type. """
    # "futures", "options", "optionplus", "cash", "btst", "margin"
    if asset.is_opt():
        return "options"
    if asset.is_futures():
        return "futures"

    # For equity
    if product_type == ProductType.DELIVERY:
        return "cash"
    elif product_type == ProductType.INTRADAY:
        return "margin"
    
    return "cash"

@registry.register()
def isec_right(asset:Asset, **kwargs) -> str:
    if not asset.is_opt():
        return "others"
    
    asset = cast(Option, asset)
    if asset.option_type == OptionType.CALL:
        return "call"
    elif asset.option_type == OptionType.PUT:
        return "put"
    
    raise ValueError(f'unsupported option type {asset.option_type}')

@registry.register()
def isec_expiry(asset:Asset, **kwargs) -> str:
    if not asset.is_opt() and not asset.is_futures():
        return ""
    
    # ISO 8601
    asset = cast(Option, asset)
    return asset.expiry_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')

# -------------------------------------------------------------------------
# Broker Config
# -------------------------------------------------------------------------
BROKER_SPEC = {
    "calendar": "NSE",
    "credentials": {
        "fields": ["api_key", "secret_key", "session_token", "user_id", "api_token", "session_key"],
        "validator": "credentials.api_key and credentials.session_token",
    },
    "options": {
        "timeout": 10,
        "lazy_session":True,
        "rate_limit": 5, # Conservative
        "rate_limit_period": 1,
        "max_tickers": 50,
        "supported_modes": ["LIVE"],
        "fractional_trading": False,
        "expiry_dispatch":{"BFO":"BFO","NFO":"NFO"},
    },
    #"assets": ["equity", "equity-futures", "equity-options"],
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
        },
        "default_value": OrderValidity.DAY,
    },
    "order_status": {
        "map": {
            OrderStatus.OPEN: ["Ordered", "Modified", "Partly Executed"],
            OrderStatus.COMPLETE: "Executed",
            OrderStatus.CANCELLED: ["Cancelled", "Expired"],
            OrderStatus.REJECTED: "Rejected"
        },
        "default_value": OrderStatus.OPEN,
    },
    "product_type": {
        "map": {
            ProductType.DELIVERY: "cash",
            ProductType.INTRADAY: "margin",
            ProductType.MARGIN: "margin",
        },
        "default_value": ProductType.DELIVERY,
    },
}

# ---------------------------------------------------------------------
# API Config
# ---------------------------------------------------------------------

COMMON_HEADERS = {
    'custom':'isec_headers'
}


TRADING_ENDPOINTS = {
    "session":{
        "endpoint": "/customerdetails",
        "method": "GET",
        "request": {
            "custom":"isec_init_session",
        },
        "response": {
            "payload_type":"custom",
            "custom":"noop",
        },  
    },
    "get_account": { # Mapping 'funds' to get_account
        "endpoint": "/funds",
        "method": "GET",
        "request": {"body": {"custom": "isec_jsonify"}},
        "response": {
            "payload_type": "object",
            "result": {
                "fields":{
                    "name":{"source":"result['Success']['bank_account']"},
                    "cash": {"source": "result['Success'].get('total_bank_balance', 0)"},
                    "currency": {"source": "'INR'"},
                }
            },
        },
    },
    
    "get_orders": {
        "endpoint": "/order",
        "method": "GET",
        "request": {
            "body": {
                "custom": "isec_jsonify",
                "fields": {
                    "exchange_code": {"source": "'NSE'"}, # Defaulting to NSE for now
                    "from_date": {"source": "str(pd.Timestamp.now() - pd.Timedelta(days=1))[:10] + 'T00:00:00.000Z'"},
                    "to_date": {"source": "str(pd.Timestamp.now() + pd.Timedelta(days=1))[:10] + 'T00:00:00.000Z'"},
                }
            }
        },
        "response": {
            "payload_type": "array",
            "payload_path": "response['Success'] if response.get('Status') == 200 else []",
            "items": {
                "fields":{
                    "oid": {"source": "item['order_id']"},
                    "symbol": {"source": "item['stock_code']"},
                    "quantity": {"source": "float(item.get('quantity', 0))"},
                    "filled": {"source": "float(item.get('executed_quantity', 0) or 0)"},
                    "side": {"source": "mappings.order_side.to_blueshift(item.get('action'))"},
                    "order_type": {"source": "mappings.order_type.to_blueshift(item.get('order_type'))"},
                    "price": {"source": "float(item.get('price', 0))"},
                    "status": {"source": "mappings.order_status.to_blueshift(item.get('status'))"},
                    "timestamp": {"source": "pd.Timestamp(item.get('order_date')).tz_convert(broker.tz)"},
                    "exchange_timestamp": {"source": "pd.Timestamp(item.get('order_date')).tz_convert(broker.tz)"},
                    "average_price": {"source": "float(item.get('average_price', 0) or 0)"},
                }
            },
        },
    },

    "place_order": {
        "endpoint": "/order",
        "method": "POST",
        "request": {
            "query": {
                "custom":"isec_jsonify",
                "fields": {
                    "stock_code": {"source": "order.asset.broker_symbol or order.asset.symbol"},
                    "exchange_code": {"source": "isec_exchange_code(order.asset)"},
                    "product": {"source": "isec_product_type(order.asset, order.product_type)"},
                    "action": {"source": "mappings.order_side.from_blueshift(order.side)"},
                    "order_type": {"source": "mappings.order_type.from_blueshift(order.order_type)"},
                    "stoploss": {"source": "''"}, # Optional
                    "quantity": {"source": "str(int(order.quantity))"},
                    "price": {"source": "str(order.price)"},
                    "validity": {"source": "mappings.order_validity.from_blueshift(order.order_validity)"},
                    "expiry_date": {"source": "isec_expiry(order.asset)"},
                    "right": {"source": "isec_right(order.asset)"},
                    "strike_price": {"source": "str(order.asset.strike) if order.asset.is_opt() else '0'"},
                }
            }
        },
        "response": {
            "payload_type": "object",
            "result": {"fields":{"order_id": {"source": "result['Success']['order_id']"}}},
        },
    },
    
    "cancel_order": {
        "endpoint": "/order",
        "method": "DELETE",
        "request": {
            "body": {
                "custom":"isec_jsonify",
                "fields": {
                    "order_id": {"source": "order.broker_order_id or order.oid"},
                    "exchange_code": {"source": "isec_exchange_code(order.asset)"},
                }
            }
        },
        "response": {
            "payload_type": "object",
             # Success response has order_id
             "result": {"fields":{"order_id": {"source": "result['Success']['order_id']"}}},
        }
    },

    "get_positions": {
        "endpoint": "/portfoliopositions",
        "method": "GET",
        "request": {"body": {"custom": "isec_jsonify"}},
        "response": {
            "payload_type": "array",
            "payload_path": "response['Success'] if response.get('Status') == 200 else []",
            "items": {
                "fields":{
                    "symbol": {"source": "item['stock_code']"},
                    "quantity": {"source": "float(item['quantity'])"},
                    "average_price": {"source": "float(item.get('average_price', 0))"},
                    "product_type": {"source": "ProductType.DELIVERY"},
                }
            },
        },
    }
}

@registry.register()
def isec_stream_auth_token(credentials, **kwargs) -> dict:
    return {"user": credentials.user_id, "token": credentials.session_token}

@registry.register()
def isec_market_data_token(subscribe_assets, **kwargs) -> list[str]:
    """ Convert list[Asset] -> list of 4.1!Token for NSE etc. """
    if not isinstance(subscribe_assets, list):
        subscribe_assets = [subscribe_assets]
    
    tokens = []
    for asset in subscribe_assets:
        # Default to NSE (4)
        exch_code = "4"
        if asset.exchange_name == 'BSE':
            exch_code = "1"
        elif asset.exchange_name == 'NFO':
             exch_code = "4"
        elif asset.exchange_name == 'NDX':
             exch_code = "13"
        elif asset.exchange_name == 'MCX':
             exch_code = "6"
        
        # 1 for Trades, 2 for Depth.
        if asset.security_id:
            tokens.append(f"{exch_code}.1!{asset.security_id}")
            
    return tokens

@registry.register()
def isec_market_quote_token(subscribe_assets, **kwargs) -> list[str]:
    """ Convert list[Asset] -> list of 4.1!Token for NSE etc. """
    if not isinstance(subscribe_assets, list):
        subscribe_assets = [subscribe_assets]
    
    tokens = []
    for asset in subscribe_assets:
        # Default to NSE (4)
        exch_code = "4"
        if asset.exchange_name == 'BSE':
            exch_code = "1"
        elif asset.exchange_name == 'NFO':
             exch_code = "4"
        elif asset.exchange_name == 'NDX':
             exch_code = "13"
        elif asset.exchange_name == 'MCX':
             exch_code = "6"
        
        if asset.security_id:
            tokens.append(f"{exch_code}.2!{asset.security_id}")
            
    return tokens

STREAMING_SPEC = {
    "connections": {
        "market_data": {
            "url": "https://livestream.icicidirect.com",
            "backend": {"type": "socketio", "options": {"transports": ["websocket"]}},
            "streams": ["data","quote"],
            #"events": {"data": "stock"},
            "auth": {
                "mode": "first_message",
                "first_message": {
                    "format": "json",
                    "json": {
                        "fields": {
                            "user":{"source": "credentials.user_id"},
                            "token":{"source": "credentials.session_key"}
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
                                "action": {"source": "'join'"},
                                "data": {"source": "isec_market_data_token(subscribe_assets)"}
                            }
                        }
                    },
                    "quote":{
                        "format": "json",
                        "json": {
                            "fields": {
                                "action": {"source": "'join'"},
                                "data": {"source": "isec_market_quote_token(subscribe_assets)"}
                            }
                        }
                    }
                }
            },
            "converters": {
                "data": [
                    {
                        "fields": {
                             "asset": {"source": "broker.infer_asset(security_id=str(data[0]).split('!')[1])"}, # data[0] = '4.1!1594'
                             "timestamp": {"source": "pd.Timestamp(data[6]).tz_convert(broker.tz)"}, # SDK index 6 is time
                             "data": {
                                 "close": {"source": "float(data[1])"} # SDK index 1 is last rate
                             }
                        }
                    }
                ]
            }
        },
        # "trading": {
        #     "url": "https://livefeeds.icicidirect.com",
        #     "backend": {"type": "socketio", "options": {"transports": ["websocket"]}},
        #     "streams": ["order"],
        #     "events": {"order": "order"},
        #     "auth": {
        #          "mode": "first_message",
        #          "first_message": {
        #              "format": "json",
        #              "json": {"fields": {"auth": {"source": "isec_stream_auth_token(credentials)"}}}
        #         }
        #     },
        #     "converters": {
        #         "order": [
        #             {
        #                 "fields": {
        #                     "order_id": {"source": "data['order_id']"},
        #                     # ... map other fields
        #                 }
        #             }
        #         ]
        #     }
        # }
    }
}

MARKET_DATA_ENDPOINTS = {
    "get_history": {
        "endpoint": "https://breezeapi.icicidirect.com/api/v2//historicalcharts",
        "method": "GET",
        "headers":{
            "fields":{
                "Content-Type": {"source": "'application/json'"},
                "X-SessionToken": {"source": "config.broker.credentials.api_token"},
                "apikey": {"source": "config.broker.credentials.api_key"},
            }
        },
        "use_global_headers":False,
        "request": {
            "query": {
                "fields": {
                    "interval": {"source": "isec_timeframe(freq)"},
                    "from_date": {"source": "from_dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')"},
                    "to_date": {"source": "to_dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')"},
                    "stock_code": {"source": "asset.broker_symbol or asset.symbol"},
                    "exch_code": {"source": "isec_exchange_code(asset)"},
                    "product_type": {"source": "isec_product_type(asset, 'delivery')"}, # charts usually need product type? 'cash' for equity
                    "expiry_date": {"source": "isec_expiry(asset)"},
                    "right": {"source": "isec_right(asset)"},
                    "strike_price": {"source": "str(asset.strike) if asset.is_opt() else '0'"},
                }
            }
        },
        "response": {
            "payload_type": "data",
            "payload_path": "response['Success'] if response.get('Status') == 200 else []",
            "data": {
                "frame": "records",
                "timestamp": {"field": "datetime", "format": "%Y-%m-%d %H:%M:%S"}, 
                "rename": {"timestamp": "datetime", "open": "open", "high": "high", "low": "low", "close": "close", "volume": "volume"},
            }
        }
    },
    
    "get_quote": {
        "endpoint": "/quotes",
        "method": "GET",
        "request": {
            "query": {
                "fields": {
                    "stock_code": {"source": "asset.broker_symbol or asset.symbol"},
                    "exchange_code": {"source": "isec_exchange_code(asset)"},
                    #"product_type": {"source": "isec_product_type(asset, 'delivery')"},
                    "expiry_date": {"source": "isec_expiry(asset)"},
                    "right": {"source": "isec_right(asset)"},
                    "strike_price": {"source": "str(asset.strike) if asset.is_opt() else '0'"},
                }
            }
        },
        "response": {
            "payload_type": "quote",
            "payload_path": "response['Success'] if response.get('Status') == 200 else {}",
            "quote": {
                # "timestamp": ... (not in example response?)
                "last": {"source": "float(quote.get('ltp', 0))"},
                # "bid": ...
                # "ask": ...
            }
        }
    }
}

API_SPEC = {
    'base_url': 'https://api.icicidirect.com/breezeapi/api/v1',
    'headers': COMMON_HEADERS,
    'endpoints': {**TRADING_ENDPOINTS, **MARKET_DATA_ENDPOINTS},
}

# ---------------------------------------------------------------------
# Master Data Config
# ---------------------------------------------------------------------
MASTER_DATA_SPEC = [
    {
        "endpoint": {
            "endpoint": "https://traderweb.icicidirect.com/Content/File/txtFile/ScripFile/StockScriptNew.csv",
            "method": "GET",
            "use_global_headers":False,
            "response": {"payload_type": "object","result":{"fields":{}}} # Dummy
        },
        "mode": "file",
        "format": "csv",
        "csv_options": {"dtype": "str"},
        "assets": [
            {
                # NSE Equity
                "filter": "(data['EC'] == 'NSE') & (data['SG'] == 'EQUITY') & (data['SR'] == 'EQ') & (data['TS'].astype('float') > 0)",
                "asset_class": "equity",
                "vectorized":True,
                "mapping": {
                    "symbol": {"source": "data['NS']"}, # Short Name as symbol
                    "broker_symbol": {"source": "data['SC']"}, # Stock Code
                    "security_id": {"source": "data['TK']"}, # Token
                    "name": {"source": "data['SN']"},
                    "tick_size":{"source": "1/data['TS'].astype(float)"}, # Tick size
                    "exchange_name": {"source": "'NSE'"},
                    "calendar_name": {"source": "'NSE'"},
                }
            },
            {
                # BSE Equity
                "filter": "(data['EC'] == 'BSE') & (data['SG'] == 'EQUITY') & (data['SR'] == 'C') & (data['TS'].astype('float') > 0)",
                "asset_class": "equity",
                "vectorized":True,
                "mapping": {
                    "symbol": {"source": "data['NS']"}, # Short Name as symbol
                    "broker_symbol": {"source": "data['SC']"}, # Stock Code
                    "security_id": {"source": "data['TK']"}, # Token
                    "name": {"source": "data['SN']"},
                    "tick_size":{"source": "1/data['TS'].astype(float)"}, # Tick size
                    "exchange_name": {"source": "'BSE'"},
                    "calendar_name": {"source": "'BSE'"},
                }
            },
            {
                # NSE Indices
                "filter": "(data['EC'] == 'NSE') & (data['ISIN'].isna()) & (data['SR'] == '0') & (data['TS'].astype('float') == 0)",
                "asset_class": "mktdata",
                "vectorized":True,
                "mapping": {
                    "symbol": {"source": "data['NS'].str.replace(' ','')"}, # Short Name as symbol
                    "broker_symbol": {"source": "data['SC']"}, # Stock Code
                    "security_id": {"source": "data['TK']"}, # Token
                    "name": {"source": "data['SN']"},
                    "exchange_name": {"source": "'NSE'"},
                    "calendar_name": {"source": "'NSE'"},
                }
            },
            {
                # NSE Futures
                "filter": "(data['EC'] == 'NFO') & (data['CD'].str.contains('FUT-'))",
                "asset_class": "equity-futures",
                "vectorized":True,
                "details":{
                    "underlying_exchange":{"source":"'NSE'"},
                    "expiry_types":{"source":"['monthly']"},
                },
                "mapping": {
                    "symbol": {"source": "''"}, # infer from underlying and expiry
                    "broker_symbol": {"source": "data['SC']"}, # Stock Code
                    "security_id": {"source": "data['TK']"}, # Token
                    "name": {"source": "data['SN']"},
                    "tick_size":{"source": "100/data['TS'].astype(float)"}, # Tick size
                    "exchange_name": {"source": "'NFO'"},
                    "calendar_name": {"source": "'NFO'"},
                    "underlying": {"source": "data['NS'].str.replace(' ','')"},
                    "root": {"source": "data['NS']"},
                    "mult": {"source": "data['LS'].astype(int)"},
                    "expiry_date":{"source":"pd.to_datetime(data['SM'].str.split(':').str[1])"},
                }
            },
            {
                # BSE Futures
                "filter": "(data['EC'] == 'BFO') & (data['CD'].str.contains('FUT-'))",
                "asset_class": "equity-futures",
                "vectorized":True,
                "details":{
                    "underlying_exchange":{"source":"'BSE'"},
                    "expiry_types":{"source":"['monthly']"},
                },
                "mapping": {
                    "symbol": {"source": "''"}, # infer from underlying and expiry
                    "broker_symbol": {"source": "data['SC']"}, # Stock Code
                    "security_id": {"source": "data['TK']"}, # Token
                    "name": {"source": "data['SN']"},
                    "tick_size":{"source": "100/data['TS'].astype(float)"}, # Tick size
                    "exchange_name": {"source": "'BFO'"},
                    "calendar_name": {"source": "'BFO'"},
                    "underlying": {"source": "data['NS'].str.replace(' ','')"},
                    "root": {"source": "data['NS']"},
                    "mult": {"source": "data['LS'].astype(int)"},
                    "expiry_date":{"source":"pd.to_datetime(data['SM'].str.split(':').str[1])"},
                }
            },
            {
                # BSE Options
                "filter": "(data['EC'] == 'BFO') & (data['CD'].str.contains('OPT-'))",
                "asset_class": "equity-options",
                "vectorized":True,
                "details":{
                    "underlying_exchange":{"source":"'BSE'"},
                },
                "mapping": {
                    "symbol": {"source": "''"}, # infer from underlying and expiry
                    "broker_symbol": {"source": "data['SC']"}, # Stock Code
                    "security_id": {"source": "data['TK']"}, # Token
                    "name": {"source": "data['SN']"},
                    "tick_size":{"source": "100/data['TS'].astype(float)"}, # Tick size
                    "exchange_name": {"source": "'BFO'"},
                    "calendar_name": {"source": "'BFO'"},
                    "underlying": {"source": "data['NS'].str.replace(' ','')"},
                    "root": {"source": "data['NS']"},
                    "mult": {"source": "data['LS'].astype(int)"},
                    "expiry_date":{"source":"pd.to_datetime(data['SM'].str.split(':').str[1])"},
                    "option_type":{"source":"data['SM'].str.split(':').str[2]"},
                    # strikes are given in paise
                    "strike":{"source":"data['SM'].str.split(':').str[3].astype(int)/100"}, 
                }
            },
            {
                # NSE Options
                "filter": "(data['EC'] == 'NFO') & (data['CD'].str.contains('OPT-'))",
                "asset_class": "equity-options",
                "vectorized":True,
                "details":{
                    "underlying_exchange":{"source":"'NSE'"},
                },
                "mapping": {
                    "symbol": {"source": "''"}, # infer from underlying and expiry
                    "broker_symbol": {"source": "data['SC']"}, # Stock Code
                    "security_id": {"source": "data['TK']"}, # Token
                    "name": {"source": "data['SN']"},
                    "tick_size":{"source": "100/data['TS'].astype(float)"}, # Tick size in paise
                    "exchange_name": {"source": "'NFO'"},
                    "calendar_name": {"source": "'NFO'"},
                    "underlying": {"source": "data['NS'].str.replace(' ','')"},
                    "root": {"source": "data['NS']"},
                    "mult": {"source": "data['LS'].astype(int)"},
                    "expiry_date":{"source":"pd.to_datetime(data['SM'].str.split(':').str[1])"},
                    "option_type":{"source":"data['SM'].str.split(':').str[2]"},
                    # strikes are given in paise
                    "strike":{"source":"data['SM'].str.split(':').str[3].astype(int)/100"}, 
                }
            },
        ]
    }
]

# ---------------------------------------------------------------------
# Object Conversions
# ---------------------------------------------------------------------
OBJECTS_SPEC = {
}

BROKER_VARIANTS = {
    'live': {
        "broker": {
            "name": "isec-live",
            "variant": "live",
            "display_name": "ICICI Direct Live",
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
        api_variant = variant_specs.get('api',{})
        obj_variant = variant_specs.get('objects',{})
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
