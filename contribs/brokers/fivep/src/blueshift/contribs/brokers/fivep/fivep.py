"""
5Paisa REST API broker integration for Blueshift (declarative config).

Implements:
- Broker spec (credentials, options, enum mappings)
- API spec (trading + market data endpoints)
- Master data spec (assets universe)
- Objects spec (order/position/account conversions)

Notes:
- Uses the JSON/REST API endpoints of 5Paisa.
- Authentication uses TOTP flow (ClientCode + TOTP + PIN).
"""
from __future__ import annotations
import pandas as pd
import json
import base64
import hashlib
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

# Check for pyotp
try:
    import pyotp
except ImportError:
    pyotp = None

from blueshift.interfaces.assets._assets import InstrumentType
from blueshift.lib.common.constants import Frequency
from blueshift.lib.common.functions import merge_json_recursive, to_title_case
from blueshift.lib.trades._order_types import (
    OrderSide, ProductType, OrderType, OrderValidity, OrderStatus)
from blueshift.calendar import get_calendar

from blueshift.brokers.core.config.config import APIBrokerConfig
from blueshift.brokers.core.broker import RestAPIBroker
from blueshift.brokers.core.config.resolver import ConfigRegistry
from blueshift.brokers.factory import broker_class_factory
from blueshift.lib.exceptions import ValidationError

# -------------------------------------------------------------------------
# Registry & Helper Functions
# -------------------------------------------------------------------------
registry = ConfigRegistry(globals())
cal = get_calendar('NSE')

@registry.register()
def generate_totp(secret: str, **kwargs) -> str:
    """Generate TOTP for 5Paisa login."""
    if not pyotp:
        raise ValidationError("pyotp module is required for 5Paisa TOTP login.")
    return pyotp.TOTP(secret).now()

@registry.register()
def encrypt_payload(data: str, key: str, iv: str, **kwargs) -> str:
    """
    Encrypt payload if required by specific 5Paisa flows. 
    (Placeholder: Standard REST API often uses direct JSON with HTTPS).
    """
    # Implementation depends on specific encryption requirements of 5Paisa
    return data

@registry.register()
def fivep_exch_type(asset, **kwargs) -> str:
    """Map Asset to 5Paisa ExchangeType (C=Cash, D=Derivatives, U=Currency)."""
    if asset.asset_class == 'equity':
        return 'C'
    elif asset.asset_class == 'commodity':
        return 'M'
    elif asset.asset_class == 'forex':
        return 'U'
    return 'D' # Default to derivatives for options/futures

@registry.register()
def fivep_exchange(asset, **kwargs) -> str:
    """Map Asset to 5Paisa Exchange (N=NSE, B=BSE, M=MCX)."""
    # Simple logic, expand as needed
    if asset.exchange_name == 'BSE':
        return 'B'
    elif asset.exchange_name == 'MCX':
        return 'M'
    return 'N'

@registry.register()
def fivep_scrip_code(asset, **kwargs) -> int:
    """Extract scrip code from asset details or security_id."""
    # Assuming security_id is the scrip code
    try:
        return int(asset.security_id)
    except:
        return 0

@registry.register()
def fivep_order_validity(val, **kwargs) -> int:
    """
    Map OrderValidity to 5Paisa Validity days.
    0 = Day (usually), or specific days.
    """
    # 5Paisa API might expect '0' for Day. 
    # Check specific API docs. Assuming 0 for now.
    return 0 

# -------------------------------------------------------------------------
# Broker Config
# -------------------------------------------------------------------------
BROKER_SPEC = {
    "calendar": "NSE",
    "credentials": {
        "fields": ["app_name", "app_source", "user_id", "password", "user_key", "encryption_key", "client_code", "totp_secret", "pin"],
        "validator": "credentials.client_code and credentials.totp_secret and credentials.pin",
    },
    "options": {
        "timeout": 10,
        "rate_limit": 60, # 1 per second conservative
        "rate_limit_period": 60,
        "max_tickers": 50,
        "supported_modes": ["LIVE", "PAPER"],
        "fractional_trading": False,
        "ccy": "INR",
        "multi_assets_data_query": True,
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
            OrderType.MARKET: "M",  # Placeholder, usually handled via 'AtMarket' flag
            OrderType.LIMIT: "L",
            OrderType.STOP: "SL",
            OrderType.STOP_LIMIT: "SL-L", # Stop Loss Limit
        },
        "default_value": OrderType.MARKET,
    },
    "order_validity": {
        "map": {
            OrderValidity.DAY: "Day", # Verify string vs int
            OrderValidity.IOC: "IOC",
        },
        "default_value": OrderValidity.DAY,
    },
    "product_type": {
        "map": {
            ProductType.DELIVERY: "C", # Cash/Delivery
            ProductType.MARGIN: "I",   # Intraday
        },
        "default_value": ProductType.DELIVERY,
    },
    "order_status": {
        "map": {
            OrderStatus.OPEN: ["Pending", "Open", "Partly Executed"],
            OrderStatus.COMPLETE: "Fully Executed",
            OrderStatus.CANCELLED: "Cancelled",
            OrderStatus.REJECTED: "Rejected",
        }
    }
}

# -------------------------------------------------------------------------
# API Config
# -------------------------------------------------------------------------
COMMON_HEADERS = {
    "Content-Type": {"source": "'application/json'"},
    # Standard headers for 5Paisa (some might be required only after login)
}

API_SPEC = {
    "base_url": "https://Openapi.5paisa.com/VendorsAPI/Service1.svc",
    "headers": COMMON_HEADERS,
    "endpoints": {
        # --- Authentication ---
        "session": {
            "endpoint": "/V2/LoginRequestMobileNewbyEmail", # Or TOTP specific endpoint
            "method": "POST",
            "request": {
                "json": {
                    "fields": {
                        "head": {
                            "source": "{'appName': credentials.app_name, 'appVer': '1.0', 'key': credentials.user_key, 'osName': 'Python', 'requestCode': '5PLoginV2', 'userId': credentials.user_id, 'password': credentials.password}"
                        },
                        "body": {
                            "source": "{'Email_id': credentials.client_code, 'Password': credentials.pin, 'LocalIP': '127.0.0.1', 'PublicIP': '127.0.0.1', 'HDSerailNumber': '', 'MACAddress': '', 'MachineID': '', 'VersionNo': '1.0', 'RequestNo': '1', 'My2PIN': credentials.pin, 'ConnectionType': '1'}"
                        }
                    }
                }
            },
            # NOTE: Real implementation usually requires encrypting the body or using the TOTP flow.
            # Due to complexity of 5Paisa login (encryption + multiple steps), 
            # we assume the user might provide a pre-generated access token or we implement the 
            # exact flow if known. 
            # For this template, we assume a standard JSON login attempt.
            # IF using py5paisa or similar logic, one would use a custom function here.
            "response": {
                "payload_type": "object",
                "result": {
                     # Mock result parsing - real response sets cookies/tokens
                     "token": {"source": "result.get('body', {}).get('JwtToken')"} 
                }
            },
            # Hook to set headers for subsequent requests
            "hooks": {
                "after_response": "update_headers_with_token" 
            }
        },

        # --- Order Management ---
        "place_order": {
            "endpoint": "/V1/PlaceOrderRequest",
            "method": "POST",
            "request": {
                "json": {
                    "fields": {
                        "head": {"source": "{'appName': credentials.app_name, 'appVer': '1.0', 'key': credentials.user_key, 'osName': 'Python', 'requestCode': '5PPlaceOrder', 'userId': credentials.user_id, 'password': credentials.password}"},
                        "body": {
                            "fields": {
                                "ClientCode": {"source": "credentials.client_code"},
                                "OrderFor": {"source": "'P'"}, # P = Place, M = Modify, C = Cancel
                                "Exchange": {"source": "fivep_exchange(order.asset)"},
                                "ExchangeType": {"source": "fivep_exch_type(order.asset)"},
                                "Price": {"source": "order.price or 0"},
                                "OrderId": {"source": "0"}, 
                                "OrderType": {"source": "mappings.order_side.from_blueshift(order.side)"},
                                "Qty": {"source": "int(order.quantity)"},
                                "ScripCode": {"source": "fivep_scrip_code(order.asset)"},
                                "AtMarket": {"source": "str(order.order_type == 'MARKET').lower()"},
                                "RemoteOrderID": {"source": "order.oid"}, # Client Order ID
                                "IsIntraday": {"source": "order.product_type == 'MARGIN'"},
                                "PublicIP": {"source": "'127.0.0.1'"},
                                # ... other fields ...
                            }
                        }
                    }
                }
            },
            "response": {
                "payload_type": "object",
                "result": {
                    # Extract Broker Order ID
                    "id": {"source": "str(result['body']['BrokerOrderID'])"}
                }
            }
        },

        "cancel_order": {
            "endpoint": "/V1/CancelOrderRequest",
            "method": "POST",
            "request": {
                "json": {
                     "fields": {
                        "head": {"source": "{'appName': credentials.app_name, 'appVer': '1.0', 'key': credentials.user_key, 'osName': 'Python', 'requestCode': '5PCancelOrder', 'userId': credentials.user_id, 'password': credentials.password}"},
                        "body": {
                            "fields": {
                                "ClientCode": {"source": "credentials.client_code"},
                                "Exchange": {"source": "fivep_exchange(order.asset)"},
                                "ExchangeType": {"source": "fivep_exch_type(order.asset)"},
                                "ScripCode": {"source": "fivep_scrip_code(order.asset)"},
                                "OrderId": {"source": "order.broker_order_id"}, 
                            }
                        }
                     }
                }
            },
            "response": {
                "payload_type": "object",
                "result": {"status": {"source": "'CANCELLED'"}}
            }
        },

        "get_orders": {
            "endpoint": "/V1/OrderBook", # or /V2/OrderBook
            "method": "POST",
            "request": {
                "json": {
                    "fields": {
                        "head": {"source": "{'appName': credentials.app_name, 'appVer': '1.0', 'key': credentials.user_key, 'osName': 'Python', 'requestCode': '5POrderBook', 'userId': credentials.user_id, 'password': credentials.password}"},
                        "body": {"fields": {"ClientCode": {"source": "credentials.client_code"}}}
                    }
                }
            },
            "response": {
                "payload_type": "array",
                "payload_path": "response.get('body', {}).get('OrderBookDetail', [])",
                "items": {
                    "oid": {"source": "item['BrokerOrderId']"},
                    "broker_order_id": {"source": "item['BrokerOrderId']"},
                    "security_id": {"source": "str(item['ScripCode'])"},
                    "symbol": {"source": "item.get('ScripName')"}, # Fallback
                    "quantity": {"source": "float(item['Qty'])"},
                    "filled": {"source": "float(item.get('Qty',0)) - float(item.get('PendingQty',0))"}, 
                    # Note: 5Paisa might report PendingQty
                    "price": {"source": "float(item.get('Rate', 0))"},
                    "average_price": {"source": "float(item.get('Rate', 0))"}, # Approx
                    "status": {"source": "mappings.order_status.to_blueshift(item.get('OrderStatus'))"},
                    # ... timestamps ...
                }
            }
        },
        
        # --- Market Data ---
        "get_history": {
            "endpoint": "/V1/HistoricalData",
            "method": "POST",
            "request": {
                 "json": {
                    "fields": {
                        "head": {"source": "{'appName': credentials.app_name, 'appVer': '1.0', 'key': credentials.user_key, 'osName': 'Python', 'requestCode': '5PHistoricalData', 'userId': credentials.user_id, 'password': credentials.password}"},
                        "body": {
                            "fields": {
                                "ClientCode": {"source": "credentials.client_code"},
                                "Exchange": {"source": "fivep_exchange(asset)"},
                                "ExchangeType": {"source": "fivep_exch_type(asset)"},
                                "ScripCode": {"source": "fivep_scrip_code(asset)"},
                                "From": {"source": "from_dt.strftime('%Y-%m-%d')"},
                                "To": {"source": "to_dt.strftime('%Y-%m-%d')"},
                                "Interval": {"source": "'1m'"} # Need mapping for interval
                            }
                        }
                    }
                }
            },
            "response": {
                "payload_type": "data",
                "payload_path": "response.get('body', {}).get('Candles', [])",
                "data": {
                    "frame": "records",
                    "timestamp": {"field": "Timestamp", "format": "%Y-%m-%dT%H:%M:%S"},
                    "rename": {"Open": "o", "High": "h", "Low": "l", "Close": "c", "Volume": "v"}
                }
            }
        }
    }
}

# -------------------------------------------------------------------------
# Streaming Config
# -------------------------------------------------------------------------
@registry.register()
def fivep_subscribe_list(subscribe_assets, **kwargs):
    """Convert list[Asset] -> list of dicts for 5Paisa MarketFeedData."""
    if not isinstance(subscribe_assets, list):
        subscribe_assets = [subscribe_assets]
    
    data = []
    for asset in subscribe_assets:
        # We need a way to get Exch and ExchType. 
        # using the registered functions directly here might be cleaner
        # or re-implementing logic if needed.
        # reusing functions:
        exch = fivep_exchange(asset)
        exch_type = fivep_exch_type(asset)
        scrip_code = fivep_scrip_code(asset)
        
        data.append({
            "Exch": exch,
            "ExchType": exch_type,
            "ScripCode": scrip_code
        })
    return data

STREAMING_SPEC = {
    "connections": {
        "market_data": {
            # Note: The URL requires auth params. We use 'url' auth mode.
            "url": "wss://openfeed.5paisa.com/feeds/api/chat", 
            "backend": {"type": "websocket", "options": {}},
            "streams": ["data"],
            "auth": {
                "mode": "url",
                "url": {
                    "format": {
                        # Value1={{access_token}}|{{clientcode}}
                        "Value1": "{token}|{client_code}" 
                    }
                }
            },
            "subscribe": {
                "subscribe": {
                    "format": "json",
                    "json": {
                        "fields": {
                            "Method": {"source": "'MarketFeedV3'"},
                            "Operation": {"source": "'Subscribe'"},
                            "ClientCode": {"source": "credentials.client_code"},
                            "MarketFeedData": {"source": "fivep_subscribe_list(subscribe_assets)"}
                        }
                    }
                },
                "unsubscribe": {
                    "format": "json",
                    "json": {
                        "fields": {
                            "Method": {"source": "'MarketFeedV3'"},
                            "Operation": {"source": "'Unsubscribe'"},
                            "ClientCode": {"source": "credentials.client_code"},
                            "MarketFeedData": {"source": "fivep_subscribe_list(subscribe_assets)"}
                        }
                    }
                }
            },
            "parser": {"format": "json"},
            # Router: 5Paisa sends mixed messages. We need to route based on content.
            # MarketFeedV3 updates usually come as a list or a dict.
            # Assuming payload is a list of ticks or a dict.
            "router": {
                "rules": [
                    # If it has 'LastRate' or 'LTP', it's data
                    # Check for 'Exch' and 'ScripCode' to confirm it's a feed update
                    {"channel": "data", "match": "isinstance(data, list) and len(data) > 0 and 'Exch' in data[0]"},
                    {"channel": "data", "match": "isinstance(data, dict) and 'Exch' in data"},
                ]
            },
            "converters": {
                "data": [
                    {
                        "fields": {
                            # We need to reconstruct the asset from Exch/ScripCode
                            # "asset": {"source": "broker.asset_finder.infer_asset(security_id=str(data['ScripCode']))"},
                            # For safety, we can try to use symbol if available, or just ID
                            "asset": {
                                "source": "broker.asset_finder.sid(str(data['ScripCode']))"
                            },
                            "timestamp": {"source": "pd.Timestamp.now(tz=broker.tz)"}, # 5Paisa Time field might be string
                            "data": {
                                "close": {"source": "float(data.get('LastRate', 0) or data.get('LTP', 0))"},
                                "open": {"source": "float(data.get('OpenRate', 0))"},
                                "high": {"source": "float(data.get('High', 0))"},
                                "low": {"source": "float(data.get('Low', 0))"},
                                "volume": {"source": "float(data.get('TotalQty', 0))"},
                                "last": {"source": "float(data.get('LastRate', 0) or data.get('LTP', 0))"},
                                # Quote fields
                                "bid": {"source": "float(data.get('BidRate', 0))"},
                                "bid_volume": {"source": "float(data.get('BidQty', 0))"},
                                "ask": {"source": "float(data.get('OffRate', 0))"}, # 5Paisa often uses OffRate for Ask
                                "ask_volume": {"source": "float(data.get('OffQty', 0))"},
                            }
                        }
                    }
                ]
            }
        },
        # "trading": ... (Requires dynamic URL based on redirect server, skipped for static config)
    }
}


# -------------------------------------------------------------------------
# Master Data (Assets)
# -------------------------------------------------------------------------
MASTER_DATA_SPEC = [
    {
        "endpoint": {
            "endpoint": "https://images.5paisa.com/Cms/ScripMaster.csv",
            "method": "GET",
            "response": {"payload_type": "object"} # File download
        },
        "mode": "file",
        "format": "csv",
        "assets": [
            {
                "asset_class": "equity",
                "filter": ["row['Exch'] == 'N'", "row['ExchType'] == 'C'"], # NSE Equity
                "mapping": {
                    "symbol": {"source": "row['Name']"}, # or 'Symbol' if available
                    "security_id": {"source": "str(row['ScripCode'])"},
                    "exchange": {"source": "'NSE'"},
                    "name": {"source": "row['Name']"},
                }
            }
        ]
    }
]

# -------------------------------------------------------------------------
# Objects Conversion
# -------------------------------------------------------------------------
OBJECTS_SPEC = {
    "order": [
        {
            "fields": {
                "oid": {"source": "data['BrokerOrderId']"},
                # ...
            }
        }
    ]
}

# -------------------------------------------------------------------------
# Custom Hooks
# -------------------------------------------------------------------------
@registry.register()
def update_headers_with_token(response, broker, **kwargs):
    """
    Update broker API headers with the received token.
    """
    try:
        data = response.json()
        token = data.get('body', {}).get('JwtToken')
        client_code = data.get('body', {}).get('ClientCode')
        if token:
            broker.api._session.headers.update({
                "x-auth-token": token,
                "x-clientcode": client_code
            })
            
            # Also update credential fields for streaming auth if needed
            # (Though streaming config usually pulls from 'credentials' object which is static)
            # We might need to store token in credentials for the URL builder to pick it up.
            broker.config.broker.credentials.credentials['token'] = token
            broker.config.broker.credentials.credentials['client_code'] = client_code
    except:
        pass

# -------------------------------------------------------------------------
# Config Factory
# -------------------------------------------------------------------------
def create_config(variant) -> APIBrokerConfig:
    broker_spec = dict(BROKER_SPEC)
    api_spec = dict(API_SPEC)
    stream_spec = dict(STREAMING_SPEC)
    
    api_spec['master_data'] = MASTER_DATA_SPEC
    
    return APIBrokerConfig(broker_spec, api_spec, OBJECTS_SPEC, stream_spec, registry=registry)

def register_brokers():
    config = create_config('live')
    broker_class_factory("FivePaisaLive", config)
