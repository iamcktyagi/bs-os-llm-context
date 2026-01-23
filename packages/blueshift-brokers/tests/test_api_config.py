
import pytest
import pandas as pd

from blueshift.lib.trades._order_types import OrderSide, OrderStatus
from blueshift.brokers.core.config.api import APIRequest, APIResponse
from blueshift.brokers.core.config.resolver import ConfigRegistry
from blueshift.lib.exceptions import ValidationError

registry = ConfigRegistry()
register = registry.register

# --- Mock Registry for Custom Functions ---
@register()
def custom_auth(api_key):
    return f"Bearer {api_key}"

@register()
def custom_timestamp(ts):
    return pd.to_datetime(ts, unit='s')

@register()
def my_limit_calc(x):
    return 2*x

# --- Test Data & Configs ---

GLOBAL_SPEC = {
    "base_url": "https://api.example.com/v1",
    "headers": {
        "fields":{"User-Agent": {"source": "'BlueShift/1.0'"}}
    },
    "timeout": 30
}

# --- APIRequest Tests ---

def test_api_request_simple_get():
    ep_spec = {
        "endpoint": "/market/quotes",
        "method": "GET",
        "request": {
            "query": {
                "fields": {
                    "symbol": {"source": "symbol"},
                    "limit": {"source": "str(limit)"}
                }
            }
        }
    }
    
    req = APIRequest.from_config(ep_spec, GLOBAL_SPEC, registry)
    url, custom, verb, headers, params, body, json_params = req.resolve(symbol="AAPL", limit=100)
    
    assert url == "https://api.example.com/v1/market/quotes"
    assert verb == "GET"
    assert headers["User-Agent"] == "BlueShift/1.0"
    assert params["symbol"] == "AAPL"
    assert params["limit"] == "100"
    assert body == {}
    assert json_params == {}

def test_api_request_complex_expression():
    ep_spec = {
        "endpoint": "/market/quotes",
        "method": "GET",
        "request": {
            "query": {
                "fields": {
                    "symbol": {"source": "symbol"},
                    "limit": {"source": "str(int(my_limit_calc(limit)/5)) + 'something'"}
                }
            }
        }
    }

    req = APIRequest.from_config(ep_spec, GLOBAL_SPEC, registry)
    url, custom, verb, headers, params, body, json_params = req.resolve(symbol="AAPL", limit=100)
    
    assert params["limit"] == "40something"

def test_api_request_illegal_expression():
    ep_spec = {
        "endpoint": "/market/quotes",
        "method": "GET",
        "request": {
            "query": {
                "fields": {
                    "symbol": {"source": "symbol"},
                    "limit": {"source": "pow(10,2)"}
                }
            }
        }
    }
    
    
    with pytest.raises(ValidationError) as exc:
        req = APIRequest.from_config(ep_spec, GLOBAL_SPEC, registry)
        url, custom, verb, headers, params, body, json_params = req.resolve(symbol="AAPL", limit=100)
        assert "pow" in str(exc)

def test_api_request_post_json():
    ep_spec = {
        "endpoint": "orders",
        "method": "POST",
        "headers": {
            "fields":{"Authorization": {"source": "custom_auth(api_key)"}}
        },
        "request": {
            "json": {
                "fields": {
                    "side": {"source": "side"},
                    "qty": {"source": "quantity"}
                }
            }
        }
    }
    
    req = APIRequest.from_config(ep_spec, GLOBAL_SPEC, registry)
    url, custom, verb, headers, params, body, json_params = req.resolve(api_key="secret123", side="BUY", quantity=10)
    
    assert url == "https://api.example.com/v1/orders"
    assert verb == "POST"
    assert headers["Authorization"] == "Bearer secret123"
    assert json_params["side"] == "BUY"
    assert json_params["qty"] == 10

def test_api_request_default_value():
    ep_spec = {
        "endpoint": "search",
        "method": "GET",
        "request": {
            "query": {
                "fields": {
                    "q": {"source": "query"},
                    "limit": {"source": "limit", "default_value": 20},
                    "offset": {"source": "offset", "default_value": 0}
                }
            }
        }
    }
    
    req = APIRequest.from_config(ep_spec, GLOBAL_SPEC, registry)
    
    # Case 1: All values provided
    _, _, _, _, params, _, _ = req.resolve(query="AAPL", limit=50, offset=10)
    assert params["q"] == "AAPL"
    assert params["limit"] == 50
    assert params["offset"] == 10
    
    # Case 2: Missing optional values (should use defaults)
    _, _, _, _, params_default, _, _ = req.resolve(query="GOOG")
    assert params_default["q"] == "GOOG"
    assert params_default["limit"] == 20
    assert params_default["offset"] == 0

def test_api_request_pagination_query():
    ep_spec = {
        "endpoint": "history",
        "method": "GET",
        "request": {
            "query": {
                "fields": {"start": {"source": "'2023-01-01'"}}
            },
            "next_page_token": {
                "parameter": "cursor",
                "location": "query"
            }
        }
    }
    
    req = APIRequest.from_config(ep_spec, GLOBAL_SPEC, registry)
    
    # First call (no token)
    _, _, _, _, params, _, _ = req.resolve()
    assert "cursor" not in params
    
    # Subsequent call (with token)
    _, _, _, _, params_paged, _, _ = req.resolve(next_page_token="xyz_token")
    assert params_paged["cursor"] == "xyz_token"

def test_api_request_pagination_body():
    ep_spec = {
        "endpoint": "scan",
        "method": "POST",
        "request": {
            "json": {
                "fields": {"filter": {"source": "'all'"}}
            },
            "next_page_token": {
                "parameter": "page_id",
                "location": "body"
            }
        }
    }
    
    req = APIRequest.from_config(ep_spec, GLOBAL_SPEC, registry)
    
    _, _, _, _, _, _, json_params = req.resolve(next_page_token="page_2")
    assert json_params["page_id"] == "page_2"

# --- APIResponse Tests ---

def test_api_response_object():
    ep_spec = {
        "response": {
            "payload_type": "object",
            "result": {
                "fields":{
                    "status": {"source": "result['status']"},
                    "id": {"source": "result['data']['id']"}
                }
            }
        }
    }
    
    resp = APIResponse.from_config(ep_spec, GLOBAL_SPEC, registry)
    
    api_data = {"status": "ok", "data": {"id": 101, "extra": "ignore"}}
    parsed, token = resp.resolve(response=api_data)
    
    assert parsed["status"] == "ok"
    assert parsed["id"] == 101
    assert token is None

def test_api_response_default_value():
    ep_spec = {
        "response": {
            "payload_type": "object",
            "result": {
                "fields":{
                    "id": {"source": "result['id']"},
                    "flags": {"source": "result['flags']", "default_value": []},
                    "status": {"source": "result['status']", "default_value": "unknown"}
                }
            }
        }
    }
    
    resp = APIResponse.from_config(ep_spec, GLOBAL_SPEC,registry)
    
    # Case 1: Data present
    api_data_full = {"id": 1, "flags": ["A"], "status": "active"}
    parsed_full, _ = resp.resolve(response=api_data_full)
    assert parsed_full["flags"] == ["A"]
    assert parsed_full["status"] == "active"
    
    # Case 2: Data missing (KeyError)
    api_data_missing = {"id": 2}
    parsed_missing, _ = resp.resolve(response=api_data_missing)
    assert parsed_missing["flags"] == []
    assert parsed_missing["status"] == "unknown"

def test_api_response_array():
    ep_spec = {
        "response": {
            "payload_type": "array",
            "items": {
                "fields":{
                    "name": {"source": "item['n']"},
                    "value": {"source": "item['v'] * 2"}
                }
            }
        }
    }
    
    resp = APIResponse.from_config(ep_spec, GLOBAL_SPEC, registry)
    
    api_data = [{"n": "A", "v": 10}, {"n": "B", "v": 20}]
    parsed, token = resp.resolve(api_data)
    
    assert len(parsed) == 2
    assert parsed[0]["name"] == "A"
    assert parsed[0]["value"] == 20
    assert parsed[1]["name"] == "B"
    assert parsed[1]["value"] == 40

def test_api_response_payload_path():
    ep_spec = {
        "response": {
            "payload_type": "object",
            "payload_path": "response['inner']['content']",
            "result": {
                "fields":{"val": {"source": "result['val']"}}
            }
        }
    }
    
    resp = APIResponse.from_config(ep_spec, GLOBAL_SPEC, registry)
    
    api_data = {"inner": {"content": {"val": 123}}}
    parsed, token = resp.resolve(api_data)
    
    assert parsed["val"] == 123

def test_api_response_pagination_token():
    ep_spec = {
        "response": {
            "payload_type": "object",
            "result": {"fields":{"id": {"source": "result['id']"}}},
            "next_page_token": "response['meta']['next_cursor']"
        }
    }
    
    resp = APIResponse.from_config(ep_spec, GLOBAL_SPEC, registry)
    
    api_data = {
        "id": 1, 
        "meta": {"next_cursor": "abc-123"}
    }
    parsed, token = resp.resolve(api_data)
    
    assert parsed["id"] == 1
    assert token == "abc-123"

def test_api_response_ohlc_data_records():
    ep_spec = {
        "response": {
            "payload_type": "data",
            "data": {
                "frame": "records",
                "rename": {
                    "open": "o", "high": "h", "low": "l", "close": "c", "volume": "v"
                },
                "timestamp": {
                    "unit": "s"
                }
            }
        }
    }
    
    resp = APIResponse.from_config(ep_spec, GLOBAL_SPEC, registry)
    
    # Mock response: List of dicts
    api_data = [
        {"timestamp": 1700000000, "o": 100, "h": 105, "l": 95, "c": 102, "v": 1000},
        {"timestamp": 1700000060, "o": 102, "h": 106, "l": 101, "c": 104, "v": 1200}
    ]
    
    df, token = resp.resolve(api_data)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "open" in df.columns
    assert "close" in df.columns
    assert isinstance(df.index, pd.DatetimeIndex)
    assert df.index[0].timestamp() == 1700000000

def test_api_response_ohlc_data_arrays():
    ep_spec = {
        "response": {
            "payload_type": "data",
            "data": {
                "frame": "dict_of_arrays",
                "rename": {"close": "c"},
                "timestamp": {"unit": "s"}
            }
        }
    }
    
    resp = APIResponse.from_config(ep_spec, GLOBAL_SPEC, registry)
    
    api_data = {
        "timestamp": [1700000000, 1700000060],
        "c": [102, 104]
    }
    
    df, _ = resp.resolve(api_data)
    
    assert len(df) == 2
    assert "close" in df.columns
    assert df.iloc[1]["close"] == 104

def test_api_response_ohlc_data_values():
    ep_spec = {
        "response": {
            "payload_type": "data",
            "data": {
                "frame": "values",
                "rename": {
                    "timestamp": "0", "open": "1", "close": "4"
                },
                "timestamp": {"unit": "ms"}
            }
        }
    }
    
    resp = APIResponse.from_config(ep_spec, GLOBAL_SPEC, registry)
    
    # List of lists
    api_data = [
        [1700000000000, 100, 105, 95, 102, 1000],
        [1700000060000, 102, 106, 101, 104, 1200]
    ]
    
    df, _ = resp.resolve(api_data)
    
    assert len(df) == 2
    assert "open" in df.columns
    assert df.iloc[0]["open"] == 100
    assert df.index[0].year == 2023 # Approx check

# --- EnumMap and Sanitization Tests ---

from enum import Enum
from blueshift.brokers.core.config.schema import sanitize_config, validate_schema, BROKER_SCHEMA
from blueshift.brokers.core.config.resolver import get_enum_map, EnumMap

class MyEnum(Enum):
    FIRST = "1"
    SECOND = "2"

def test_sanitize_config_enums_and_callables():
    def my_custom_func(x): return x
    
    raw_config = {
        MyEnum.FIRST: "value1",
        "nested": {
            "key": MyEnum.SECOND,
            "func": my_custom_func
        },
        "list": [MyEnum.FIRST, my_custom_func]
    }
    
    clean_config = sanitize_config(raw_config, registry)
    
    assert clean_config["FIRST"] == "value1"
    assert clean_config["nested"]["key"] == "SECOND"
    assert clean_config["nested"]["func"] == "my_custom_func"
    assert clean_config["list"][0] == "FIRST"
    assert clean_config["list"][1] == "my_custom_func"
    
    # Check if function was registered
    assert "my_custom_func" in registry

def test_enum_map_resolution():
    # Broker config spec for an enum map
    spec = {
        "map": {
            "BUY": ["BROKER_BUY", "LONG"], # Blueshift string -> Broker Strings
            "SELL": "BROKER_SELL"
        },
        "default_value": "BUY",
        "from_blueshift": "to_broker_side",
        "to_blueshift": "from_broker_side"
    }
    
    # Simulate processing via get_enum_map (which registers converters)
    enum_map = get_enum_map(spec, registry)
    
    assert isinstance(enum_map, EnumMap)
    
    # Test registered converter functions
    to_broker = registry["to_broker_side"]
    from_broker = registry["from_broker_side"]
    
    # Test from_blueshift (BlueShift -> Broker)
    assert to_broker("BUY") == "BROKER_BUY"
    assert to_broker("SELL") == "BROKER_SELL"
    assert to_broker("UNKNOWN") == "BROKER_BUY" # default
    
    # Test to_blueshift (Broker -> BlueShift)
    assert from_broker("BROKER_BUY") == "BUY" # Reverse maps to first item if list
    assert from_broker("LONG") == "BUY" # Reverse maps to first item if list
    assert from_broker("BROKER_SELL") == "SELL"
    assert from_broker("UNKNOWN") == "BUY" # default

def test_enum_map_with_enums_in_config():
    # Use actual Enums in the config definition (handled by sanitize_config logic logic in real flow,
    # but here testing get_enum_map direct usage which expects strings/lists if not sanitized first,
    # OR if we pass the sanitized version)
    
    # Let's test the normalization inside EnumMap.__post_init__ which handles Enum keys
    from blueshift.lib.trades._order_types import OrderSide
        
    spec = {
        "map": {
            OrderSide.BUY: "BROKER_BUY",
            OrderSide.SELL: "BROKER_SELL"
        }
    }
    
    # get_enum_map should handle this if we pass the raw spec
    enum_map = get_enum_map(spec, registry)
    
    assert enum_map.from_blueshift('OrderSide.BUY') == "BROKER_BUY"
    assert enum_map.from_blueshift(OrderSide.BUY) == "BROKER_BUY"
    assert enum_map.from_blueshift(0) == "BROKER_BUY"
    assert enum_map.to_blueshift("BROKER_SELL") == OrderSide.SELL

