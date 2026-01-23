import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from blueshift.brokers.core.api import BrokerAPIMixin
from blueshift.brokers.core.config.config import APIBrokerConfig
from blueshift.lib.exceptions import InitializationError, APIException, APIError, ValidationError

# Minimal valid configs with all required endpoints
BROKER_SPEC = {
    "name": "test_broker",
    "calendar": "NYSE",
    "credentials": {
        "fields": ["api_key"]
    },
    "data_frequencies":{},
    "order_type": {},
    "order_validity": {}
}

API_SPEC = {
    "base_url": "http://test",
    "master_data": [{
            "endpoint": {
                "endpoint": "/instruments",
                "method": "GET",
                "request": {
                    "query": {"fields": {}}
                },
                "response": {
                    "payload_type": "object",
                    "result": {"fields":{"url": {"source": "1"}}}
                }
            },
            "mode": "file",
            "format": "csv",
            "compression": "zip",
            "assets": [
                {
                    "file_match": "*.csv",
                    "asset_class":"equity",
                    "mapping": {
                        "symbol": {"source": "row['symbol']"},
                        "name": {"source": "row['name']"}
                    }
                }
            ]
    }],
    "endpoints": {
        "get_account": {
            "endpoint": "/account", "method": "GET", 
            "request": {"query": {"fields": {"dummy": {"source": "'1'"}}}}, 
            "response": {"payload_type": "array", "items": {"fields":{"balance": {"source": "100"}}}}
        },
        "get_orders": {"endpoint": "/orders", "method": "GET", "request": {"query": {"fields": {"dummy": {"source": "'1'"}}}}, "response": {"payload_type": "array", "items": {"fields":{"id": {"source": "item"}}}}},
        "get_order": {"endpoint": "/order", "method": "GET", "request": {"query": {"fields": {"dummy": {"source": "'1'"}}}}, "response": {"result": {"fields":{}}}},
        "place_order": {"endpoint": "/order", "method": "POST", "request": {"query": {"fields": {"dummy": {"source": "'1'"}}}}, "response": {"result": {"fields":{}}}},
        "cancel_order": {"endpoint": "/order", "method": "DELETE", "request": {"query": {"fields": {"dummy": {"source": "'1'"}}}}, "response": {"result": {"fields":{}}}},
        "update_order": {"endpoint": "/order", "method": "PUT", "request": {"query": {"fields": {"dummy": {"source": "'1'"}}}}, "response": {"result": {"fields":{}}}},
        "get_history": {"endpoint": "/history", "method": "GET", "request": {"query": {"fields": {"dummy": {"source": "'1'"}}}}, "response": {"payload_type": "data", "data": {"frame": "records", "timestamp": {"unit": "s"}}}},
        "get_positions": {"endpoint": "/positions", "method": "GET", "request": {"query": {"fields": {"dummy": {"source": "'1'"}}}}, "response": {"payload_type": "array", "items": {"fields":{"id": {"source": "item"}}}}},
        "get_charges": {
            "endpoint": "/charges", 
            "method": "GET", 
            "request": {"query": {"fields": {"dummy": {"source": "'1'"}}}},
            "response": {
                "result": {
                    "fields":{
                        "commission": {"source": "result['commission']"},
                        "charges": {"source": "result['charges']"}
                    }
                }
            }
        },
        "get_paginated": {
            "endpoint": "/paginated",
            "method": "GET",
            "request": {
                "query": {"fields": {}},
                "next_page_token": {"parameter": "page_token", "location": "query"}
            },
            "response": {
                "payload_type": "array",
                "payload_path": "response['data']",
                "items": {"fields":{"id": {"source": "item"}}},
                "next_page_token": "response.get('next_token')"
            }
        }
    }
}

@pytest.fixture
def config():
    return APIBrokerConfig(BROKER_SPEC, API_SPEC)

@pytest.fixture
def api(config):
    # Mocking BrokerAPI init to avoid network calls or external dependencies
    with patch("blueshift.brokers.core.api.BrokerAPI.__init__", return_value=None):
        api = BrokerAPIMixin(config, None, lazy=True)
        api._requests = MagicMock(return_value=(True, 200, {}))
        return api

def test_init_missing_endpoints():
    bad_api_spec = API_SPEC.copy()
    bad_api_spec['endpoints'] = {} # Empty endpoints
    config = APIBrokerConfig(BROKER_SPEC, bad_api_spec)
    
    with pytest.raises(ValidationError) as exc:
        BrokerAPIMixin(config, None, lazy=False)
    
    assert "Missing required endpoints" in str(exc.value)

def test_request_basic(api):
    api._requests.return_value = (True, 200, [{"balance": 100}])
    data = api.request("get_account")
    assert data == [{"balance": 100}]
    api._requests.assert_called()

def test_request_pagination(api):
    responses = [
        (True, 200, {"data": [1, 2], "next_token": "token2"}),
        (True, 200, {"data": [3, 4]})
    ]
    api._requests.side_effect = responses
    
    data = api.request("get_paginated")
    
    assert data == [{'id': 1}, {'id': 2}, {'id': 3}, {'id': 4}]
    assert api._requests.call_count == 2
    
    args1, _ = api._requests.call_args_list[0]
    # args: url, headers, params, ...
    assert "page_token" not in args1[2]
    
    args2, _ = api._requests.call_args_list[1]
    assert args2[2]["page_token"] == "token2"

def test_request_failure_api_error(api):
    api._requests.return_value = (False, 500, "Server Error")
    
    with pytest.raises(APIError) as exc:
        x = api.request("get_account")
    
    assert "request failed" in str(exc.value)

def test_wrapper_methods(api):
    # get_orders
    api._requests.return_value = (True, 200, [])
    assert api.request_get_orders() == []
    
    # get_order
    api._requests.return_value = (True, 200, {})
    assert api.request_get_order_by_id("123") == {}
    
    # get_positions
    # Test NotImplementedError if underlying request raises it (simulated by missing endpoint or config)
    # But here we have the endpoint.
    api._requests.return_value = (True, 200, [])
    assert api.request_get_positions() == []
    
    # get_charge (NotImplementedError handling)
    # If endpoint exists, it works
    api._requests.return_value = (True, 200, {'commission': 0, 'charges': 100})
    x = api.request_get_charge(None)
    assert api.request_get_charge(None) == (0, 100)

def test_request_get_charge_not_implemented(api):
    # Mock request to raise NotImplementedError
    api.request = MagicMock(side_effect=NotImplementedError)
    assert api.request_get_charge(None) == (0,0)
