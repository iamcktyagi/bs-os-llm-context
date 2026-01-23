import pytest
from blueshift.brokers.core.config.schema import validate_schema, API_SCHEMA
from blueshift.brokers.core.config.resolver import ConfigRegistry

registry = ConfigRegistry()

def test_flexible_endpoints_success():
    # Test case where get_account uses 'object' and get_orders uses 'array'
    config_1 = {
        "base_url": "https://api.example.com",
        "endpoints": {
            "get_account": { 
                "endpoint": "acc", "method": "GET",
                "request": {"query": {"fields": {}}},
                "response": {
                    "payload_type": "object",
                    "result": {"fields":{"bal": {"source": "1"}}}
                }
            },
            "get_orders": { 
                "endpoint": "ord", "method": "GET",
                "request": {"query": {"fields": {}}},
                "response": {
                    "payload_type": "array",
                    "items": {"fields":{"id": {"source": "1"}}}
                }
            }
        }
    }
    
    valid, errors = validate_schema(API_SCHEMA, config_1, registry)
    assert valid, f"Validation failed for config_1: {errors}"

    # Test case where get_account uses 'array' and get_orders uses 'object'
    config_2 = {
        "base_url": "https://api.example.com",
        "endpoints": {
            "get_account": { 
                "endpoint": "acc", "method": "GET",
                "request": {"query": {"fields": {}}},
                "response": {
                    "payload_type": "array",
                    "items": {"fields":{"bal": {"source": "1"}}}
                }
            },
            "get_orders": { 
                "endpoint": "ord", "method": "GET",
                "request": {"query": {"fields": {}}},
                "response": {
                    "payload_type": "object",
                    "result": {"fields":{"id": {"source": "1"}}}
                }
            }
        }
    }
    
    valid, errors = validate_schema(API_SCHEMA, config_2, registry)
    assert valid, f"Validation failed for config_2: {errors}"

def test_flexible_endpoints_fail_invalid_type():
    config = {
        "base_url": "https://api.example.com",
        "endpoints": {
            "get_account": { 
                "endpoint": "acc", "method": "GET",
                "request": {"query": {"fields": {}}},
                "response": {
                    "payload_type": "data", # Invalid: expects array or object
                    "data": {"frame": "records", "timestamp": {"unit": "s"}}
                }
            }
        }
    }
    
    valid, errors = validate_schema(API_SCHEMA, config, registry)
    assert not valid
