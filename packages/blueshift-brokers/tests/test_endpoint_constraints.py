import pytest
from jsonschema.exceptions import ValidationError
from blueshift.brokers.core.config.schema import validate_schema, API_SCHEMA
from blueshift.brokers.core.config.resolver import ConfigRegistry

registry = ConfigRegistry()

def test_endpoint_constraints_success():
    config = {
        "base_url": "https://api.example.com",
        "endpoints": {
            "get_history": {
                "endpoint": "history",
                "method": "GET",
                "request": {"query": {"fields": {}}},
                "response": {
                    "payload_type": "data", # Correct: data
                    "data": {"frame": "records", "timestamp": {"unit": "s"}}
                }
            },
            "get_quote": {
                "endpoint": "quote",
                "method": "GET",
                "request": {"query": {"fields": {}}},
                "response": {
                    "payload_type": "quote", # Correct: quote
                    "quote": {"last": {"source": "1"}}
                }
            },
            "get_orders": {
                "endpoint": "orders",
                "method": "GET",
                "request": {"query": {"fields": {}}},
                "response": {
                    "payload_type": "array", # Correct: array
                    "items": {"id": {"source": "1"}}
                }
            }
        }
    }
    
    valid, errors = validate_schema(API_SCHEMA, config, registry)
    assert valid, f"Validation failed: {errors}"

def test_endpoint_constraints_fail_history():
    config = {
        "base_url": "https://api.example.com",
        "endpoints": {
            "get_history": {
                "endpoint": "history",
                "method": "GET",
                "request": {"query": {"fields": {}}},
                "response": {
                    "payload_type": "object", # Incorrect: expected data
                    "result": {"val": {"source": "1"}}
                }
            }
        }
    }
    
    valid, errors = validate_schema(API_SCHEMA, config, registry)
    assert not valid
    # Check if we get an error related to payload_type const constraint
    assert any("payload_type" in e for e in errors)

def test_endpoint_constraints_fail_quote():
    config = {
        "base_url": "https://api.example.com",
        "endpoints": {
            "get_quote": {
                "endpoint": "quote",
                "method": "GET",
                "request": {"query": {"fields": {}}},
                "response": {
                    "payload_type": "data", # Incorrect: expected quote
                    "data": {"frame": "records", "timestamp": {"unit": "s"}}
                }
            }
        }
    }
    
    valid, errors = validate_schema(API_SCHEMA, config, registry)
    assert not valid

def test_endpoint_constraints_fail_orders():
    config = {
        "base_url": "https://api.example.com",
        "endpoints": {
            "get_orders": {
                "endpoint": "orders",
                "method": "GET",
                "request": {"query": {"fields": {}}},
                "response": {
                    "payload_type": "data", # Incorrect: expected array or object
                    "data": {"frame": "records", "timestamp": {"unit": "s"}}
                }
            }
        }
    }
    
    valid, errors = validate_schema(API_SCHEMA, config, registry)
    assert not valid
