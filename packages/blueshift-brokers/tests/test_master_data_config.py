import pytest
from blueshift.brokers.core.config.schema import validate_schema, API_SCHEMA
from blueshift.brokers.core.config.resolver import ConfigRegistry

registry = ConfigRegistry()

def test_master_data_config_valid():
    config = {
        "base_url": "https://api.example.com",
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
        }]
    }
    
    valid, errors = validate_schema(API_SCHEMA, config, registry)
    assert valid, f"Validation failed: {errors}"

def test_master_data_config_api_list():
    config = {
        "base_url": "https://api.example.com",
        "master_data": [{
            "endpoint": {
                "endpoint": "/instruments",
                "method": "GET",
                "request": {
                    "query": {"fields": {}}
                },
                "response": {
                    "payload_type": "array",
                    "items": {"fields":{"sym": {"source": "item['symbol']"}}}
                }
            },
            "mode": "list",
            "assets": [
                {
                    "filter": "item['type'] == 'EQ'",
                    "asset_class":"equity",
                    "mapping": {
                        "symbol": {"source": "item['sym']"},
                        "exchange": {"source": "'NYSE'"}
                    }
                }
            ]
        }]
    }
    
    valid, errors = validate_schema(API_SCHEMA, config, registry)
    assert valid, f"Validation failed: {errors}"

def test_master_data_config_missing_required():
    config = {
        "base_url": "https://api.example.com",
        "master_data": [{
            "mode": "list",
            # missing endpoint and assets
        }]
    }
    
    valid, errors = validate_schema(API_SCHEMA, config, registry)
    assert not valid
    assert any("Missing required" in e for e in errors)