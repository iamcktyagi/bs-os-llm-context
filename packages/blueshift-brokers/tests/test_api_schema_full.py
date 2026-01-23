import pytest
from blueshift.brokers.core.config.api import get_api_config, APIConfig, APIEndPoint
from blueshift.brokers.core.config.resolver import ConfigRegistry
from blueshift.brokers.core.config.schema import sanitize_config
from blueshift.lib.exceptions import ValidationError

registry = ConfigRegistry()
register = registry.register

# --- Helper for registering dummy functions ---
@register()
def my_custom_hook(response, **kwargs):
    pass

@register()
def my_custom_endpoint(**kwargs):
    return {"status": "ok"}

# --- Test Cases ---

def test_api_config_minimal():
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
        }],
    }
    
    api_config, master_config = get_api_config(config, registry)
    
    assert isinstance(api_config, APIConfig)
    assert api_config.endpoints == {} # No endpoints defined
    assert api_config.timeout == 60 # Default
    assert api_config.max_retries == 3 # Default

def test_api_config_full_globals():
    config = {
        "base_url": "https://api.test.com",
        "timeout": 10,
        "verify_cert": False,
        "proxies": {"http": "http://proxy.com"},
        "max_retries": 5,
        "rate_limit": 100,
        "rate_limit_period": 60,
        "api_delay_protection": 0.5,
        "headers": {
            "fields":{"Authorization": {"source": "'Bearer Token'"}}
        },
        "hooks": {
            "before_send": "my_custom_hook"
        },
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
    }
    
    api_config, master_config = get_api_config(config, registry)
    
    assert api_config.timeout == 10
    assert api_config.verify_cert is False
    assert api_config.proxies["http"] == "http://proxy.com"
    assert api_config.max_retries == 5
    assert api_config.rate_limit == 100
    assert api_config.api_delay_protection == 0.5

def test_api_config_simple_endpoint():
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
        }],
        "endpoints": {
            "get_quote": {
                "endpoint": "quote",
                "method": "GET",
                "request": {
                    "query": {"fields": {"symbol": {"source": "symbol"}}}
                },
                "response": {
                    "payload_type": "quote",
                    "quote": {"last": {"source": "data['last']"}}
                }
            }
        }
    }

    api_config, master_config = get_api_config(config, registry)    
    assert "get_quote" in api_config.endpoints
    ep = api_config.endpoints["get_quote"]
    assert isinstance(ep, APIEndPoint)
    assert ep.name == "get_quote"
    assert ep.request.verb == "GET"
    
    # Check URL resolution
    url, _, _, _, _, _, _ = ep.request.resolve(symbol="AAPL")
    assert url == "https://api.example.com/quote"

def test_api_config_endpoint_overrides():
    config = {
        "base_url": "https://api.example.com",
        "timeout": 10,
        "max_retries": 3,
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
            "slow_endpoint": {
                "endpoint": "heavy",
                "method": "POST",
                "timeout": 120, # Override
                "max_retries": 1, # Override
                "request": {"json": {"fields": {}}},
                "response": {
                    "payload_type": "object",
                    "result": {"fields":{"dummy": {"source": "1"}}}
                }
            },
            "fast_endpoint": {
                "endpoint": "ping",
                "method": "GET",
                "request": {"query": {"fields": {}}},
                "response": {
                    "payload_type": "object",
                    "result": {"fields":{"dummy": {"source": "1"}}}
                }
            }
        }
    }
    
    api_config, master_config = get_api_config(config, registry)
    
    slow = api_config.endpoints["slow_endpoint"]
    fast = api_config.endpoints["fast_endpoint"]
    
    assert slow.timeout == 120
    assert slow.max_retries == 1
    
    assert fast.timeout == 10 # Inherited
    assert fast.max_retries == 3 # Inherited

def test_api_config_custom_endpoint():
    config_fixed = {
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
        }],
        "endpoints": {
            "manual_action": {
                "endpoint": "dummy",
                "method": "GET",
                "request": {
                    "custom": "my_custom_endpoint"
                },
                "response": {
                    "payload_type": "object",
                    "result": {"fields":{"dummy": {"source": "1"}}}
                }
            }
        }
    }
    
    api_config, master_config = get_api_config(config_fixed, registry)
    ep = api_config.endpoints["manual_action"]
    
    # Resolve
    _, custom_fn, _, _, _, _, _ = ep.request.resolve()
    assert custom_fn and custom_fn.fn == my_custom_endpoint

def test_api_config_hooks_inheritance():
    config = {
        "base_url": "https://api.example.com",
        "hooks": {
            "before_send": "my_custom_hook"
        },
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
            "my_endpoint": {
                "endpoint": "data",
                "method": "GET",
                "request": {"query": {"fields": {}}},
                "response": {
                    "payload_type": "object",
                    "result": {"fields":{"dummy": {"source": "1"}}}
                },
                "hooks": {
                    "after_response": "my_custom_hook"
                }
            }
        }
    }
    
    api_config, master_config = get_api_config(config, registry)
    ep = api_config.endpoints["my_endpoint"]
    
    assert "before_send" in ep.hooks
    assert "after_response" in ep.hooks

def test_api_config_missing_base_url():
    config = {
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
            "some_endpoint": {
                "endpoint": "foo",
                "method": "GET"
            }
        }
    }
    
    # Validation should fail because base_url is required
    with pytest.raises(ValidationError) as excinfo:
        get_api_config(config, registry)
        assert "base_url" in str(excinfo.value)

def test_api_config_invalid_schema():
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
        }],
        "endpoints": {
            "bad_endpoint": {
                "endpoint": "foo",
                "method": "INVALID_METHOD", # Invalid Enum
                "request": {"query": {"fields": {}}},
                "response": {
                    "payload_type": "object",
                    "result": {"fields":{"dummy": {"source": "1"}}}
                }
            }
        }
    }
    
    with pytest.raises(ValidationError) as excinfo:
        get_api_config(config, registry)
        assert "INVALID_METHOD" in str(excinfo.value)