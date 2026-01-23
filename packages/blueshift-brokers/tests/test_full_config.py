import pytest
from blueshift.brokers.core.config.config import APIBrokerConfig
from blueshift.brokers.core.config.broker import BrokerConfig
from blueshift.brokers.core.config.api import APIConfig
from blueshift.lib.exceptions import ValidationError

BROKER_SPEC = {
    "name": "test_broker",
    "calendar": "NYSE",
    "credentials": {
        "fields":["api_key_id"],
    },
    "data_frequencies":{},
    "order_type": {},
    "order_validity": {}
}

API_SPEC = {
    "base_url": "https://api.test.com",
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
        "status": {
            "endpoint": "/status",
            "method": "GET",
            "request": {
                "query": {"fields": {"dummy": {"source": "'1'"}}}
            },
            "response": {"result": {"fields":{"status": {"source": "'ok'"}}}}
        }
    }
}

def test_api_broker_config_success():
    config = APIBrokerConfig(config=BROKER_SPEC, api_config=API_SPEC)
    
    assert isinstance(config.broker, BrokerConfig)
    assert config.broker.name == "test_broker"
    
    assert isinstance(config.api, APIConfig)
    assert config.api.endpoints["status"].request.url.error_message == "https://api.test.com/status"

def test_api_broker_config_invalid_broker():
    bad_broker_spec = BROKER_SPEC.copy()
    del bad_broker_spec["name"] # Missing required field
    
    with pytest.raises(ValidationError) as exc:
        APIBrokerConfig(config=bad_broker_spec, api_config=API_SPEC)
    
    assert "Invalid Broker Config" in str(exc.value)

def test_api_broker_config_invalid_api():
    bad_api_spec = API_SPEC.copy()
    del bad_api_spec["base_url"] # Missing required field
    
    with pytest.raises(ValidationError) as exc:
        APIBrokerConfig(config=BROKER_SPEC, api_config=bad_api_spec)
    
    assert "Invalid API Config" in str(exc.value)
