import pytest
from blueshift.lib.exceptions import ValidationError
from blueshift.brokers.core.config.config import APIBrokerConfig
from blueshift.brokers.core.config.api import MasterDataConfig

BROKER_SPEC = {
    "name": "test_broker",
    "calendar": "NYSE",
    "credentials": {
        "fields": ["api_key"]
    },
    "data_frequencies": {},
    "order_type": {},
    "order_validity": {}
}

API_SPEC_WITH_MASTER = {
    "base_url": "https://api.example.com",
    "master_data": [{
        "endpoint": {
            "endpoint": "/instruments",
            "method": "GET",
            "request": {"query": {"fields": {}}},
            "response": {"payload_type": "object", "result": {"fields":{"url": {"source": "1"}}}}
        },
        "mode": "file",
        "assets": [
            {
                "file_match": "*.csv",
                "asset_class":"equity",
                "mapping": {
                    "symbol": {"source": "row['symbol']"}
                }
            }
        ]
    }],
    # minimal valid endpoints
    "endpoints": {} 
}

def test_api_broker_config_with_master_data():
    config = APIBrokerConfig(BROKER_SPEC, API_SPEC_WITH_MASTER)
    
    assert config.master_data is not None
    assert isinstance(config.master_data, list)
    assert len(config.master_data) == 1
    assert isinstance(config.master_data[0], MasterDataConfig)
    assert config.master_data[0].mode == "file"
    assert len(config.master_data[0].assets) == 1

def test_api_broker_config_without_master_data():
    api_spec = API_SPEC_WITH_MASTER.copy()
    del api_spec['master_data']
    
    with pytest.raises(ValidationError) as exc:
        config = APIBrokerConfig(BROKER_SPEC, api_spec)
        # The validation error message might change slightly, checking for master_data context
        assert "master_data" in str(exc) or "configuration is missing" in str(exc)