import pytest
import pandas as pd
from blueshift.brokers.core.config.api import APIResponse
from blueshift.brokers.core.config.resolver import ConfigRegistry

registry = ConfigRegistry()

GLOBAL_SPEC = {
    "base_url": "https://api.example.com/v1",
    "timeout": 30
}

def test_api_response_quote_simple():
    ep_spec = {
        "response": {
            "payload_type": "quote",
            "quote": {
                "timestamp": {"source": "quote['ts']"},
                "last": {"source": "quote['last']"},
                "bid": {"source": "quote['bid']"},
                "ask": {"source": "quote['ask']"}
            }
        }
    }
    
    resp = APIResponse.from_config(ep_spec, GLOBAL_SPEC, registry)
    
    api_data = {
        "ts": "2023-01-01T10:00:00",
        "last": 150.5,
        "bid": 150.4,
        "ask": 150.6
    }
    
    parsed, token = resp.resolve(response=api_data)
    
    assert parsed['timestamp'] == "2023-01-01T10:00:00"
    assert parsed['last'] == 150.5
    assert parsed['bid'] == 150.4
    assert parsed['ask'] == 150.6

def test_api_response_quote_market_depth():
    ep_spec = {
        "response": {
            "payload_type": "quote",
            "quote": {
                "last": {"source": "quote['l']"},
                "market_depth": {
                    "bids": [
                        [{"source": "quote['bids'][0][0]"}, {"source": "quote['bids'][0][1]"}],
                        [{"source": "quote['bids'][1][0]"}, {"source": "quote['bids'][1][1]"}]
                        ],
                    "asks": [
                        [{"source": "quote['asks'][0][0]"}, {"source": "quote['asks'][0][1]"}],
                        [{"source": "quote['asks'][1][0]"}, {"source": "quote['asks'][1][1]"}]
                    ]
                }
            }
        }
    }
    
    resp = APIResponse.from_config(ep_spec, GLOBAL_SPEC, registry)
    
    api_data = {
        "l": 100,
        "bids": [[99, 100], [98, 200]],
        "asks": [[101, 100], [102, 200]]
    }
    
    parsed, token = resp.resolve(response=api_data)
    
    assert parsed['last'] == 100
    assert 'market_depth' in parsed
    assert parsed['market_depth']['bids'] == [(99, 100), (98, 200)]
    assert parsed['market_depth']['asks'] == [(101, 100), (102, 200)]
