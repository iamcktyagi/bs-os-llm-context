import pytest
from blueshift.brokers.core.config.broker import get_broker_config, BrokerConfig, BrokerOptions
from blueshift.brokers.core.config.resolver import ConfigRegistry
from blueshift.lib.common.enums import BlotterType, AlgoMode, ExecutionMode
from blueshift.calendar import get_calendar
from blueshift.lib.trades._order_types import OrderSide, OrderType, OrderValidity, ProductType
from blueshift.interfaces.assets._assets import Equity, Crypto

registry = ConfigRegistry()
register = registry.register

def validate_session(**context):
    creds = context.get('credentials')
    if not creds:
        raise ValueError(f'no credentials found')
    
    assert creds.api_key_id is not None

BROKER_SPEC = {
    "name": "mock_broker",
    "display_name": "Mock Broker",
    "version": "1.2.3",
    "calendar": "NYSE",
    "credentials": {
        "fields":["api_key_id", "api_secret_key"],
        "validate":{
            "custom":validate_session,
        }
    },
    "options": {
        "timeout": 10.0,
        "ccy": "USD",
        "supported_modes": ["LIVE", "BACKTEST"],
        "max_tickers": 50
    },
    "assets": ["equity", "crypto"],
    "data_frequencies":{
        "map": {
            "1m": "Min",
            "1d": "Day",
        },
    },
    "order_type": {
        "map": {
            "MARKET": "mkt",
            "LIMIT": "lmt",
            "STOPLOSS": "stp"
        },
        "default_value": "MARKET"
    },
    "order_validity": {
        "map": {
            "DAY": "day",
            "GTC": "gtc"
        }
    },
    "product_type": {
         "map": {
             "DELIVERY": "cash",
             "INTRADAY": "margin"
         }
    }
}

def test_broker_config_parsing():
    config = get_broker_config(BROKER_SPEC, registry)
    
    assert isinstance(config, BrokerConfig)
    assert config.name == "mock_broker"
    assert config.display_name == "Mock Broker"
    assert config.version == "1.2.3"
    assert config.description == "Blueshift Broker implementation for mock_broker" # default
    assert config.calendar == get_calendar("NYSE")
    assert config.credentials.api_key_id is None
    assert config.assets == [Equity, Crypto]

    # Check credentials
    broker_config = {'api_key_id':'12345', 'api_secret_key':'abcd'}
    config.credentials.resolve(**broker_config)
    assert config.credentials.api_key_id == '12345'
    assert config.credentials.api_secret_key == 'abcd'

    is_valid = config.credentials.validate()
    assert is_valid == True

    # Check Options
    assert isinstance(config.options, BrokerOptions)
    assert config.options.timeout == 10.0
    assert config.options.ccy == "USD"
    assert config.options.max_tickers == 50
    assert config.options.agent == "blueshift" # default
    assert config.options.blotter_type == BlotterType.VIRTUAL # default
    assert isinstance(config.options.rms, dict) and config.options.rms['rms'] == 'no-rms' # default
    
    # Check Enum Maps
    assert config.mappings.order_type.from_blueshift(OrderType.MARKET) == "mkt"
    assert config.mappings.order_type.from_blueshift(OrderType.LIMIT) == "lmt"
    assert config.mappings.order_type.to_blueshift("lmt") == OrderType.LIMIT
    
    assert config.mappings.order_validity.from_blueshift(OrderValidity.DAY) == "day"
    assert config.mappings.product_type.from_blueshift(ProductType.DELIVERY) == "cash" 

def test_broker_config_minimal():
    # Minimal config with only required fields
    MINIMAL_SPEC = {
        "name": "minimal",
        "calendar": "NSE",
        "credentials": {
            "fields":["api_key_id"],
        },
        "data_frequencies":{},
        "order_type": {},
        "order_validity": {},
    }
    
    config = get_broker_config(MINIMAL_SPEC, registry)
    
    assert config.name == "minimal"
    assert config.display_name == "minimal"
    assert config.version == "0.0.0"
    assert config.assets == [Equity] # default
    assert config.options.timeout == 5.0 # default
    assert config.options.execution_modes == [ExecutionMode.AUTO] # default
