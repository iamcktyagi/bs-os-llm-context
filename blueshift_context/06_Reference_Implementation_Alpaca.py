from blueshift.brokers.core import ConfigRegistry
from blueshift.brokers.core.config.schema import BROKER_SCHEMA, API_SCHEMA, STREAMING_SCHEMA
# ... other imports ...

# 1. Registry Definition
registry = ConfigRegistry()

# 2. Helper Functions (Auth, Parsing, etc.)
@registry.register("alpaca_auth_headers")
def alpaca_auth_headers(context):
    return {
        "APCA-API-KEY-ID": context.get("api_key"),
        "APCA-API-SECRET-KEY": context.get("api_secret")
    }

# 3. Configuration Definitions
BROKER_SPEC = {
    "name": "alpaca",
    "calendar": "NYSE",
    "credentials": {
        "fields": ["api_key", "api_secret"],
        "validator": "len(api_key) > 0 and len(api_secret) > 0"
    },
    # ...
}

API_SPEC = {
    "base_url": "https://paper-api.alpaca.markets",
    "endpoints": {
        "get_account": {
            "endpoint": "/v2/account",
            "method": "GET",
            "headers": {"custom": "alpaca_auth_headers"}
        },
        # ...
    }
}

STREAMING_SPEC = {
    "connections": {
        "main": {
            "url": "wss://stream.data.alpaca.markets/v2/iex",
            "backend": {"type": "websocket"},
            "auth": {
                "mode": "first_message",
                "first_message": {
                    "format": "json",
                    "json": {
                        "fields": {
                            "action": "auth",
                            "key": "{{ api_key }}",
                            "secret": "{{ api_secret }}"
                        }
                    }
                }
            },
            # ...
        }
    }
}

# 4. Factory Creation
alpaca_broker = broker_class_factory(
    BROKER_SPEC, API_SPEC, STREAMING_SPEC, registry
)
