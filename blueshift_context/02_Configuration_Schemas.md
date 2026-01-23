# Configuration Schemas

This document defines the structure of the configuration dictionaries used to implement a broker.

## 1. Field Specification (`field_spec`)
Almost every value in the configuration is a `field_spec`. It tells the framework how to derive a value.

```python
{
    "source": "expression",      # Python expression evaluated in runtime context
    "condition": "expression",   # Optional: If False, field is skipped (NULL sentinel)
    "transform": "callable_ref", # Optional: Callable applied after source (gets value + context)
    "custom": "callable_ref",    # Optional: Callable that returns the final value (ignores source/transform)
    "default_value": "value"     # Optional: Fallback if source evaluation fails
}
```
**Examples:**
*   `{"source": "credentials.api_key"}` -> Variable access
*   `{"source": "'constant_string'"}` -> Constant string
*   `{"source": "mappings.order_side.from_blueshift(order.side)"}` -> Enum conversion
*   `{"source": "None", "custom": "sign_request"}` -> Delegate to custom Python logic (auth/signing)

Notes:
*   `source` expressions are evaluated in a safe Python environment that includes both the runtime context (e.g., `order`, `asset`, `credentials`, `response`) and any functions registered in `ConfigRegistry`.
*   `callable_ref` values must reference a registered function name (via `@registry.register(...)`) or an importable reference supported by the framework.

## 2. `BROKER_SPEC`
Defines global options and enum mappings.

```python
BROKER_SPEC = {
    "name": "mybroker",          # Required: broker identifier
    "variant": "live",           # Optional: variant name (e.g., "live", "paper")
    "calendar": "NYSE",          # Trading calendar (NYSE, NSE, etc.)
    "credentials": {
        "fields": ["api_key", "secret"], # Required fields in blueshift.yaml
        "validator": "credentials.api_key and credentials.secret"
    },
    "options": {
        "timeout": 10,           # HTTP timeout
        "rate_limit": 180,       # Requests per period
        "rate_limit_period": 60, # Period in seconds
        "max_tickers": 200,      # Max tickers in one request
        "supported_modes": ["LIVE", "PAPER"],
        "fractional_trading": False
    },
    "assets": ["equity"],        # Supported asset classes
    
    # Mappings: Map Blueshift Enums to Broker Strings
    "order_side": {
        "map": {
            OrderSide.BUY: "buy",
            OrderSide.SELL: "sell"
        }
    },
    "order_type": { ... },
    "order_validity": { ... },
    "order_status": { ... }
}
```

## 3. `API_SPEC`
Defines the REST endpoints.

```python
API_SPEC = {
    "base_url": "https://api.broker.com",
    "headers": {
        "fields": {
            "Authorization": {"source": "f'Bearer {credentials.api_key}'"}
        }
    },
    "master_data": [ ... ], # Optional but recommended: instrument universe & symbol/id mapping
    "endpoints": {
        "get_orders": { ... },
        "place_order": { ... },
        "cancel_order": { ... },
        "get_account": { ... },
        "get_positions": { ... },
        "get_history": { ... },
        "get_quote": { ... }
    }
}
```

Minimum contract:
*   The framework requires `get_orders`, `place_order`, `cancel_order`, and `get_account`.
*   It also requires at least one of `get_history` or `get_history_multi`.
See `07_Broker_Integration_Contract.md` for return-shape requirements.
See `08_Master_Data_and_Streaming_Converters.md` for master data patterns.

### Endpoint Definition
```python
"place_order": {
    "endpoint": "/v1/orders",    # URL Path
    "method": "POST",            # HTTP Method
    "request": {
        "body": {                # or "json", "query", "path"
            "fields": {
                "symbol": {"source": "order.asset.symbol"},
                "qty": {"source": "int(order.quantity)"}
            }
        }
    },
    "response": {
        "payload_type": "object",
        "result": {
            "fields": {
                "order_id": {"source": "result['id']"} # Extracts ID to return
            }
        }
    }
}
```

## 4. `STREAMING_SPEC`
Defines real-time streaming connections (WebSocket, SocketIO, MQTT).

For detailed patterns and examples for each protocol, see **[05_Streaming_Patterns.md](./05_Streaming_Patterns.md)**.
For converter pipelines and master-data-driven asset mapping, see **[08_Master_Data_and_Streaming_Converters.md](./08_Master_Data_and_Streaming_Converters.md)**.

```python
STREAMING_SPEC = {
    "connections": {
        "market_data": {
            "url": "wss://stream.broker.com",
            
            # Backend Configuration
            "backend": {
                "type": "websocket", # Options: "websocket", "socketio", "mqtt"
                "options": {
                    # Protocol-specific options (e.g., "transports" for socketio, "keepalive" for mqtt)
                }
            },
            
            "streams": ["data", "quote"], # Channels this connection handles
            
            # Authentication (Optional)
            "auth": {
                "mode": "first_message", # Options: "url", "headers", "first_message", "none"
                "first_message": { ... }
            },
            
            # Subscription Logic
            "subscribe": {
                "subscribe": { ... },   # Message to send to subscribe
                "unsubscribe": { ... }  # Message to send to unsubscribe
            },
            
            # Parsing Logic
            "parser": {
                "format": "json",       # Options: "json", "text", "binary"
                "payload_path": "data"  # Path to extract actual payload
            },
            
            # Message Routing (Optional)
            "router": {
                "enabled": True,
                "rules": [
                    {"channel": "data", "match": "msg.get('T') == 't'"}
                ]
            },
            
            # Object Conversion (Raw Dict -> Blueshift Object)
            "converters": {
                "data": [ ... ], # List of conversion rules
                "quote": [ ... ]
            }
        }
    }
}
```

## 5. `OBJECTS_SPEC`
Defines how to convert API responses into standard Blueshift objects (`Account`, `Position`). Note: `Order` conversion is usually handled directly in the `get_orders` endpoint response.

```python
OBJECTS_SPEC = {
    "account": [
        {
            "fields": {
                "name": {"source": "data['id']"},
                "cash": {"source": "float(data['balance'])"},
                "currency": {"source": "'USD'"}
            }
        }
    ],
    "position": [ ... ]
}
```
