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
    "headers": { ... },          # Optional: per-endpoint headers (field_spec group)
    "use_global_headers": True,  # Default True. Set False to skip global headers for this endpoint
    "timeout": 30,               # Optional: per-endpoint timeout override
    "request": {
        "body": {                # or "json", "query", "path"
            "fields": {
                "symbol": {"source": "order.asset.symbol"},
                "qty": {"source": "int(order.quantity)"}
            }
        },
        "custom": "my_function", # Optional: bypass all field resolution, delegate to callable
        # Pagination (for paginated endpoints):
        "next_page_token": {
            "parameter": "page_token",  # Name of the query/body param to inject token
            "location": "query"         # "query" or "body"
        }
    },
    "response": {
        "payload_type": "object", # "object", "array", "data", "quote", "custom"
        "payload_path": "...",    # Optional: expression to extract payload from response
        "next_page_token": "...", # Optional: expression to extract next page token
        "result": {               # For payload_type "object"
            "fields": {
                "order_id": {"source": "result['id']"}
            }
        }
    },
    "errors": [ ... ],           # Optional: per-endpoint error rules (see below)
    "hooks": {                   # Optional: lifecycle hooks
        "before_send": "sign_request",
        "after_response": "store_token"
    }
}
```

### Response `payload_type` Options

| Type | Description | Context Variable | Use Case |
|------|-------------|-----------------|----------|
| `"object"` | Single dict result | `result` | place_order, cancel_order, get_account |
| `"array"` | List of dicts | `item` (per element) | get_orders, get_positions |
| `"data"` | OHLCV DataFrame | *(automatic)* | get_history |
| `"quote"` | Quote data | `quote` | get_quote |
| `"custom"` | Fully delegated | all context | Complex/irregular responses |

### OHLCV Data Response (`payload_type: "data"`)

```python
"response": {
    "payload_type": "data",
    "payload_path": "response.get('bars', [])",
    "data": {
        "frame": "records",        # "records" (list of dicts), "dict_of_arrays", "values" (list of lists)
        "timestamp": {
            "field": "t",          # Column name containing timestamps
            "format": "%Y-%m-%dT%H:%M:%SZ",  # For string timestamps (strftime format)
            # OR
            "unit": "s"           # For numeric timestamps: "s", "ms", "us", "ns"
        },
        # rename: {Target_Standard_Name: Source_API_Name}
        # Keys = what Blueshift expects, Values = what the API returns
        "rename": {"open": "o", "high": "h", "low": "l", "close": "c", "volume": "v"},
        # For multi-asset responses (get_history_multi):
        "symbol": "key"           # Top-level dict keys are symbols
    }
}
```

### Error Rules Schema

```python
"errors": [
    {
        "condition": "status_code == 401",         # Expression with: success, status_code, response, text
        "exception": "AuthenticationError",        # Exception class name
        "message": "Authentication failed"         # Error message
    },
    {
        "condition": "not success and response.get('error')",
        "exception": "APIException",
        "message": "API error"
    }
]
```

Available exception names: `AuthenticationError`, `APIException`, `APIError`, `ServerError`, `OrderError`, `ValidationError`, `BrokerError`.

### Global Headers with `custom`

For brokers that need dynamic headers (e.g., HMAC signing on every request):

```python
API_SPEC = {
    "headers": {
        "custom": "compute_headers"   # Called with full request context, must return a headers dict
    },
    ...
}
```

The `custom` function receives: `credentials`, `params`/`json`/`body` (the request payload), and all standard context.

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
Defines conversion pipelines for API dicts -> Blueshift objects. Each key is a list of conversion rules tried in order.

The context variable for all OBJECTS_SPEC pipelines is `data` (the raw dict from the API). For `order` and `position` pipelines, `asset` is also available (inferred from `symbol`/`security_id` in `data`).

```python
OBJECTS_SPEC = {
    "order": [
        {
            # Optional: only apply this rule if condition is true
            "condition": "data.get('order_type') != 'internal'",
            "fields": {
                "oid": {"source": "str(data['order_id'])"},
                "broker_order_id": {"source": "str(data['order_id'])"},
                "quantity": {"source": "float(data['qty'])"},
                "filled": {"source": "float(data.get('filled_qty', 0))"},
                "price": {"source": "float(data.get('limit_price', 0))"},
                "average_price": {"source": "float(data.get('avg_fill_price', 0))"},
                "side": {"source": "mappings.order_side.to_blueshift(data['side'])"},
                "order_type": {"source": "mappings.order_type.to_blueshift(data['type'])"},
                "status": {"source": "mappings.order_status.to_blueshift(data['status'])"},
                "timestamp": {"source": "pd.Timestamp(data['created_at'])"},
            }
        }
    ],
    "position": [
        {
            "fields": {
                # REQUIRED fields for Position:
                "quantity": {"source": "float(data['qty'])"},
                "average_price": {"source": "float(data['avg_entry_price'])"},
                "last_price": {"source": "float(data['current_price'])"},
                # Optional:
                "margin": {"source": "float(data.get('margin', 0))"},
                "product_type": {"source": "ProductType.DELIVERY"},
            }
        }
    ],
    "account": [
        {
            "fields": {
                "name": {"source": "data.get('account_id', 'default')"},
                "cash": {"source": "float(data['cash'])"},
                "currency": {"source": "'USD'"}
            }
        }
    ],
    "asset": [
        {
            # Used by broker.infer_asset() as fallback
            "fields": {
                "asset_type": {"source": "'equity'"},
                "symbol": {"source": "data['symbol']"},
                "security_id": {"source": "data.get('token')"},
                "exchange_name": {"source": "data.get('exchange', 'NSE')"},
            }
        }
    ]
}
```

### Pipeline Execution
- Rules are tried in order. First rule that produces a valid object wins.
- If a rule has a `condition` and it evaluates to False, that rule is skipped.
- If a rule has a `custom` callable, it's called instead of field resolution.
- The pipeline must produce a dict that can be passed to `Order.from_dict()`, `Position.from_dict()`, etc.
- If `OBJECTS_SPEC` is empty `{}`, the framework falls back to passing the raw API dict directly to `from_dict()`.
