# Broker Integration Contract (What Must Be True)

This document is the missing “contract” layer between the broker API docs and the configuration-driven framework. It defines the **minimum required endpoints**, the **expected return shapes**, and the **configuration semantics** that must be satisfied for a broker integration to work reliably.

## 1) Minimum Required Deliverable

A broker integration is a **single Python file** that defines:
- `registry = ConfigRegistry(...)` and any registered helper callables
- Configuration dictionaries:
  - `BROKER_SPEC`
  - `API_SPEC`
  - optional `STREAMING_SPEC`
  - optional `OBJECTS_SPEC`
- A `register_brokers()` function that creates `APIBrokerConfig` and registers the broker via `broker_class_factory(cls_name, config)`

## 2) Required API Endpoints (Hard Requirement)

The REST API configuration must define the following endpoints under `API_SPEC["endpoints"]`:
- `get_orders`
- `place_order`
- `cancel_order`
- `get_account`

And it must define **at least one** of:
- `get_history`
- `get_history_multi`

If any required endpoint is missing, the broker initialization fails with a validation error.

## 3) Endpoint Return Contracts (Runtime Requirement)

The framework invokes configured endpoints and then validates the **shape** of the resolved result.

### A. `place_order`
- Context: `order` (Order object)
- Must resolve to a `dict` containing:
  - `order_id` (str)

### B. `cancel_order`
- Context: `order` (Order object)
- Must resolve to a `dict` containing:
  - `order_id` (str)

### C. `update_order`
- Context: `order` (Order object), `quantity` (float|None), `price` (float|None)
- Must resolve to a `dict` containing:
  - `order_id` (str)

### D. `get_orders`
- Context: *(none extra)*
- Must resolve to either:
  - `list[dict]` (preferred), or
  - `dict` (will be wrapped into a single-item list)
- Each dict must contain enough fields to create an `Order` object:
  - **Required**: `oid` (str), `quantity` (float), `filled` (float), `price` (float), `status` (OrderStatus)
  - **Required for asset inference**: `symbol` (str) or `security_id` (str)
  - **Recommended**: `side`, `order_type`, `average_price`, `timestamp`, `exchange_timestamp`, `broker_order_id`

### E. `get_positions`
- Context: *(none extra)*
- Must resolve to either:
  - `list[dict]` (preferred), or
  - `dict` (will be wrapped into a single-item list)
- Each dict **MUST** contain:
  - `quantity` (float) — signed; negative = short
  - `average_price` (float)
  - `last_price` (float)
- Each dict **MUST** contain one of (for asset inference):
  - `symbol` (str), or
  - `security_id` (str)
- Optional fields:
  - `margin` (float, default 0)
  - `product_type` (ProductType, defaults to broker's first supported product)
  - `timestamp` (pd.Timestamp|None)

Example:
```python
"get_positions": {
    "endpoint": "/positions",
    "method": "GET",
    "response": {
        "payload_type": "array",
        "payload_path": "response.get('positions', [])",
        "items": {
            "fields": {
                "symbol": {"source": "item['symbol']"},
                "quantity": {"source": "float(item['qty'])"},
                "average_price": {"source": "float(item['avg_entry_price'])"},
                "last_price": {"source": "float(item['current_price'])"},
                "product_type": {"source": "ProductType.DELIVERY"},
            }
        }
    }
}
```

### F. `get_account`
- Context: *(none extra)*
- Must resolve to either:
  - `list[dict]` (preferred), or
  - `dict` (will be wrapped into a single-item list)
- Required fields for Account creation:
  - `name` (str) — account identifier
  - `cash` (float) — available cash balance
  - `currency` (str) — e.g. "USD", "INR"

### G. `get_quote`
- Context: `asset` (Asset object)
- Must resolve to a `dict`
- Uses `payload_type: "quote"` with quote fields
- Available quote fields: `last`, `close`, `bid`, `bid_volume`, `ask`, `ask_volume`, `open`, `high`, `low`, `timestamp`

Example:
```python
"get_quote": {
    "endpoint": "/quotes/{symbol}",
    "method": "GET",
    "request": {
        "path": {
            "fields": {"symbol": {"source": "asset.broker_symbol or asset.symbol"}}
        }
    },
    "response": {
        "payload_type": "quote",
        "payload_path": "response.get('quote', {})",
        "quote": {
            "last": {"source": "float(quote.get('last_price', 0))"},
            "bid": {"source": "float(quote.get('bid', 0))"},
            "ask": {"source": "float(quote.get('ask', 0))"},
            "volume": {"source": "float(quote.get('volume', 0))"}
        }
    }
}
```

### H. `session` (Optional Authentication Endpoint)
- Context: `credentials`
- Called during broker initialization (`initialize()`)
- If not defined, initialization succeeds silently
- For complex auth (TOTP, encryption, multi-step), use `request.custom`
- Common patterns:
  - Store token via `credentials.update(token=value)` in a hook or custom function
  - Update API session headers for subsequent calls

Example using custom request:
```python
"session": {
    "endpoint": "/auth/token",
    "method": "POST",
    "request": {
        "custom": "my_auth_function"  # registered callable handles the full auth flow
    },
    "response": {
        "payload_type": "custom",
        "custom": "noop"  # custom request already handled everything
    }
}
```

Example using standard fields with after_response hook:
```python
"session": {
    "endpoint": "/auth/login",
    "method": "POST",
    "request": {
        "json": {
            "fields": {
                "username": {"source": "credentials.api_key"},
                "password": {"source": "credentials.secret_key"}
            }
        }
    },
    "response": {
        "payload_type": "object",
        "result": {
            "fields": {
                "token": {"source": "result.get('access_token')"}
            }
        }
    },
    "hooks": {
        "after_response": "store_session_token"
    }
}
```

### I. Historical data: `get_history` / `get_history_multi`
- `get_history` context: `asset`, `freq`, `from_dt`, `to_dt`, `nbars`
- Must resolve to a `pandas.DataFrame` (handled automatically by `payload_type: "data"`)
- `get_history_multi` must resolve to `dict[str, pandas.DataFrame]`

## 4) Config Semantics: `source` vs `custom` vs Streaming Templates

### A. `source` (Expression)
Use when a value can be expressed as a safe Python expression evaluated against a runtime context.

Examples:
- `"source": "credentials.api_key"`
- `"source": "order.asset.broker_symbol or order.asset.symbol"`
- `"source": "mappings.order_side.from_blueshift(order.side)"`

Key properties:
- Expressions are evaluated with a “safe” eval environment.
- The runtime context overrides registry entries when keys collide.

### B. `custom` (Callable Reference)
Use when you need arbitrary Python logic (auth signing, parsing, non-trivial mapping).

`custom` references a callable by name (registered via `@registry.register(...)`), and the framework invokes it with the same runtime context as keyword arguments.

Practical rule:
- Prefer `source` for simple field-level transforms.
- Use `custom` for anything that needs conditionals, loops, hashing, or multi-field assembly.

### C. Streaming Message Templates (`message_def`)
Streaming “messages” (auth/subscribe/unsubscribe) support:
- `format: "json"`: build a dict using `fields_group` semantics (same `source`/`custom` machinery)
- `format: "text"`: a Python `str.format(...)` template with `tokens` computed via `fields_group`
- `format: "binary"`: provide a `custom` encoder callable

This is intentionally **not** mustache-style `{{ token }}` templating.

## 5) Pagination

If the broker API paginates results (common for `get_orders`, `get_positions`), configure pagination tokens.

### How It Works
1. Framework makes the first request normally
2. After parsing response, evaluates `response.next_page_token` expression
3. If token is non-None and response is a `list`, framework loops:
   - Injects token into the next request via `request.next_page_token`
   - Concatenates results
   - Stops when token is None or `max_page_fetch` reached (default 50)

### Configuration

```python
"get_orders": {
    "endpoint": "/orders",
    "method": "GET",
    "request": {
        "query": {
            "fields": {
                "status": {"source": "'all'"},
                "limit": {"source": "100"},
            }
        },
        # Where to inject the pagination token in subsequent requests
        "next_page_token": {
            "parameter": "after",       # query/body parameter name
            "location": "query"         # "query" or "body"
        }
    },
    "response": {
        "payload_type": "array",
        "payload_path": "response.get('orders', [])",
        # Expression to extract next page token from response
        "next_page_token": "response.get('next_page_token')",
        "items": {
            "fields": {
                "oid": {"source": "item['id']"},
                # ... other fields
            }
        }
    }
}
```

### Key Rules
- `response.next_page_token` is a Python expression evaluated with `response` in context
- Return `None` (or falsy) to stop pagination
- The `payload_type` MUST be `"array"` for pagination to work
- Results from all pages are concatenated into a single list
- Maximum pages controlled by `BROKER_SPEC.options.max_page_fetch` (default 50)

## 6) Error Handling

Define error rules to map API error responses to appropriate Blueshift exceptions.

### Configuration

Errors can be defined globally (`API_SPEC["errors"]`) and/or per-endpoint (`endpoint["errors"]`). Per-endpoint rules are checked first.

```python
API_SPEC = {
    "base_url": "...",
    "errors": [
        {
            "condition": "status_code == 401",
            "exception": "AuthenticationError",
            "message": "Authentication failed"
        },
        {
            "condition": "status_code == 429",
            "exception": "APIException",
            "message": "Rate limit exceeded"
        },
        {
            "condition": "not success and status_code >= 500",
            "exception": "ServerError",
            "message": "Server error"
        }
    ],
    "endpoints": {
        "place_order": {
            # Per-endpoint error rules (checked before global)
            "errors": [
                {
                    "condition": "not success and 'insufficient' in str(response.get('message','')).lower()",
                    "exception": "OrderError",
                    "message": "Insufficient funds for order"
                }
            ],
            # ... rest of endpoint config
        }
    }
}
```

### Error Rule Fields

| Field | Type | Description |
|-------|------|-------------|
| `condition` | `str` | Python expression. Context: `success`, `status_code`, `response`, `text` |
| `exception` | `str` | Exception class name: `AuthenticationError`, `APIException`, `APIError`, `ServerError`, `OrderError`, `ValidationError` |
| `message` | `str` | Error message (can use f-string style with context vars) |

### How It Works
1. After HTTP response, the framework evaluates error rules in order
2. First matching `condition` raises the corresponding exception
3. If no error rules match and `success` is False, a generic `APIError` is raised
4. If `success` is True, error handling is skipped

## 7) Hooks

Hooks allow custom logic at specific points in the request/response cycle.

### `before_send`

Called after request fields are resolved, before the HTTP call is made. Use for request signing, HMAC, nonce injection.

```python
@registry.register()
def sign_request(url, verb, headers, query, body, json, credentials, **kwargs):
    """Sign the request with HMAC."""
    import hmac, hashlib
    payload = json_lib.dumps(body) if body else ""
    signature = hmac.new(credentials.secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
    headers['X-Signature'] = signature
    return url, verb, headers, query, body, json

# In endpoint config:
"hooks": {"before_send": "sign_request"}
```

**Signature:** `f(url, verb, headers, query, body, json, **context) -> (url, verb, headers, query, body, json)`

### `after_response`

Called after HTTP response is received, before error handling and response parsing. Use for token refresh, response normalization.

```python
@registry.register()
def store_session_token(response, credentials, **kwargs):
    """Extract and store session token from login response."""
    token = response.get('data', {}).get('token')
    if token:
        credentials.update(api_token=token)

# In endpoint config:
"hooks": {"after_response": "store_session_token"}
```

**Signature:** `f(**context) -> None` (modifies context in-place)

## 8) Asset Universe / Master Data (Usually Required for Live Trading)

If the broker requires symbol/id lookup (security_id, broker_symbol, exchange, etc.), configure `API_SPEC["master_data"]` to fetch and parse instrument master data. This is what powers consistent `Asset` creation and broker symbol mapping.

If you skip master data, you typically must rely on user-provided symbols and accept limitations (no security_id mapping, weaker validation, harder streaming routing).

