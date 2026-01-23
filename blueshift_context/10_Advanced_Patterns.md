# 10 - Advanced Patterns

This document covers patterns that appear in production broker integrations but are not needed for basic implementations. Refer to this when the broker requires non-standard behavior.

## 1. Custom Request Functions

When the standard field resolution is insufficient (complex signing, multi-step auth, encrypted payloads), bypass it entirely:

```python
@registry.register()
def my_custom_request(**context):
    """
    Called instead of normal field resolution.
    Context contains: credentials, api, url, verb, broker, mappings, config
    Plus endpoint-specific vars (order, asset, etc.)
    """
    credentials = context['credentials']
    api = context['api']
    url = context['url']
    verb = context['verb']

    # Build request manually
    headers = {"Authorization": f"Bearer {credentials.access_token}"}
    body = json.dumps({"key": "value"})

    # Make the raw HTTP call
    success, status_code, response = api.raw_request(url, verb, headers, {}, body, {})

    if not success:
        raise APIError(f"Request failed: {status_code}")

    # Store results if needed
    credentials.update(token=response.get('token'))

# In endpoint config:
"session": {
    "endpoint": "/auth/login",
    "method": "POST",
    "request": {
        "custom": "my_custom_request"
    },
    "response": {
        "payload_type": "custom",
        "custom": "noop"  # Request already handled everything
    }
}
```

### `api.raw_request()` Signature
```python
success, status_code, response = api.raw_request(
    url: str,           # Full URL
    verb: str,          # HTTP method
    headers: dict,      # Request headers
    params: dict,       # Query parameters
    body: str|dict,     # Request body (string or dict)
    json_params: dict   # JSON body (alternative to body)
)
```
Returns: `(bool, int, dict|str)` — success flag, HTTP status, parsed response

## 2. Session Management Patterns

### Pattern A: Simple Token Auth (session endpoint with field resolution)
```python
"session": {
    "endpoint": "/auth/token",
    "method": "POST",
    "request": {
        "json": {
            "fields": {
                "username": {"source": "credentials.api_key"},
                "password": {"source": "credentials.api_secret"}
            }
        }
    },
    "hooks": {
        "after_response": "store_token"
    },
    "response": {
        "payload_type": "object",
        "result": {"fields": {"token": {"source": "result.get('access_token')"}}}
    }
}

@registry.register()
def store_token(response, credentials, **kwargs):
    credentials.update(access_token=response.get('access_token'))
```

### Pattern B: Complex Auth (TOTP, encryption, multi-step)
```python
"session": {
    "endpoint": "/auth/init",
    "method": "POST",
    "request": {"custom": "complex_auth"},
    "response": {"payload_type": "custom", "custom": "noop"}
}

@registry.register()
def complex_auth(**context):
    import pyotp
    credentials = context['credentials']
    api = context['api']

    # Step 1: Get request token
    success, _, resp = api.raw_request(
        "https://api.broker.com/auth/init", "GET", {}, {}, {}, {})
    request_token = resp.get('request_token')

    # Step 2: Generate TOTP
    totp = pyotp.TOTP(credentials.totp_secret).now()

    # Step 3: Login with token + TOTP
    body = json.dumps({"request_token": request_token, "totp": totp})
    success, status_code, resp = api.raw_request(
        "https://api.broker.com/auth/login", "POST",
        {"Content-Type": "application/json"}, {}, body, {})

    if not success:
        from blueshift.lib.exceptions import AuthenticationError
        raise AuthenticationError(f"Auth failed: {status_code}")

    credentials.update(access_token=resp['access_token'])
```

### Pattern C: Lazy Session (`lazy_session: True`)
If `BROKER_SPEC.options.lazy_session` is `True`, the `session` endpoint is NOT called during `initialize()`. Instead, it's called lazily on the first API request that fails with 401. Useful when tokens are pre-generated externally.

## 3. Custom Headers with Dynamic Signing

For brokers that require per-request HMAC/checksum signing:

```python
COMMON_HEADERS = {
    "custom": "compute_signed_headers"  # Called on EVERY request
}

@registry.register()
def compute_signed_headers(**kwargs):
    """
    Custom header function receives the full request context.
    Must return a dict of headers.
    """
    credentials = kwargs.get('credentials')
    body = kwargs.get('body', {})  # or 'json', 'params'

    if not isinstance(body, str):
        body = json.dumps(body, separators=(',', ':'))

    timestamp = str(int(time.time()))
    checksum = hashlib.sha256(
        (timestamp + body + credentials.secret_key).encode()
    ).hexdigest()

    return {
        "Content-Type": "application/json",
        "X-Checksum": f"token {checksum}",
        "X-Timestamp": timestamp,
        "X-AppKey": credentials.api_key,
        "X-SessionToken": credentials.access_token,
    }
```

## 4. Per-Endpoint Overrides

Any endpoint can override global settings:

```python
"get_history": {
    "endpoint": "https://data.broker.com/v2/bars/{symbol}",  # Full URL overrides base_url
    "method": "GET",
    "use_global_headers": False,     # Skip global headers for this endpoint
    "timeout": 30,                   # Override timeout for slow endpoints
    "headers": {                     # Endpoint-specific headers
        "fields": {
            "X-Data-Key": {"source": "credentials.data_api_key"},
            "Content-Type": {"source": "'application/json'"},
        }
    },
    ...
}
```

### When to Use Full URLs
If the broker uses different base URLs for different services (common for market data vs trading):
- Market data: `https://data.broker.com/v2/...`
- Trading: `https://api.broker.com/v1/...` (the `base_url`)

Set the full URL as the `endpoint` value — the framework detects URLs starting with `http` and skips `base_url` prepending.

## 5. Conditional Fields (`condition`)

Include request fields only when a condition is met:

```python
"place_order": {
    "request": {
        "json": {
            "fields": {
                "symbol": {"source": "order.asset.symbol"},
                "qty": {"source": "str(int(order.quantity))"},
                "side": {"source": "mappings.order_side.from_blueshift(order.side)"},
                "type": {"source": "mappings.order_type.from_blueshift(order.order_type)"},
                # Only include limit_price for LIMIT orders
                "limit_price": {
                    "source": "str(order.price)",
                    "condition": "order.order_type == OrderType.LIMIT"
                },
                # Only include stop_price for STOP orders
                "stop_price": {
                    "source": "str(order.trigger_price)",
                    "condition": "order.order_type in (OrderType.STOP, OrderType.STOP_LIMIT)"
                },
                # Only include expiry for derivative assets
                "expiry": {
                    "source": "order.asset.expiry_date.strftime('%Y-%m-%d')",
                    "condition": "order.asset.is_futures() or order.asset.is_opt()"
                },
            }
        }
    }
}
```

When `condition` evaluates to `False`, the field is completely omitted from the request body (not sent as None/null).

## 6. Request Body Serialization with `custom`

Some brokers require non-standard body serialization (e.g., URL-encoded form data presented as JSON string):

```python
@registry.register()
def custom_serialize(values, **kwargs):
    """Custom body serializer: convert dict to compact JSON string."""
    return json.dumps(values, separators=(',', ':'))

# In endpoint config:
"request": {
    "body": {
        "custom": "custom_serialize",  # Applied to the resolved fields dict
        "fields": {
            "stock_code": {"source": "order.asset.broker_symbol"},
            "action": {"source": "mappings.order_side.from_blueshift(order.side)"},
        }
    }
}
```

When `custom` is on a `body`/`query`/`json` block alongside `fields`, the fields are resolved first, then the custom callable is called with the resolved dict.

## 7. BROKER_SPEC Options Reference

Complete list of supported options with defaults:

```python
"options": {
    # HTTP Settings
    "timeout": 10,                    # HTTP request timeout (seconds)
    "rate_limit": 10,                 # Max requests per period
    "rate_limit_period": 1,           # Period in seconds for rate limiting
    "agent": "blueshift",             # User-Agent string

    # Trading Behavior
    "supported_modes": ["LIVE"],      # ["LIVE", "PAPER"]
    "execution_modes": [],            # Advanced: execution mode overrides
    "blotter_type": "VIRTUAL",        # Blotter type (usually leave as default)
    "fractional_trading": False,      # Allow fractional quantities
    "check_host_before_trade": True,  # Verify host before trading
    "unwind_before_trade": False,     # Close positions before trading
    "intraday_cutoff": [15, 20],      # [hour, minute] for intraday cutoff

    # Session
    "lazy_session": False,            # If True, session endpoint called lazily
    "interface": "broker",            # "broker" (full), "trader" (trade-only), "data-portal" (data-only)

    # Data Settings
    "max_tickers": 200,               # Max tickers per subscription
    "max_subscription": 100,          # Max concurrent subscriptions
    "max_nbars": 1000,                # Max bars per history request
    "max_data_fetch": 1000,           # Max data points per fetch
    "max_page_fetch": 50,             # Max pages for pagination
    "multi_assets_data_query": False,  # Enable get_history_multi
    "multi_assets_data_subscribe": False,  # Multi-asset streaming subscribe

    # Currency
    "ccy": "LOCAL",                   # Account currency ("LOCAL", "USD", "INR", etc.)

    # Derivatives
    "expiry_dispatch": {},            # Exchange-specific expiry handling, e.g. {"NFO": "NFO"}
}
```

## 8. BROKER_SPEC `assets` Field

Declare which asset classes the broker supports:

```python
"assets": ["equity"]                          # Equity only
"assets": ["equity", "equity-futures"]        # Equity + Futures
"assets": ["equity", "equity-futures", "equity-options"]  # Full derivatives
"assets": ["crypto"]                          # Crypto
"assets": ["fx"]                              # Forex
"assets": ["equity", "mktdata"]              # Equity + Index data
```

The `mktdata` class is for non-tradeable instruments (indices, benchmarks) used only for data subscriptions.

## 9. Broker Variants

Variants allow the same broker code to support multiple environments:

```python
BROKER_VARIANTS = {
    'live': {
        "broker": {
            "name": "mybroker-live",
            "variant": "live",
            "display_name": "My Broker Live",
        },
        "api": {
            "base_url": "https://api.broker.com"
        }
    },
    'paper': {
        "broker": {
            "name": "mybroker-paper",
            "variant": "paper",
            "display_name": "My Broker Paper",
        },
        "api": {
            "base_url": "https://sandbox.broker.com"
        },
        "streaming": {
            "connections": {
                "market_data": {
                    "url": "wss://sandbox-stream.broker.com"
                }
            }
        }
    }
}
```

Variant overrides use `merge_json_recursive` — only the specified keys are overridden; everything else inherits from the base config.

## 10. `noop` Sentinel

The `noop` callable is a no-op function that returns nothing. Use it as a placeholder:

```python
from blueshift.lib.common.sentinels import noop

# In custom response when request already handled everything:
"response": {"payload_type": "custom", "custom": "noop"}
```

It's automatically available in the registry (registered via `globals()` if imported).

## 11. The `__init__.py` Entry Point

Each broker contrib package needs an `__init__.py` that calls `register_brokers()`:

```python
# src/blueshift/contribs/brokers/mybroker/__init__.py
from .mybroker import register_brokers

register_brokers()
```

This ensures brokers are registered when the package is imported by the Blueshift runtime.

## 12. `pyproject.toml` Structure

Each broker is a separate Python package:

```toml
[project]
name = "blueshift-broker-mybroker"
version = "0.1.0"
description = "My Broker integration for Blueshift"
requires-python = ">=3.10"
dependencies = [
    "blueshift-core",
    "blueshift-brokers",
]

[project.entry-points."blueshift.brokers"]
mybroker = "blueshift.contribs.brokers.mybroker"
```

The entry point `blueshift.brokers` allows the Blueshift runtime to auto-discover and load broker packages.
