# Blueshift Brokers Configuration System

The `blueshift.brokers.core.config` package provides a robust, declarative configuration framework for defining broker integrations. Instead of writing boilerplate code for every new broker, this system allows developers to define broker behavior, API endpoints, data mappings, and object conversions using data structures (dictionaries/JSON) validated against strict schemas.

## Overview

The configuration system is designed around three main pillars:
1.  **Strict Schemas**: JSON schemas define the structure for Broker options, API endpoints, and Object conversions.
2.  **Dynamic Resolution**: A powerful resolver engine (`resolver.py`) allows fields to be computed dynamically using Python expressions (`expr`) or custom functions (`callable_ref`).
3.  **Pipeline Processing**: Data flows through configurable pipelines to convert raw API responses into Blueshift domain objects (Orders, Positions, Assets).

## Architecture

### Modules

*   **`config.py`**: The entry point. Contains `APIBrokerConfig`, which orchestrates the loading and resolution of the three primary configuration sections (Broker, API, Objects).
*   **`schema.py`**: Defines the JSON Schemas (`BROKER_SCHEMA`, `API_SCHEMA`, `OBJECTS_SCHEMA`, `STREAMING_SCHEMA`) and common definitions for expressions and computed fields.
*   **`resolver.py`**: The runtime engine. It handles:
    *   `CompiledExpr`: Safe evaluation of string expressions (e.g., `"data['price'] * 100"`).
    *   `ComputedField`: Logic to extract, transform, and validate data fields.
    *   `ConfigRegistry`: A registry to look up custom Python functions referenced in the config.
*   **`broker.py`**: Handles parsing of broker-specific options, credentials validation, and Enum mappings (e.g., mapping Broker OrderType "MKT" to Blueshift `OrderType.MARKET`).
*   **`api.py`**: Parsers for API definitions. It defines `APIEndPoint`, `APIRequest`, and `APIResponse` logic, handling pagination, rate limits, and payload extraction.
*   **`objects.py`**: Defines conversion pipelines (`ObjectConversionPipeline`) that transform raw API dictionaries into Blueshift objects (`Order`, `Position`, `Account`).
*   **`instruments.py`**: specialized logic (`AssetDef`) for parsing master data files or endpoints to generate the asset universe.

## Schema Concepts

The system relies on a few core reusable definitions found in `schema.py`.

### 1. Expressions (`expr`)
String-based Python expressions evaluated at runtime with the current context (e.g., `row`, `data`, `response`).
```json
"quantity": {"source": "data['qty']"}
"is_open": {"source": "data['status'] == 'OPEN'"}
```

### 2. Computed Fields (`field_spec`)
A definition for extracting and transforming data.
```json
"price": {
    "source": "data['px']",
    "transform": "float",
    "default_value": 0.0
}
```

### 3. Enum Maps
Mappings between Broker specific strings/integers and Blueshift Enums.
```json
"order_type": {
    "map": {
        "MARKET": "mkt",
        "LIMIT": "lmt"
    }
}
```

## Configuration Sections

A full broker configuration consists of three parts passed to `APIBrokerConfig`.

### 1. Broker Config (`BROKER_SCHEMA`)
Defines identity, authentication, limits, and behavior.
*   **`credentials`**: Fields required for login and a validator expression.
*   **`options`**: Timeouts, rate limits, supported modes (Live/Backtest), and feature flags (fractional trading, etc.).
*   **`mappings`**: Enum maps for Order types, Validities, and Product types.

### 2. API Config (`API_SCHEMA`)
Defines the REST endpoints.
*   **`base_url`**: Root URL for the API.
*   **`endpoints`**: Dictionary of endpoint definitions. Standard keys like `get_orders`, `place_order`, `get_history` are expected by the `BrokerAPIMixin`.
    *   **`request`**: query params, headers, body definition.
    *   **`response`**: payload parsing logic (`result` for single objects, `items` for lists, `data` for OHLCV).
    *   **`pagination`**: automated handling via `next_page_token`.

### 3. Objects Config (`OBJECTS_SCHEMA`)
Defines how to build objects from data.
*   **`order`**, **`position`**, **`account`**: Lists of conversion rules.
*   **Pipeline Logic**: The system tries rules in order. A rule can have a `condition`. If the condition fails, it tries the next rule.

## Example Usage

```python
from blueshift.brokers.core.config.config import APIBrokerConfig

# Define configurations (usually loaded from YAML/JSON)
broker_spec = {
    "name": "my_broker",
    "calendar": "NYSE",
    "credentials": {"fields": ["api_key"]},
    # ... mappings ...
}

api_spec = {
    "base_url": "https://api.mybroker.com",
    "endpoints": {
        "get_account": {
            "endpoint": "/user/account",
            "method": "GET",
            "response": {
                "payload_type": "object",
                "result": {
                    "cash": {"source": "result['cash_balance']"},
                    "name": {"source": "result['account_id']"}
                }
            }
        }
    }
}

objects_spec = {
    "account": [
        {
            "fields": {
                "name": {"source": "data['name']"},
                "cash": {"source": "data['cash']"}
            }
        }
    ]
}

# Initialize
config = APIBrokerConfig(broker_spec, api_spec, objects_spec)

# The config object is now ready to be used by RestAPIBroker
```

## Gotchas and Important Notes

### 1. Security and `eval()`
The `resolver.py` engine uses Python's `eval()` to process expressions (`expr`). While the implementation sanitizes AST nodes to block imports and private attribute access, it is **not a full sandbox**.
*   **Action**: Never load broker configurations from untrusted or unverified sources.

### 2. Context Variable Names
Expressions rely on specific variables being present in the local context. Using the wrong key will result in a `KeyError` at runtime.
*   **`row` / `item`**: Used in `instruments.py` and `AssetDef`.
*   **`result`**: Used in `APIResponse` when `payload_type` is `object`.
*   **`data`**: Standard alias used during object conversion pipelines.
*   **`response`**: The raw API response, available in `payload_path` resolution.

### 3. The `NULL` Sentinel
If a `field_spec` has a `condition` that evaluates to `False`, the resolver returns a internal `NULL` sentinel. 
*   **Behavior**: `ComputedFields` automatically filters out these `NULL` values. This allows you to conditionally include or exclude keys from the final dictionary sent to object constructors.

### 4. Mutual Exclusivity
In many schema definitions (like `object_conversion` or `endpoint_def`), you must choose between a **declarative** approach (`fields`, `mapping`) or a **custom** approach (`custom`). 
*   Providing both will either trigger a validation error or cause the `custom` function to take precedence, ignoring the declarative fields.

### 5. Enum Map Reverse Lookups
The `EnumMap` class creates a reverse mapping automatically.
*   If your broker uses multiple strings for the same Blueshift Enum (e.g., `["BUY", "LONG"] -> OrderSide.BUY`), the reverse lookup (`to_blueshift`) will map both back to `OrderSide.BUY`. 
*   However, the forward lookup (`from_blueshift`) will always return the **first** element in the list (`"BUY"`).

### 6. Late Resolution
Some fields like `rms` in `BrokerOptions` or `credentials` setters are not fully instantiated until `.resolve(**context)` is called. This allows the configuration to bind to the actual `broker` instance or runtime secrets that shouldn't be hardcoded in the static config.


