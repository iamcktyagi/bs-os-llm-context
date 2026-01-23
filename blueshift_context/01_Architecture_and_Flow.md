# Architecture and Data Flow

The Blueshift Broker Framework is **Configuration-Driven**. This means the behavior of the broker is defined primarily by data (JSON-like dictionaries), not by writing boilerplate code.

## Core Concepts

### 1. The Configuration Dictionary
The integration is defined by a set of nested dictionaries:
*   **`BROKER_SPEC`**: Defines global options (rate limits, timeouts) and Enum Mappings (e.g., how "BUY" maps to `OrderSide.BUY`).
*   **`API_SPEC`**: Defines the REST endpoints. It maps abstract actions (e.g., `place_order`) to concrete HTTP requests (URL, Method, Body).
*   **`STREAMING_SPEC`**: Defines WebSocket connections for real-time data.
*   **`OBJECTS_SPEC`**: Defines how to convert raw API JSON responses into Blueshift objects (`Order`, `Position`, `Account`).

### 2. The Registry (`ConfigRegistry`)
The configuration is static data, but sometimes you need dynamic logic (e.g., generating a signature, parsing a binary WebSocket message).
The **Registry** bridges this gap. You write a standard Python function, decorate it with `@registry.register()`, and then reference it by name in your configuration.

**Example:**
```python
@registry.register()
def my_auth_header(api_key, **kwargs):
    return f"Bearer {api_key}"
```

**Config Usage:**
```python
"headers": {
    "Authorization": {"source": "my_auth_header(credentials.api_key)"}
}
```

There are two common ways to “invoke” registered logic:
*   **Inline via `source`**: Call the function directly inside a `source` expression (as above). This behaves like normal Python evaluation.
*   **Via `custom` callable references**: Many schema sections accept a `custom` field containing the registry function name. In this mode, the framework calls the function with the runtime context as keyword arguments.

### 3. The Expression Engine
The framework evaluates string values in the configuration as Python expressions.
*   **Context**: The expressions have access to a context containing:
    *   `credentials` (your API keys)
    *   `order` (the Order object being placed)
    *   `asset` (the Asset object)
    *   any functions registered into `ConfigRegistry` (by name)
*   **Syntax**: Standard Python syntax.
    *   `"source": "credentials.api_key"` -> Accesses the API key.
    *   `"source": "'active'"` -> Returns the string "active".
    *   `"source": "100 * float(data['price'])"` -> Math operations.

Streaming note:
*   Streaming message definitions support JSON messages built from `fields_group` (same `source` rules), and text messages built using Python `str.format(...)` templates (not `{{ }}` templating).

## Data Flow

### 1. Request Flow (e.g., Placing an Order)
1.  **Trigger**: Algorithm calls `order(asset, 10)`.
2.  **Lookup**: Framework finds the `place_order` definition in `API_SPEC`.
3.  **Resolution**: Framework evaluates the `request` fields:
    *   Resolves `url` (e.g., `/v2/orders`).
    *   Resolves `headers` (e.g., adds Auth).
    *   Resolves `body` (maps `order.quantity` to the API's expected field `qty`).
4.  **Execution**: Sends the HTTP POST request.

### 2. Response Flow
1.  **Receive**: Framework gets the JSON response.
2.  **Parse**: Framework finds the `response` definition in `API_SPEC`.
3.  **Extraction**: It extracts the `order_id` using the defined path (e.g., `result['id']`).
4.  **Return**: The Order ID is returned to the algorithm.

### 3. Streaming Flow
1.  **Connect**: Framework connects to the WebSocket URL defined in `STREAMING_SPEC`.
2.  **Auth**: Sends the `auth` message (if defined).
3.  **Subscribe**: Sends `subscribe` messages for assets.
4.  **Stream**:
    *   Incoming messages are passed to the `parser`.
    *   The `router` decides if it's a `data` message (price update) or `order` message (execution report).
    *   The `converters` map the raw message to a `MarketData` or `Order` update.
