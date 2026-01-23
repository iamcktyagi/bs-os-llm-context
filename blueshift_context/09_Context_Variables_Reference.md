# 09 - Expression Context Variables Reference

Every `source` expression in the configuration is evaluated with a specific set of context variables. This document is the **authoritative reference** for what variables are available in each location.

## How Expressions Work

All `"source": "..."` strings are compiled and evaluated as Python expressions. The variables listed below are injected into the expression's local scope at evaluation time.

---

## 1. API Request Context

These variables are available in ALL endpoint `request` field expressions (headers, query, body, json):

| Variable | Type | Description |
|----------|------|-------------|
| `credentials` | `APICredentials` | The broker's credential store. Access fields via `credentials.field_name` |
| `mappings` | `Mappings` | Enum mappings object. Use `mappings.order_side.from_blueshift(val)` to convert |
| `config` | `APIBrokerConfig` | Full broker configuration object |
| `broker` | `RestAPIBroker` | The broker instance |
| `api` | `BrokerAPIMixin` | The REST API handler |
| `endpoint` | `str` | The endpoint name being called |

### Per-Endpoint Additional Variables

| Endpoint | Extra Variables | Types |
|----------|----------------|-------|
| `place_order` | `order` | `Order` — has `.asset`, `.quantity`, `.price`, `.side`, `.order_type`, `.order_validity`, `.product_type`, `.oid` |
| `cancel_order` | `order` | `Order` — use `.broker_order_id` or `.oid` for the ID |
| `update_order` | `order`, `quantity`, `price` | `Order`, `float|None`, `float|None` |
| `get_order` | `order_id` | `str` |
| `get_history` | `asset`, `freq`, `from_dt`, `to_dt`, `nbars` | `MarketData`, `Frequency`, `pd.Timestamp`, `pd.Timestamp`, `int` |
| `get_history_multi` | `asset` (list), `freq`, `from_dt`, `to_dt`, `nbars` | `list[MarketData]`, `Frequency`, `pd.Timestamp`, `pd.Timestamp`, `int` |
| `get_quote` | `asset` | `Asset` |
| `get_orders` | *(none extra)* | |
| `get_positions` | *(none extra)* | |
| `get_account` | *(none extra)* | |
| `session` | *(none extra)* | |

---

## 2. API Response Context

After the HTTP call completes, the response is parsed. The context depends on `payload_type`.

### Common Response Variables (available in all response expressions)

| Variable | Type | Description |
|----------|------|-------------|
| `response` | `dict` | The parsed JSON response body |
| `text` | `str` | Raw text response (if response wasn't JSON) |
| `success` | `bool` | Whether HTTP status was 2xx |
| `status_code` | `int` | HTTP status code |
| `credentials` | `APICredentials` | Credential store |
| `mappings` | `Mappings` | Enum mappings |
| `broker` | `RestAPIBroker` | Broker instance |

### `payload_type: "object"` — Result Field Expressions

| Variable | Type | Description |
|----------|------|-------------|
| `result` | `dict` | The extracted payload (= `response` if no `payload_path`, else the resolved path) |

Example:
```python
"response": {
    "payload_type": "object",
    "payload_path": "response.get('data', {})",
    "result": {
        "fields": {
            "order_id": {"source": "str(result['id'])"},
            "status": {"source": "result.get('status', 'unknown')"}
        }
    }
}
```

### `payload_type: "array"` — Item Field Expressions

| Variable | Type | Description |
|----------|------|-------------|
| `item` | `dict` | The current element in the response array |

Example:
```python
"response": {
    "payload_type": "array",
    "payload_path": "response.get('orders', [])",
    "items": {
        "fields": {
            "oid": {"source": "item['order_id']"},
            "quantity": {"source": "float(item['qty'])"},
            "status": {"source": "mappings.order_status.to_blueshift(item['status'])"}
        }
    }
}
```

### `payload_type: "data"` — OHLCV Data Expressions

The `payload_path` expression extracts raw data. The `data` block controls DataFrame construction:

| Field | Description |
|-------|-------------|
| `frame` | `"records"` (list of dicts), `"dict_of_arrays"` (dict of lists), `"values"` (list of lists) |
| `rename` | `{target_column: source_column}` mapping. Keys = standard names (`open`, `high`, `low`, `close`, `volume`), Values = API field names |
| `timestamp.field` | Column name containing timestamps |
| `timestamp.format` | strftime format string for string timestamps (e.g. `"%Y-%m-%dT%H:%M:%S"`) |
| `timestamp.unit` | For numeric timestamps: `"s"`, `"ms"`, `"us"`, `"ns"` |
| `symbol` | For multi-asset responses: `"key"` means top-level dict keys are symbols |

### `payload_type: "quote"` — Quote Response

| Variable | Type | Description |
|----------|------|-------------|
| `quote` | `dict` | The extracted quote payload |

Available quote fields: `timestamp`, `last`, `close`, `open`, `high`, `low`, `bid`, `bid_volume`, `ask`, `ask_volume`, `upper_circuit`, `lower_circuit`, `market_depth`.

Example:
```python
"response": {
    "payload_type": "quote",
    "payload_path": "response['data']",
    "quote": {
        "last": {"source": "float(quote['ltp'])"},
        "bid": {"source": "float(quote['best_bid'])"},
        "ask": {"source": "float(quote['best_ask'])"}
    }
}
```

### `payload_type: "custom"` — Fully Custom Response

| Variable | Type | Description |
|----------|------|-------------|
| All common response vars | | |

The `custom` callable receives the full context and must return the final result directly.

---

## 3. Hooks Context

### `before_send` Hook

Called after request resolution, before HTTP call. Receives:

| Variable | Type | Description |
|----------|------|-------------|
| `url` | `str` | Resolved URL |
| `verb` | `str` | HTTP method |
| `headers` | `dict` | Request headers |
| `query` | `dict` | Query parameters |
| `body` | `dict` | Request body |
| `json` | `dict` | JSON parameters |
| + all common request context | | |

**Must return:** `(url, verb, headers, params, body, json_params)` as a tuple.

### `after_response` Hook

Called after HTTP response, before error handling. Receives:

| Variable | Type | Description |
|----------|------|-------------|
| `success` | `bool` | HTTP success |
| `status_code` | `int` | HTTP status |
| `response` | `dict` | Parsed response |
| `text` | `str` | Raw text |
| + all common request context | | |

**Returns nothing.** Modifies context dict in-place (e.g., updating `credentials`).

---

## 4. Error Handling Context

Error `condition` expressions receive:

| Variable | Type | Description |
|----------|------|-------------|
| `success` | `bool` | Whether HTTP status was 2xx |
| `status_code` | `int` | HTTP status code |
| `response` | `dict` | Parsed response body |
| `text` | `str` | Raw text response |

---

## 5. OBJECTS_SPEC Pipeline Context

Object conversion pipelines (for `order`, `position`, `account`, `asset`) receive:

| Variable | Type | Description |
|----------|------|-------------|
| `data` | `dict` | The raw data dict from the API response |
| `asset` | `Asset` | *(order/position only)* The inferred asset |

The pipeline extracts `symbol` and `security_id` from `data` before passing to rules. Asset is inferred from these if not provided.

---

## 6. Master Data Context

### Non-Vectorized Mode (row-by-row)

| Variable | Type | Description |
|----------|------|-------------|
| `row` / `item` | `dict` | Current row from the data file |

### Vectorized Mode (pandas)

| Variable | Type | Description |
|----------|------|-------------|
| `data` | `pd.DataFrame` | The full DataFrame (for filter expressions and mapping expressions) |

Filter expressions must return a boolean Series. Mapping expressions must return a Series.

---

## 7. Streaming Context

### Subscribe/Unsubscribe Message Expressions

| Variable | Type | Description |
|----------|------|-------------|
| `subscribe_assets` | `list[Asset]` | Assets to subscribe/unsubscribe |
| `credentials` | `APICredentials` | Credential store |
| `broker` | `RestAPIBroker` | Broker instance |
| `mappings` | `Mappings` | Enum mappings |

### Parser `payload_path` Expression

| Variable | Type | Description |
|----------|------|-------------|
| `data` | `dict` | The parsed (JSON-decoded) message |

### Router `match` Expressions

| Variable | Type | Description |
|----------|------|-------------|
| `data` | `dict\|list\|str` | The parsed and extracted message |

### Converter Field Expressions

| Variable | Type | Description |
|----------|------|-------------|
| `data` | `dict` | The routed message payload |
| `broker` | `RestAPIBroker` | Broker instance (use `broker.infer_asset(security_id=..., symbol=..., broker_symbol=...)`) |
| `credentials` | `APICredentials` | Credential store |
| `mappings` | `Mappings` | Enum mappings |
| `pd` | module | pandas module (for `pd.Timestamp`, etc.) |

---

## 8. Credential Expressions

### `credentials.validator` Expression

| Variable | Type | Description |
|----------|------|-------------|
| `credentials` | `APICredentials` | The credential object itself |
| `api` | `BrokerAPIMixin` | The API handler |

Must evaluate to a truthy value for validation to pass.

### `credentials.setters` Field Expressions

| Variable | Type | Description |
|----------|------|-------------|
| `credentials` | `APICredentials` | Current credential state |
| + any kwargs passed to `resolve()` | | |

---

## 9. Mappings Usage Reference

The `mappings` object provides bidirectional enum conversion:

```python
# Blueshift enum -> Broker string
mappings.order_side.from_blueshift(OrderSide.BUY)       # e.g. "buy"
mappings.order_type.from_blueshift(OrderType.LIMIT)     # e.g. "LIMIT"
mappings.order_status.from_blueshift(OrderStatus.OPEN)  # e.g. "open"

# Broker string -> Blueshift enum
mappings.order_side.to_blueshift("buy")                 # OrderSide.BUY
mappings.order_status.to_blueshift("Executed")          # OrderStatus.COMPLETE
mappings.data_frequency.from_blueshift(Frequency('1m')) # e.g. "1Min"
```

Available mapping attributes: `order_side`, `order_type`, `order_validity`, `order_status`, `product_type`, `option_type`, `expiry_type`, `data_frequency`.

---

## 10. Credentials Dynamic Update

To store session tokens or derived values during authentication:

```python
# In a custom function (e.g., session hook):
credentials.update(api_token=token, user_id=user_id)

# Later, access via:
credentials.api_token  # returns the stored value
```

Fields must be declared in `BROKER_SPEC.credentials.fields` to be accessible.
