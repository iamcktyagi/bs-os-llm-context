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
- Must resolve to a `dict` containing:
  - `order_id` (str)

### B. `cancel_order`
- Must resolve to a `dict` containing:
  - `order_id` (str)

### C. `get_orders`
- Must resolve to either:
  - `list[dict]` (preferred), or
  - `dict` (will be wrapped into a single-item list)

### D. `get_account`
- Must resolve to either:
  - `list[dict]` (preferred), or
  - `dict` (will be wrapped into a single-item list)

### E. Historical data: `get_history` / `get_history_multi`
- `get_history` must resolve to a `pandas.DataFrame`
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

## 5) Pagination, Errors, and Hooks (Recommended)

### Pagination
If the broker uses pagination, configure:
- `request.next_page_token` (where to send the token: query/body)
- `response.next_page_token` (how to extract the next token)

### Error Handling
If the broker wraps errors in an envelope (common pattern), define:
- Global `API_SPEC["errors"]` and/or per-endpoint `errors` rules
- Optional endpoint hooks:
  - `hooks.before_send`: mutate url/headers/body/json before request
  - `hooks.after_response`: normalize response payload and status before parsing/error rules

## 6) Asset Universe / Master Data (Usually Required for Live Trading)

If the broker requires symbol/id lookup (security_id, broker_symbol, exchange, etc.), configure `API_SPEC["master_data"]` to fetch and parse instrument master data. This is what powers consistent `Asset` creation and broker symbol mapping.

If you skip master data, you typically must rely on user-provided symbols and accept limitations (no security_id mapping, weaker validation, harder streaming routing).

