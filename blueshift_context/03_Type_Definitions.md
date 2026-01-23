# Type Definitions and Imports

This file serves as a reference for all the Types, Enums, and Classes you will need to import and use in your broker integration.

## 1. Standard Imports
Use this block at the top of your file:

```python
from __future__ import annotations
import pandas as pd
import json
import hashlib
import hmac
import base64

# Blueshift Core Imports
from blueshift.interfaces.assets._assets import InstrumentType, OptionType
from blueshift.lib.common.constants import Frequency
from blueshift.lib.common.functions import merge_json_recursive, to_title_case
from blueshift.lib.trades._order_types import (
    OrderSide, ProductType, OrderType, OrderValidity, OrderStatus)
from blueshift.calendar import get_calendar

# Blueshift Broker Framework Imports
from blueshift.brokers.core.config.config import APIBrokerConfig
from blueshift.brokers.core.broker import RestAPIBroker
from blueshift.brokers.core.config.resolver import ConfigRegistry
from blueshift.brokers.factory import broker_class_factory
```

## 2. Enumerations (Enums)
You must map the broker's API strings to these Blueshift Enums in `BROKER_SPEC`.

### `OrderSide`
*   `OrderSide.BUY`
*   `OrderSide.SELL`

### `OrderType`
*   `OrderType.MARKET`
*   `OrderType.LIMIT`
*   `OrderType.STOP` (Stop Loss Limit)
*   `OrderType.STOP_LIMIT` (Stop Limit)
*   `OrderType.STOPLOSS` (alias for STOP)
*   `OrderType.STOPLOSS_MARKET`

### `OrderValidity`
*   `OrderValidity.DAY`
*   `OrderValidity.IOC` (Immediate or Cancel)
*   `OrderValidity.FOK` (Fill or Kill)
*   `OrderValidity.GTC` (Good Till Cancelled)

### `ProductType`
*   `ProductType.INTRADAY` (MIS/Margin)
*   `ProductType.DELIVERY` (CNC/Cash)
*   `ProductType.MARGIN`

### `OrderStatus`
*   `OrderStatus.COMPLETE` (Filled)
*   `OrderStatus.OPEN` (New, Working)
*   `OrderStatus.REJECTED`
*   `OrderStatus.CANCELLED`

### `InstrumentType`
*   `InstrumentType.SPOT` (Equity)
*   `InstrumentType.FUTURES`
*   `InstrumentType.OPT` (Options)
*   `InstrumentType.FUNDS` (ETF)

### `OptionType`
*   `OptionType.CALL`
*   `OptionType.PUT`

## 3. Core Objects
These are the objects available in the context (e.g., as `order` or `asset`).

### `Asset` / `MarketData`
When you see `asset` in an expression (e.g., `order.asset`), it has these attributes:
*   `symbol` (str): The Blueshift symbol.
*   `broker_symbol` (str): The symbol used by the broker (if mapped).
*   `security_id` (str): The broker's internal ID (if mapped).
*   `exchange_name` (str): e.g., 'NSE', 'NYSE'.
*   `name` (str): Full name.
*   `tick_size` (float).
*   `mult` (float): Lot size / Multiplier.
*   `instrument_type` (InstrumentType).

### `Order` (Request Context)
When placing/modifying an order, the `order` variable is available with these attributes:
*   `asset` (Asset): The asset being traded
*   `quantity` (float): Order quantity
*   `price` (float): Limit price (for Limit orders)
*   `trigger_price` (float): Stop/trigger price
*   `side` (OrderSide): Buy or Sell
*   `order_type` (OrderType): Market, Limit, etc.
*   `product_type` (ProductType): Delivery, Margin, etc.
*   `order_validity` (OrderValidity): DAY, IOC, GTC, etc.
*   `oid` (str): Internal order ID
*   `broker_order_id` (str): Broker-assigned ID (for cancel/update)
*   `placed_by` (str): ID of the algo placing the order

### `Order` (Response/OBJECTS_SPEC - Required Fields for `Order.from_dict()`)
When the framework constructs an Order from API response data, these fields are used:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `oid` | str | Yes | Unique order identifier |
| `symbol` or `security_id` | str | Yes* | For asset inference (one required) |
| `quantity` | float | Yes | Order quantity |
| `filled` | float | Yes | Filled quantity |
| `price` | float | Yes | Limit/order price |
| `status` | OrderStatus | Yes | Current order status |
| `side` | OrderSide | Recommended | Buy/Sell |
| `order_type` | OrderType | Recommended | Market/Limit/etc. |
| `average_price` | float | Recommended | Average fill price |
| `timestamp` | pd.Timestamp | Recommended | Order creation time |
| `exchange_timestamp` | pd.Timestamp | Optional | Exchange fill time |
| `broker_order_id` | str | Optional | Broker's own order ID |

*If `asset` is directly provided (e.g., via `broker.infer_asset()`), `symbol`/`security_id` is not needed.

### `Position` (Required Fields for Position Creation)
When the framework constructs a Position from API response data:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `quantity` | float | **Yes** | Net quantity (negative = short) |
| `average_price` | float | **Yes** | Average entry price |
| `last_price` | float | **Yes** | Current market price |
| `symbol` or `security_id` | str | Yes* | For asset inference |
| `margin` | float | Optional | Margin used (default 0) |
| `product_type` | ProductType | Optional | Delivery/Margin (default: broker's first) |
| `timestamp` | pd.Timestamp | Optional | Position open time |

### `Account` (Required Fields)
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | str | Yes | Account identifier |
| `cash` | float | Yes | Available cash balance |
| `currency` | str | Yes | Currency code (e.g., "USD", "INR") |

### `Asset` — Derivatives Attributes
For futures/options, assets have additional attributes:
*   `is_opt()` → bool: True if option
*   `is_futures()` → bool: True if futures
*   `expiry_date` (pd.Timestamp): Contract expiry
*   `strike` (float): Strike price (options only)
*   `option_type` (OptionType): CALL or PUT (options only)
*   `underlying` (str): Underlying symbol
*   `root` (str): Root symbol
*   `mult` (int): Contract multiplier/lot size
*   `exchange_name` (str): Derivatives exchange (e.g., "NFO")

## 4. Frequency
Used in `get_history` to map interval strings.
*   `Frequency('1m')` → 1 Minute
*   `Frequency('5m')` → 5 Minutes
*   `Frequency('15m')` → 15 Minutes
*   `Frequency('30m')` → 30 Minutes
*   `Frequency('1h')` → 1 Hour
*   `Frequency('1d')` → Daily

## 5. Credentials Object

The `credentials` object stores broker authentication fields declared in `BROKER_SPEC.credentials.fields`.

### Accessing Fields
```python
credentials.api_key       # Direct attribute access
credentials.api_secret
credentials.access_token  # Returns None if not set
```

### Updating Credentials Dynamically
Use `credentials.update()` to store session tokens or derived values at runtime:
```python
# In a custom function (e.g., session hook):
credentials.update(access_token=token, user_id=uid, session_key=key)
```
All fields passed to `update()` must be declared in `BROKER_SPEC.credentials.fields`.

## 6. Mappings Object

The `mappings` object provides bidirectional enum conversion between Blueshift enums and broker strings:

```python
# Blueshift enum -> Broker string (used in requests)
mappings.order_side.from_blueshift(OrderSide.BUY)          # → "buy"
mappings.order_type.from_blueshift(OrderType.LIMIT)        # → "limit"
mappings.order_validity.from_blueshift(OrderValidity.DAY)  # → "day"
mappings.order_status.from_blueshift(OrderStatus.OPEN)     # → "new"
mappings.product_type.from_blueshift(ProductType.DELIVERY) # → "cash"

# Broker string -> Blueshift enum (used in responses)
mappings.order_side.to_blueshift("buy")         # → OrderSide.BUY
mappings.order_status.to_blueshift("filled")    # → OrderStatus.COMPLETE
mappings.order_status.to_blueshift("canceled")  # → OrderStatus.CANCELLED
```

### How Multi-Value Mappings Work
When `BROKER_SPEC` maps one enum to multiple broker strings:
```python
OrderStatus.OPEN: ["new", "partially_filled", "pending_new"]
```
- `from_blueshift(OrderStatus.OPEN)` → returns the **first** value (`"new"`)
- `to_blueshift("partially_filled")` → returns `OrderStatus.OPEN` (any value in the list matches)

### Default Values
If `default_value` is set in the mapping spec and no match is found:
- `to_blueshift("unknown_status")` → returns the `default_value` instead of raising an error

## 7. Sentinel Functions

*   `noop` — A no-op callable. Import from `blueshift.lib.common.sentinels`. Use as `"custom": "noop"` when you need a placeholder that does nothing (e.g., for session endpoints where the custom request already handled everything).

## 8. Exception Classes

Available for use in error handling `"exception"` fields:

| Exception | Use Case |
|-----------|----------|
| `AuthenticationError` | Invalid credentials, expired tokens |
| `APIException` | Generic API errors (rate limits, bad requests) |
| `APIError` | API response errors |
| `ServerError` | 5xx server errors |
| `OrderError` | Order-specific failures (insufficient funds, invalid params) |
| `ValidationError` | Configuration or input validation errors |
| `BrokerError` | General broker errors |
| `SymbolNotFound` | Asset lookup failures |

Import from: `blueshift.lib.exceptions`
