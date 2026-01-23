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
*   `OrderType.STOPLOSS`
*   `OrderType.STOPLOSS_MARKET`

### `OrderValidity`
*   `OrderValidity.DAY`
*   `OrderValidity.IOC` (Immediate or Cancel)
*   `OrderValidity.FOK` (Fill or Kill)

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

### `Order`
When placing an order (`place_order` endpoint), the `order` variable is available:
*   `asset` (Asset): The asset being traded.
*   `quantity` (float/int).
*   `price` (float): Limit price (for Limit orders).
*   `trigger_price` (float): Stop price.
*   `side` (OrderSide).
*   `order_type` (OrderType).
*   `product_type` (ProductType).
*   `order_validity` (OrderValidity).
*   `placed_by` (str): ID of the algo placing the order.

## 4. Frequency
Used in `get_history` to map interval strings.
*   `Frequency('1m')` -> 1 Minute
*   `Frequency('5m')` -> 5 Minutes
*   `Frequency('1h')` -> 1 Hour
*   `Frequency('1d')` -> Daily
