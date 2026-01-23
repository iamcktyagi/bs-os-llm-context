# Blueshift Calendar

The `blueshift.calendar` sub-package is responsible for managing trading calendars, which define the trading sessions, business days, holidays, and hours of operation for various financial exchanges. It serves as the temporal backbone for the Blueshift platform, ensuring that all trading logic, backtesting, and data processing align with the actual market schedule.

## Responsibility

- **Session Management**: Defines when markets open and close.
- **Holiday Handling**: Tracks market holidays to skip non-trading days.
- **Timezone Management**: Ensures all time-related operations are timezone-consistent.
- **Derivative Expiries**: Provides advanced utilities to calculate expiration dates for futures and options, supporting general, asset-specific, and exchange-specific expiry rules.

## Relationship to Other Sub-packages

- **`blueshift.core`**: The `AlgoContext` uses the `TradingCalendar` to schedule events (e.g., `schedule_function`) and check market status (`is_market_open`).
- **`blueshift.lib.clocks`**: The simulation engine's clock uses the calendar to generate `BAR` and `SESSION` events, ensuring the backtester skips holidays and weekends.
- **`blueshift.providers`**: Data sources and brokers rely on the calendar to validate request dates and align historical data queries.

## Concepts and Terminology

- **`TradingCalendar`**: The core class representing an exchange's schedule. It holds properties like `open_time`, `close_time`, `tz` (timezone), and methods to query the schedule.
- **Registry**: A global `CalendarDispatch` system that allows retrieving calendars by name (e.g., 'NYSE', 'NSE') and registering custom ones.
- **Session**: Represents a single trading day. In Blueshift, a session is usually identified by the date (normalized to midnight in the calendar's timezone).
- **Expiries**: Specific dates when derivative contracts settle. The package includes robust logic to calculate these based on rules (e.g., "3rd Thursday of the month"), supporting dynamic parameterization and asset-specific overrides.
- **`DateChunker`**: A utility for segmenting time series data into chunks (e.g., by year, quarter, month) for efficient storage and retrieval.
- **`ExpiryChunker`**: A specialized chunker that manages and calculates derivative expiry dates based on specified frequency, weekday rules, roll days, and dynamic parameters.
- **`ExpiryDispatch`**: A central registry and dispatcher for `ExpiryChunker` instances, allowing for the retrieval of appropriate expiry rules based on the underlying asset and expiry frequency.

## Common Usage Patterns

### Getting a Calendar

You typically retrieve a calendar instance using the `get_calendar` function.

```python
from blueshift.calendar import get_calendar

# Get the National Stock Exchange of India calendar
nse_cal = get_calendar('NSE')

# Get the New York Stock Exchange calendar
nyse_cal = get_calendar('NYSE')
```

### Checking Market Status

Check if a specific time falls within a trading session or if the market is open.

```python
import pandas as pd

# Define a timezone-aware timestamp
dt = pd.Timestamp('2023-10-27 10:00:00', tz='Asia/Calcutta')

# Check if the date is a valid trading session (ignoring time)
is_session = nse_cal.is_session(dt)

# Check if the market is currently open at this exact time
is_open = nse_cal.is_open(dt)

# Check if a date is a holiday
is_holiday = nse_cal.is_holiday('2023-10-24') # Dussehra
```

### Navigating Time

Move between market events relative to a given time.

```python
# Get the opening time of the next session
next_open = nse_cal.next_open(dt)

# Get the closing time of the current or previous session
prev_close = nse_cal.previous_close(dt)

# Get the last N trading minutes
last_5_mins = nse_cal.last_n_minutes(dt, 5)
```

### Calculating Expiries

Calculate derivative expiry dates, handling weekends and holidays automatically. The `get_expiries` function now supports dynamic expiry parameters.

```python
from blueshift.calendar import get_expiries

# Get weekly expiries (e.g., Thursdays) for a date range
# If Thursday is a holiday, it bumps to the previous business day.
expiries = get_expiries(
    nse_cal, 
    start_dt='2023-01-01', 
    end_dt='2023-03-31', 
    frequency='W', 
    weekday=3 # Thursday (0=Monday)
)

# Example with dynamic expiry parameters (e.g., change from Thursday to Wednesday after a date)
banknifty_expiry_params = {
    '2023-09-03': {'expiry_day': 3} # Effective until Sep 3, 2023, expiry is Wednesday (3)
}
expiries_banknifty = get_expiries(
    nse_cal, 
    start_dt='2023-08-01', 
    end_dt='2023-09-30', 
    frequency='W', 
    weekday=3, # Default: Thursday
    expiry_params=banknifty_expiry_params
)
```

### Using ExpiryDispatch for Asset-Specific Rules

To manage complex expiry rules for different underlying assets or frequencies, you can configure and use an `ExpiryDispatch` instance.

```python
from blueshift.calendar import ExpiryChunker, ExpiryDispatch
import pandas as pd

# Define a custom ExpiryChunker for a specific asset
nifty_weekly_chunker = ExpiryChunker(
    name='NIFTY_WEEKLY',
    frequency='W',
    expiry_day=3, # Thursday
    calendar=nse_cal # Assuming nse_cal is already defined
)

# Define another chunker, e.g., for BankNifty with different rules
banknifty_weekly_chunker = ExpiryChunker(
    name='BANKNIFTY_WEEKLY',
    frequency='W',
    expiry_day=2, # Wednesday
    calendar=nse_cal
)

# Create an ExpiryDispatch
expiry_dispatcher = ExpiryDispatch(
    chunkers={
        nifty_weekly_chunker.name: nifty_weekly_chunker,
        banknifty_weekly_chunker.name: banknifty_weekly_chunker
    },
    asset_dispatch={
        'NIFTY': {'W': nifty_weekly_chunker},
        'BANKNIFTY': {'W': banknifty_weekly_chunker}
    },
    default_dispatch={'W': nifty_weekly_chunker} # Default weekly is Nifty's
)

# Get the chunker for Nifty weekly options
nifty_chunker = expiry_dispatcher.get('NIFTY', frequency='W')
if nifty_chunker:
    nifty_expiries = nifty_chunker.expiries_between('2023-01-01', '2023-03-31')

# Get the chunker for BankNifty weekly options
banknifty_chunker = expiry_dispatcher.get('BANKNIFTY', frequency='W')
if banknifty_chunker:
    banknifty_expiries = banknifty_chunker.expiries_between('2023-01-01', '2023-03-31')
```

## Typical Configuration

Calendars are pre-configured for major supported exchanges. You can view available calendars using `list_calendars`.

```python
from blueshift.calendar import list_calendars

print(list_calendars())
# Output: ['NYSE', 'NSE', 'BSE', 'FX', 'CRYPTO', ...]
```

To register a custom calendar or an alias:

### Creating and Registering Custom Calendars

You can create a custom `TradingCalendar` if the built-in ones don't meet your needs. You can either pass the parameters directly to `register_calendar` or instantiate a `TradingCalendar` object first.

```python
from blueshift.calendar import register_calendar, TradingCalendar
from datetime import time

# Option 1: Register directly with parameters
# This creates a calendar named 'MY_EXCHANGE' with custom hours and timezone
register_calendar(
    'MY_EXCHANGE',
    tz='Asia/Tokyo',
    opens=(9, 0, 0),    # 9:00 AM
    closes=(15, 0, 0),  # 3:00 PM
    weekends=[5, 6]     # Saturday (5), Sunday (6)
)

# Option 2: Create instance and then register
custom_cal = TradingCalendar(
    name='SPECIAL_MARKET',
    tz='US/Eastern',
    opens=time(9, 30),
    closes=time(16, 0),
    weekends=[5, 6]
)
register_calendar('SPECIAL_MARKET', custom_cal)
```

You can also register a simple alias for an existing calendar:

```python
from blueshift.calendar import register_calendar_alias

# Create an alias 'Nifty' for the existing 'NSE' calendar
register_calendar_alias('NSE', 'Nifty')
```

For extending holidays, some built-in calendars (like 'NSE') look for CSV files (e.g., `nse_holidays.csv`) in the `BLUESHIFT_HOME` directory.

## Gotchas and Limitations

1.  **Timezone Strictness**: The `TradingCalendar` is strictly timezone-aware. Passing naive datetime objects can lead to `ValueError` or unexpected behavior. Always use timezone-aware timestamps or helper functions like `make_consistent_tz`.
    ```python
    # BAD
    dt = pd.Timestamp('2023-01-01 10:00')
    
    # GOOD
    dt = pd.Timestamp('2023-01-01 10:00', tz='Asia/Calcutta')
    ```
2.  **Session Boundaries**: The current model assumes a simple open and close time. Markets with complex breaks (like a lunch break) are modeled as a single continuous session for start/end purposes, though specific logic might be needed if intraday breaks are critical for your use case.
3.  **Global Registry**: Calendars are singletons within the registry. Modifying a calendar instance retrieved via `get_calendar` affects the global state for that calendar name.
