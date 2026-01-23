# Blueshift Core

This sub-package contains the core components that orchestrate and manage the lifecycle and execution of trading algorithms within the Blueshift platform. It provides the fundamental infrastructure for defining algorithms, handling their state, managing execution environments, and enforcing risk controls.

## Responsibility

-   **Algorithm Lifecycle Management**: Manages the complete execution flow of a trading algorithm, from initialization to completion, including handling various events and transitions between trading states.
-   **Execution Environment Setup**: Configures and links all necessary infrastructure components like brokers, data feeds, trading calendars, and logging.
-   **Algorithm State Management**: Provides the `AlgoContext` as the central interface for algorithms to access their environment, market data, account information, and positions.
-   **Risk Management**: Integrates pre-trade controls and continuous monitoring to enforce trading limits and policies.
-   **Error Handling and Communication**: Centralizes error management, logging, and communication of algorithm status and events to external systems or users.
-   **Strategy Implementation Framework**: Offers a base `Strategy` class that users can extend to implement their trading logic in an event-driven manner.

## Relationship to Other Sub-packages

-   **`blueshift.calendar`**: `blueshift.core` extensively uses the `TradingCalendar` to define the trading schedule, manage sessions, and ensure all time-based operations are market-aware and consistent. For instance, the `BlueshiftEnvironment` initializes the trading calendar, and `TradingAlgorithm` dispatches events based on its schedule.
-   **`blueshift.providers`**: `blueshift.core` dynamically integrates with various `blueshift.providers` (e.g., `broker`, `data`, `streaming`) to connect to external systems for order execution, market data retrieval, and real-time event updates. The `BlueshiftEnvironment` is responsible for instantiating the correct broker and data portal based on configuration.
-   **`blueshift.lib`**: `blueshift.core` leverages common utilities, exceptions, and data structures defined in `blueshift.lib` (e.g., `blueshift.lib.common`, `blueshift.lib.exceptions`) for internal operations, cross-cutting concerns, and robust error handling.

## Concepts and Terminology

-   **`TradingAlgorithm`**: The overarching class that encapsulates the entire trading algorithm. It manages the algorithm's lifecycle, dispatches events to user-defined handlers (or `Strategy` methods), and coordinates with other core components.
-   **`AlgoContext`**: The primary interface object provided to the user's algorithm. It offers access to market data (`data_portal`), order placement (`blotter`), account details (`account`), portfolio (`portfolio`), and the `trading_calendar`. It also serves as a container for user-defined variables.
-   **`BlueshiftEnvironment`**: The builder and container class responsible for setting up the entire runtime environment for an algorithm. This includes initializing the broker, clock, logger, and loading configurations.
-   **`Strategy`**: An optional base class for user-defined trading logic. Users can subclass `Strategy` to implement their algorithm's behavior through a set of predefined lifecycle methods (e.g., `initialize`, `handle_data`). This is especially useful for modular or multi-strategy setups.
-   **`TradingControl`**: Abstract base class for implementing pre-trade risk checks and limits. Concrete implementations include controls for order quantity, trade value, exposure, leverage, and whitelisting/blacklisting of assets.
-   **`RiskMonitor`**: A component that continuously monitors the algorithm's performance, positions, and compliance with risk management policies (e.g., loss-based "kill switch"). It operates primarily in live and paper trading modes.
-   **`BlueshiftAlertManager`**: A centralized system for managing and handling errors, logging messages, publishing algorithm status updates, and facilitating communication with external services or a Blueshift server.
-   **Algo Lifecycle Hooks**: A set of predefined methods in `TradingAlgorithm` (and mirrored in the `Strategy` class) that are called at specific points in the algorithm's execution (e.g., `initialize`, `before_trading_start`, `handle_data`, `after_trading_hours`, `analyze`).
-   **Execution Modes**: The various operational modes for running an algorithm:
    -   `BACKTEST`: Historical simulation.
    -   `PAPER`: Simulation of live trading without real money.
    -   `LIVE`: Real-money trading on an exchange.
    -   `EXECUTION`: For smart orders, often a headless execution service.

## Common Usage Patterns

This section focuses on how to instantiate, configure, and run a `TradingAlgorithm` within the Blueshift ecosystem.

### Instantiating and Running a `TradingAlgorithm`

The primary way to create and execute a `TradingAlgorithm` is through the `algo_factory` function. This factory handles the internal setup of the `BlueshiftEnvironment` and `AlgoContext` based on the provided parameters.

```python
from blueshift.core import create_alert_manager, algo_factory
from blueshift.core.alerts.signals_handlers import BlueshiftInterruptHandler
from blueshift.lib.common.enums import AlgoMode
import os
import pandas as pd

# --- 1. Define your strategy (e.g., in a separate file) ---
# For this example, we'll create a simple strategy file dynamically.
# In a real scenario, this would be your pre-written strategy.py file.
strategy_code = '''
import pandas as pd
from blueshift.api import symbol, order_target_percent, schedule_function, date_rules, time_rules
from blueshift.types import AlgoContext, DataPortal

def initialize(context: AlgoContext):
    context.log.info("MySimpleStrategy initialized!")
    context.set_benchmark('NIFTY')
    
    # Define assets using the symbol API
    # Ensure these assets are available in your chosen broker/data provider
    context.assets = [symbol('RELIANCE.NSE'), symbol('TCS.NSE')] 
    context.traded_today = False # Flag to ensure trading happens only once per day

    # Schedule our `before_trading_start` logic to run at the very start of each trading day
    schedule_function(
        before_trading_start_cb, # Renamed to avoid confusion with core callback 'before_trading_start'
        date_rules.every_day(),
        time_rules.market_open(minutes=0) # Run exactly at market open
    )

    # Schedule our main `handle_data` trading logic to run at each minute bar during market hours
    schedule_function(
        handle_data_cb, # Renamed to avoid confusion with core callback 'handle_data'
        date_rules.every_day(),
        time_rules.every_nth_minute(n=1) # Run every minute
    )

def handle_data_cb(context: AlgoContext, data: DataPortal):
    # This example simply logs prices and attempts to buy once per day.
    # It runs at each bar, so we use a flag to prevent multiple orders per day.
    if not context.traded_today:
        for asset in context.assets:
            current_price = data.current(asset, 'close')
            if not pd.isna(current_price): # Check for valid price
                context.log.info(f"Current price of {asset.symbol}: {current_price}")
                # If not holding the asset, buy to achieve 50% allocation (divided among assets)
                if asset not in context.portfolio.positions or context.portfolio.positions[asset].quantity == 0:
                    # order_target_percent automatically calculates quantity needed for target allocation
                    order_target_percent(asset, 0.5 / len(context.assets)) 
                    context.log.info(f"Placed order for {asset.symbol} to target 50% allocation.")
        context.traded_today = True # Set flag to prevent further trading today

def before_trading_start_cb(context: AlgoContext, data: DataPortal):
    """
    Called at the start of each trading day, typically before handle_data.
    Useful for resetting daily state or performing daily setup.
    """
    # Reset the flag at the start of each trading day
    context.traded_today = False
    context.log.info("New trading day started. Flag reset.")

def analyze(context: AlgoContext, perf: pd.DataFrame):
    """
    Called once at the very end of the algorithm run.
    """
    context.log.info(f"Algorithm finished. Final PnL: {perf['pnl']:.2f}")
    context.log.info(f"Total trades: {perf['total_trades']}")
'''

# Create a temporary strategy file for the example run
temp_strategy_file = "temp_simple_strategy.py"
with open(temp_strategy_file, "w") as f:
    f.write(strategy_code)

# --- 2. Configure algorithm parameters ---
algo_name_backtest = 'my_backtest_algo'
# Parameters for a backtest run
params_backtest = {
    'algo_file': temp_strategy_file, # Path to your strategy file
    'initial_capital': 100000,
    'mode': AlgoMode.BACKTEST,
    'start_date': '2023-01-01',
    'end_date': '2023-12-31',
    'broker': 'backtest', # must be configured in .blueshift-config.json
    'frequency': 'minute',
    'verbose': True,
}

# Parameters for a live trading run (conceptual example)
# Note: For actual live trading, ensure 'your_live_broker_name' is
# correctly configured in your blueshift_config.json
algo_name_live = 'my_live_algo'
params_live = {
    'algo_file': temp_strategy_file,
    'initial_capital': None,
    'mode': AlgoMode.LIVE,
    'broker': 'your_live_broker_name', # e.g., 'zerodha', 'alpaca' (configured in blueshift_config.json)
    'verbose': True,
}


# --- 3. Create Alert Manager and TradingAlgorithm instances ---
# For backtest execution:
alert_mgr_backtest = create_alert_manager(algo_name_backtest, maxsize=None, **params_backtest)
algo_backtest = algo_factory(algo_name_backtest, **params_backtest)

# For live execution (uncomment and configure 'your_live_broker_name' in blueshift_config.json):
# alert_mgr_live = create_alert_manager(algo_name_live, maxsize=None, **params_live)
# algo_live = algo_factory(algo_name_live, **params_live)


# --- 4. Initialize and Run the Algorithm (Backtest Example) ---
print(f"--- Starting Backtest for {algo_name_backtest} ---")
try:
    alert_mgr_backtest.initialize() # Initialize the Alert Manager first
    with BlueshiftInterruptHandler(alert_mgr_backtest):
        # The .run() method orchestrates the entire algorithm execution.
        # For backtest, it returns a pandas DataFrame of daily performance.
        performance_df = algo_backtest.run()
        print("\nBacktest Performance Head:")
        print(performance_df.head())
        print("\nBacktest Performance Tail:")
        print(performance_df.tail())
except Exception as e:
    print(f"An error occurred during backtest: {e}")
finally:
    alert_mgr_backtest.finalize() # Ensure proper cleanup and final status updates
    # Clean up the temporary strategy file
    if os.path.exists(temp_strategy_file):
        os.remove(temp_strategy_file)
print(f"--- Backtest for {algo_name_backtest} Finished ---")


# --- 5. Running a Live Algorithm (Conceptual Example) ---
# For a live algorithm, algo.run() would typically enter a continuous loop
# that processes real-time data and executes orders. This loop runs until
# explicitly stopped or terminated by the system.
#
# print(f"--- Starting Live Algorithm for {algo_name_live} (Conceptual) ---")
# try:
#     alert_mgr_live.initialize()
#     with BlueshiftInterruptHandler(alert_mgr_live):
#         algo_live.run() # This would run indefinitely, processing real-time events
# except Exception as e:
#     print(f"An error occurred during live trading: {e}")
# finally:
#     alert_mgr_live.finalize()
# print(f"--- Live Algorithm for {algo_name_live} Finished (Conceptual) ---")

```

## Gotchas and Limitations

1.  **Context Mutability**: Many core attributes of the `AlgoContext` (e.g., `broker`, `blotter`, `data_portal`) are read-only to ensure the integrity of the trading environment. Direct attempts to modify these will raise `AttributeError`. User-defined variables on the context are mutable.
2.  **Timezone Consistency**: As with the `blueshift.calendar` package, strict adherence to timezone-aware `pandas.Timestamp` objects is paramount. All time-related inputs and outputs from the core components are expected to be timezone-aware. Mixing naive and aware datetimes can lead to subtle and hard-to-debug errors.
3.  **Error Handling in User Code**: While the `BlueshiftAlertManager` provides a safety net for system-level errors, unhandled exceptions within user-defined strategy code can still lead to algorithm termination. It is highly recommended to implement `on_error` callbacks in your `Strategy` class to manage specific error scenarios gracefully.
4.  **Sub-Context Interaction**: When using `SubContext` for multi-strategy setups, remember that all sub-contexts operate within the same overall `BlueshiftEnvironment` and typically share the same `broker` and `data_portal` resources as the main algorithm. Care must be taken to avoid unintended interactions between strategies or race conditions if not properly synchronized.
5.  **Performance Overhead**: Frequent calls to expensive operations (e.g., complex data queries, extensive custom logging) within high-frequency lifecycle hooks like `handle_data` can impact backtest speed or live trading responsiveness. Be mindful of computational complexity in performance-critical sections.
6.  **`context.record()` Limitations**: The `record()` function is convenient for debugging and post-analysis but has limits on the number of variables and the size/type of data that can be recorded to prevent excessive memory usage or performance degradation. Only float-like values can be recorded for each variable.