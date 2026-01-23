# Blueshift Core

**Blueshift Core** is a comprehensive, event-driven algorithmic trading engine designed for backtesting, paper trading, and live execution of quantitative strategies. It provides a modular framework for managing market data, executing orders, and enforcing risk controls across various asset classes and timeframes.

## Who is this for?

-   **Quantitative Researchers**: To backtest strategies with high fidelity using historical data.
-   **Algo Traders**: To deploying strategies to live markets with robust risk management and execution handling.
-   **Platform Developers**: To build custom trading platforms or broker integrations on top of a proven engine.

## Quickstart

### Installation

Requires Python 3.10 or later.

```bash
pip install blueshift-core
```

### Hello World Strategy

Here is a minimal example of a strategy that buys a stock and holds it, following the standard Blueshift development style.

**1. Create a strategy file (`buy_and_hold.py`):**

```python
from blueshift.api import symbol, order, schedule_function, date_rules, time_rules
from blueshift.types import AlgoContext, DataPortal

def initialize(context: AlgoContext):
    """
    Called once at the start of the algorithm.
    """
    # Define the asset we want to trade using the symbol API
    context.asset = symbol('AAPL') 
    
    # Schedule our trading logic to run once a day at market open
    schedule_function(
        rebalance,
        date_rules.every_day(),
        time_rules.market_open()
    )

def rebalance(context: AlgoContext, data: DataPortal):
    """
    This function is called by the scheduler.
    """
    # Check if we already hold the asset
    if context.asset not in context.portfolio.positions:
        # If not, buy 10 shares using the order API
        order(context.asset, 10)
        print(f"Bought 10 shares of {context.asset.symbol}")
```

**2. Run a backtest:**

You can run this using the `algo_factory`.

```python
from blueshift.core import create_alert_manager, algo_factory
from blueshift.core.alerts.signals_handlers import BlueshiftInterruptHandler
from blueshift.lib.common.enums import AlgoMode

# Configuration for the run
params = {
    'algo_file': 'buy_and_hold.py',
    'initial_capital': 10000,
    'start_date': '2023-01-01',
    'end_date': '2023-01-30',
    'mode': AlgoMode.BACKTEST,
    'broker': 'backtest',  # Use the built-in backtester
    'frequency': 'minute'
}

# Setup the environment
name = 'hello_world_run'
manager = create_alert_manager(name, maxsize=None, **params)
algo = algo_factory(name, **params)

# Execute
manager.initialize()
with BlueshiftInterruptHandler(manager):
    result = algo.run()

print("Backtest complete!")
print(result.tail())
```

### Running from the Command Line

The `blueshift-core` package provides a powerful command-line interface (`CLI`) to execute your algorithmic trading strategies, both in backtesting and live trading modes.

**1. Running a Single Algorithm (using `blueshift-oss run`)**

To quickly run a single algorithm script, use the `blueshift-oss run` command. You'll need to specify your algorithm file (`--algo-file`) and desired parameters.

Let's assume you saved the "Hello World" strategy above as `buy_and_hold.py` in your current directory. You can execute it from your terminal like this:

```bash
blueshift-oss run --algo-file buy_and_hold.py --start-date 2023-01-01 --end-date 2023-01-05 --initial-capital 100000 --run-mode backtest --name "BuyAndHoldBacktest"
```

This command will run your `buy_and_hold.py` for the specified period in backtest mode, producing a performance report.

**2. Starting Blueshift in Server Mode (using `blueshift-oss start`)**

For more advanced use cases, such as sending multiple algorithms to be run concurrently or managing algorithms via a programmatic interface, you can start `blueshift-core` in server mode using `blueshift-oss start`. This sets up a ZMQ queue to receive commands and allows for persistent operation.

**Example: Starting the Blueshift Server**

```bash
blueshift-oss start --name "MyBlueshiftServer" --mode backtest --queue-size 5
```

Once the server is running, you can send algorithm execution commands to it using a ZMQ client. This allows for flexible control and integration with other systems.


## Architecture

Blueshift Core follows an **event-driven architecture**. 

1.  **The Clock**: A central clock (simulated or real-time) emits "ticks" (events) like `BEFORE_TRADING_START`, `TRADING_BAR`, `AFTER_TRADING_HOURS`.
2.  **The Algorithm**: The `TradingAlgorithm` listens for these events and dispatches them to your `Strategy` code (e.g., `handle_data`).
3.  **The Context**: Your strategy interacts with the world via the `AlgoContext` object. It provides access to:
    -   **Data**: Current and historical prices.
    -   **Blotter**: Order placement and position tracking.
    -   **Account**: Capital and leverage information.
4.  **Providers**: Behind the scenes, abstract interfaces connect to concrete providers:
    -   **Broker**: Handles order execution (Backtest vs. Live).
    -   **Data Portal**: Fetches market data.

### Key Sub-packages

-   **[blueshift.core](src/blueshift/core/README.md)**: The heart of the system. Contains `TradingAlgorithm`, `AlgoContext`, and the main execution loops. It manages the lifecycle of your strategy.
-   **[blueshift.interfaces](src/blueshift/interfaces/README.md)**: Defines the abstract contracts (`IBroker`, `DataPortal`, `IBlotter`) that allow the system to be modular. It ensures you can swap a backtester for a live broker without changing your strategy code.
-   **[blueshift.providers](src/blueshift/providers/README.md)**: Concrete implementations of the interfaces. Includes the `BacktestBroker`, `PaperBroker`, data stores, and standard risk management modules.
-   **[blueshift.calendar](src/blueshift/calendar/README.md)**: Manages market sessions, holidays, and timezones. Essential for ensuring your algo only trades when the market is open.
-   **[blueshift.finance](src/blueshift/finance/README.md)**: Contains financial models for transaction costs (commission, tax), slippage, and margin calculations.

## Contribution & Development

We welcome contributions!

1.  **Setup**: Clone the repo and install dependencies.
    ```bash
    git clone https://github.com/blueshift-opensource/blueshift-core.git
    cd blueshift-core
    pip install -e .[dev]
    ```
2.  **Tests**: Run the test suite to ensure everything is working.
    ```bash
    pytest
    ```
3.  **Plugins**: If you want to add a new broker or data source, refer to the [Interfaces documentation](src/blueshift/interfaces/README.md) on how to implement and register new plugins via entry points.
