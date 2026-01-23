# Blueshift Providers

The `blueshift.providers` sub-package contains the concrete implementations of the abstract interfaces defined in `blueshift.interfaces`. While `blueshift.core` manages the logic and flow of an algorithm, `blueshift.providers` connects that logic to the real (or simulated) world—providing market data, executing orders, and managing asset databases.

## Responsibility

-   **Concrete Implementations**: Provides the actual working code for brokers, data stores, asset finders, and performance trackers.
-   **Plugin Architecture**: Acts as a plugin system where different implementations can be swapped (e.g., switching from a backtest broker to a live broker) without changing the core algorithm code.
-   **Lazy Registration**: Implements a lazy-loading mechanism to ensure that heavy dependencies or resources are only loaded when specifically requested by the runtime.

## Relationship to Other Sub-packages

-   **`blueshift.interfaces`**: This package implements the contracts defined in `blueshift.interfaces`. For example, `blueshift.providers.trading.broker.backtester.BacktestBroker` implements `blueshift.interfaces.trading.broker.IBroker`.
-   **`blueshift.core`**: The core engine requests these providers via factory methods. It relies on them to perform the actual heavy lifting of trading and data retrieval.

## Key Sub-modules

### 1. `blueshift.providers.trading`
Contains implementations related to trade execution and management.
-   **Brokers**: Implementations of `IBroker`.
    -   `BacktestBroker`: Simulates execution using historical data.
    -   `PaperBroker`: A wrapper that pairs a live broker connection for data with a simulator for execution.
    -   `SharedBroker`: Used in server mode to allow multiple algorithms to share a single broker connection.
-   **Blotters**: Implementations of `IBlotter` for tracking orders, positions, and performance.
    -   `BacktestBlotter`, `LiveBlotter`, `PaperBlotter`.
-   **Risk Management**: Concrete risk management systems (RMS).

### 2. `blueshift.providers.data`
Handles data ingestion, storage, and retrieval.
-   **Stores**: Implementations of `IDataStore`.
    -   `DataFrameStore`: A fast, in-memory/file-based store for backtesting data.
    -   `BrokerStore`: Retrieves data directly from the connected broker.
-   **Sources**: Implementations for fetching data streams (e.g., ZMQ sources).
-   **Library**: Implementations of the data library interface for organizing bundles.

### 3. `blueshift.providers.assets`
-   **Asset Finders**: specialized implementations for looking up asset details (though typically brokers provide this functionality directly).

## Concepts: Lazy Loading and Registration

To maintain performance and modularity, Blueshift uses a **lazy loading** pattern. Providers do not register their classes directly at import time. Instead, they register "installer" or "loader" functions with the interface factories.

**How it works:**
1.  **Registration**: When `blueshift.providers` is imported, it calls `set_builtin_..._loader` functions in `blueshift.interfaces`.
2.  **Request**: When a user or the engine calls a factory (e.g., `broker_factory('backtest')`), the interface layer calls the registered loader.
3.  **Loading**: The loader imports the specific module (e.g., `blueshift.providers.trading.broker.backtester`) and returns the class.

This ensures that running a backtest doesn't waste resources importing live trading libraries, and vice-versa.

## Common Usage Patterns

Users generally **do not** import from `blueshift.providers` directly. Instead, they configure the environment (via `blueshift_config.json` or `BlueshiftEnvironment`) and let the system load the appropriate provider.

### Configuration Example (Broker)

In your `blueshift_config.json`, you define the broker type. The system uses this string to look up and load the provider.

```json
"brokers": {
    "backtest_broker": {
        "name": "backtest",  // triggers loader for 'backtest'
        "initial_capital": 100000,
        "commission": 0.001
    },
    "my_live_broker": {
        "name": "alpaca",    // triggers loader for 'alpaca' (if installed)
        "api_key": "...",
        "secret": "..."
    }
}
```

### Manual Instantiation (Advanced)

If you are building a custom workflow and need to instantiate a provider manually, use the factory methods from `blueshift.interfaces`, not the class constructor directly.

```python
from blueshift.interfaces.trading.broker import broker_factory

# Correct way: uses the factory to trigger the lazy loader
broker = broker_factory('backtest', initial_capital=10000)
```

## Gotchas

1.  **Direct Imports**: Avoid importing classes directly from `blueshift.providers...` in your algorithm code. The internal structure might change. Always work through the interfaces or the `context` object (e.g., `context.broker`, `context.data_portal`).
2.  **Configuration Dependencies**: Some providers (like specific live brokers) require specific configuration keys to be present. If the configuration is missing or incorrect, the factory method will raise an error when it attempts to initialize the provider.
3.  **Registration Order**: The `blueshift.providers` package must be imported at least once for the registration hooks to run. This is handled automatically by `blueshift.core`, but if you are using components in isolation, ensure you import the package.
