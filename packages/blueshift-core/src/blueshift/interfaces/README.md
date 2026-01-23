# Blueshift Interfaces

The `blueshift.interfaces` sub-package is the foundational layer of the Blueshift platform, defining the abstract contracts (interfaces) for all pluggable components. It establishes a clear separation between the "what" (the interface) and the "how" (the implementation or provider). This design promotes modularity, extensibility, and testability across the entire system.

## Responsibility

-   **Define Contracts**: Specifies the methods, properties, and expected behavior for various core components of a trading system, such as brokers, data portals, asset finders, and blotters.
-   **Promote Modularity**: Enables a flexible architecture where different concrete implementations (providers) can be easily swapped without requiring changes to the core trading engine or user-defined algorithms.
-   **Standardize Interaction**: Ensures a consistent API for `blueshift.core` components and user algorithms to interact with underlying services (data, trading, assets, logging).
-   **Enforce Design Patterns**: By using Abstract Base Classes (ABCs), it ensures that any new provider adheres to a predefined structure, guaranteeing compatibility.

## Relationship to Other Sub-packages

-   **Used by `blueshift.core`**: The core components of Blueshift, such as `TradingAlgorithm`, `AlgoContext`, and `BlueshiftEnvironment`, are designed to operate against these abstract interfaces. This means `blueshift.core` interacts with an `IBroker` or a `DataPortal` without needing to know the specific concrete implementation being used.
-   **Implemented by `blueshift.providers`**: The `blueshift.providers` sub-package contains the concrete classes that inherit from and implement these interfaces. For instance, `blueshift.providers.trading.broker.BacktestBroker` implements the `IBacktestBroker` interface.

## Key Sub-modules and Interfaces

### 1. `blueshift.interfaces.trading`
Defines contracts for components related to trade execution and management.
-   **`broker.py`**:
    -   `IBroker`: Base interface for all brokers, covering login, order placement, fetching positions, and account details.
    -   `IBacktestBroker`: Extends `IBroker` with specific functionalities for backtesting.
    -   `ILiveBroker`: Extends `IBroker` with live trading concerns (e.g., connection status, callbacks).
    -   `IPaperBroker`: Combines aspects of live and backtest brokers for paper trading.
    -   `IBrokerCollection`: Interface for managing multiple broker instances.
-   **`blotter.py`**: Defines `IBlotter` for tracking an algorithm's orders, positions, and performance.
-   **`algo_orders.py`**: Interfaces for advanced algorithmic order types.
-   **`rms.py`**: Defines `IRMS` for Risk Management Systems.

### 2. `blueshift.interfaces.data`
Defines contracts for market data access and management.
-   **`data_portal.py`**: Defines `DataPortal` for fetching current and historical market data (OHLCV, quotes, fundamentals).
-   **`store.py`**: Defines `IDataStore` for managing data storage.
-   **`library.py`**: Defines `ILibrary` for organizing and accessing data bundles.
-   **`ingestor.py`**: Defines `IIngestor` for data ingestion processes.

### 3. `blueshift.interfaces.assets`
Defines contracts for asset management.
-   **`assets.py`**: Defines `IAsset` and `IAssetFinder` for representing financial instruments and looking them up.
-   **`opt_chains.py`**: Defines `IOptionChains` for managing and querying option chains (expiries, strikes, contract details).

### 4. Other Key Interfaces
-   **`context.py`**: Defines `IContext`, `IAccount`, and `IPortfolio`, which are abstract representations of the algorithm's runtime context, trading account, and investment portfolio.
-   **`logger.py`**: Defines `BlueshiftLogger`, the standard logging interface.
-   **`streaming.py` / `nostreaming.py`**: Define interfaces for streaming data and message publishers/subscribers.

## Concepts

-   **Abstract Base Classes (ABCs)**: Python's `abc` module is used extensively to define these interfaces. Classes that implement these interfaces must provide concrete implementations for all abstract methods and properties, ensuring adherence to the contract.
-   **Polymorphism**: The core Blueshift engine interacts with these abstract interfaces. This means it can seamlessly work with any concrete provider (e.g., a `BacktestBroker`, `AlpacaBroker`, or `ZerodhaBroker`) as long as that provider correctly implements the `IBroker` interface.
-   **Factory Pattern**: Many interfaces are accompanied by factory functions (e.g., `broker_factory`, `data_portal_factory`) that abstract the process of creating concrete instances. These factories often incorporate lazy loading mechanisms to improve performance.

## Usage

-   **For `blueshift.core` Developers**: When designing new features or extending the core engine, developers program against these interfaces, ensuring that the core logic remains decoupled from specific implementations.
-   **For `blueshift.providers` Developers**: Anyone building a new integration (e.g., a new broker, a custom data source) must implement the relevant interfaces defined in this package. This guarantees that their new component will integrate correctly with the rest of the Blueshift ecosystem.
-   **For User Algorithms**: End-users typically interact with these interfaces indirectly through the `context` object provided to their algorithms (e.g., `context.data_portal.history()`, `context.broker.place_order()`). While users don't usually import directly from `blueshift.interfaces`, understanding these contracts helps in comprehending the capabilities and expected behavior of the platform's components.

## Extending Blueshift with Custom Implementations

Developers can extend Blueshift's functionality by providing their own implementations of these interfaces. This allows for integration with new brokers, data sources, or custom risk management systems.

### Steps to Add a Custom Implementation

1.  **Define Your Provider Class**: Create a Python class that inherits from the relevant abstract interface(s) (e.g., `IBroker`, `DataPortal`, `IRMS`). You must implement all abstract methods and properties defined by the interface.

    ```python
    from blueshift.interfaces.trading.broker import IBroker
    from blueshift.calendar import TradingCalendar
    from blueshift.lib.trades._accounts import Account
    # ... other necessary imports and implementations

    class MyCustomBroker(IBroker):
        def __init__(self, name, *args, **kwargs):
            super().__init__(name, *args, **kwargs)
            # ... custom initialization

        @property
        def calendar(self) -> TradingCalendar:
            # ... return your custom calendar
            pass

        # ... implement all other abstract methods
    ```

2.  **Register Your Provider**: Once your provider class is defined, you need to register it with Blueshift's factory system. This is typically done using `register_` functions provided by the interfaces.

    For example, to register a custom broker:
    ```python
    from blueshift.interfaces.trading.broker import register_broker
    # Assuming MyCustomBroker is defined in my_custom_package.my_broker_module

    # Call this function to register your broker, usually at the module level
    register_broker('my_custom_broker_name', MyCustomBroker)
    ```
    Similarly, other interfaces will have their respective registration functions (e.g., `set_builtin_data_store_loader` for data stores, `register_calendar` for calendars).

3.  **Automatic Plugin Discovery via Entry Points**:
    Blueshift now fully supports standard Python plugin discovery using `entry_points` defined in `pyproject.toml` or `setup.py`. This is the recommended way to extend Blueshift with custom implementations.
    
    To register your custom implementations (e.g., a new broker, data source, or financial model), package them as a Python package and define entry points under the `blueshift.plugins` group, followed by the specific category (e.g., `blueshift.plugins.broker`, `blueshift.plugins.data`, `blueshift.plugins.finance`, `blueshift.plugins.assets`, `blueshift.plugins.oneclick`, `blueshift.plugins.rms`, `blueshift.plugins.exit_handler`, `blueshift.plugins.blotter`, `blueshift.plugins.algo_order_handler`, `blueshift.plugins.strategy`, `blueshift.plugins.security`, `blueshift.plugins.streaming`).
    
    When your package is installed, Blueshift will automatically discover and load these extensions at runtime. This eliminates the need for explicit `register_` calls in your main application code, promoting a cleaner and more modular plugin architecture.
    
    **Example `pyproject.toml` configuration for a custom broker:**
    ```toml
    [project.entry-points."blueshift.plugins.broker"]
    my_custom_broker_plugin = "my_custom_package.my_broker_module:initialize_my_custom_broker"
    ```
    In this example, `initialize_my_custom_broker` would be a function (or a class constructor) within `my_custom_package.my_broker_module` that performs the registration of `MyCustomBroker` when called by the plugin manager. This initializer typically calls the appropriate `register_` function internally (e.g., `register_broker`).

    **Note**: While explicit `register_` functions (e.g., `register_broker('my_custom_broker_name', MyCustomBroker)`) are still available for direct usage, using entry points is the standard, recommended, and most robust way to extend Blueshift in a distributable manner.


## Gotchas and Limitations

1.  **Abstract Definitions**: This package contains only abstract definitions. You cannot directly instantiate classes from `blueshift.interfaces` (e.g., `IBroker()`). Attempting to do so will result in a `TypeError`. You must use concrete implementations provided by `blueshift.providers` or obtained via factory functions.
2.  **Blueprint, Not Implementation**: `blueshift.interfaces` provides the "what" (the contract) but not the "how" (the functional logic). It serves as a blueprint for system components and does not contain any executable business logic itself.
3.  **Strict Contracts**: Implementations must strictly adhere to the methods, properties, and their signatures (including type hints) as defined in the interfaces. Failure to do so will lead to runtime errors or incorrect behavior when integrated into the Blueshift ecosystem.
4.  **No Direct Import for Users**: For most algorithm developers, direct imports from `blueshift.interfaces` are unnecessary. Interaction should primarily be through the `AlgoContext` or via higher-level API calls.
