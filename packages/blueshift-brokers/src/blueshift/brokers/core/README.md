# Blueshift Core Broker Implementation

The `blueshift.brokers.core` package provides the runtime implementation for the configuration-driven broker framework. It consumes the configurations defined in `blueshift.brokers.core.config` to instantiate a fully functional broker capable of trading, asset resolution, and data fetching.

## Overview

This core package implements the standard Blueshift interfaces (`ILiveBroker`, `IAssetFinder`, `DataPortal`) by delegating actual logic to the declarative configurations.

### Key Components

*   **`broker.py` (`RestAPIBroker`)**: The central class. It implements `ILiveBroker` and orchestrates the other components. It handles connection management, order placement/updates, and account syncing.
*   **`api.py` (`BrokerAPIMixin`)**: A generic REST API client. It reads endpoint definitions from the config to construct HTTP requests (headers, params, body), handle pagination, enforce rate limits, and parse responses.
*   **`assets.py` (`RestAPIBrokerAssetFinder`)**: Implements `IAssetFinder`. It resolves symbols (equity, options, futures) to Blueshift `Asset` objects. It supports loading master data from files or API endpoints as defined in the config.
*   **`data.py` (`RestAPIBrokerDataPortal`)**: Implements `DataPortal`. It fetches historical and current market data. It includes built-in support for computing Option Greeks if the broker provides raw option prices and underlying data.
*   **`conversions.py`**: A set of factory functions used to transform raw API dictionaries into Blueshift objects (`Order`, `Position`, `Account`) using the configured conversion pipelines.

## Module Details

### Broker (`broker.py`)
The `RestAPIBroker` initializes by taking an `APIBrokerConfig`.
- **Initialization**: connect -> authenticates -> fetches initial state (orders, positions, account).
- **Trading**: Implements `place_order`, `cancel_order`, etc., by mapping them to specific API endpoints defined in the config (`api.endpoints.place_order`, etc.).
- **Streaming**: Initializes `ingestor` and `streamer` if streaming configuration is present (WIP).

### API Client (`api.py`)
The `BrokerAPIMixin` is the transport layer.
- **Dynamic Request Construction**: Uses `APIRequest` from the config to dynamically build URLs and payloads.
- **Pagination**: Automatically handles paginated responses (via `next_page_token`) to fetch full datasets (e.g., order history).
- **Rate Limiting**: Enforces strict rate limits defined in the schema.
- **Latency Check**: Provides mechanisms to check host reachability before placing trades.

### Data Portal (`data.py`)
Handles data requests.
- **Caching**: Implements LRU caching for historical data requests to minimize API hits.
- **Multi-Asset Support**: Optimizes fetching by using bulk API endpoints if `multi_assets_data_query` is enabled in options.
- **Options Intelligence**: If requested columns include Greeks (`delta`, `gamma`, etc.) and the asset is an Option, it attempts to calculate them using Black-Scholes models, interpolating the underlying price if necessary.

### Asset Finder (`assets.py`)
- **Lookup**: Supports finding assets by `symbol`, `security_id`, or `broker_symbol`.
- **Derivatives**: Specialized logic for handling Option and Future symbols, including mapping proprietary broker strings to standard OCC-style symbols.

## Usage

Typically, you do not instantiate these classes directly. They are instantiated by the Blueshift factory when loading a plugin that uses this framework.

```python
from blueshift.brokers.core.config.config import APIBrokerConfig
from blueshift.brokers.core.broker import RestAPIBroker

# Load config (usually from YAML/JSON)
config = APIBrokerConfig(...)

# Instantiate broker (name is derived from config.broker.name)
broker = RestAPIBroker(config=config)

# Connect
broker.connect()
```

## Gotchas and Important Notes

### 1. Initialization and Name
Ensure your configuration object is correctly populated before instantiation. See `README` under the 
config for more details.

### 2. Data Caching
The `RestAPIBrokerDataPortal` implements an internal LRU cache for historical data requests.
*   **Default Behavior**: Data is cached for a short duration (e.g., 15 seconds) to prevent hitting the broker's API rate limits during repeated calls in a loop.
*   **Implication**: If you need strictly real-time historical data updates within that window, you might see cached results.

### 3. Asset Loading
`RestAPIBrokerAssetFinder` loads the entire asset universe into memory upon initialization (or the first access).
*   **Updates**: If new assets are added on the broker side (e.g., new option strikes), they will not appear until `refresh_data(forced=True)` is called or the broker is restarted.

### 4. Option Greeks
If you request Greek columns (e.g., `delta`, `gamma`), the `RestAPIBrokerDataPortal` attempts to calculate them locally using Black-Scholes models.
*   **Requirements**: This requires fetching the underlying price and the risk-free rate (or futures price for implied rate). This can trigger additional API calls behind the scenes.

### 5. Thread Safety
The broker utilizes `TABLE_LOCKS` (defined in `utils.py`) to synchronize access to critical data structures like `orders` and `positions`.
*   **Caution**: Avoid long-running blocking operations inside callbacks or custom API hooks that might contend for these locks.
