# Blueshift Finance

The `blueshift.finance` sub-package provides a comprehensive collection of configurable financial models essential for accurately simulating and tracking trading operations. This includes models for calculating transaction costs (commissions and charges), margin requirements, and market slippage. These models are crucial for realistic backtesting, performance attribution, and risk management within the Blueshift platform.

## Responsibility

-   **Financial Modeling**: Offers a variety of models to simulate real-world financial effects of trading.
-   **Cost Calculation**: Computes commissions, exchange charges, and other transaction costs.
-   **Margin Management**: Determines margin requirements for various asset types and trading strategies.
-   **Slippage Simulation**: Models the impact of order size and market conditions on execution prices.
-   **Configurable and Extensible**: Provides a framework for configuring built-in models and extending with custom implementations.

## Relationship to Other Sub-packages

-   **`blueshift.interfaces.trading.simulations`**: `blueshift.finance` registers its concrete implementations (e.g., `FixedSlippage`, `NSECommission`, `RegTMargin`) with factory functions managed by `blueshift.interfaces.trading.simulations`. This allows for a pluggable architecture.
-   **`blueshift.core`**: Specifically, the `IBroker` implementations (found in `blueshift.providers.trading.broker`) utilize these financial models to accurately reflect transaction costs, margin impacts, and price slippage during order execution and position management in both simulated and live trading environments.

## Concepts and Terminology

-   **Slippage Model**: Defines how the actual executed price of an order might deviate from its intended price due to factors like market liquidity, order size, and latency. Examples include fixed slippage, bid-ask spread slippage, or volume-based slippage.
-   **Margin Model**: Determines the amount of collateral (margin) required by a broker to open and maintain trading positions. Different models exist for various regulations (e.g., Reg T) or asset classes.
-   **Commission Model**: Calculates the fees charged by the brokerage for executing trades. These can be per-share, per-dollar, per-order, or specific to an exchange.
-   **Charge Model**: Computes additional statutory or exchange-levied charges and taxes associated with trading activities.
-   **Model Registry**: All these models are registered by a unique name (e.g., 'NSE' for NSE-specific models) and can be configured and loaded dynamically.

## Common Usage Patterns

These financial models are primarily configured within the `blueshift_config.json` file, typically as part of your chosen broker's setup. The broker then automatically applies the specified models during order placement, execution, and performance calculation.

### Example: Broker Configuration in `blueshift_config.json`

```json
{
  "brokers": {
    "my_backtest_broker": {
      "name": "backtest",
      "initial_capital": 100000,
      "commission_model": "NSE",  // Use NSE-specific commission model
      "slippage_model": "NSE",    // Use NSE-specific slippage model
      "margin_model": "NSE"       // Use NSE-specific margin model
    },
    "my_paper_broker": {
      "name": "paper",
      "simulator": "india-eq-broker", // Underlying simulator for paper trading
      "initial_capital": 50000,
      "commission_model": {
        "name": "per-share",        // Custom per-share commission
        "rate": 0.01                // $0.01 per share
      },
      "slippage_model": "bid-ask"   // Use bid-ask spread for slippage
    }
  }
}
```

## Gotchas and Limitations

1.  **Realism vs. Simplification**: The models provided aim for a balance between realism and computational efficiency. Highly complex, ultra-realistic models may slow down simulations. Understand the assumptions behind each model.
2.  **Configuration Accuracy**: The accuracy of your backtest results and P&L attribution is highly dependent on how accurately these financial models reflect the actual costs and conditions of your trading environment. Misconfigured models can lead to misleading performance metrics.
3.  **Market Specifics**: Different exchanges and asset classes often have unique fee structures, margin rules, and slippage characteristics. Ensure you select or implement models appropriate for your specific trading context.
4.  **No Direct Algorithm Interaction**: Most users will not directly interact with these model classes in their algorithm code. Instead, the effects of these models are automatically applied by the `IBroker` implementation.
