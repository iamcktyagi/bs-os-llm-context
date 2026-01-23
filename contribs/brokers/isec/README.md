The ICICIDirect (I-Sec) Breeze API broker has been implemented in `contribs/brokers/isec`.

### Key Features
- **Declarative Configuration**: Uses `isec.py` to define endpoints, mappings, and object conversions.
- **Authentication**: Implements the required `X-Checksum` logic using a custom `before_send` hook.
- **Master Data**: automatically downloads and parses the daily Scrip Master CSV file to build the asset universe.
- **Trading & Data**: Supports order placement, cancellation, order book fetching, funds check, historical data, and live quotes.

### Setup & Usage
1.  **Install**:
    ```bash
    pip install -e contribs/brokers/isec
    ```
2.  **Configuration**:
    Add the following to your Blueshift configuration (YAML):
    ```yaml
    brokers:
      my_isec_broker:
        name: isec-live
        api_key: "YOUR_APP_KEY"
        secret_key: "YOUR_SECRET_KEY"
        session_token: "YOUR_SESSION_TOKEN" # Generated daily
    ```

### Implementation Details
- **Path**: `contribs/brokers/isec/src/blueshift/contribs/brokers/isec/isec.py`
- **Master Data URL**: `https://traderweb.icicidirect.com/Content/File/txtFile/ScripFile/StockScriptNew.csv`
- **API Base URL**: `https://api.icicidirect.com/breezeapi/api/v1`

Note: The `session_token` is dynamic and expires. You must generate it daily by logging into the Breeze API or using their login flow and update your configuration or pass it dynamically at runtime.
