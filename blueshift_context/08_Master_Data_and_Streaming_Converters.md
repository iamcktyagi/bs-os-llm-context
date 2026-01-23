# Master Data & Streaming Converters

This document explains two parts that typically determine whether a broker integration feels “production-grade”:
- **Master data**: building a correct asset universe and stable symbol/id mapping
- **Streaming converters**: translating raw stream messages into Blueshift market data / objects

## 1) Master Data (`API_SPEC["master_data"]`)

### Why it matters
Most brokers use at least one of:
- a broker-specific symbol (`broker_symbol`)
- an internal instrument/security id (`security_id`)
- an exchange-qualified ticker (`exchange_ticker`)

To avoid hardcoding these mappings (and to make streaming routing possible), configure **master data** so the framework can create assets consistently.

### Shape (high level)
`API_SPEC["master_data"]` is a list of master-data definitions. Each definition fetches some instrument dataset (CSV/JSON) and declares how to extract one or more Blueshift asset classes from it.

```python
API_SPEC = {
    "base_url": "https://api.broker.com",
    "master_data": [
        {
            "preload": True,
            "endpoint": {
                "endpoint": "/instruments",
                "method": "GET",
                "response": {
                    "payload_type": "array",
                    "payload_path": "response",
                },
            },
            "mode": "list",
            "format": "json",
            "compression": "none",
            "assets": [
                {
                    "asset_class": "equity",
                    "filter": "row.get('segment') == 'EQ'",
                    "mapping": {
                        "symbol": {"source": "row['tradingsymbol']"},
                        "broker_symbol": {"source": "row['tradingsymbol']"},
                        "security_id": {"source": "str(row['instrument_token'])"},
                        "exchange_name": {"source": "row.get('exchange', 'NSE')"},
                    },
                    "details": {
                        "tick_size": {"source": "float(row.get('tick_size', 0.05))"},
                        "mult": {"source": "float(row.get('lot_size', 1))"},
                    }
                }
            ],
        }
    ],
}
```

### Key fields you will use
- `endpoint`: a normal endpoint definition (same schema as other REST endpoints)
- `mode`:
  - `"list"`: endpoint resolves to `list[dict]` (or similar data convertible to a DataFrame)
  - `"file"`: endpoint provides a file payload; use `format` + `compression` + `file_match` on assets
- `assets`: list of asset extractors (`instrument_def`)
  - `asset_class`: `"equity"`, `"equity-futures"`, `"equity-options"`, `"crypto"`, `"fx"`, `"mktdata"`
  - `filter`: expression (string) or list of expressions; evaluated against `row`/`item`
  - `mapping`: field rules evaluated against `row`/`item`
  - `details`: extra fields assembled into `details` dict on the asset
  - `custom`: for non-tabular or unusual payloads; gets the context (and optionally the dataset)
  - `vectorized`: if `True`, filters/mappings should be vectorized against `data` (a DataFrame)

Context variables for master-data expressions:
- `row` / `item`: current record (dict) in non-vectorized mode
- `data`: full DataFrame in vectorized mode

## 2) Streaming Converters (`STREAMING_SPEC["connections"][...]["converters"]`)

### Why it matters
Streaming systems typically deliver mixed message types on one connection:
- trades / bars
- quotes / depth
- order updates

Converters define how parsed messages become:
- **market data updates** (channels: `data`, `quote`)
- **object updates** (channels like `order`, `position`, etc.)

### Converter pipelines
Each channel is a list of conversion rules applied in order until one succeeds.

#### A. `data` channel (streaming price data)
`data` converters must produce a tuple-like structure:
- `asset`: a Blueshift `MarketData` / `Asset` instance
- `timestamp`: a `pd.Timestamp`
- `data`: a dict of values (e.g., `open/high/low/close/volume/last`)

Example:
```python
"converters": {
    "data": [
        {
            "condition": "data.get('type') == 'trade'",
            "fields": {
                "asset": {"source": "broker.infer_asset(symbol=data['symbol'])"},
                "timestamp": {"source": "pd.Timestamp(data['ts'], unit='ms', tz='UTC')"},
                "data": {
                    "last": {"source": "float(data['price'])"},
                    "volume": {"source": "float(data.get('size', 0))"},
                },
            },
        }
    ]
}
```

#### B. `quote` channel (streaming quotes)
Quote converters use quote field groups (bid/ask/last/etc.). If the broker has multiple quote message types, create multiple conversion rules with `condition`.

#### C. `order` / `position` / other object channels
These channels use the generic conversion pipeline format:
- either `fields` (field mapping rules), or
- `custom` (callable for complex parsing)

```python
"converters": {
    "order": [
        {
            "condition": "data.get('type') == 'order_update'",
            "fields": {
                "order_id": {"source": "data['order_id']"},
                "status": {"source": "mappings.order_status.to_blueshift(data['status'])"},
            }
        }
    ]
}
```

## 3) Practical Guidance

- Start with REST-only and master data first; it makes the streaming side much easier.
- Use `condition` aggressively in streaming converters; real feeds often multiplex message types.
- Prefer `fields` for readability; switch to `custom` when the message structure is irregular or binary.
