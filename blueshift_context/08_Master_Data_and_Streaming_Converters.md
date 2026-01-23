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

## 3) Vectorized Mode (`vectorized: True`)

For large instrument files (10K+ instruments), vectorized mode uses pandas operations on the full DataFrame instead of row-by-row iteration. This is dramatically faster.

### Key Differences from Row Mode
- `filter`: must be a pandas boolean expression (returns a Series, not a scalar)
- `mapping` expressions: operate on `data` (DataFrame), must return a Series
- Context variable: `data` (DataFrame) instead of `row`/`item` (dict)

### Example: Equity + Indices from CSV
```python
{
    "endpoint": {
        "endpoint": "https://broker.com/instruments.csv",
        "method": "GET",
        "use_global_headers": False,
        "response": {"payload_type": "object", "result": {"fields": {}}}  # Dummy for file mode
    },
    "mode": "file",
    "format": "csv",
    "csv_options": {"dtype": "str"},  # Force all columns to string (prevents type issues)
    "assets": [
        {
            # Equity: filter by segment and series
            "filter": "(data['Exchange'] == 'NSE') & (data['Series'] == 'EQ') & (data['TickSize'].astype('float') > 0)",
            "asset_class": "equity",
            "vectorized": True,
            "mapping": {
                "symbol": {"source": "data['TradingSymbol']"},
                "broker_symbol": {"source": "data['InstrumentCode']"},
                "security_id": {"source": "data['Token']"},
                "name": {"source": "data['Name']"},
                "tick_size": {"source": "1/data['TickSize'].astype(float)"},
                "exchange_name": {"source": "'NSE'"},
                "calendar_name": {"source": "'NSE'"},
            }
        },
        {
            # Indices: no tick size, different filter
            "filter": "(data['Exchange'] == 'NSE') & (data['Series'] == '0') & (data['TickSize'].astype('float') == 0)",
            "asset_class": "mktdata",
            "vectorized": True,
            "mapping": {
                "symbol": {"source": "data['TradingSymbol'].str.replace(' ','')"},
                "broker_symbol": {"source": "data['InstrumentCode']"},
                "security_id": {"source": "data['Token']"},
                "name": {"source": "data['Name']"},
                "exchange_name": {"source": "'NSE'"},
                "calendar_name": {"source": "'NSE'"},
            }
        }
    ]
}
```

## 4) Derivatives Master Data (Futures & Options)

For brokers supporting derivatives, configure `equity-futures` and `equity-options` asset classes:

### Futures
```python
{
    "filter": "(data['Exchange'] == 'NFO') & (data['ContractDesc'].str.contains('FUT-'))",
    "asset_class": "equity-futures",
    "vectorized": True,
    "details": {
        "underlying_exchange": {"source": "'NSE'"},  # Where the underlying trades
        "expiry_types": {"source": "['monthly']"},   # ['monthly', 'weekly']
    },
    "mapping": {
        "symbol": {"source": "''"},  # Leave empty: framework infers from underlying + expiry
        "broker_symbol": {"source": "data['InstrumentCode']"},
        "security_id": {"source": "data['Token']"},
        "name": {"source": "data['Name']"},
        "tick_size": {"source": "100/data['TickSize'].astype(float)"},
        "exchange_name": {"source": "'NFO'"},
        "calendar_name": {"source": "'NFO'"},
        "underlying": {"source": "data['UnderlyingSymbol'].str.replace(' ','')"},
        "root": {"source": "data['UnderlyingSymbol']"},
        "mult": {"source": "data['LotSize'].astype(int)"},
        "expiry_date": {"source": "pd.to_datetime(data['Expiry'])"},
    }
}
```

### Options
```python
{
    "filter": "(data['Exchange'] == 'NFO') & (data['ContractDesc'].str.contains('OPT-'))",
    "asset_class": "equity-options",
    "vectorized": True,
    "details": {
        "underlying_exchange": {"source": "'NSE'"},
    },
    "mapping": {
        "symbol": {"source": "''"},  # Inferred from underlying + expiry + strike + type
        "broker_symbol": {"source": "data['InstrumentCode']"},
        "security_id": {"source": "data['Token']"},
        "name": {"source": "data['Name']"},
        "tick_size": {"source": "100/data['TickSize'].astype(float)"},
        "exchange_name": {"source": "'NFO'"},
        "calendar_name": {"source": "'NFO'"},
        "underlying": {"source": "data['UnderlyingSymbol'].str.replace(' ','')"},
        "root": {"source": "data['UnderlyingSymbol']"},
        "mult": {"source": "data['LotSize'].astype(int)"},
        "expiry_date": {"source": "pd.to_datetime(data['Expiry'])"},
        "option_type": {"source": "data['OptionType']"},   # 'CE'/'PE' or 'call'/'put'
        "strike": {"source": "data['StrikePrice'].astype(float)"},
    }
}
```

### Key Fields for Derivatives

| Field | Required | Description |
|-------|----------|-------------|
| `underlying` | Yes | Symbol of the underlying asset |
| `root` | Yes | Root symbol (same as underlying usually) |
| `mult` | Yes | Contract multiplier / lot size |
| `expiry_date` | Yes | Contract expiry (pd.Timestamp or parseable string) |
| `option_type` | Options only | `"CE"`/`"PE"` or `"call"`/`"put"` |
| `strike` | Options only | Strike price (float) |
| `exchange_name` | Yes | Derivatives exchange (e.g., `"NFO"`, `"BFO"`) |

## 5) File Mode Details

For brokers that provide instrument data as downloadable files:

```python
{
    "endpoint": {
        "endpoint": "https://broker.com/data/instruments.csv",
        "method": "GET",
        "use_global_headers": False,  # Public URL, no auth needed
        "response": {"payload_type": "object", "result": {"fields": {}}}
    },
    "mode": "file",
    "format": "csv",          # "csv" or "json"
    "compression": "none",    # "none", "zip", "gzip"
    "csv_options": {           # Passed to pd.read_csv()
        "dtype": "str",       # Read all as string (prevents int overflow, float precision issues)
        "sep": ",",           # Delimiter
        "encoding": "utf-8",
    },
    "assets": [ ... ]
}
```

### Compression
- `"none"`: file is plain text
- `"zip"`: file is a ZIP archive (first CSV inside is used)
- `"gzip"`: file is gzip-compressed

### API Mode (JSON from REST endpoint)
```python
{
    "endpoint": {
        "endpoint": "/api/instruments",
        "method": "GET",
        "request": {
            "query": {"fields": {"status": {"source": "'active'"}}}
        },
        "response": {"payload_type": "array", "items": {"fields": {}}}
    },
    "mode": "api",       # or "list"
    "format": "json",
    "assets": [
        {
            "asset_class": "equity",
            "filter": "item.get('tradable') and item.get('status') == 'active'",
            "mapping": {
                "symbol": {"source": "item['symbol']"},
                "security_id": {"source": "item.get('id', item['symbol'])"},
                "name": {"source": "item.get('name', '')"},
                "exchange_name": {"source": "item.get('exchange', 'NYSE')"},
                "calendar_name": {"source": "'NYSE'"},
            }
        }
    ]
}
```

## 6) Practical Guidance

- Start with REST-only and master data first; it makes the streaming side much easier.
- Use `condition` aggressively in streaming converters; real feeds often multiplex message types.
- Prefer `fields` for readability; switch to `custom` when the message structure is irregular or binary.
- For large instrument files (10K+), always use `vectorized: True` with `csv_options: {"dtype": "str"}`.
- Set `use_global_headers: False` on master data endpoints that hit public URLs (no auth needed).
- If `symbol` is left as `""` for derivatives, the framework auto-generates it from underlying + expiry + strike + type.
- For `option_type`, the framework normalizes `"CE"`→`OptionType.CALL`, `"PE"`→`OptionType.PUT`, `"call"`→`OptionType.CALL`, `"put"`→`OptionType.PUT`.
