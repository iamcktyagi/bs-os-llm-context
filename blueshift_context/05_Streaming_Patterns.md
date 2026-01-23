# Streaming Configuration Patterns

This document details the configuration patterns for the supported streaming backends: **WebSocket**, **SocketIO**, and **MQTT**.

## 1. WebSocket Backend (`websocket`)
Used for standard WebSocket connections (ws:// or wss://).

### Configuration Structure
```python
"backend": {
    "type": "websocket",
    "options": {
        # Optional: Custom headers or connection options
    }
}
```

### Authentication Patterns

#### A. Header-Based Authentication
Send API keys in the initial connection headers.
```python
"auth": {
    "mode": "headers",
    "headers": {
        "fields": {
            "Authorization": {"source": "f'Bearer {credentials.api_key}'"},
            "APCA-API-KEY-ID": {"source": "credentials.api_key"},
            "APCA-API-SECRET-KEY": {"source": "credentials.api_secret"}
        }
    }
}
```

#### B. First-Message Authentication (Login Packet)
Send a specific JSON message immediately after connecting.
```python
"auth": {
    "mode": "first_message",
    "first_message": {
        "format": "json",
        "json": {
            "fields": {
                "action": {"source": "'auth'"},
                "key": {"source": "credentials.api_key"},
                "secret": {"source": "credentials.api_secret"}
            }
        }
    }
}
```

#### C. URL Parameter Authentication
Append tokens to the connection URL.
```python
"auth": {
    "mode": "url",
    "url": {
        "query": {
            "fields": {
                "token": {"source": "credentials.access_token"}
            }
        }
    }
}
```

### Subscription Patterns

The `subscribe_assets` context variable is a `list[Asset]` provided by the framework when resolving subscribe/unsubscribe messages.

#### Per-Channel Subscription (Recommended)
When different streams need different subscribe messages, use per-channel keys:
```python
"subscribe": {
    "subscribe": {
        "data": {  # Subscribe message for the 'data' stream
            "format": "json",
            "json": {
                "fields": {
                    "action": {"source": "'subscribe'"},
                    "trades": {"source": "[a.broker_symbol or a.symbol for a in subscribe_assets]"},
                }
            }
        },
        "quote": {  # Different message for the 'quote' stream
            "format": "json",
            "json": {
                "fields": {
                    "action": {"source": "'subscribe'"},
                    "quotes": {"source": "[a.broker_symbol or a.symbol for a in subscribe_assets]"},
                }
            }
        }
    },
    "unsubscribe": {
        "data": {
            "format": "json",
            "json": {
                "fields": {
                    "action": {"source": "'unsubscribe'"},
                    "trades": {"source": "[a.broker_symbol or a.symbol for a in subscribe_assets]"},
                }
            }
        },
        "quote": {
            "format": "json",
            "json": {
                "fields": {
                    "action": {"source": "'unsubscribe'"},
                    "quotes": {"source": "[a.broker_symbol or a.symbol for a in subscribe_assets]"},
                }
            }
        }
    }
}
```

#### Unified Subscription (Single message for all channels)
When one message subscribes to all streams at once:
```python
"subscribe": {
    "subscribe": {
        "all": {
            "format": "json",
            "json": {
                "fields": {
                    "action": {"source": "'subscribe'"},
                    "trades": {"source": "[a.broker_symbol or a.symbol for a in subscribe_assets]"},
                    "quotes": {"source": "[a.broker_symbol or a.symbol for a in subscribe_assets]"}
                }
            }
        }
    }
}
```

### Parsing
```python
"parser": {
    "format": "json",       # "json", "text", or "binary"
    # If the relevant data is nested under a key:
    "payload_path": "data.get('payload', data)"  # Expression with `data` = parsed message
}
```

---

## 2. SocketIO Backend (`socketio`)
Used for Socket.IO based servers (common in some Asian markets).

### Configuration Structure
```python
"backend": {
    "type": "socketio",
    "options": {
        # SocketIO specific options
        "path": "/socket.io",
        "transports": ["websocket"]
    }
}
```

### Authentication
SocketIO often uses query parameters or initial handshake headers.
```python
"auth": {
    "mode": "url",
    "url": {
        "query": {
            "fields": {
                "token": {"source": "credentials.access_token"},
                "uid": {"source": "credentials.user_id"}
            }
        }
    }
}
```

### SocketIO Events Mapping

SocketIO communicates via named "events". Map Blueshift channels to SocketIO event names using the `events` field on the connection:

```python
"market_data": {
    "url": "https://stream.broker.com",
    "backend": {"type": "socketio", "options": {"transports": ["websocket"]}},
    "streams": ["data", "quote"],
    # Map channel names -> SocketIO event names the server emits
    "events": {
        "data": "stock",          # Server emits 'stock' event for trade data
        "quote": "quote_update",  # Server emits 'quote_update' for quotes
    },
    ...
}
```

Without `events`, the framework listens for events matching the channel name directly.

### Subscription (Events)
SocketIO uses "events" rather than raw messages. The subscribe payload is emitted as a SocketIO event:
```python
"subscribe": {
    "subscribe": {
        "data": {
            "format": "json",
            "json": {
                "fields": {
                    "action": {"source": "'join'"},
                    "data": {"source": "[a.security_id for a in subscribe_assets]"}
                }
            }
        }
    }
}
```

---

## 3. MQTT Backend (`mqtt`)
Used for MQTT brokers (common in IoT-like or lightweight implementations).

### Configuration Structure
```python
"backend": {
    "type": "mqtt",
    "options": {
        "client_id": "my_client_id",
        "transport": "tcp", # or "websockets"
        "keepalive": 60,
        "tls_kwargs": {
            # Optional TLS settings
            "tls_version": 2 # ssl.PROTOCOL_TLSv1_2
        }
    }
}
```

### Authentication
MQTT uses username/password auth defined in the `options` or `auth` block.
```python
"credentials": {
    "fields": ["username", "password", "host", "port"],
    # ...
}
```

### Subscription (Topics)
MQTT subscriptions are based on Topics and QoS (Quality of Service).
```python
"subscribe": {
    "subscribe": {
        # For MQTT, the structure is specific:
        # A list of tuples: (topic, qos)
        "format": "json", # Internal representation
        "json": {
            "custom": "create_mqtt_subscription_payload" 
        }
    }
}
```
*Note: For complex MQTT payloads, it is often easier to use a `custom` function in `blueshift_registry` to generate the exact subscription list expected by the underlying `paho-mqtt` client.*

---

## 4. Text Template Messages (`format: "text"`)

Some providers expect text frames instead of JSON. Use Python `str.format(...)` with `tokens`.

```python
"subscribe": {
    "subscribe": {
        "all": {
            "format": "text",
            "text": "SUB {symbol}",
            "tokens": {
                "fields": {
                    "symbol": {"source": "asset.broker_symbol or asset.symbol"}
                }
            }
        }
    }
}
```

---

## 5. Custom Parsing & Handling

For any protocol, if the standard JSON/Text parsers are insufficient (e.g., binary protocols, complex nested structures), use `custom` callables.

### Binary Parsing Example
```python
"parser": {
    "format": "binary",
    "decode": "parse_binary_message" # Function registered in ConfigRegistry
}
```

### Complex Routing
If a single connection receives multiple data types (Quotes, Trades, Order Updates) that need to be routed differently:
```python
"router": {
    "enabled": True,
    "rules": [
        {
            "channel": "data",  # Trades/Bars
            "match": "data.get('type') == 'trade'"  # Expression with `data` in context
        },
        {
            "channel": "quote", # Ticks/Depth
            "match": "data.get('type') == 'quote'"
        },
        {
            "channel": "order", # Order updates
            "match": "data.get('type') == 'order_update'"
        }
    ],
    "default_channel": "data"  # Fallback if no rules match
}
```

Router `match` expressions receive `data` (the parsed/extracted message) in their context. They must evaluate to a truthy value for a match.

---

## 6. Available Streaming Channels

The framework recognizes these channel names:

| Channel | Purpose | Converter Output |
|---------|---------|-----------------|
| `data` | Trade/bar data (OHLCV ticks) | Ingested into data store |
| `quote` | Level-1 quotes (bid/ask/last) | Quote updates |
| `order` | Order status updates | Order state changes |
| `position` | Position updates | Position state changes |
| `account` | Account balance updates | Account state |
| `heartbeat` | Keep-alive messages | Ignored (internal) |

---

## 7. Connection `frame` Field

Controls how the framework interprets each incoming message:

```python
"market_data": {
    "url": "...",
    "frame": "record",    # Default: each message = one record
    # OR
    "frame": "array",     # Each message = list of records (iterate and process each)
    ...
}
```

- `"record"` (default): Each WebSocket/SocketIO message is one data point
- `"array"`: Each message is a list; the framework iterates and processes each element through router/converters independently

---

## 8. Streaming Lifecycle Hooks

Define hooks for connection lifecycle events:

```python
"market_data": {
    "url": "...",
    "hooks": {
        "on_connect": "my_on_connect",       # Called when connection established
        "on_disconnect": "my_on_disconnect", # Called on disconnection
        "on_error": "my_on_error",           # Called on error
        "on_message": "my_on_message",       # Called on every raw message (before parser)
    },
    ...
}
```

Hook functions receive `**context` with `broker`, `credentials`, `connection_name`, and event-specific args:
```python
@registry.register()
def my_on_connect(broker, **kwargs):
    """Called when streaming connection is established."""
    broker.logger.info("Streaming connected")

@registry.register()
def my_on_disconnect(broker, **kwargs):
    """Called on disconnection. Framework handles reconnection automatically."""
    broker.logger.warning("Streaming disconnected")
```

---

## 9. Asset Inference in Converters (`broker.infer_asset`)

In converter expressions, use `broker.infer_asset()` to map streaming symbols/tokens back to Asset objects:

```python
# Lookup by symbol (exact match against master data)
"asset": {"source": "broker.infer_asset(symbol=data['symbol'])"}

# Lookup by security_id/token (for numeric-token based brokers)
"asset": {"source": "broker.infer_asset(security_id=str(data['token']))"}

# Lookup by broker_symbol (if different from symbol)
"asset": {"source": "broker.infer_asset(broker_symbol=data['instrument_key'])"}
```

- At least one keyword argument is required
- Returns an `Asset` object from the asset finder (populated by master data)
- Raises `SymbolNotFound` if no match — the framework logs a warning and skips the message
- The keyword used must correspond to a field in your `MASTER_DATA_SPEC` mapping

---

## 10. ZMQ Backend (`zmq`)

For ZeroMQ-based streaming (used in some internal/proprietary systems):

```python
"backend": {
    "type": "zmq",
    "options": {
        "socket_type": "SUB",      # ZMQ socket type
        "subscribe_filter": "",     # ZMQ topic filter (empty = all)
    }
}
```

ZMQ auth is typically handled at the transport/network level rather than via application-layer messages.

---

## 11. Complete Streaming Example (Multi-Connection)

A broker with separate connections for market data and order updates:

```python
STREAMING_SPEC = {
    "connections": {
        "market_data": {
            "url": "wss://stream.broker.com/market",
            "backend": {"type": "websocket"},
            "streams": ["data", "quote"],
            "frame": "array",
            "auth": {
                "mode": "first_message",
                "first_message": {
                    "format": "json",
                    "json": {"fields": {"token": {"source": "credentials.access_token"}}}
                }
            },
            "subscribe": {
                "subscribe": {
                    "data": {
                        "format": "json",
                        "json": {"fields": {
                            "type": {"source": "'subscribe'"},
                            "channel": {"source": "'trades'"},
                            "symbols": {"source": "[a.broker_symbol for a in subscribe_assets]"}
                        }}
                    },
                    "quote": {
                        "format": "json",
                        "json": {"fields": {
                            "type": {"source": "'subscribe'"},
                            "channel": {"source": "'quotes'"},
                            "symbols": {"source": "[a.broker_symbol for a in subscribe_assets]"}
                        }}
                    }
                }
            },
            "parser": {"format": "json"},
            "router": {
                "enabled": True,
                "rules": [
                    {"channel": "data", "match": "data.get('channel') == 'trades'"},
                    {"channel": "quote", "match": "data.get('channel') == 'quotes'"},
                ]
            },
            "converters": {
                "data": [{
                    "fields": {
                        "asset": {"source": "broker.infer_asset(symbol=data['symbol'])"},
                        "timestamp": {"source": "pd.Timestamp(data['time'])"},
                        "data": {
                            "close": {"source": "float(data['price'])"},
                            "volume": {"source": "float(data['volume'])"},
                        }
                    }
                }],
                "quote": [{
                    "fields": {
                        "asset": {"source": "broker.infer_asset(symbol=data['symbol'])"},
                        "timestamp": {"source": "pd.Timestamp(data['time'])"},
                        "bid": {"source": "float(data['bid'])"},
                        "ask": {"source": "float(data['ask'])"},
                        "last": {"source": "float(data.get('last', 0))"},
                    }
                }]
            }
        },
        "trading": {
            "url": "wss://stream.broker.com/trading",
            "backend": {"type": "websocket"},
            "streams": ["order"],
            "auth": {
                "mode": "first_message",
                "first_message": {
                    "format": "json",
                    "json": {"fields": {"token": {"source": "credentials.access_token"}}}
                }
            },
            "converters": {
                "order": [{
                    "fields": {
                        "oid": {"source": "str(data['order_id'])"},
                        "status": {"source": "mappings.order_status.to_blueshift(data['status'])"},
                        "filled": {"source": "float(data.get('filled_qty', 0))"},
                        "average_price": {"source": "float(data.get('avg_price', 0))"},
                    }
                }]
            }
        }
    }
}
```
