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

### Subscription & Parsing
```python
"subscribe": {
    "subscribe": {
        "all": {
            "format": "json",
            "json": {
                "fields": {
                    "action": {"source": "'subscribe'"},
                    "trades": {"source": "[asset.broker_symbol or asset.symbol]"},
                    "quotes": {"source": "[asset.broker_symbol or asset.symbol]"}
                }
            }
        }
    },
    "unsubscribe": {
        "all": {
            "format": "json",
            "json": {
                "fields": {
                    "action": {"source": "'unsubscribe'"},
                    "trades": {"source": "[asset.broker_symbol or asset.symbol]"},
                    "quotes": {"source": "[asset.broker_symbol or asset.symbol]"}
                }
            }
        }
    }
},
"parser": {
    "format": "json",
    # If the relevant data is nested under a key (e.g., "data")
    "payload_path": "data" 
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

### Subscription (Events)
SocketIO uses "events" rather than raw messages. The `subscribe` payload should define the event name and data.
```python
"subscribe": {
    "subscribe": {
        "all": {
            "format": "json",
            "json": {
                "fields": {
                    "type": {"source": "'subscribe'"},
                    "channels": {"source": "[asset.broker_symbol or asset.symbol]"}
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
            "channel": "data", # Trades/Bars
            "match": "is_trade_message" # Function returning Bool
        },
        {
            "channel": "quote", # Ticks/Depth
            "match": "is_quote_message"
        },
        {
            "channel": "order", # Order updates
            "match": "is_order_update"
        }
    ],
    "default_channel": "unknown"
}
```
