import pytest
from blueshift.brokers.core.config.streaming import (
    get_streaming_config, StreamingConfig, StreamConnection, 
    StreamingAuth, Parser, MessageDef, Router, ToStreamingData
)
from blueshift.brokers.core.config.resolver import ConfigRegistry, ToObject
from blueshift.lib.exceptions import ValidationError
from blueshift.lib.trades._order import Order

registry = ConfigRegistry()
register = registry.register

@register()
def custom_encoder(data, **kwargs):
    return str(data).encode('utf-8')

@register()
def custom_decode(data, **kwargs):
    return {"parsed": data}

@register()
def custom_converter(data, **kwargs):
    return data

STREAMING_SPEC = {
    "connections": {
        "market_data": {
            "url": "wss://api.example.com/stream",
            "backend": {"type": "websocket"},
            "streams": ["data", "quote"],
            "auth": {
                "mode": "headers",
                "headers": {"fields":{"Authorization": {"source": "'Bearer token'"}}}
            },
            "subscribe": {
                "subscribe": {
                    "format": "json",
                    "json": {"fields": {"action": {"source": "'sub'"}, "channels": {"source": "channels"}}}
                }
            },
            "parser": {
                "format": "json",
                "payload_path": "message['payload']"
            },
            "router": {
                "enabled": True,
                "rules": [
                    {"channel": "data", "match": "'trade' in message['type']"},
                    {"channel": "quote", "match": "'quote' in message['type']"}
                ]
            },
            "converters": {
                "data": [
                    {
                        "fields": {
                            "asset": {"source": "data['sym']"},
                            "timestamp": {"source": "data['ts']"},
                            "data": {"data": {"source": "data['p']"}}
                        }
                    }
                ],
                "quote": [
                    {
                        "custom": "custom_converter"
                    }
                ]
            }
        },
        "order_updates": {
            "url": "wss://api.example.com/orders",
            "backend": {"type": "socketio"},
            "streams": ["order"],
            "auth": {
                "mode": "url",
                "url": {
                    "query": {"fields":{"token": {"source": "'123'"}}}
                }
            },
            "subscribe": {
                "subscribe": {
                    "format": "json",
                    "json": {"fields": {"action": {"source": "'sub'"}}}
                }
            },
            "parser": {
                "format": "json"
            },
            "converters": {
                "order": [
                    {
                        "fields": {
                            "order_id": {"source": "data['id']"},
                            # asset is usually required for Order, 
                            # assuming it's handled or mocked in real usage
                            "quantity": {"source": "data['qty']"} 
                        }
                    }
                ]
            }
        }
    }
}

def test_streaming_config_parsing():
    config = get_streaming_config(STREAMING_SPEC, registry)
    
    assert isinstance(config, StreamingConfig)
    assert len(config.connections) == 2
    
    # Check market_data connection
    md = config.connections['market_data']
    assert isinstance(md, StreamConnection)
    assert md.url == "wss://api.example.com/stream"
    assert md.backend.type == "websocket"
    
    # Auth
    assert isinstance(md.auth, StreamingAuth)
    assert md.auth.mode == "headers"
    # Resolve headers
    assert md.auth.headers is not None
    headers = md.auth.headers.resolve()
    assert headers['Authorization'] == "Bearer token"
    
    # Subscribe
    assert md.subscribe is not None
    sub_msg = md.subscribe['subscribe']
    assert isinstance(sub_msg, MessageDef)
    assert sub_msg.format == "json"
    # Resolve subscription message
    msg = sub_msg.resolve(channels=['A', 'B'])
    assert msg['action'] == 'sub'
    assert msg['channels'] == ['A', 'B']
    
    # Parser
    assert isinstance(md.parser, Parser)
    assert md.parser.format == "json"
    assert md.parser.payload_path is not None
    
    # Router
    assert isinstance(md.router, Router)
    assert md.router.enabled is True
    assert len(md.router.rules) == 2
    assert md.router.rules[0].channel == "data"
    
    # Converters - Price (ToStreamingData)
    assert md.converters is not None
    assert 'data' in md.converters
    price_pipeline = md.converters['data']
    assert isinstance(price_pipeline[0], ToStreamingData)
    # Converters - Quote (Custom)
    assert 'quote' in md.converters
    quote_pipeline = md.converters['quote']
    assert isinstance(quote_pipeline[0], ToStreamingData)
    
    # Check order_updates connection
    ou = config.connections['order_updates']
    assert ou.url == "wss://api.example.com/orders"
    assert ou.auth is not None
    assert ou.auth.mode == "url"
    
    # Converters - Order (ToObject)
    assert ou.converters is not None
    assert 'order' in ou.converters
    order_pipeline = ou.converters['order']
    assert isinstance(order_pipeline[0], ToObject)
    assert order_pipeline[0].target == Order

def test_streaming_config_minimal():
    spec = {
        "connections": {
            "simple": {
                "url": "ws://echo.websocket.org",
                "backend": {"type": "websocket"},
                "streams": ["data"],
                "subscribe": {"subscribe": {"format": "text", "text": "sub"}},
                "parser": {"format": "text"}
            }
        }
    }
    
    config = get_streaming_config(spec, registry)
    conn = config.connections['simple']
    
    assert conn.url == "ws://echo.websocket.org"
    assert conn.auth is None
    assert conn.router is None
    assert conn.converters is None

def test_streaming_config_invalid():
    # Missing required 'connections'
    spec = {}
    with pytest.raises(ValidationError):
        get_streaming_config(spec, registry)
        
    # Missing required 'url' in connection
    spec_bad_conn = {
        "connections": {
            "bad": {
                "backend": {"type": "websocket"}
            }
        }
    }
    with pytest.raises(ValidationError):
        get_streaming_config(spec_bad_conn, registry)

def test_message_def_binary_encoder():
    spec = {
        "format": "binary",
        "encoder": "custom_encoder"
    }
    
    # Test direct resolution of MessageDef via get_message_def (internal helper, but accessible implicitly via config parsing)
    # We'll construct a minimal config to trigger it
    full_spec = {
        "connections": {
            "binary_test": {
                "url": "ws://test",
                "backend": {"type": "websocket"},
                "streams": ["data"],
                "subscribe": {
                    "subscribe": spec
                },
                "parser": {"format": "binary"}
            }
        }
    }
    
    config = get_streaming_config(full_spec, registry)
    conn = config.connections['binary_test']
    assert conn.subscribe is not None
    msg_def = conn.subscribe['subscribe']
    assert msg_def is not None
    encoded = msg_def.resolve(data="hello")
    assert encoded == b"hello"

def test_streaming_auth_first_message():
    spec = {
        "connections": {
            "auth_test": {
                "url": "ws://test",
                "backend": {"type": "websocket"},
                "streams": ["data"],
                "auth": {
                    "mode": "first_message",
                    "first_message": {
                        "format": "text",
                        "text": "AUTH {token}",
                        "tokens": {"fields":{"token": {"source": "'secret'"}}}
                    }
                },
                "subscribe": {"subscribe": {"format": "text", "text": "sub"}},
                "parser": {"format": "text"}
            }
        }
    }
    
    config = get_streaming_config(spec, registry)
    auth = config.connections['auth_test'].auth
    
    assert auth is not None
    assert auth.mode == "first_message"
    assert auth.first_message is not None
    resolved_msg = auth.first_message.resolve()
    assert resolved_msg == "AUTH secret"
