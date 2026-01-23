import pytest
from blueshift.brokers.core.config.objects import get_objects_config, ObjectConfig
from blueshift.brokers.core.config.resolver import ConfigRegistry
from blueshift.lib.trades._order import Order
from blueshift.lib.trades._position import Position
from blueshift.lib.trades._accounts import Account
from blueshift.interfaces.assets.assets import asset_factory
from blueshift.interfaces.assets._assets import Asset
from blueshift.lib.trades._order_types import OrderSide, ProductType

@pytest.fixture
def registry():
    return ConfigRegistry()

def test_get_objects_config_empty(registry):
    config = {}
    objects_config = get_objects_config(config, registry)
    assert isinstance(objects_config, ObjectConfig)
    assert len(objects_config.asset) == 0
    assert len(objects_config.order) == 0
    assert len(objects_config.position) == 0
    assert len(objects_config.account) == 0

def test_order_conversion_pipeline(registry):
    # Order requires: quantity, side, asset
    config = {
        "order": [
            {
                "fields": {
                    "quantity": {"source": "data['qty']"},
                    "side": {"source": "0 if data['side']=='BUY' else 1"},
                    "price": {"source": "data['price']"},
                    # In real usage, 'asset' is usually passed in context or resolved separately.
                    # For Order.from_dict, if asset is in the dict, it uses it.
                    # Here we simulate passing asset in context to resolve, 
                    # but ToObject uses fields to build a dict for from_dict.
                    # So we need to put the asset into the resolved dict.
                    "asset": {"source": "asset"} 
                }
            }
        ]
    }
    
    objects_config = get_objects_config(config, registry)
    assert objects_config.order is not None
    
    asset = asset_factory(symbol="TEST")
    data = {"qty": 100, "side": "BUY", "price": 10.5, 'asset':asset}
    
    # Simulate context passed during resolution
    # Order.from_dict handles string side "BUY" -> OrderSide.BUY conversion
    result = None
    for rule in objects_config.order:
        result = rule.resolve(data=data, asset=asset)
        if isinstance(result, dict):
            result = Order.from_dict(result)
    
    assert isinstance(result, Order)
    assert result.quantity == 100
    assert result.side == OrderSide.BUY
    assert result.price == 10.5
    assert result.asset == asset

def test_position_conversion_pipeline(registry):
    # Position from_dict needs asset, quantity etc.
    # It also handles some field renaming/parsing inside from_dict if needed
    config = {
        "position": [
            {
                "fields": {
                    "quantity": {"source": "data['net_qty']"},
                    "asset": {"source": "asset"},
                    "side": {"source":"0 if data['side']=='BUY' else 1"},
                    "instrument_id": {"source": "asset.sid"},
                    "product_type": {"source": "1"},
                    "average_price": {"source": "data['avg_px']"},
                    "margin": {"source": "0"},
                    "timestamp": {"source": "None"},
                    "exchange_timestamp": {"source": "None"}
                }
            }
        ]
    }
    
    objects_config = get_objects_config(config, registry)
    assert objects_config.position is not None
    
    asset = asset_factory(symbol="TEST")
    data = {"net_qty": 50, "avg_px": 150.0, 'asset':asset, 'side':'BUY'}
    
    result = None
    for rule in objects_config.position:
        result = rule.resolve(data=data, asset=asset)
        if isinstance(result, dict):
            result = Position.from_dict(result)
    
    assert isinstance(result, Position)
    assert result.quantity == 50
    assert result.asset == asset
    assert result.product_type == ProductType.DELIVERY

def test_account_conversion_pipeline(registry):
    # Account from_dict valid keys: name, cash, margin...
    config = {
        "account": [
            {
                "fields": {
                    "name": {"source": "data['id']"},
                    "cash": {"source": "data['balance']"},
                }
            }
        ]
    }
    
    objects_config = get_objects_config(config, registry)
    assert objects_config.account is not None
    
    data = {"id": "ACCT123", "balance": 10000.0}
    
    result = None
    for rule in objects_config.account:
        result = rule.resolve(data=data)
        if isinstance(result, dict):
            result = Account.from_dict(result)
    
    assert isinstance(result, Account)
    assert result.name == "ACCT123"
    assert result.cash == 10000.0
    # Currency conversion handled in Account.from_dict if implemented correctly
    # Account.from_dict uses CCY[str] lookup

def test_pipeline_with_condition(registry):
    config = {
        "order": [
            {
                "condition": "data['status'] == 'ignore'",
                # This rule should be skipped if condition is false (wait, resolve checks condition)
                # If condition is true, it proceeds. If resolve returns NULL, it tries next rule.
                # Here we want to test:
                # 1. First rule matches but condition fails -> NULL -> next rule
                # 2. First rule matches -> returns object
                "fields": {
                    "quantity": {"source": "0"}
                }
            },
            {
                "fields": {
                    "quantity": {"source": "data['qty']"},
                    "side": {"source": "0 if data['side']=='BUY' else 1"},
                    "asset": {"source": "asset"}
                }
            }
        ]
    }
    
    objects_config = get_objects_config(config, registry)
    asset = asset_factory(symbol="TEST")
    
    # Case 1: First rule condition False (status != ignore)
    data = {"qty": 10, "side": "SELL", "status": "active","asset":asset}
    # First rule condition eval: 'active' == 'ignore' -> False
    # ToObject.resolve returns NULL
    # Pipeline tries next rule.
    # Second rule has no condition, executes.
    assert objects_config.order is not None

    result = None
    for rule in objects_config.order:
        result = rule.resolve(data=data, asset=asset)
        if isinstance(result, dict):
            result = Order.from_dict(result)

    assert isinstance(result, Order)
    assert result.quantity == 10

def test_pipeline_custom_function(registry):
    asset = asset_factory(symbol="TEST")
    def create_custom_order(data, asset, **kwargs):
        return Order(
            quantity=data['q'],
            side=OrderSide.BUY,
            asset=asset
        )
    
    registry.register("my_custom_order")(create_custom_order)
    
    config = {
        "order": [
            {
                "custom": "my_custom_order"
            }
        ]
    }
    
    objects_config = get_objects_config(config, registry)
    data = {"q": 99}
    
    assert len(objects_config.order) > 0

    result = None
    for rule in objects_config.order:
        result = rule.resolve(data=data, asset=asset)
        if isinstance(result, dict):
            result = Order.from_dict(result)

    assert isinstance(result, Order)
    assert result.quantity == 99
