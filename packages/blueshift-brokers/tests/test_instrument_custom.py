import pytest
from blueshift.brokers.core.config.instruments import AssetDef
from blueshift.brokers.core.config.resolver import ConfigRegistry

registry = ConfigRegistry()
register = registry.register

@register()
def my_custom_mapper(row, **kwargs):
    if row.get('status') != 'active':
        return None
    return {
        'symbol': row['sym'],
        'name': row['name'],
        'custom_flag': True
    }

def test_asset_def_custom_function():
    spec = {
        "custom": "my_custom_mapper"
    }
    
    asset_def = AssetDef.from_config(spec, registry)
    
    # Test pass case
    row_pass = [{"status": "active", "sym": "AAPL", "name": "Apple Inc."}]
    res = asset_def.resolve(row_pass)
    assert res is not None
    assert res[0]['symbol'] == "AAPL"
    assert res[0]['custom_flag'] is True
    
    # Test filter case (handled inside custom function)
    row_fail = [{"status": "inactive", "sym": "GOOG", "name": "Alphabet"}]
    res = asset_def.resolve(row_fail)
    assert len(res) == 0

def test_asset_def_custom_function_with_declarative_filter():
    # Both filters apply: declarative first, then custom
    spec = {
        "filter": "row['idx'] > 10",
        "custom": "my_custom_mapper"
    }
    
    asset_def = AssetDef.from_config(spec, registry)
    
    # Pass both
    row_pass = [{"idx": 20, "status": "active", "sym": "MSFT", "name": "Microsoft"}]
    res = asset_def.resolve(row_pass)
    assert res is not None
    assert res[0]['symbol'] == "MSFT"
    
    # Fail declarative filter
    row_fail_1 = [{"idx": 5, "status": "active", "sym": "MSFT", "name": "Microsoft"}]
    assert len(asset_def.resolve(row_fail_1)) == 0
    
    # Fail custom filter
    row_fail_2 = [{"idx": 20, "status": "inactive", "sym": "MSFT", "name": "Microsoft"}]
    assert len(asset_def.resolve(row_fail_2)) == 0
