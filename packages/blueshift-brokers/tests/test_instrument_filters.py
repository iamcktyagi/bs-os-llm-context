import pytest
from blueshift.brokers.core.config.instruments import AssetDef
from blueshift.brokers.core.config.resolver import ConfigRegistry

registry = ConfigRegistry()

def test_asset_def_single_filter():
    spec = {
        "filter": "row['type'] == 'EQ'",
        "mapping": {"symbol": {"source": "row['sym']"}}
    }
    
    asset_def = AssetDef.from_config(spec, registry)
    
    # Should pass
    row_pass = [{"type": "EQ", "sym": "AAPL"}]
    res = asset_def.resolve(row_pass)
    assert res is not None
    assert res[0]['symbol'] == "AAPL"
    
    # Should fail
    row_fail = [{"type": "OPT", "sym": "AAPL"}]
    res = asset_def.resolve(row_fail)
    assert len(res) == 0

def test_asset_def_multiple_filters():
    spec = {
        "filter": [
            "row['type'] == 'EQ'",
            "row['active'] == True"
        ],
        "mapping": {"symbol": {"source": "row['sym']"}}
    }
    
    asset_def = AssetDef.from_config(spec, registry)
    
    # Should pass (both true)
    row_pass = [{"type": "EQ", "active": True, "sym": "GOOG"}]
    res = asset_def.resolve(row_pass)
    assert len(res) > 0
    assert res[0]['symbol'] == "GOOG"
    
    # Should fail (first false)
    row_fail_1 = [{"type": "OPT", "active": True, "sym": "GOOG"}]
    assert len(asset_def.resolve(row_fail_1)) == 0
    
    # Should fail (second false)
    row_fail_2 = [{"type": "EQ", "active": False, "sym": "GOOG"}]
    assert len(asset_def.resolve(row_fail_2)) == 0

def test_asset_def_no_filter():
    spec = {
        "mapping": {"symbol": {"source": "row['sym']"}}
    }
    asset_def = AssetDef.from_config(spec, registry)
    
    row = [{"sym": "MSFT"}]
    res = asset_def.resolve(row)
    assert len(res) > 0
    assert res[0]['symbol'] == "MSFT"
