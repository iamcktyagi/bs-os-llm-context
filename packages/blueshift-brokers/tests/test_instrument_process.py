import pytest
import pandas as pd
from blueshift.brokers.core.config.instruments import AssetDef
from blueshift.brokers.core.config.resolver import ConfigRegistry

registry = ConfigRegistry()

def test_asset_def_process_list():
    spec = {
        "filter": "row['val'] > 10",
        "mapping": {"v": {"source": "row['val']"}}
    }
    asset_def = AssetDef.from_config(spec, registry)
    
    data = [{"val": 5}, {"val": 15}, {"val": 20}]
    res = asset_def.resolve(data)
    
    assert len(res) == 2
    assert res[0]['v'] == 15
    assert res[1]['v'] == 20

def test_asset_def_process_dataframe():
    spec = {
        "filter": "row['val'] > 10",
        "mapping": {"v": {"source": "row['val'] * 2"}}
    }
    asset_def = AssetDef.from_config(spec, registry)
    
    data = pd.DataFrame({"val": [5, 15, 20]})
    res = asset_def.resolve(data)
    
    assert isinstance(res, list)
    assert len(res) == 2
    assert res[0]['v'] == 30
    assert res[1]['v'] == 40

def test_asset_def_process_dataframe_multiple_filters():
    spec = {
        "filter": ["row['val'] > 10", "row['active'] == True"],
        "mapping": {"v": {"source": "row['val']"}}
    }
    asset_def = AssetDef.from_config(spec, registry)
    
    data = pd.DataFrame({
        "val": [5, 15, 20, 25],
        "active": [True, True, False, True]
    })
    res = asset_def.resolve(data)
    
    assert isinstance(res, list)
    assert len(res) == 2
    # 5 -> Fail val
    # 15 -> Pass
    # 20 -> Fail active
    # 25 -> Pass
    assert res[0]['v'] == 15
    assert res[1]['v'] == 25

def test_asset_def_process_dataframe_custom():
    #registry.register("my_df_mapper")(lambda data, **kwargs: data[data['val'] > 5])
    registry.register("my_df_mapper")(lambda row, **kwargs: row if row['val']> 5 else None)
    
    spec = {
        "custom": "my_df_mapper"
    }
    asset_def = AssetDef.from_config(spec, registry)
    
    data = pd.DataFrame({"val": [1, 10]})
    res = asset_def.resolve(data)
    
    assert len(res) == 1
    assert res[0]['val'] == 10
