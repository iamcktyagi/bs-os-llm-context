from typing import cast, List

from blueshift.interfaces.assets._assets import MarketData
from blueshift.lib.common.types import ListLike
from blueshift.interfaces.assets.assets import asset_factory
from blueshift.interfaces.assets.assetdb import AssetDBDriver, AssetDBBackend, register_assetdb_driver

class DictDBDriver(AssetDBDriver):
    """ Asset metadata interface using dictionary. """
    def __init__(self, assets, supported_assets=None, supported_exchanges=None):
        self._url = None
        self._backend = AssetDBBackend.DICT
        self._metadata = {}
        self._membership_maps = {}
        self._exchanges = set()
        
        if isinstance(assets, dict):
            table = assets
        else:
            table = dict(assets)
            
        self._table:dict[str, MarketData] = {asset.exchange_ticker:asset for asset in table.values()}
            
        self._asset_types = [type(asset) for key, asset in self._table.items()]
        self._exchanges = set([asset.exchange_name for key, asset in self._table.items()])
        self._supported_exchanges = supported_exchanges or []
        
        supported_assets = supported_assets or []
        for cls in supported_assets:
            if cls not in self._asset_types:
                self._asset_types.append(cls)
        self._asset_types = list(set(self._asset_types))
        
    def __str__(self):
        return f"Blueshift AssetDB Driver[{self.backend}]"
    
    @property
    def default_exchange(self):
        if self._supported_exchanges:
            return self._supported_exchanges[0]
        elif self._exchanges:
            return sorted(self._exchanges)[0]
        else:
            return "any"
    
    def list_assets(self):
        """ List supported assets - support all assets as it is schema-less."""
        #return list(ASSET_DESCRIPTIONS.keys())
        return self._asset_types
    
    def connect(self, *args, **kwargs):
        pass
        
    def initialize(self, *args, **kwargs):
        self._table = {}
        
    def list(self):
        return {'main': len(self._table)}
    
    def exists(self, symbols, table=None, key=None):
        """ 
            Check if symbol exists, `table` and `key` arguments 
            are ignored.
        """
        if not isinstance(symbols, ListLike):
            return self.search(symbols) is not None
        
        symbols = cast(list, symbols)
        return [self.search(sym) is not None for sym in symbols]
    
    def find(self, assets, key=None, convert=True):
        """ 
            Find matching assets in db, `key` and `convert` arguments 
            are ignored.
        """
        if not isinstance(assets, ListLike):
            assets = cast(MarketData,assets)
            return self._table.get(assets.exchange_ticker, None)
        
        assets = cast(list, assets)
        return [self._table.get(asset.exchange_ticker, None) for asset in assets]
    
    def search(self, symbols=None, **kwargs) -> List[MarketData]:
        """ 
            Find matching symbols in db.
        """
        if symbols is None:
            return list(self._table.values())
        
        def _find_sym(sym:str):
            if ":" not in sym:
                exchanges = self._supported_exchanges or self._exchanges
                if exchanges:
                    for exchange in self._exchanges:
                        symbol = f"{exchange}:{sym}"
                        asset = self._table.get(symbol, None)
                        if asset:
                            return asset
                else:
                    symbol = f"{self.default_exchange}:{sym}"
                    asset = self._table.get(symbol, None)
                    if asset:
                        return asset
            else:
                return self._table.get(sym, None)
        
        
        if not isinstance(symbols, ListLike):
            symbols = cast(str, symbols)
            asset = _find_sym(symbols)
            if asset:
                return [asset]
            return []
        
        assets = [_find_sym(sym) for sym in symbols]
        return [asset for asset in assets if asset is not None]
    
    def fuzzy_search(self, symbols, **kwargs):
        """ 
            Find matching symbols in db.
        """
        return self.search(symbols)
        
    def add(self, asset):
        """ add an asset to the database. """
        self._table[asset.exchange_ticker] = asset
        
        if asset.exchange_name:
            self._exchanges.add(asset.exchange_name)
        if asset.exchange_name not in self._supported_exchanges:
            self._supported_exchanges.append(asset.exchange_name)
        
    def update(self, assets, data={}, key=None):
        """ Update existing assets. Arguments `data` and `key` ignored. """
        if not isinstance(assets, ListLike):
            assets = cast(MarketData,assets)
            if assets.exchange_ticker not in self._table:
                return
            self._table[assets.exchange_ticker] = assets
        else:
            assets = cast(list, assets)
            for asset in assets:
                if asset.exchange_ticker not in self._table:
                    continue
                self._table[asset.exchange_ticker] = asset
        
    def upsert(self, assets, key=None):
        """ Update or insert asset objects, argument `key` ignored. """
        if not isinstance(assets, ListLike):
            assets = cast(MarketData,assets)
            self._table[assets.exchange_ticker] = assets
        else:
            assets = cast(list, assets)
            for asset in assets:
                self._table[asset.exchange_ticker] = asset
        
    def rename(self, from_, to, long_name=None, table=None,
               dependency_field='underlying'):
        """ 
            Rename an existing asset and returns a list of dependent
            assets that are affected.
        """
        from_asset = self.search(from_)
        to_asset = self.search(to)
        if from_asset is None or to_asset is None:
            return []
        
        from_key = from_asset.exchange_ticker   # type: ignore
        to_key = to_asset.exchange_ticker       # type: ignore
        
        asset = self._table.pop(from_key, None)
        if not asset:
            return []
        data = asset.to_dict()
        data['class']=type(asset)
        data['symbol']=to
        if long_name:
            data['name'] = long_name
        asset = asset_factory(**data)
        self._table[to_key] = asset
        
        deps = [sym for sym in self._table if \
                getattr(self._table[sym],dependency_field, None) == from_]
        
        for dep in deps:
            asset = self._table[dep]
            data = asset.to_dict()
            data['class']=type(asset)
            data[dependency_field]=to
            if 'root' in data and from_ in data['root']:
                data['root'] = data['root'].replace(from_, to)
            asset = asset_factory(**data)
            self._table[to_key] = asset
        
        return deps
        
    def remove(self, symbol, table=None, dependency_field='underlying'):
        asset = self.search(symbol)
        if not asset:
            return []
        
        sym_key = asset.exchange_ticker # type: ignore
        asset = self._table.pop(sym_key, None)
        if not asset:
            return []
        
        deps = [sym for sym in self._table if \
                getattr(self._table[sym],dependency_field, None) == symbol]
        
        for dep in deps:
            self._table.pop(dep, None)
        
        return deps
    
register_assetdb_driver('dict', DictDBDriver)