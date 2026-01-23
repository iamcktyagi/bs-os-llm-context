from __future__ import annotations
from typing import TYPE_CHECKING, Callable
import json
from collections import OrderedDict, deque
import os

from blueshift.lib.trades._order import Order
from blueshift.lib.trades._position import Position
from blueshift.lib.trades._accounts import BlotterAccount
from blueshift.interfaces.assets._assets import Asset
from blueshift.interfaces.assets.assets import asset_factory, IAssetFinder
from .json import BlueshiftJSONEncoder
from blueshift.lib.exceptions import DataWriteException, DataReadException

if TYPE_CHECKING:
    import pandas as pd
else:
    import blueshift.lib.common.lazy_pandas as pd

def read_positions_from_dict(positions_dict:dict, asset_finder:IAssetFinder) -> dict[Asset, Position]:
    """read saved position data from a dict keyed by asset symbol """
    current_pos = {}
    for sym in positions_dict:
        pos = positions_dict[sym]
        product_type=pos.get('product_type')
        asset = asset_finder.symbol(sym, product_type=product_type)
        pos['asset'] = asset
        pos['timestamp'] = pd.Timestamp(pos['timestamp'])
        
        try:
            pos['entry_time'] = pd.Timestamp(pos['entry_time'])
        except Exception:
            pos['entry_time'] = pos['timestamp']
        
        position = Position.from_dict(positions_dict[sym])
        current_pos[asset] = position

    return current_pos

def read_transactions_from_dict(txns_dict:dict, asset_finder:IAssetFinder,
                                key_transform:Callable=lambda x:x, **kwargs) -> tuple[dict, set]:
    """ read saved txns data from a timestamped ordered dict of transactions """
    txns = OrderedDict()
    order_ids = set()
    for key in txns_dict:
        values = txns_dict[key]
        transactions = []
        for value in values:
            product_type=value.get('product_type')
            asset = asset_finder.symbol(value['asset'], product_type=product_type)
            value['asset'] = asset
            transactions.append(Order.from_dict(value))
            order_ids.add(value['oid'])
        if key_transform:
            txns[key_transform(key, **kwargs)] = transactions
        else:
            txns[key] = transactions

    return txns, order_ids

def read_orders(orders:dict, asset_finder:IAssetFinder, key_transform:Callable=str) -> tuple[dict, set]:
    """ read saved order data from a order_id keyed dictionary of orders """
    out = {}
    order_ids = set()
    for key in orders:
        order = orders[key]
        product_type=order.get('product_type')
        asset = asset_finder.symbol(order['asset'], product_type=product_type)
        order['asset'] = asset
        if key_transform:
            out[key_transform(key)] = Order.from_dict(order)
        else:
            out[key] = Order.from_dict(order)

        order_ids.add(key)

    return out, order_ids

def save_portfolio_to_disk(positions_dict:dict, path:str|None=None):
    """ Save a portfolio - dict of positions keyed by assets - to disk. """
    if not isinstance(positions_dict, dict):
        msg = f'expected positions as dict, got {type(positions_dict)}.'
        raise DataWriteException(msg)
        
    out = []
    for asset, pos in positions_dict.items():
        item = (asset.to_dict(), pos.to_json())
        out.append(item)
        
    results = {'version':'2.1','type':'portfolio','data':out}
        
    if path:
        if not path.endswith('.json'):
            path += '.json'
        with open(path, 'w') as fp:
            json.dump(results, fp, cls=BlueshiftJSONEncoder)
    else:
        return json.dumps(results, cls=BlueshiftJSONEncoder)
    
def save_roundtrips_to_disk(round_trips:list|deque, path:str|None=None):
    """ Save a roundtrips - list of closed positions - to disk. """
    if not isinstance(round_trips, (list, deque)):
        msg = f'expected positions as list, got {type(round_trips)}.'
        raise DataWriteException(msg)
        
    # roundtrips is a list of dict objects, we can directly save it   
    results = {'version':'2.1','type':'roundtrips','data':list(round_trips)}
        
    if path:
        if not path.endswith('.json'):
            path += '.json'
        with open(path, 'w') as fp:
            json.dump(results, fp, cls=BlueshiftJSONEncoder)
    else:
        return json.dumps(results, cls=BlueshiftJSONEncoder)
        
def save_orderbook_to_disk(orders_dict:dict, path:str|None=None):
    """ Save an orderbook - dict of orders keyed by order id - to disk. """
    if not isinstance(orders_dict, dict):
        msg = f'expected orders as dict, got {type(orders_dict)}.'
        raise DataWriteException(msg)
        
    out = []
    for oid, order in orders_dict.items():
        item = (order.asset.to_dict(), order.to_json())
        out.append(item)
        
    results = {'version':'2.1','type':'orderbook','data':out}
        
    if path:
        if not path.endswith('.json'):
            path += '.json'
        with open(path, 'w') as fp:
            json.dump(results, fp, cls=BlueshiftJSONEncoder)
    else:
        return json.dumps(results, cls=BlueshiftJSONEncoder)
    
def save_transactions_to_disk(txns:dict, path:str|None=None):
    """ Save transactions - dict of orders list keyed by date - to disk. """
    if not isinstance(txns, dict):
        msg = f'expected positions as dict, got {type(txns)}.'
        raise DataWriteException(msg)
        
    out = {}
    for dt in txns:
        details = []
        for order in txns[dt]:
            item = (order.asset.to_dict(), order.to_json())
            details.append(item)
        dt = str(pd.Timestamp(dt))
        out[dt] = details
        
    results = {'version':'2.1','type':'transactions','data':out}
        
    if path:
        if not path.endswith('.json'):
            path += '.json'
        with open(path, 'w') as fp:
            json.dump(results, fp, cls=BlueshiftJSONEncoder)
    else:
        return json.dumps(results, cls=BlueshiftJSONEncoder)
        
def save_blotter_account_to_disk(account:BlotterAccount, path:str|None=None):
    if not isinstance(account, BlotterAccount):
        msg = f'expected a blotter account, got {type(account)}.'
        raise DataWriteException(msg)
        
    results = {'version':'2.1','type':'account','data':account.to_dict()}
    
    if path:
        if not path.endswith('.json'):
            path += '.json'
        with open(path, 'w') as fp:
            json.dump(results, fp, cls=BlueshiftJSONEncoder)
    else:
        return json.dumps(results, cls=BlueshiftJSONEncoder)
    
def _read_and_validate_data(path, type_) -> dict:
    if isinstance(path, str) and os.path.exists(path):
        with open(path) as fp:
            data = json.load(fp)
    elif isinstance(path, dict):
        data = path
    else:
        msg = f'illegal data or file path {path}.'
        raise DataReadException(msg)
        
    if 'version' not in data or data['version'] != '2.1':
        msg = f'missing or unknown version.'
        raise DataReadException(msg)
        
    if 'type' not in data or data['type'] != type_:
        msg = f'data is not of type {type_}.'
        raise DataReadException(msg)
        
    if 'data' not in data:
        msg = f'missing details.'
        raise DataReadException(msg)
        
    return data

def read_portfolio_from_disk(path:str, asset_finder:IAssetFinder|None=None):
    """ Read a portfolio - dict of positions keyed by assets - from disk. """
    try:
        data = _read_and_validate_data(path, 'portfolio')
    except Exception as e:
        msg = f'Failed to read positions data from disk:{str(e)}.'
        raise DataReadException(msg)
        
    positions_dict = data.get('data', [])
    positions = {}
    try:
        for item in positions_dict:
            if not asset_finder:
                asset = asset_factory(**item[0])
            else:
                sym = item[0]['symbol']
                exchange = item[0]['exchange_name']
                if ':' not in sym and exchange is not None:
                    sym = exchange + ':' + sym
                product_type = item[1].get('product_type', None)
                try:
                    asset = asset_finder.symbol(sym, product_type=product_type)
                except Exception:
                    asset = asset_factory(**item[0])
            item[1]['asset'] = asset
            pos = Position.from_dict(item[1])
            positions[asset] = pos
    except Exception as e:
        msg = f'Failed to convert positions data from disk:{str(e)}.'

    return positions

def read_roundtrips_from_disk(path:str):
    try:
        data = _read_and_validate_data(path, 'roundtrips')
    except Exception as e:
        msg = f'Failed to read roundtrips data from disk:{str(e)}.'
        raise DataReadException(msg)
        
    round_trips = data.get('data', [])
    # round trips are dict objects, we can directly return it

    return round_trips

def read_orderbook_from_disk(path:str, asset_finder:IAssetFinder|None=None):
    """ Read an orderbook - dict of orders keyed by order id - from disk. """
    try:
        data = _read_and_validate_data(path, 'orderbook')
    except Exception as e:
        msg = f'Failed to read orderbook data from disk:{str(e)}.'
        raise DataReadException(msg)
        
    orders_dict = data.get('data',[])
    orders = {}
    try:
        for item in orders_dict:
            if not asset_finder:
                asset = asset_factory(**item[0])
            else:
                sym = item[0]['symbol']
                exchange = item[0]['exchange_name']
                if ':' not in sym and exchange is not None:
                    sym = exchange + ':' + sym
                product_type = item[1].get('product_type', None)
                try:
                    asset = asset_finder.symbol(sym, product_type=product_type)
                except Exception:
                    asset = asset_factory(**item[0])
            item[1]['asset'] = asset
            order = Order.from_dict(item[1])
            orders[order.oid] = order
    except Exception as e:
        msg = f'Failed to convert orderbook data from disk:{str(e)}.'
        raise DataReadException(msg)

    return orders

def read_transactions_from_disk(path:str, asset_finder:IAssetFinder|None=None):
    try:
        data = _read_and_validate_data(path, 'transactions')
    except Exception as e:
        msg = f'Failed to read orderbook data from disk:{str(e)}.'
        raise DataReadException(msg)
        
    transactions = data.get('data', {})
    txns = {}
    
    for dt, entry in transactions.items():
        dt = pd.Timestamp(dt)
        order_list = []
        for item in entry:
            if not asset_finder:
                asset = asset_factory(**item[0])
            else:
                sym = item[0]['symbol']
                exchange = item[0]['exchange_name']
                if ':' not in sym and exchange is not None:
                    sym = exchange + ':' + sym
                product_type = item[1].get('product_type', None)
                try:
                    asset = asset_finder.symbol(sym, product_type=product_type)
                except Exception:
                    asset = asset_factory(**item[0])
            item[1]['asset'] = asset
            order = Order.from_dict(item[1])
            order_list.append(order)
        txns[dt] = order_list
        
    return txns

def read_blotter_account_from_disk(path:str):
    try:
        data = _read_and_validate_data(path, 'account')
    except Exception as e:
        msg = f'Failed to read account data from disk:{str(e)}.'
        raise DataReadException(msg)
        
    account_dict = data['data']
    account = BlotterAccount.from_dict(account_dict)
    return account