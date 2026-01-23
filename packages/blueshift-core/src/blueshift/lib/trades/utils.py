from __future__ import annotations
from typing import cast
from blueshift.lib.trades._order_types import ProductType, OrderSide
from blueshift.lib.trades._position import Position
from blueshift.interfaces.assets._assets import Asset
from blueshift.interfaces.assets.assets import DefaultAssetFinder, IAssetFinder
from blueshift.lib.exceptions import ValidationError

def create_position(**kwargs):
    """create positions from input data """
    import pandas as pd

    data = kwargs
    
    tz = data.pop('tz', None)
    asset_finder = data.pop('asset_finder', None)
    if not asset_finder:
        asset_finder = DefaultAssetFinder('default')
    
    asset_finder = cast(IAssetFinder, asset_finder)
    try:
        asset = data['asset']
    except Exception:
        msg = f'Failed to create position, missing asset.'
        raise ValidationError(msg)
        
    if not isinstance(asset, Asset):
        asset = str(asset)
        product_type=data.get('product_type')
        if product_type:
            try:
                if isinstance(product_type, str):
                    product_type = ProductType[product_type.upper()]
                else:
                    product_type = ProductType(product_type)
            except Exception:
                msg = f'Failed to create position, invalid product type:{product_type}'
                raise ValidationError(msg)
            else:
                asset = asset_finder.symbol(asset, product_type=product_type)
                asset = cast(Asset, asset)
        else:
            asset = asset_finder.symbol(asset)
            asset = cast(Asset, asset)
            product_type = asset.get_product_type()
    else:
        product_type = asset.get_product_type()
        
    try:
        quantity = float(data['quantity'])
    except Exception:
        msg = f'Failed to create position, missing quantity.'
        raise ValidationError(msg)
    else:
        side = OrderSide.BUY if quantity > 0 else OrderSide.SELL
        
    try:
        average_price = data.get('entry_price', None)
        average_price = data.get('price', average_price)
        average_price = float(average_price) # type: ignore
    except Exception:
        msg = f'Failed to create position, missing entry price.'
        raise ValidationError(msg)
        
    instrument_id = data.get('instrument_id', -1)
    margin = data.get('margin',0)
    timestamp = data.get('timestamp',0)
    exchange_timestamp = data.get('entry_timestamp',0)
    exchange_timestamp = data.get('exchange_timestamp',exchange_timestamp)
    last_fx = data.get('fx_rate',1.0)
    last_fx = data.get('last_fx',last_fx)
    last_price = data.get('last_price',None)
    underlying_price = data.get('underlying_price',0)
    fractional = data.get('fractional',False)
    
    if not timestamp:
        timestamp = pd.Timestamp.now(tz=tz)
    else:
        timestamp = pd.Timestamp(timestamp)
        if tz and not timestamp.tz:
            timestamp = timestamp.tz_localize(tz=tz)
        elif tz:
            timestamp = timestamp.tz_convert(tz=tz)
    
    if not exchange_timestamp:
        exchange_timestamp = timestamp
    else:
        exchange_timestamp = pd.Timestamp(exchange_timestamp)
        if tz and not exchange_timestamp.tz:
            exchange_timestamp = exchange_timestamp.tz_localize(tz=tz)
        elif tz:
            exchange_timestamp = exchange_timestamp.tz_convert(tz=tz)
        
        
    if side == OrderSide.BUY:
        buy_quantity = abs(quantity)
        buy_price = average_price
        sell_quantity = 0
        sell_price = 0
    else:
        buy_quantity = 0
        buy_price = 0
        sell_quantity = abs(quantity)
        sell_price = average_price
        
    if last_price is not None:
        side = -1
    else:
        quantity = abs(quantity)
        last_price = 0
        
    pos = Position(
            asset, quantity, side, instrument_id, product_type=product_type,
            average_price=average_price, margin=margin, timestamp=timestamp,
            exchange_timestamp=exchange_timestamp, buy_quantity=buy_quantity,
            buy_price=buy_price, sell_quantity=sell_quantity, 
            sell_price=sell_price, last_price=last_price, last_fx=last_fx,
            underlying_price=underlying_price, fractional=fractional)

    return pos

def merge_positions(positions:dict[Asset, Position], *args, check_duplicates:bool=True):
    """ merge positions by asset from input portfolios. """
    def check_duplicated(pos1:Position, pos2:Position) -> bool:
        # asset is g'teed to be the same, we check if the position is
        # different in qty or product_type, if yes, we add to the 
        # position, else we retain one
        if pos1.quantity == pos2.quantity and pos1.product_type == pos2.product_type:
            return True
        return False
    
    out = positions.copy()

    for arg in args:
        assets = list(out.keys())
        if not isinstance(arg, dict):
            continue
        holdings = {k:v for k,v in arg.items() if isinstance(k, Asset) and isinstance(v, Position)}
        if not holdings:
            continue

        for asset in assets:
            if asset in holdings:
                pos, hold = out[asset], holdings[asset]
                if check_duplicates and check_duplicated(pos, hold):
                    # keep the position in positions container
                    holdings.pop(asset)
                else:
                    # merge the positions
                    pos.add_to_position(hold)
                    holdings.pop(asset)
                    out[asset] = pos
                
        out = {**out, **holdings}

    return out
