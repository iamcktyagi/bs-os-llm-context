from __future__ import annotations
from typing import TYPE_CHECKING, Any, cast

from blueshift.lib.trades._order import Order
from blueshift.lib.trades._position import Position
from blueshift.lib.trades._accounts import Account
from blueshift.lib.trades._order_types import PositionSide, ProductType
from blueshift.interfaces.assets.assets import asset_factory
from blueshift.interfaces.assets._assets import MarketData
from blueshift.lib.exceptions import ValidationError
from blueshift.lib.common.functions import to_enum

if TYPE_CHECKING:
    from blueshift.interfaces.assets._assets import Asset
    from .broker import RestAPIBroker

def create_order(broker: RestAPIBroker, data: dict[str, Any], asset: Asset|None=None) -> Order:
    """
    Create an Order object from API data using the configured conversion pipeline.

    Args:
        broker: The broker instance.
        asset: The asset associated with the order.
        data: The data dictionary from the API.

    Returns:
        Order: The created Blueshift Order object.

    Raises:
        ValidationError: If the order cannot be created.
    """
    pipeline = broker.config.objects.order
    security_id = data.pop('security_id', None)
    symbol = data.pop('symbol', None)
    
    if asset is None:
        try:
            if security_id:
                asset = cast(Asset, broker.asset_finder.sid(security_id))
        except Exception:
            try:
                if symbol:
                    asset = cast(Asset, broker.asset_finder.symbol(symbol))
            except Exception:
                pass

    if not asset:
        raise ValidationError(f'cannot create order - no asset specified, nor can be inferred.')

    try:
        # The pipeline expects context variables. We pass the data as 'data'
        # so fields can reference it (e.g. source="data['id']").
        # We also pass 'asset' and 'broker' to the context.
        err = None

        if not pipeline:
            data['asset'] = asset
            return Order.from_dict(data)
        else:
            for rule in pipeline:
                result = rule.resolve(asset=asset, data=data)
                
                if isinstance(result, Order):
                    return result
                if isinstance(result, dict):
                    # Ensure asset is in the dict for Order.from_dict
                    if 'asset' not in result:
                        result['asset'] = asset
                    try:
                        return Order.from_dict(result)
                    except Exception as e:
                        err = e
                        continue
            
        err = str(err) if err else "unexpected return type from conversion rule"
        raise ValidationError(f'could not convert to order, no conversion rules or none succeeded:{err}.')
    except Exception as e:
        raise ValidationError(f"Failed to create order from data: {e}") from e
    

def _process_position_packet(broker, asset, packet):
    required = ['quantity', 'average_price', 'last_price']
    for k in required:
        if k not in packet:
            raise ValidationError(f'cannot create position - missing one or more of required keys {required}')
    
    packet['quantity'] = float(packet['quantity'])
    packet['margin'] = float(packet.get('margin', 0.0))
    packet['asset'] = packet.get('asset', asset)
    packet['instrument_id'] = packet.get('instrument_id', None)
    packet['side'] = PositionSide.LONG if packet['quantity'] > 0 else PositionSide.SHORT
    packet['timestamp'] = packet.get('timestamp', None)
    packet['exchange_timestamp'] = packet.get('exchange_timestamp', packet['timestamp'])
    packet['product_type'] = to_enum(
        packet.get('product_type', broker.supported_products[0]), ProductType)
    
    packet['buy_price'] = packet['buy_quantity'] = packet['sell_price'] = packet['sell_quantity'] = 0
    if packet['quantity'] >= 0:
        packet['buy_price'] = packet['average_price']
        packet['buy_quantity'] = packet['quantity']
    else:
        packet['sell_price'] = packet['average_price']
        packet['sell_quantity'] = abs(packet['quantity'])

    return packet


def create_position(broker: RestAPIBroker, data: dict[str, Any], asset: Asset|None=None) -> Position:
    """
    Create an Position object from API data using the configured conversion pipeline.

    Args:
        broker: The broker instance.
        asset: The asset associated with the Position.
        data: The data dictionary from the API.

    Returns:
        Position: The created Blueshift Position object.

    Raises:
        ValidationError: If the position cannot be created.
    """
    pipeline = broker.config.objects.position
    security_id = data.pop('security_id', None)
    symbol = data.pop('symbol', None)
    
    if asset is None:
        try:
            if security_id:
                asset = cast(Asset, broker.asset_finder.sid(security_id))
        except Exception:
            try:
                if symbol:
                    asset = cast(Asset, broker.asset_finder.symbol(symbol))
            except Exception:
                pass

    if not asset:
        raise ValidationError(f'cannot create position - no asset specified, nor can be inferred.')

    try:
        # The pipeline expects context variables. We pass the data as 'data'
        # so fields can reference it (e.g. source="data['id']").
        # We also pass 'asset' and 'broker' to the context.
        err = None
        for rule in pipeline:
            result = rule.resolve(asset=asset, data=data)
            
            if isinstance(result, Position):
                return result
            
            if isinstance(result, dict):
                result = _process_position_packet(broker, asset, result)
                
                try:
                    return Position.from_dict(result)
                except Exception as e:
                    err = e
                    continue
            
        err = str(err) if err else "unexpected return type from conversion rule"
        raise ValidationError(f'could not convert to position, no conversion rules or none succeeded:{err}.')
    except Exception as e:
        raise ValidationError(f"Failed to create position from data: {e}") from e
    
def create_account(broker: RestAPIBroker, data: dict[str, Any]) -> Account:
    """
    Create an Account object from API data using the configured conversion pipeline.

    Args:
        broker: The broker instance.
        data: The data dictionary from the API.

    Returns:
        Account: The created Blueshift Account object.

    Raises:
        ValidationError: If the account cannot be created.
    """
    pipeline = broker.config.objects.account

    try:
        # The pipeline expects context variables. We pass the data as 'data'
        # so fields can reference it (e.g. source="data['id']").
        # We also pass 'asset' and 'broker' to the context.
        err = None

        if not pipeline:
            return Account.from_dict(data)
        else:
            for rule in pipeline:
                result = rule.resolve(data=data)
                
                if isinstance(result, Account):
                    return result
                
                if isinstance(result, dict):
                    try:
                        return Account.from_dict(result)
                    except Exception as e:
                        err = e
                        continue
            
        err = str(err) if err else "unexpected return type from conversion rule"
        raise ValidationError(f'could not convert to account, no conversion rules or none succeeded:{err}')
    except Exception as e:
        raise ValidationError(f"Failed to create account from data: {e}") from e

def create_asset(broker: RestAPIBroker, data: dict[str, Any]) -> MarketData:
    """
    Create an asset object from API data using the configured conversion pipeline. Note, we 
    primarily fallback on the broker methods `symbol` and `sid`, but in case they fail for a 
    given scenario, this method will be invoked (see broker method `infer_asset`).

    Args:
        broker: The broker instance.
        data: The data dictionary from the API.

    Returns:
        MarketData: The created Blueshift asset object.

    Raises:
        ValidationError: If the asset cannot be created.
    """
    pipeline = broker.config.objects.asset

    try:
        # The pipeline expects context variables. We pass the data as 'data'
        # so fields can reference it (e.g. source="data['id']").
        # We also pass 'broker' to the context.
        err = None

        if not pipeline:
            asset_type = data.pop('asset_type', None)
            if not asset_type:
                    raise ValidationError(f'no asset type provided.')
            
            return asset_factory(asset_type, **data)

        for rule in pipeline:
            result = rule.resolve(data=data)
            
            if isinstance(result, MarketData):
                return result
            
            if isinstance(result, dict):
                asset_type = result.pop('asset_type', None)
                if not asset_type:
                    raise ValidationError(f'no asset type provided.')
                
                try:
                    return asset_factory(asset_type, **result)
                except Exception as e:
                    err = e
                    continue
            
        err = str(err) if err else "unexpected return type from conversion rule"
        raise ValidationError(f'could not convert to asset, no conversion rules or none succeeded:{err}')
    except Exception as e:
        raise ValidationError(f"Failed to create account from data: {e}") from e

