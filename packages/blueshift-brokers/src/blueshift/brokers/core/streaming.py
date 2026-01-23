from __future__ import annotations
from typing import TYPE_CHECKING, Any, cast, Callable, Generator

from blueshift.interfaces.data.source import data_source_factory
from blueshift.interfaces.data.ingestor import ingestor_factory, StreamingIngestor
from blueshift.interfaces.assets._assets import Asset
from blueshift.lib.trades._order import Order
from blueshift.lib.common.enums import AlgoCallBack
from blueshift.lib.common.sentinels import noop
from blueshift.lib.exceptions import ValidationError, TerminationError, ServerError
from blueshift.lib.common.enums import BrokerTables
from blueshift.config import BLUESHIFT_DEBUG_MODE

if TYPE_CHECKING:
    from blueshift.interfaces.data.store import DataStore
    from .config.streaming import StreamConnection
    from .broker import RestAPIBroker

def on_connect(broker:RestAPIBroker, config:StreamConnection):
        is_trader = broker.config.broker.options.interafce == 'trader'
        broker._initialized = False
        
        try:
            if not is_trader and broker.ingestor:
                broker.ingestor.reset(assetdb=broker.assets)
        except TerminationError:
            raise
        except Exception as e:
            msg = f'Failed in fetching broker data:{str(e)}'
            broker.logger.error(msg)
            raise ServerError(msg)
        finally:
            broker._initialized = True
            broker._initialized_event.set()

        if 'on_connect' in config.hooks:
            try:
                f = config.hooks['on_connect']
                ctx = broker.get_context()
                f.resolve(**ctx)
            except Exception as e:
                msg = f'Failed in running on connect custom hook:{str(e)}'
                broker.logger.error(msg)

        try:
            broker._algo_callback(AlgoCallBack.CONNECT, config.streams.copy())
        except Exception as e:
            msg = f'Failed in running on algo connect event callback:{str(e)}'
            broker.logger.error(msg)

def on_disconnect(broker:RestAPIBroker, config:StreamConnection):
        if 'on_disconnect' in config.hooks:
            try:
                f = config.hooks['on_disconnect']
                ctx = broker.get_context()
                f.resolve(**ctx)
            except Exception as e:
                msg = f'Failed in running on connect custom hook:{str(e)}'
                broker.logger.error(msg)

        try:
            broker._algo_callback(AlgoCallBack.DISCONNECT, config.streams.copy())
        except Exception as e:
            msg = f'Failed in running on algo disconnect event callback:{str(e)}'
            broker.logger.error(msg)


def on_order(broker:RestAPIBroker, config:StreamConnection, data:dict[str,Any]):
    if BLUESHIFT_DEBUG_MODE:
        broker.logger.info(f'received order update {data}')

    try:
        if config.converters and 'order' in config.converters:
            converters = config.converters['order']
            ctx:dict[str, Any] = broker.get_context()

            for converter in converters:
                ctx['data'] = data
                res = converter.resolve(**ctx)
                if isinstance(res, dict):
                    # expecting a dict
                    data = res
        
        if not isinstance(data, dict):
            raise ValueError(f'got unexpected type {type(data)}')
    except Exception as e:
        broker.logger.error(f'failed resolving streaming update {data}:{str(e)}')
        return

    order_id = data.get('order_id') or data.get('oid')
    if not order_id:
        broker.logger.error(f'unknown order update received without any order ID: {data}')
        return

    try:
        with broker._order_lock:
            existing = broker._open_orders.get(order_id)
            if not existing:
                existing = broker._closed_orders.get(order_id, None)
                if not existing:
                    broker.logger.info(f'ignoring order update for unknown order ID {order_id}')
                    return
    except Exception as e:
        broker.logger.error(f'failed processing update for order ID {order_id}:{str(e)}')
        return
    
    try:
        with broker._table_locks[BrokerTables.ORDERS]:
            print(f"Updating order: {data}")
            existing.update_from_dict(data, preserve_trackers=True)
            if existing.is_final():
                broker._open_orders.pop(order_id, None)
                broker._closed_orders[order_id] = existing
            else:
                broker._open_orders[order_id] = existing
    except Exception as e:
        import traceback
        traceback.print_exc()
        broker.logger.error(f'failed updating status for order ID {order_id}:{str(e)}')
    
    try:
        broker._algo_callback(AlgoCallBack.TRADE, [order_id])
    except Exception as e:
        broker.logger.error(f'failed running algo callback for order ID {order_id}:{str(e)}')

def on_trade(broker:RestAPIBroker, config:StreamConnection, data):
    if BLUESHIFT_DEBUG_MODE:
        broker.logger.info(f'received trade update {data}')

def on_position(broker:RestAPIBroker, config:StreamConnection, data):
    if BLUESHIFT_DEBUG_MODE:
        broker.logger.info(f'received position update {data}')

    try:
        broker._algo_callback(AlgoCallBack.POSITION, None)
    except Exception as e:
        broker.logger.error(f'failed running algo callback on position update {data}:{str(e)}')

def on_account(broker:RestAPIBroker, config:StreamConnection, data):
    if BLUESHIFT_DEBUG_MODE:
        broker.logger.info(f'received account update {data}')

    try:
        broker._algo_callback(AlgoCallBack.ACCOUNT, None)
    except Exception as e:
        broker.logger.error(f'failed running algo callback on account update {data}:{str(e)}')

def on_heartbeat(broker:RestAPIBroker, config:StreamConnection, msg):
    if BLUESHIFT_DEBUG_MODE:
        broker.logger.info(f'received heatbeat update {msg}')

def on_info(broker:RestAPIBroker, config:StreamConnection, msg):
    if BLUESHIFT_DEBUG_MODE:
        broker.logger.info(f'received info update {msg}')

def on_news(broker:RestAPIBroker, config:StreamConnection, msg):
    if BLUESHIFT_DEBUG_MODE:
        broker.logger.info(f'received news update {msg}')

    try:
        broker._algo_callback(AlgoCallBack.NEWS, [msg])
    except Exception as e:
        broker.logger.error(f'failed running algo callback on news message {msg}:{str(e)}')

def on_unknown(broker:RestAPIBroker, config:StreamConnection, msg):
    if BLUESHIFT_DEBUG_MODE:
        broker.logger.info(f'received unknown update {msg}')

def on_error(broker:RestAPIBroker, config:StreamConnection, msg):
    if BLUESHIFT_DEBUG_MODE:
        broker.logger.info(f'received error update {msg}')

    try:
        broker._algo_callback(AlgoCallBack.ERROR, [msg])
    except Exception as e:
        broker.logger.error(f'failed running algo callback on error message {msg}:{str(e)}')

def on_data(broker:RestAPIBroker, config:StreamConnection, data:dict[str, Any]):
    out = (None, None, None)
    if config.converters and 'data' in config.converters:
        converters = config.converters['data']
        ctx:dict[str, Any] = broker.get_context()

        for converter in converters:
            ctx['data'] = data
            res = converter.resolve(**ctx)
            # the output should be a tuple of (asset, timestamp, results)
            if isinstance(res, tuple):
                out = res

    # algo callback is handled separately for data by the ingestor itself
    return out

def on_quote(broker:RestAPIBroker, config:StreamConnection, data:dict[str, Any]):
    if config.converters and 'quote' in config.converters:
        converters = config.converters['quote']
        ctx:dict[str, Any] = broker.get_context()

        for converter in converters:
            ctx['data'] = data
            res = converter.resolve(**ctx)
            if isinstance(res, dict):
                data = res

        asset = None
        try:
            asset = broker.infer_asset(**data)
            broker.data_interface.update_market_quotes(cast(Asset, asset), data)
        except Exception as e:
            broker.logger.error(f'failed to parse quotes data {data}:{str(e)}')

        try:
            asset = cast(Asset, asset)
            broker._algo_callback(AlgoCallBack.DATA, [asset])
        except Exception as e:
            broker.logger.error(f'failed running algo callback on error message {data}:{str(e)}')

_base_callbacks = {
    'data':on_data,
    'order':on_order,
    'trade':on_trade,
    'position':on_position,
    'account':on_account,
    'quote':on_quote,
    "info":on_info,
    'news':on_news,
    'on_error':on_error,
    'heartbeat':on_heartbeat,
    'unknown':on_unknown,
}

def get_bound_callback(broker:RestAPIBroker, config:StreamConnection, channel:str):
    if channel in _base_callbacks:
        f = lambda msg:_base_callbacks[channel](broker, config, msg)
        return f
    
    raise ValidationError(f'no event handler available for event channel {channel}')

def set_up_streaming(broker:RestAPIBroker, config:StreamConnection) -> StreamingIngestor:
    authentication = config.auth
    url_auth = auth = None
    auth_in_header:bool = False
    verify = broker.config.api.verify_cert
    timeout = broker.config.api.timeout
    proxies = broker.config.api.proxies
    logger = broker.logger
    config.streams

    ctx = broker.get_context()

    if authentication:
        if authentication.mode == 'url' and authentication.url_query:
            url_auth = authentication.url_query.resolve(**ctx)
        elif authentication.mode == 'headers' and authentication.headers:
            auth_in_header = True
            auth = authentication.headers.resolve(**ctx)
        elif authentication.mode == 'first_message' and authentication.first_message:
            auth_in_header = False
            auth = authentication.first_message.resolve(**ctx)
    
    _on_connect = lambda:on_connect(broker, config)
    _on_disconnect = lambda:on_disconnect(broker, config)
    callbacks:dict[str, Callable] = {'on_connect':_on_connect, 'on_disconnect':_on_disconnect}
    interface = broker.config.broker.options.interafce
    
    for channel in config.streams:
        if interface == 'trader':
            if channel in ('data','quote'):
                continue
        elif interface == 'data-portal':
            if channel not in ('data','quote'):
                continue

        callbacks[channel] = get_bound_callback(broker, config, channel)

    opts = {
        'root':config.url,
        'url_auth':url_auth, 
        'auth':auth, 
        'timeout':30,
        'auth_in_header':auth_in_header,
        'logger':logger,
        'timeout':timeout, 
        'proxies':proxies,
        'verify':verify,
        }
    
    if config.backend.type == 'socketio':
        # no parser for socketio, if events mapping specified, remap callbacks dict
        callbacks_mapping = {}
        for event in callbacks:
            if event in config.events:
                callbacks_mapping[config.events[event]] = callbacks[event]
            else:
                callbacks_mapping[event] = callbacks[event]
        callbacks = callbacks_mapping
    else:
        opts['parser'] = lambda msg:parser(broker, config, msg)

    opts['on_data'] = callbacks
    opts = {**opts, **config.backend.options}
    source = data_source_factory(config.backend.type, **opts)

    streaming_opts = {'source':source, 'callback':broker._algo_callback, 'logger':broker.logger}
    if 'data' in config.streams:
        streaming_opts['assetdb'] = {}
        streaming_opts['store'] = broker.store
        streaming_opts['max_tickers'] = broker.config.broker.options.max_subscription
        streaming_opts['unsubscribe_func'] = broker.data_interface.unsubscribe
    
    streaming = ingestor_factory('streaming', **streaming_opts)
    return cast(StreamingIngestor, streaming)

def parser(broker:RestAPIBroker, config:StreamConnection, raw:Any) -> Generator[tuple[str, Any], None, None]:
    if not isinstance(raw, list):
        raw_list = [raw]
    else:
        raw_list = raw

    ctx = broker.get_context()
    
    for raw in raw_list:
        parsed = raw
        
        if config.parser:
            try:
                parsed = config.parser.resolve(parsed, **ctx)
            except Exception as e:
                msg = f'failed to parse streaming data, raw data {raw}'
                broker.logger.error(msg)
                yield 'error', msg
            
        if not isinstance(parsed, list):
            parsed = [parsed]
        
        for item in parsed:
            if config.router:
                channel = config.router.resolve(item, **ctx)
            else:
                channel = config.streams[0]
            yield channel, item