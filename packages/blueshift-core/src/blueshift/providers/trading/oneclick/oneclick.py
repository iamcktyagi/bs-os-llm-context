from __future__ import annotations
from typing import TYPE_CHECKING, Any

from blueshift.lib.common.decorators import ensure_connected
from blueshift.providers.data.ingestor.stream_ingestor import StreamingData
from blueshift.lib.common.types import MaxSizedOrderedDict
from blueshift.lib.common.enums import (
        OneclickMethod, OneClickStatus, OneClickState, AlgoMessageType as MessageType, AlgoCallBack)
from blueshift.lib.common.functions import create_message_envelope
from blueshift.lib.exceptions import (
    InternalError, ServerError, InitializationError, BlueshiftException, UserDefinedException)
from blueshift.interfaces.trading.oneclick import OneClickObject, IOneClickNotifier, register_oneclick
from blueshift.interfaces.context import IContext

from .api import OneClickServiceAPI

if TYPE_CHECKING:
    import pandas as pd
    from blueshift.lib.trades._order import Order
    from blueshift.core.algorithm.algorithm import TradingAlgorithm
else:
    import blueshift.lib.common.lazy_pandas as pd

class OneClickService(IOneClickNotifier, OneClickServiceAPI):
    """
        Oneclick service implementation. This class creates notification
        by posting http request at a given server url. The updates 
        are received via a socketIO connection. In case socketIO settings
        are missing, it falls back to the algo zmq command line.
    """
    def __init__(self, config, env, *args, **kwargs):
        super(OneClickService, self).__init__(config, env, *args, **kwargs)
        self._create(config, *args, **kwargs)

    def _create(self, config, **kwargs):
        self._tz:str = config.get('tz', 'Etc/UTC')
        url = self._env.alert_manager.get_blueshift_callback_url(
                config.get("server_url"))
        if not url:
            raise InitializationError('Missing oneclick server url.')
        
        self._server_url = url
        self._api_username:str|None = config.get("api_username", None)
        self._api_password:str|None = config.get("api_password", None)
        self._proxies:dict = config.get("proxies", {})
        self._agent:str = config.get("agent", "blueshift")
        self._basic_auth:bool = config.get("basic_auth", True)
        self._verify_cert:bool = config.get("verify_cert", False)
        
        # use the zmq source in the alert manager
        alert_manager = self._env.alert_manager
        self._streamer = alert_manager.streamer
        if self._logger:
            self._logger.info('Using zmq channel for oneclick.')
            
        # required for ensure_connected decorator
        setattr(self, 'connect', self._streamer.connect)
        setattr(self, 'disconnect', self._streamer.disconnect)
        setattr(self, 'reconnect', self._streamer.reconnect)
        setattr(self, 'ensure_connected', self._streamer.ensure_connected)
        
    @property
    def is_connected(self) -> bool:
        if hasattr(self, '_streamer'):
            return self._streamer.is_connected
        return True

    def _create_oneclick(self, data:dict, order:Order) -> OneClickObject:
        _id = data["id"]
        _type = data["orderDetails"]["type"]
        expiry_time = pd.Timestamp(
                data["expiredAt"], unit='s', tz=self._tz)
        state = data["status"]
        return OneClickObject(id=_id, type=_type,
                              expiry_time=expiry_time, state=state,
                              order=order)

    def _make_details(self, method:OneclickMethod, unique_id:str, name:str, broker_name:str, 
                      order:Order, timeout:float) -> dict[str, Any]:
        return {
            "type": method.value,
            "id": unique_id,
            "name": name,
            "broker_name": broker_name,
            "asset": order.asset.exchange_ticker,
            "side": order.side,
            "quantity": order.quantity,
            "timeout":timeout
        }

    def _emit_oneclick_msg(self, ctx:str, data:dict[str,Any], publish_packets:bool=True):
        """
            Emit oneclick packet to streaming server.

            Args:

                ``data(object)``: Data to send

            Returns:
                None.
        """
        alert_manager = self._env.alert_manager

        publisher_handle = alert_manager.publisher
        if publisher_handle and publish_packets:
            packet = create_message_envelope(ctx, MessageType.ONECLICK, data=data)
            publisher_handle.send_to_topic(ctx, packet)
            
    def cancel(self, notification_id:str, ctx:IContext|None=None):
        obj:OneClickObject|None = None
        if not ctx:
            obj = self._processing.get(notification_id)
        else:
            notifications = self._notifications_by_context.get(ctx, {})
            if notification_id in notifications:
                obj = self._processing.get(notification_id)
                
        if obj:
            self._discard_notification(notification_id, obj)
    
    @ensure_connected()
    def notify(self, ctx:IContext, method:OneclickMethod, order:Order, name:str, broker_name:str, 
               timeout:float|None=None, **kwargs) -> str:
        """
            Notify to the target for a new order. User must confirm this
            order for it to be sent to the execution broker. An optional
            timeout (in number of minutes) can be specified to set the 
            expiry of the notification.

            Args:
                `ctx (Context)`: Current context.
                
                `method (num)`: Enum for notification method

                `order (object)`: Notification object (usually `order`)

                `name (str)`: Name of the execution

                `broker_name (str)`: Name of the broker for this execution.
                
                `timeout (int)`: Timeout of this notification (optional).

            Returns:
                Str. Returns the unique notification ID.
                `
        """
        import uuid
        #TODO: the notification id should be fixed by blueshift, not server
        # add this to make POST idempotent and safe
        unique_hash = str(uuid.uuid4())
        if unique_hash in self._processing or unique_hash in self._processed:
            # we should never be here ideally
            # TODO: check if raising exception is better?
            raise InternalError(f'oneclick notification illegal state.')
        
        if not timeout:
            timeout = self._timeout
        data_to_send = self._make_details(
                method, unique_hash, name, broker_name, order, timeout)
        
        data = self.request_post(data_to_send)
        if not isinstance(data, dict):
            raise ServerError(f'illegal response from oneclick service, got: {data}')
        
        notification_hash = str(data["notificationHash"])
        if self._logger:
            self._logger.info(f'notifcation {notification_hash} sent for confirmation.')

        if notification_hash != unique_hash:
            # potentially dangerous, we do not know what is going on,
            # we shut down the algo as a safety measure here.
            raise InternalError('mismatch in notification hash.')

        obj = self._create_oneclick(data, order)
        self._processing[notification_hash] = obj
        self._add_to_context(ctx, notification_hash, obj)
        self._emit_oneclick_msg(ctx.name, data)

        return notification_hash
    
    def handle_notifications(self, data:list[dict[str, Any]], **kwargs):
        """ handle socketIO notifications. """
        # wrap this in try-block as this will run in a separate thread
        alert_manager = self._env.alert_manager
        try:
            algo = self._env.algo
            if not algo:
                raise InternalError(msg='the algo object not set.')
            callback = {}
            for msg in data:
                id_, state = self.confirm(algo, msg)
                if id_ and state:
                    if isinstance(state, str):
                        callback[id_] = OneClickState(state.lower())
                    else:
                        callback[id_] = state
            if callback:
                try:
                    if algo.queue:
                        event_type = AlgoCallBack.ONECLICK
                        func = algo.queue.put_no_repeat
                        packet = (callback, event_type)
                        algo.loop.call_soon_threadsafe(func, packet, algo)
                except RuntimeError:
                    pass
        except (BlueshiftException, UserDefinedException) as e:
                algo = self._env.algo
                if algo:
                    alert_manager.handle_error(algo, e,'oneclick')
                else:
                    raise
                    
    def _expire_notification(self, notification_id:str, obj:OneClickObject):
        obj.expire_order()
        self._processing.pop(notification_id, None)
        self._processed[notification_id] = obj
        self._update_with_ctx(notification_id, obj)
        
    def _discard_notification(self, notification_id:str, obj:OneClickObject):
        obj.discard_order()
        self._processing.pop(notification_id, None)
        self._processed[notification_id] = obj
        self._update_with_ctx(notification_id, obj)
        
    def _finalize_notification(self, notification_id:str, obj:OneClickObject, msg:str):
        obj.reject_order(msg)
        self._processing.pop(notification_id, None)
        self._processed[notification_id] = obj
        self._update_with_ctx(notification_id, obj)
        
    def _make_response(self, notification_id:str, status:OneClickState, msg:str):
        callback = {"notificationId": notification_id, "type": "app"}
        callback['name'] = self._env.name
        callback['status'] = status.value
        callback['msg'] = msg
        self.request_put(callback, throwOnly5xx=True)
        
    def check_expiry(self, id_:str|None=None):
        """
            Check and update if a notification has expired.
        """
        if id_ is None:
            pending = list(self._processing.keys())
        else:
            pending = [id_]
        
        current_time = pd.Timestamp.now(tz=self._tz)
        for id_ in pending:
            obj = self._processing.get(id_, None)
            if not obj:
                continue
            if current_time > obj.expiry_time:
                self._expire_notification(id_, obj)

    def confirm(self, algo:TradingAlgorithm, packet:dict[str, Any], **kwargs
                ) -> tuple[None, None]|tuple[str, OneClickState]:
        """
            Process notification after user confirmation.

            Args:
                ``algo(object)``: A reference to the current algo object
                
                ``packet(dict)``: command channel message

            Returns:
                None
        """
        notification_id = packet.get("id")
        if not notification_id:
            return None, None
        else:
            notification_id = str(notification_id)
        
        current_time = pd.Timestamp.now(tz=self._tz)
        obj = self._processing.get(notification_id)
        
        if not obj:
            err_msg = f'Notification ID is not open.'
            if self._logger:self._logger.warning(err_msg)
            self._make_response(
                    notification_id, OneClickState.ERRORED, err_msg)
            return None, None
        
        status = packet.get("status")
        valid_states = (OneClickState.ACTIONED, OneClickState.ACKNOWLEDGED,
                        OneClickState.APPROVED, OneClickState.DISCARDED)
        try:
            if isinstance(status, str):
                status = status.lower()
            status = OneClickState(status)
        except Exception:
            # illegal status message
            err_msg = f'Illegal confirmation status {status}.'
            if self._logger:self._logger.warning(err_msg)
            self._make_response(
                    notification_id, OneClickState.ERRORED, err_msg)
            return None, None
        else:
            if status not in valid_states:
                err_msg = f'Confirmation status {status} is not valid.'
                if self._logger:self._logger.warning(err_msg)
                self._make_response(
                        notification_id, OneClickState.ERRORED, err_msg)                
                return None, None
        
        try:
            if current_time > obj.expiry_time:
                self._expire_notification(notification_id, obj)
                cb_status = OneClickState.EXPIRED
                cb_msg = f'order expired at {obj.expiry_time}'
                if self._logger:self._logger.warning(cb_msg)
                algo.log_warning(cb_msg)
            elif obj.type == OneclickMethod.PLACE_ORDER and \
                status==OneClickState.DISCARDED:
                self._discard_notification(notification_id, obj)
                cb_status = OneClickState.DISCARDED
                cb_msg = f'oneclick notification {notification_id} discared by user.'
                if self._logger:self._logger.warning(cb_msg)
                algo.log_warning(cb_msg)
            elif obj.type == OneclickMethod.PLACE_ORDER and \
                status in (OneClickState.APPROVED, OneClickState.ACTIONED,
                           OneClickState.ACKNOWLEDGED):
                # TODO: ACTIONED added for platform compatibility, at present
                # 'actioned' is sent on confirm and nothing on discard.
                obj.status = OneClickStatus.PROCESSED
                obj.state = OneClickState.ACTIONED
                self._processing.pop(notification_id, None)
                self._processed[notification_id] = obj
                self._update_with_ctx(notification_id, obj)
                
                order = obj.order
                order_id = algo.confirm_order(order)
                
                if order_id:
                    sent_order = algo.get_order(order_id)
                    if not sent_order:
                        msg = 'Cannot find order object for the confirmed '
                        msg += 'order with notification ID {notification_id}.'
                        raise InternalError(msg)
                    self._sent_orders[notification_id] = order_id
                    obj.order = sent_order
                    cb_msg = f'Oneclick notification {notification_id} '
                    cb_msg += 'confirmed by user and sent '
                    cb_msg += f'to broker with order ID {order_id}.'
                    cb_status = OneClickState.COMPLETED
                    obj.complete_order(sent_order)
                    algo.log_info(cb_msg)
                else:
                    # for some reason, no order was placed, e.g. risk rules
                    # violations etc.
                    cb_msg = 'Oneclick notification {notification_id} '
                    cb_msg += 'confirmed by user, but no broker order '
                    cb_msg += 'was generated. Check risk rules violations.'
                    cb_status = OneClickState.ERRORED
                    obj.reject_order(cb_msg)
                    if self._logger:self._logger.warning(cb_msg)
                
                self._update_with_ctx(notification_id, obj)
                if self._logger:self._logger.info(cb_msg)
                algo.log_warning(cb_msg)
            else:
                msg = f'error handling notification {notification_id}'
                msg = msg + ': unknown status {status.value}.'
                self._finalize_notification(notification_id, obj, msg)
                cb_status = OneClickState.ERRORED
                cb_msg = msg
                if self._logger:self._logger.warn(cb_msg)
                algo.log_warning(cb_msg)
        except InternalError as e:
            msg = f'Fatal error while handling oneclick confirmation:{str(e)}.'
            raise InternalError(msg)
        except Exception as e:
            msg = f'error handling notification {notification_id}'
            msg = msg + f': {str(e)}'
            self._finalize_notification(notification_id, obj, msg)
            cb_status = OneClickState.ERRORED
            cb_msg = msg
            if self._logger:self._logger.warn(cb_msg)
            algo.log_warning(cb_msg)
        
        self._make_response(
                notification_id, cb_status, cb_msg)
        return notification_id, obj.state


register_oneclick('default', OneClickService)