from __future__ import annotations
from typing import TYPE_CHECKING, Callable, Any, Optional, Literal
import logging
import threading
from functools import wraps
import time
from threading import RLock

import paho.mqtt.client as mqtt  # type: ignore -> optional dependency

from blueshift.lib.common.sentinels import NOTHING, noops
from blueshift.lib.exceptions import DataSourceException
from blueshift.lib.common.decorators import set_event_on_finish
from blueshift.lib.common.enums import ConnectionStatus
from blueshift.interfaces.data.source import DataSource, DataSourceType, register_data_source
from blueshift.interfaces.logger import get_logger

if TYPE_CHECKING:
    import pandas as pd
else:
    import blueshift.lib.common.lazy_pandas as pd

class MQTTSource(DataSource):
    _blank = {}, None
    _DATA_CHANNEL = "data"
    _instance = None
    _max_subscriptions = 500

    def __init__(
        self,
        host:str,
        port:int,
        username:str|None=None,
        password:str|None=None,
        client_kwargs: dict|None = None,
        tls_kwargs: dict|None = None,
        frequency:str|None=None,
        master_data=pd.DataFrame(),
        on_data:Callable|dict|None = None,
        data_parser:Callable|None = None,
        topic_parser:Callable|None = None,
        timeout:int=5,
        keepalive:int=20,
        logger:logging.Logger|None=None,
    ):

        self._type = DataSourceType.SOCKETIO
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._client_kwargs = client_kwargs or {}
        self._tls_kwargs = tls_kwargs or {}
        self._handlers = (
            {}
            if on_data is None
            else {self._DATA_CHANNEL: on_data} if callable(on_data) else on_data
        )
        self._keepalive = keepalive
        self._frequency = frequency
        self._metadata = {"frequency": self.frequency}
        self._data_parser = data_parser if callable(data_parser) else None
        self._topic_parser = topic_parser if topic_parser else (lambda x: x)

        if not (host and port):
            raise DataSourceException("host and port must be provided.")

        self._timeout = timeout
        self._connection_status = ConnectionStatus.UNSET
        self._mqtt_client = None

        if logger:
            self._logger = logger
        else:
            self._logger = get_logger()

        if isinstance(on_data, dict):
            for key, func in on_data.items():
                if isinstance(key, str):
                    self.register(key, func)
        else:
            self._logger.error("illegal handler type, will be ignored.")

        self._master_data = master_data

        self._reply = False
        self._reply_on_connect = False
        self._subscriptions = set()
        self._wants_to_close = False
        self._mqtt_lock = RLock()
        self._finalizer = noops

    @property
    def type(self):
        """Returns the data source type."""
        return self._type.name
    
    @property
    def timeout(self) -> int:
        return self._timeout
    
    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @property
    def frequency(self):
        """Data frequency in Pandas format."""
        return self._frequency

    @property
    def root(self):
        """Returns root of the connection url."""
        return f"{self._host}:{self._port}"

    @property
    def connection_status(self):
        return self._connection_status

    @property
    def connected(self):
        """Check if connected to the source."""
        is_socket = (
            self._mqtt_client is not None
            and self._mqtt_client.is_connected()
            and self._connection_status == ConnectionStatus.ESTABLISHED
        )

        return is_socket
    
    @property
    def data_channel(self) -> str|None:
        return self._DATA_CHANNEL

    @property
    def metadata(self):
        """Returns metadata about the source."""
        return self._metadata

    @property
    def finalizer(self):
        """Returns data finalizer method."""
        return self._finalizer

    @finalizer.setter
    def finalizer(self, func):
        """Returns data finalizer method."""
        self._logger.debug(f"Finalizer set to {func}")
        if callable(func):
            self._finalizer = func
        else:
            self._logger.debug(f"Finalizer not callable: {func}")

    def reset(self):
        """Close current connection and remove all subscriptions."""
        self._subscriptions = set()
        self.close()

    def add_masterdata(self, master_data:pd.DataFrame):
        """
        Add metadata symbol-wise. Add a master data dataframe, with
        a `symbol` column that provides additional fields for
        enriching metadata for a given symbol.
        """
        if len(master_data) > 0 and "symbol" not in master_data.columns:
            raise DataSourceException("master data missing symbol column.")
        self._master_data = master_data

    def add_metadata(self, **kwargs):
        """Add extra metadata to be applied for **all** symbols."""
        self._metadata = {**self._metadata, **kwargs}

        for key in list(self._metadata.keys()):
            if self._metadata[key] == NOTHING:
                self._metadata.pop(key)

    def register(self, event, handler, **kwargs):
        if event == self._DATA_CHANNEL:
            # inject metadata for data callback.
            handler = self._data_func_wrapper(handler)
        self._handlers[event] = handler

    def remove_handler(self, event, handler=None):
        """Remove a channel handler."""
        self._handlers.pop(event, None)

    def __iter__(self):
        raise NotImplementedError

    def _apply_metadata(self, sym):
        mdata = {}
        if len(self._master_data) > 0 and sym in self._master_data.symbol:
            mdata = self._master_data[self._master_data.symbol == sym].iloc[0].to_dict()

        return mdata

    def _release_resources(self, close=True):
        if close:
            self.close()

    def _handle_error(self, msg, error=None, log=True, close=True, exception=False):
        if error:
            err_msg = str(error)
            if not err_msg:
                try:
                    err_msg = type(error).__name__
                except Exception:
                    pass
            msg += f":{err_msg}"

        if log:
            self._logger.error(msg)

        if close:
            self._release_resources()

        if exception:
            raise DataSourceException(msg)

    @set_event_on_finish()
    def connect(self, event:threading.Event|None=None):
        """connect to a server, optionally with namespace(s)."""
        self.error = None
        if self.connected:
            return

        if self._connection_status == ConnectionStatus.PENDING:
            time.sleep(self._timeout)
            if self.connected:
                return

        self._wants_to_close = False
        self._connection_status = ConnectionStatus.PENDING

        client_id = self._client_kwargs.get("client_id", "")
        clean_session = self._client_kwargs.get("clean_session", None)
        userdata = self._client_kwargs.get("userdata", None)
        # protocol = self._client_kwargs.get("protocol", 4)
        transport = self._client_kwargs.get("transport", "tcp")
        reconnect_on_failure = self._client_kwargs.get("reconnect_on_failure", False)

        with self._mqtt_lock:
            try:
                # using callback api version 2 for future compatibility
                self._mqtt_client = mqtt.Client(
                    mqtt.CallbackAPIVersion.VERSION2,
                    client_id=client_id,
                    userdata=userdata,
                    protocol=mqtt.MQTTv5,
                    transport=transport,
                    reconnect_on_failure=reconnect_on_failure,
                )
                self._set_tls_kwargs()
                self._set_callbacks()

                if self._username and self._password:
                    self._mqtt_client.username_pw_set(
                        username=self._username, password=self._password
                    )
                # self._mqtt_client.clean_session = True
                res = self._mqtt_client.connect(
                    host=self._host,
                    port=self._port,
                    keepalive=self._keepalive,
                    clean_start=True  # we are always cleaning session once disconnected,
                    # it will not subscribe to old topics, with qos >0
                )
                if res == mqtt.MQTT_ERR_SUCCESS:
                    self._mqtt_client.loop_start()  # it starts a thread by itself
                    for x_ in range(self._timeout):
                        time.sleep(1)
                        if self.connected:
                            if event:
                                event.set()
                                break
                    if self.connected is False:
                        raise ValueError("Still not connected")
                    self.error = None
                    self._logger.info(f"successfully started connection to {self.root}")
                else:
                    self.error = f"Connection failed with result code {res}"
                    self._mqtt_client = None
                    self._connection_status = ConnectionStatus.ABORTED
                    self._handle_error(
                        "error during mqtt connect", error=self.error, exception=True
                    )
            except Exception as e:
                self.error = e
                self._mqtt_client = None
                self._connection_status = ConnectionStatus.ABORTED
                self._handle_error("error during mqtt connect", error=e, exception=True)

    def _set_tls_kwargs(self):
        if self._tls_kwargs and self._mqtt_client is not None:
            ca_certs = self._tls_kwargs.get("ca_certs", None)
            certfile = self._tls_kwargs.get("certfile", None)
            keyfile = self._tls_kwargs.get("keyfile", None)
            cert_reqs = self._tls_kwargs.get("cert_reqs", None)
            tls_version = self._tls_kwargs.get("tls_version", None)
            ciphers = self._tls_kwargs.get("ciphers", None)
            keyfile_password = self._tls_kwargs.get("keyfile_password", None)
            tls_insecure_set = self._tls_kwargs.get("tls_insecure_set", None)
            self._mqtt_client.tls_set(
                ca_certs=ca_certs,
                certfile=certfile,
                keyfile=keyfile,
                cert_reqs=cert_reqs,
                tls_version=tls_version,
                ciphers=ciphers,
                keyfile_password=keyfile_password,
            )

            if isinstance(tls_insecure_set, bool):
                self._mqtt_client.tls_insecure_set(tls_insecure_set)

    def _set_callbacks(self):
        if self._mqtt_client is None:
            self._logger.error("mqtt client not initialized to set callbacks.")
            return
        self._mqtt_client.on_connect = self.on_connect
        self._mqtt_client.on_disconnect = self.on_disconnect
        self._mqtt_client.on_message = self.on_message
        self._mqtt_client.on_subscribe = self.on_message
        self._mqtt_client.on_unsubscribe = self.on_message

    def on_connect(self, client, userdata, flags, reason_code, properties):
        self._connection_status = ConnectionStatus.ESTABLISHED
        if self._subscriptions:
            payloads = self._subscriptions.copy()
            self._subscriptions = set()
            for payload in payloads:
                self._logger.info(f"sending subscription request {payload}")
                self.subscribe(payload, validate=False)

        callback = self._handlers.get("connect", noops)
        callback()

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
    # def on_disconnect(self, *args, **kwargs):
        """callback handler for `disconnect` event."""
        self._connection_status = ConnectionStatus.DISCONNECTED
        callback = self._handlers.get("disconnect", noops)
        callback()

    def on_message(self, *args, **kwargs):
        if len(args) == 5:  # subscribe and unsubscribe message args are 5
            # subscribe and unsubscribe message. IGNORED
            return
        if len(args) == 3:  # data message args are 3
            _, _, message = args
            if self._data_parser:
                event, data = self._data_parser(message)
            else:
                event, data = self._DATA_CHANNEL, message
            self._dispatcher(event, data)

    def _dispatcher(self, event, data):
        if not event:
            return self._blank
        self._logger.debug(f"Routing for {event}: {data}")
        if event == self._DATA_CHANNEL and self._DATA_CHANNEL in self._handlers:
            callback = self._handlers[self._DATA_CHANNEL]
        else:
            callback = self._handlers.get(event, noops)

        callback(data)

    def close(self):  # done
        """request a disconnect of the socketIO connection."""
        if not self._mqtt_client:
            return
        if not self._mqtt_client.is_connected():
            return

        if self._connection_status == ConnectionStatus.PENDING:
            time.sleep(self._timeout)
            if not self.connected:
                return

        self._wants_to_close = True
        last_status = self._connection_status
        self._connection_status = ConnectionStatus.PENDING

        with self._mqtt_lock:
            try:
                if self._mqtt_client.is_connected():
                    self._mqtt_client.disconnect()
                    self._mqtt_client.loop_stop()
                    self._mqtt_client = None
            except Exception as e:
                self._connection_status = last_status
                self._handle_error("mqtt connection close failed", error=e, close=False)
            else:
                self._logger.info(f"successfully disconnected from {self.root}")
                self._connection_status = ConnectionStatus.DISCONNECTED

    def disconnect(self):  # done
        """equivalent to the method close."""
        self.close()

    def reconnect(self, event=None):
        self.close()
        time.sleep(self._timeout)
        self.connect(event=event)

    def subscribe(self, payload:dict, validate:bool=True):
        """

        :param payload: {subscribe/unsubscribe: [(topic, qos), ...]}
        :param validate: bool
        :return: None
        """

        if validate and not self.connected:
            return self._handle_error("cannot subscribe, not connected", close=False)
        sub = payload.get("subscribe", None)
        unsub = payload.get("unsubscribe", None)
        if sub:
            self._logger.debug(f"Subscribing for {sub}")
            self._subscribe(sub)
        if unsub:
            self._unsubscribe(unsub)

    def _subscribe(self, payload=None):
        """[(payload0,0), (payload1,0), (payload2,0)]"""
        if not payload:
            self._logger.info("empty payload, nothing to subscribe.")
            return
        try:
            self._mqtt_client.subscribe(payload) # type: ignore
        except Exception as e:
            self._handle_error(
                f"failed in subscription {payload}", error=e, close=False
            )
        else:
            self._update_subs(payload, "subscribe")
        self._logger.debug(f"subscribed successfully for payload {payload}.")

    def _unsubscribe(self, payload=None):
        if not self.connected:
            return self._handle_error("cannot unsubscribe, not connected", close=False)

        if not payload:
            self._logger.info("empty payload, nothing to unsubscribe.")
            return

        try:
            self._mqtt_client.unsubscribe(payload) # type: ignore
        except Exception as e:
            self._handle_error(
                f"failed in unsubscription {payload}", error=e, close=False
            )
        else:

            self._update_subs(payload, "unsubscribe")
            self._logger.debug(f"unsubscribed successfully for payload {payload}.")

    def _update_subs(self, payload, type_: Optional[Literal["subscribe", "unsubscribe"]],
    ):
        self._logger.debug(f"updating subscriptions {type_} {payload}")
        sub_unsub = []
        if isinstance(payload, list):
            for p in payload:
                if isinstance(p, (tuple, list)):
                    p = p[0]
                    sub_unsub.append(p)
        elif isinstance(payload, str):
            sub_unsub.append(payload)
        else:
            raise DataSourceException("invalid payload type")

        if type_ == "subscribe":
            self._subscriptions.update(sub_unsub)
        elif type_ == "unsubscribe":
            self._subscriptions.difference_update(sub_unsub)
        else:
            raise ValueError("Correction in _update_subs method")

    def _data_func_wrapper(self, callback):
        @wraps(callback)
        def decorated(*args, **kwargs):
            try:
                self._logger.debug(f"Callback is: {callback}")
                sym_timestamp_values = callback(*args, **kwargs)
                self._logger.debug(f"sym_timestamp_values: {sym_timestamp_values}")
                sym, timestamp, values = sym_timestamp_values
                timestamp = pd.Timestamp(timestamp)
                self._logger.debug(f"Callback response: {sym, timestamp, values}")
            except (ValueError, TypeError):
                msg = "data handler function must return a tuple"
                msg += " of (asset, timestamp, value)"
                raise DataSourceException(msg)
            else:
                metadata = {
                    "symbol": sym,
                    "timestamp": timestamp,
                }  # datapoint specific metadata
                mdata = self._apply_metadata(sym)  # symbol specific metadata
                metadata = {
                    **self.metadata,
                    **mdata,
                    **metadata,
                }  # squash metadata including the general part
                # return metadata, values

                return_ = self._finalizer(
                    metadata, values
                )  # process the metadata and data inputs
                self._logger.debug(f"return_ from _finalizer: {return_}")
                return return_

        return decorated


register_data_source("mqtt", MQTTSource)