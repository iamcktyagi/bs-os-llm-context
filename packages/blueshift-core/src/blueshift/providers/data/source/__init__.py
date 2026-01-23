from blueshift.interfaces.data.source import set_builtin_data_source_loader

def source_installer(source_type=None):
    if source_type is None or source_type=='no-source':
        from .nosource import NoSource

    if source_type is None or source_type=='csv':
        from .csvsource import CSVSource

    if source_type is None or source_type=='zmq':
        from .zmq_stream import ZMQSource
    
    if source_type is None or source_type=='socketio':
        try:
            from socketio.client import Client # type: ignore -> optiona dependency
        except ImportError:
            if source_type=='socketio':
                raise ImportError(f'socketIO is not installed, run `pip install python-socketio`')
        else:
            from .socketio import SocketIOSource

    if source_type is None or source_type=='websocket':
        try:
            import websockets # type: ignore -> optiona dependency
        except ImportError:
            if source_type=='websocket':
                raise ImportError(f'websockets is not installed, run `pip install websockets`')
        else:
            from .websocket import WSSource

    if source_type is None or source_type=='mqtt':
        try:
            import paho.mqtt.client as mqtt # type: ignore -> optiona dependency
        except ImportError:
            if source_type=='mqtt':
                raise ImportError(f'zmq is not installed, run `pip install paho-mqtt`')
            pass
        else:
            from .mqtt import MQTTSource

set_builtin_data_source_loader(source_installer)