from typing import Any
from .streaming import StreamingPublisher, StreamingSubscriber, CmdReqClient, CmdRepServer

class NoPublisher(StreamingPublisher):
    """ A dummy streaming publisher. """
    def __init__(self, *args, **kwargs) -> None:
        super().__init__()

    @property
    def socket(self):
        pass
    
    @property
    def is_connected(self) -> bool:
        return False
    
    def connect(self, *args, **kwargs):
        pass
    
    def close(self, *args, **kwargs):
        pass

    def send(self, msg:str|dict, **kwargs):
        pass
    
    def send_to_topic(self, topic:str, msg:str|dict, **kwargs):
        pass

class NoSubscriber(StreamingSubscriber):
    """ a dummy streaming subscriber. """
    @property
    def socket(self):
        pass
    
    @property
    def is_connected(self) -> bool:
        return False

    def connect(self):
        pass
            
    def subscribe(self, topic):
        pass
    
    def unsubscribe(self, topic):
        pass

    def recv(self, *args, **kwargs) -> tuple[str, Any]|tuple[None, None]:
        return None, None

    def recv_all(self, *args, **kwargs):
        pass

    def close(self):
        pass
        
class NoReqClient(CmdReqClient):
    """dummy request-reply client. """
    def connect(self, timeout=None):
        pass
    def send_command(self, strcmd=None) -> Any:
        pass

    def dispatch(self, msg:str):
        pass

    def close(self):
        pass

class NoRepServer(CmdRepServer):
    """dummy request-reply server. """
    def connect(self, timeout=None):
        pass
    
    def process_command(self) -> Any:
        pass
    
    def close(self):
        pass

__all__ = [
    'NoPublisher',
    'NoSubscriber',
    'NoReqClient',
    'NoRepServer',
    ]