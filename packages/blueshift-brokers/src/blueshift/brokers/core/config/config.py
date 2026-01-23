from __future__ import annotations
from typing import TYPE_CHECKING, Any

from blueshift.interfaces.logger import get_logger
from blueshift.lib.exceptions import ValidationError

from .broker import get_broker_config, BrokerConfig
from .api import get_api_config
from .objects import get_objects_config, ObjectConfig
from .streaming import get_streaming_config, StreamingConfig
from .resolver import ConfigRegistry

if TYPE_CHECKING:
    from blueshift.interfaces.logger import BlueshiftLogger

class APIBrokerConfig:
    """
        Base for API broker config. This class, along with the BrokerAPIMixin 
        class provides all the functionalities for RestAPIBroker.
    """
    def __init__(self, config:dict[str, Any], api_config:dict[str, Any]|None=None, 
                 objects_config:dict[str, Any]|None=None, streaming_config:dict[str, Any]|None=None, 
                 logger:BlueshiftLogger|None=None, registry:ConfigRegistry|None=None):
        self.logger = logger if logger else get_logger()
        registry = registry or ConfigRegistry()

        if api_config is None:
            api_config = config.pop('api', {})
        if not isinstance(api_config, dict) or not api_config:
            raise ValidationError(f'api config is required and must be a dict')

        if objects_config is None:
            objects_config = config.pop('objects', {})
        if not isinstance(objects_config, dict):
            raise ValidationError(f'objects config must be a dict')

        if streaming_config is None:
            streaming_config = config.pop('streaming', {})
        if not isinstance(streaming_config, dict):
            raise ValidationError(f'streaming config must be a dict')

        if 'master_data' not in api_config:
            master_data = config.pop('master_data', {})
            api_config['master_data'] = master_data

        try:
            self.broker: BrokerConfig = get_broker_config(config, registry)
        except Exception as e:
            raise ValidationError(f"Invalid Broker Config: {e}") from e
            
        try:
            api, master = get_api_config(api_config, registry)
            self.api = api
            self.master_data = master
        except Exception as e:
            raise ValidationError(f"Invalid API Config: {e}") from e
            
        if objects_config is None:
            objects_config = {}
            
        try:
            self.objects: ObjectConfig = get_objects_config(objects_config, registry)
        except Exception as e:
            raise ValidationError(f"Invalid Objects Config: {e}") from e

        self.streaming: StreamingConfig | None = None
        if streaming_config:
            try:
                self.streaming = get_streaming_config(streaming_config, registry)
            except Exception as e:
                raise ValidationError(f"Invalid Streaming Config: {e}") from e

    def resolve(self, **context):
        self.broker.resolve(**context)
        self.api.resolve(**context)
        self.objects.resolve(**context)
        if self.streaming:
            self.streaming.resolve(**context)


__all__ = ['APIBrokerConfig']