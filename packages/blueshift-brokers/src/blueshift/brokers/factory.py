from __future__ import annotations
from typing import Type, cast

from blueshift.interfaces.trading.broker import register_broker
from .core.broker import RestAPIBroker
from .core.config.config import APIBrokerConfig

def broker_class_factory(cls_name:str, config:APIBrokerConfig) -> Type[RestAPIBroker]:
    """ 
        Factory function to create sub-classes of `RestAPIBroker` based on specific config. 
        This also automatically register this with the blueshift system.
    """
    def __init__(self, **kwargs):
        RestAPIBroker.__init__(self, config, **kwargs)

    Klass = type(
        cls_name,
        (RestAPIBroker,),
        {
            "__init__": __init__,
        },
    )
    Klass = cast(Type[RestAPIBroker], Klass)
    register_broker(config.broker.name, Klass, config.broker.calendar)

    return Klass