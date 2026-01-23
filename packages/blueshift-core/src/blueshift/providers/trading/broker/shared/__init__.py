from .shared import SharedBroker, SharedBrokerCollection
from .client import SharedBrokerClient

from blueshift.interfaces.trading.broker import register_custom_broker_factory
from blueshift.lib.exceptions import BrokerError

def _shared_broker_factory(name, *args, **kwargs):
    if not 'collection' in kwargs or not kwargs['collection']:
        raise BrokerError(f'A valid broker collection is required.')
        
    if not 'algo' in kwargs or not kwargs['algo']:
        raise BrokerError(f'A valid algo name is required.')
        
    if not 'algo_user' in kwargs:
        raise BrokerError(f'An algo user is required.')
        
    collection:SharedBrokerCollection = kwargs.pop('collection')
    algo = kwargs.pop('algo')
    algo_user = kwargs.pop('algo_user')
    return SharedBrokerClient(collection, algo, algo_user, name, *args, **kwargs)

def install_shared_broker():
    register_custom_broker_factory(_shared_broker_factory, key="shared")

__all__ =['SharedBrokerClient', 
          'SharedBroker', 
          'SharedBrokerCollection',
          ]