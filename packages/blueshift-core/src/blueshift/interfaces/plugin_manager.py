import sys
from importlib.metadata import entry_points
from blueshift.interfaces.logger import get_logger
from blueshift.lib.common.platform import get_exception

_logger = get_logger()

def load_plugins(group: str, name: str|None = None, load:bool=True):
    """ search plugins components and load them. """
    eps = entry_points(group=group)

    if name:
        eps = [ep for ep in eps if ep.name==name]
    else:
        eps = [ep for ep in eps]

    if load:
        for ep in eps:
            try:
                ep.load()
            except Exception as e:
                exc_type, exc_value, exc_tb = sys.exc_info()
                err = get_exception(exc_type, exc_value, exc_tb)
                _logger.error(f'failed to load plugin {ep}: {str(e)}')
                _logger.error(err)
    
    return eps
