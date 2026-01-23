from blueshift.interfaces.callbacks import set_builtin_callback_loader
from .assets import *
from .data.source import *
from .data.store import *
from .data.adjs import *
from .data.ingestor import *
from .data import *
from .trading.blotter import *
from .trading.broker import *
from .trading.oneclick import *
from .trading.rms import *
from .trading import *

def load_callbacks():
    from blueshift.providers.callbacks import BlueshiftCallback


set_builtin_callback_loader(load_callbacks)