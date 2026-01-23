from blueshift.interfaces.trading.algo_orders import set_builtin_algo_order_handler_loader
from blueshift.interfaces.trading.exit_handler import (
    set_builtin_exit_handler_loader, register_exit_handler)

def install_algo_order_handlers(handler_type):
    if handler_type is None or handler_type=='default':
        from .algo_orders import IAlgoOrderHandler

def install_exit_handlers(handler_type):
    if handler_type is None or handler_type=='default':
        from ._exit_handler import ExitHandler
        register_exit_handler('default', ExitHandler)

set_builtin_algo_order_handler_loader(install_algo_order_handlers)
set_builtin_exit_handler_loader(install_exit_handlers)