from __future__ import annotations
from typing import cast
from os import path as os_path
from shutil import rmtree
from os import getenv as os_getenv, listdir, remove as os_remove, PathLike

from blueshift.lib.common.platform import ensure_directory
from blueshift.lib.exceptions import BlueshiftPathException
from blueshift.lib.common.decorators import retry_on_error
from .config import BLUESHIFT_DIR, BLUESHIFT_PRODUCT
from .config import get_config, _default_config

from pathlib import Path
import inspect
src = Path(inspect.getfile(inspect.currentframe())) # type: ignore
BLUESHIFT_PATH = str(src.parent.parent.parent.absolute())

import ast
src = Path(ast.__spec__.origin)
SITE_PACKAGE_PATH = str(src.parent.parent.absolute())

def get_product_name() -> str:
    return BLUESHIFT_PRODUCT

def blueshift_root(environ:dict[str, str]|None=None) -> str:
    config = get_config()
    root = ''

    if config:
        root = os_path.expanduser(config.user_space['root'])
    elif environ:
        root = os_path.expanduser(environ.get("BLUESHIFT_ROOT", ''))
    
    if not root:
        root = os_path.expanduser(BLUESHIFT_DIR)
    
    ensure_directory(root)
    return root

def blueshift_dir(path:str|list[str], environment:dict[str, str]|None=None, create:bool=True) -> str:
    root = blueshift_root(environment)
    if isinstance(path, (list, set)):
        path = os_path.join(root, *path)
    else:
        path = os_path.join(root, path)
    
    if create:
        ensure_directory(path)
    
    return path

def blueshift_temp_dir(path:str|None=None, environment:dict[str, str]|None=None, create:bool=True) -> str:
    import tempfile
    root = os_path.join(tempfile.gettempdir(),BLUESHIFT_PRODUCT)
    
    if path is None:
        path = root
    elif isinstance(path, (list, set)):
        path = os_path.join(root, *path)
    else:
        path = os_path.join(root, path)
    
    if create:
        ensure_directory(path)
    
    return path

def blueshift_cache_dir(environment:dict[str, str]|None=None, create:bool=True) -> str:
    return blueshift_dir('__cache__', environment=environment, create=create)

def blueshift_data_path(environment:dict[str, str]|None=None, create:bool=True) -> str:
    config = get_config()
    
    if config:
        target_dir = config.user_space['data']
    else:
        target_dir = "data"
    
    return blueshift_dir(target_dir, environment=environment, create=create)

def blueshift_source_path(environment:dict[str, str]|None=None, create:bool=True) -> str:
    config = get_config()
    
    if config:
        target_dir = config.user_space['code']
    else:
        target_dir = "source"
    
    return blueshift_dir(target_dir, environment=environment, create=create)

def blueshift_run_path(run_name:str, environment:dict[str, str]|None=None, create:bool=True) -> str:
    target_dir = os_path.join("runs", run_name)
    
    return blueshift_dir(target_dir, environment=environment, create=create)

def blueshift_log_path(run_name:str, environment:dict[str, str]|None=None, create:bool=True) -> str:
    blotter_root =blueshift_run_path(run_name)
    target_dir = os_path.join(blotter_root, "logs")
    
    return blueshift_dir(target_dir, environment=environment, create=create)

def blueshift_saved_objs_path(run_name:str, environment:dict[str, str]|None=None, create:bool=True) -> str:
    blotter_root =blueshift_run_path(run_name)
    target_dir = os_path.join(blotter_root, "objects")
    
    return blueshift_dir(target_dir, environment=environment, create=create)

def blueshift_saved_orders_path(run_name, environment:dict[str, str]|None=None, create:bool=True) -> str:
    blotter_root =blueshift_run_path(run_name)
    target_dir = os_path.join(blotter_root, "orders")
    
    return blueshift_dir(target_dir, environment=environment, create=create)

def blueshift_saved_transactions_path(run_name:str) -> str:
    blotter_root =blueshift_run_path(run_name)
    target_dir = os_path.join(blotter_root, "transactions")
    
    return blueshift_dir(target_dir)

def blueshift_saved_positions_path(run_name:str, environment:dict[str, str]|None=None, create:bool=True) -> str:
    blotter_root =blueshift_run_path(run_name)
    target_dir = os_path.join(blotter_root, "positions")
    
    return blueshift_dir(target_dir, environment=environment, create=create)

def blueshift_saved_performance_path(run_name:str, environment:dict[str, str]|None=None, create:bool=True) -> str:
    blotter_root =blueshift_run_path(run_name)
    target_dir = os_path.join(blotter_root, "performance")
    
    return blueshift_dir(target_dir, environment=environment, create=create)

@retry_on_error(retry=5,sleep=0.1, inc=0.2)
def blueshift_reset_run_path(run_name:str, ignore_log_dir:bool=False, 
                             environment:dict[str, str]|None=None) -> None:
    blotter_root =blueshift_run_path(run_name, environment=environment, create=False)
    
    if not os_path.exists(blotter_root):
        return
    
    dirs = listdir(blotter_root)
    
    for d in dirs:
        is_dir = False
        try:
            path = os_path.join(blotter_root, d)
            if os_path.isfile(path) or os_path.islink(path):
                os_remove(path)
            else:
                is_dir = True
                if d == 'logs' and ignore_log_dir:
                    continue
                rmtree(path)
        except Exception as e:
            if is_dir and d == 'logs' and ignore_log_dir:
                continue
            msg = str(e)
            raise BlueshiftPathException(msg=msg)

def get_config_alerts() -> dict[str, str]:
    config = get_config()
    
    if config:
        alerts = config.alerts
        if not alerts:
            alerts = _default_config["alerts"]
    else:
        alerts = _default_config["alerts"]
        
    if isinstance(alerts, dict):
        return alerts.copy()
    return alerts

def get_config_recovery(error_type:str) ->str:
    config = get_config()
    recovery = 'stop'
    
    if config:
        recovery = config.recovery.get(error_type, None)
        if not recovery and 'recovery' in _default_config:
            recovery = _default_config["recovery"].get(error_type, 'stop')
    elif 'recovery' in _default_config:
        recovery = _default_config["recovery"].get(error_type, 'stop')
    
    return recovery

def get_config_channel(channel_name:str) -> str|dict:
    config = get_config()
    
    if config:
        channel = config.channels.get(channel_name, None)
        if not channel:
            channel = _default_config["channels"][channel_name]
    else:
        channel = _default_config["channels"][channel_name]
        
    if isinstance(channel, dict):
        return channel.copy()
    return channel

def get_blueshift_server() -> str|dict:
    ret = get_config_channel('blueshift_server')
    return ret

def get_config_resource(resource_name:str) -> str|dict|float:
    config = get_config()
    
    if config:
        resource = config.resource.get(resource_name, None)
        if not resource:
            resource = _default_config["resource"].get(resource_name, None)
    else:
        resource = _default_config["resource"].get(resource_name, None)
        
    if isinstance(resource, dict):
        return resource.copy()
    return resource

def get_config_broker_details(broker:str|None=None, algo_user:str|None=None) -> dict:
    config = get_config(algo_user=algo_user)
    if config:
        brkr_dict = config.get_broker_details(broker)
    else:
        if broker is not None:
            return {}
        
        default_brkr = _default_config["defaults"]["broker"]
        brkr_dict = _default_config["brokers"][default_brkr]

    if isinstance(brkr_dict, dict):
        return brkr_dict.copy()
    return brkr_dict

def get_config_env_vars(var_name:str|None=None) -> str|dict:
    """
        Search for env var - first in the current blueshift config, 
        then fall back to os env vars and finally default config env 
        vars. If no `var_name` is specified, it returns only the config
        (or default config) env variables.
    """
    var = {}
    config = get_config()
    
    if config:
        if not var_name:
            var = config.env_vars
        else:
            var = config.env_vars.get(var_name, None)
            if not var:
                var = os_getenv(var_name)
            if not var:
                var = _default_config["environment"].get(var_name)
    else:
        if not var_name:
            var = _default_config["environment"]
        else:
            var = os_getenv(var_name)
            if not var:
                var = _default_config["environment"].get(var_name)
        
    if isinstance(var, dict):
        return var.copy()
    return var

def get_config_oneclick() -> dict:
    config = get_config()
    
    if config:
        return config.oneclick.copy()
    else:
        return {'enabled':False}
    
def get_config_smart_orders() -> dict:
    config = get_config()
    
    if config:
        return config.smart_orders.copy()
    else:
        return {'enabled':False}

