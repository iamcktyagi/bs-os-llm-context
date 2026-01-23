from __future__ import annotations
from typing import cast, Any
import json
from os import environ as os_environ
from os import getenv as os_getenv
from os.path import join, expanduser, exists

from blueshift.lib.exceptions import InitializationError, ValidationError
from blueshift.lib.common.functions import str2bool, merge_json_recursive
from blueshift.version import __version__

DEFAULT_BLUESHIFT_PRODUCT = 'Blueshift'
DEFAULT_BLUESHIFT_PLATFORM = 'blueshift'
DEFAULT_BLUESHIFT_DIR = "~/.blueshift"
DEFAULT_BLUESHIFT_CONFIG = ".blueshift_config.json"
DEFAULT_BLUESHIFT_USER_CONFIG = ".user_config.json"
DEFAULT_BLUESHIFT_SECURE_CODE = True
DEFAULT_SESSION_ID = ''

# broker credentials env vars
BROKER_TOKEN = "BLUESHIFT_BROKER_TOKEN_{}"
BROKER_API_KEY = "BLUESHIFT_BROKER_ACCESS_KEY_ID_{}"
BROKER_SECRET_ACCESS_KEY = "BLUESHIFT_BROKER_SECRET_ACCESS_KEY+{}"


_default_config =\
{
 "workspace":
      {
           "root": "~/.blueshift",
           "code": "source",
           "data": "data"
       },
 "alerts":
     {
          "error": ["email", "msg", "console"],
          "warning": ["log", "console", "msg"],
          "log": ["log"],
          "platform_msg": ["email"],
          "third_party_msg": ["email"],
          "tag": "blueshift"
      },
 "defaults":
     {
          "broker":"backtest"
      },
 "brokers":
     {
      "forex":
          {
                  "name": "forex",
                  "library":"~/.blueshift/data/library/forex"
            }
     },
 "datasets": {
         "library":"~/.blueshift/data/library"
     },
 "data_portal":
     {},         
 "oneclick":
      {
           "enabled": False,
           "timeout":1
     },
 "smart-orders":
      {
           "enabled": False,
           "timeout":1
     },
 "channels":
     {
          "cmd_addr": "127.0.0.1:9000",
          "msg_addr": "127.0.0.1:9001",
          "msg_type": "zmq",
          "health_addr": "127.0.0.1:9002",
          "zmq_addr":None,
          "blueshift_server":None,
          "callback_url":None,
          "timeout": 10
      },
 "resource": 
     {
          "stdout": None, 
          "timeout": None,
          "runtime":30,
          "memory": None
      },
 "error_handling":
     {
          "broker_error": "warn",
          "user_error": "stop"
     },
 "risk_management":
     {
          "max_order_per_day":None,
          "max_order_qty":None,
          "kill":None,
          "cooloff_period":30
      },
 "environment":
     {
          "BLUESHIFT_ACCESS_KEY_ID": None,
          "BLUESHIFT_SECRET_ACCESS_KEY": None,
          "BLUESHIFT_EMAIL": None,
          "BLUESHIFT_PHONE": None
     }
}
     

BLUESHIFT_PRODUCT = os_getenv('BLUESHIFT_PRODUCT_NAME', DEFAULT_BLUESHIFT_PRODUCT)
BLUESHIFT_PLATFORM = os_getenv('BLUESHIFT_PLATFORM', DEFAULT_BLUESHIFT_PLATFORM)
BLUESHIFT_SECURE_CODE = os_getenv('BLUESHIFT_SECURE_CODE', DEFAULT_BLUESHIFT_SECURE_CODE)
BLUESHIFT_SECURE_CODE = str2bool(BLUESHIFT_SECURE_CODE)
BLUESHIFT_DIR = os_getenv("BLUESHIFT_PATH", DEFAULT_BLUESHIFT_DIR)
BLUESHIFT_CONFIG = os_getenv("BLUESHIFT_CONFIG_FILE", DEFAULT_BLUESHIFT_CONFIG)
BLUESHIFT_USER_CONFIG = os_getenv("BLUESHIFT_USER_CONFIG_FILE", DEFAULT_BLUESHIFT_USER_CONFIG)
BLUESHIFT_API_KEY_ID = os_getenv("BLUESHIFT_ACCESS_KEY_ID","")
BLUESHIFT_SECRET_ACCESS_KEY = os_getenv("BLUESHIFT_SECRET_ACCESS_KEY","")
BLUESHIFT_CALENDAR_START = os_getenv("BLUESHIFT_CALENDAR_START_DATE","2000-01-01 00:00:00")

BLUESHIFT_COMMAND_ACCESS = os_getenv("BLUESHIFT_COMMAND_ACCESS",False)
BLUESHIFT_COMMAND_ACCESS = str2bool(BLUESHIFT_COMMAND_ACCESS)
BLUESHIFT_HEALTHCHECK = os_getenv("BLUESHIFT_HEALTHCHECK", False)
BLUESHIFT_HEALTHCHECK = str2bool(BLUESHIFT_HEALTHCHECK)
BLUESHIFT_STREAMING_DATA = os_getenv("BLUESHIFT_STREAMING_DATA", False)
BLUESHIFT_STREAMING_DATA = str2bool(BLUESHIFT_STREAMING_DATA)
BLUESHIFT_DISABLE_IPV6 = os_getenv("BLUESHIFT_DISABLE_IPV6", True)
BLUESHIFT_DISABLE_IPV6 = str2bool(BLUESHIFT_DISABLE_IPV6)
ELAPSED_TIME_THRESHOLD = os_getenv("BLUESHIFT_API_ELAPSED_THREHOSLD", 5)
ELAPSED_TIME_THRESHOLD = float(ELAPSED_TIME_THRESHOLD)
LOCK_TIMEOUT = os_getenv("BLUESHIFT_LOCK_TIMEOUT", 300)
LOCK_TIMEOUT = int(LOCK_TIMEOUT)
STREAMING_TIMEOUT = os_getenv("BLUESHIFT_STREAMING_TIMEOUT", 600)
STREAMING_TIMEOUT = int(STREAMING_TIMEOUT)
MARKET_ORDER_TIMEOUT = os_getenv("BLUESHIFT_MARKET_ORDER_TIMEOUT", 30)
MARKET_ORDER_TIMEOUT = int(MARKET_ORDER_TIMEOUT)
OPEN_ORDER_TIMEOUT = os_getenv("BLUESHIFT_OPEN_ORDER_TIMEOUT", 120)
OPEN_ORDER_TIMEOUT = int(OPEN_ORDER_TIMEOUT)
BLOTTER_UPDATE_TIMEOUT = os_getenv("BLUESHIFT_BLOTTER_UPDATE_TIMEOUT", 300)
BLOTTER_UPDATE_TIMEOUT = int(BLOTTER_UPDATE_TIMEOUT)
BLOTTER_MISSING_ORDER_THRESHOLD = os_getenv("BLUESHIFT_BLOTTER_MISSING_ORDER_THRESHOLD", 10)
BLOTTER_MISSING_ORDER_THRESHOLD = int(BLOTTER_MISSING_ORDER_THRESHOLD)
USE_IOC_SQUAREOFF = os_getenv("BLUESHIFT_USE_IOC_SQUAREOFF", False)
USE_IOC_SQUAREOFF = str2bool(USE_IOC_SQUAREOFF)
USE_IOC_EXIT_SQUAREOFF = os_getenv("BLUESHIFT_USE_IOC_EXIT_SQUAREOFF", False)
USE_IOC_EXIT_SQUAREOFF = str2bool(USE_IOC_EXIT_SQUAREOFF)
USE_MULTI_SQUAREOFF = os_getenv("BLUESHIFT_USE_MULTI_SQUAREOFF", False)
USE_MULTI_SQUAREOFF = str2bool(USE_MULTI_SQUAREOFF)
MAX_ITER_IOC_EXIT = os_getenv("BLUESHIFT_IOC_SQUAREOFF_MAX_ITER", 3)
MAX_ITER_IOC_EXIT = int(MAX_ITER_IOC_EXIT)
DEFAULT_IOC_ORDER = os_getenv("BLUESHIFT_USE_IOC_DEFAULT", False)
DEFAULT_IOC_ORDER = str2bool(DEFAULT_IOC_ORDER)
IOC_WAIT_TIME = os_getenv("BLUESHIFT_IOC_WAIT_TIME", 10)
IOC_WAIT_TIME = int(IOC_WAIT_TIME)
EXIT_ORDER_TIMEOUT = os_getenv("BLUESHIFT_EXIT_ORDER_TIMEOUT", 10)
EXIT_ORDER_TIMEOUT = int(EXIT_ORDER_TIMEOUT)
ORDER_RETRY_TIME = os_getenv("BLUESHIFT_ORDER_RETRY_TIME", 3)
ORDER_RETRY_TIME = int(ORDER_RETRY_TIME)
ORDER_CHECK_TIMEOUT = os_getenv("BLUESHIFT_ORDER_CHECK_TIMEOUT", 10)
ORDER_CHECK_TIMEOUT = int(ORDER_CHECK_TIMEOUT)
DEFAULT_COOLOFF = os_getenv("BLUESHIFT_DEFAULT_COOLOFF_PERIOD", 30)
DEFAULT_COOLOFF = int(DEFAULT_COOLOFF)
DEFAULT_CAPITAL = os_getenv("BLUESHIFT_DEFAULT_CAPITAL", 10000)
DEFAULT_CAPITAL = float(DEFAULT_CAPITAL)
RAISE_CAPITAL_ERROR = os_getenv("BLUESHIFT_RAISE_CAPITAL_ERROR", False)
RAISE_CAPITAL_ERROR = str2bool(RAISE_CAPITAL_ERROR)
RMS_LIMIT_THRESHOLD = os_getenv("BLUESHIFT_RMS_LIMIT_THRESHOLD", 0.7)
RMS_LIMIT_THRESHOLD = float(RMS_LIMIT_THRESHOLD)

BLUESHIFT_PRIMING_RUN = os_getenv("BLUESHIFT_PRIMING_RUN_NAME", 'priming')
BLUESHIFT_BACKTEST_BROKER_NAME = os_getenv("BLUESHIFT_BACKTEST_BROKER_NAME", None)
BLUESHIFT_LIVE_BROKER_NAME = os_getenv("BLUESHIFT_LIVE_BROKER_NAME", None)
BLUESHIFT_EAGER_SCHEDULING = os_getenv("BLUESHIFT_EAGER_SCHEDULING", False)
BLUESHIFT_EAGER_SCHEDULING = str2bool(BLUESHIFT_EAGER_SCHEDULING)
BLUESHIFT_DEBUG_MODE = os_getenv("BLUESHIFT_DEBUG_MODE", False)
BLUESHIFT_DEBUG_MODE = str2bool(BLUESHIFT_DEBUG_MODE)
BLUESHIFT_BROKER_SERVICE = os_getenv('BLUESHIFT_BROKER_SERVICE',False)
BLUESHIFT_BROKER_SERVICE = str2bool(BLUESHIFT_BROKER_SERVICE)
BLUESHIFT_DISABLE_KILL_SWITCH = os_getenv('BLUESHIFT_DISABLE_KILL_SWITCH',False)
BLUESHIFT_DISABLE_KILL_SWITCH = str2bool(BLUESHIFT_DISABLE_KILL_SWITCH)
ALLOW_USER_IMPORT = os_getenv('BLUESHIFT_ALLOW_USER_IMPORT',True)
ALLOW_USER_IMPORT = str2bool(ALLOW_USER_IMPORT)
ALLOW_EXTERNAL_EVENT = os_getenv('BLUESHIFT_ALLOW_EXTERNAL_EVENT',False)
ALLOW_EXTERNAL_EVENT = str2bool(ALLOW_EXTERNAL_EVENT)
SEND_POSITION_DATA = os_getenv('BLUESHIFT_SEND_POSITION_DATA',False)
SEND_POSITION_DATA = str2bool(SEND_POSITION_DATA)
SEND_TRADE_DATA = os_getenv('BLUESHIFT_SEND_TRADE_DATA',False)
SEND_TRADE_DATA = str2bool(SEND_TRADE_DATA)

BLUESHIFT_BACKTEST_INIT_SCRIPT = os_getenv("BLUESHIFT_BACKTEST_INIT_SCRIPT", "")
BLUESHIFT_LIVE_INIT_SCRIPT = os_getenv("BLUESHIFT_LIVE_INIT_SCRIPT", "")

MULTI_TENANCY = os_getenv('BLUESHIFT_MULTI_TENANCY', False)
MULTI_TENANCY = str2bool(MULTI_TENANCY)

MAX_ONECLICK_NOTIFICATIONS = os_getenv('BLUESHIFT_MAX_ONECLICK_NOTIFICATIONS', 5000)
MAX_ONECLICK_NOTIFICATIONS = int(MAX_ONECLICK_NOTIFICATIONS)
ONECLICK_EXPIRY = os_getenv('BLUESHIFT_ONECLICK_EXPIRY', 60) # oneclick expiry in seconds
ONECLICK_EXPIRY = int(ONECLICK_EXPIRY)

version = os_getenv('BLUESHIFT_VERSION', __version__)
if BLUESHIFT_DEBUG_MODE:
    version = f'{version}(DEBUG)'
BLUESHIFT_DISPLAY_VERSION = f'{version}'

SESSION_ID = os_getenv('BLUESHIFT_SESSION_ID',DEFAULT_SESSION_ID)

MAX_CTX_STATE_VARS = int(os_getenv('BLUESHIFT_MAX_CTX_STATE_VARS', 10))
MAX_CTX_RECORD_VARS = int(os_getenv('BLUESHIFT_MAX_CTX_RECORD_VARS', 10))

class BlueshiftConfig:
    """
        Read the supplied config file, or generate a default config,
        In case more named arguments are supplied as keywords, use
        them to replace the config params.

        Args:
            ``config_file (str)``: Path to configuration file.
            
    """

    def __init__(self, config_file:str|None=None, *args, **kwargs):
        self._root = config_file if config_file else join(expanduser(BLUESHIFT_DIR), BLUESHIFT_CONFIG)
        self._root = expanduser(self._root)
        self._create(config_file, *args, **kwargs)
        
    @property
    def root(self):
        return self._root

    def _create(self, config_file, *args, **kwargs):
        config_file = self.root

        if 'user_config' not in kwargs:
            default_user_config = join(expanduser(
                    BLUESHIFT_DIR), BLUESHIFT_USER_CONFIG)
            if exists(default_user_config):
                kwargs['user_config'] = default_user_config
        
        config:dict[str, Any] = {}
        if exists(config_file):
            try:
                with open(config_file) as fp:
                    config = json.load(fp)
            except json.JSONDecodeError as e:
                msg = f'Failed to load {BLUESHIFT_PRODUCT} config:{str(e)}.'
                raise InitializationError(msg)
        else:
            msg = 'missing config file {config_file}'
            raise InitializationError(msg=msg)
            
        user_config:dict[str, Any] = {}
        if 'user_config' in kwargs and kwargs['user_config']:
            if not isinstance(kwargs['user_config'], dict):
                try:
                    user_config = json.loads(kwargs['user_config'])
                except Exception:
                    try:
                        with open(expanduser(kwargs['user_config'])) as fp:
                            user_config = json.load(fp)
                    except Exception:
                        pass
            else:
                user_config = kwargs['user_config']

        if user_config and 'brokers' in user_config:
            broker_config = user_config['brokers']
            blueshift_config = config.get('brokers', _default_config['brokers'])
            #broker_config = {**blueshift_config, **broker_config}
            broker_config = merge_json_recursive(blueshift_config, broker_config)
        else:
            broker_config = config.get('brokers', _default_config['brokers'])
        self.brokers = broker_config
        
        if user_config and 'risk_management' in user_config:
            risk_config = user_config['risk_management']
            blueshift_config = config.get('risk_management', _default_config['risk_management'])
            #risk_config = {**blueshift_config, **risk_config}
            risk_config = merge_json_recursive(blueshift_config, risk_config)
        else:
            risk_config = config.get('risk_management', _default_config['risk_management'])
        self.risk_management = risk_config
        
        if user_config and 'data_portal' in user_config:
            data_portal_config = user_config['data_portal']
            blueshift_config = config.get('data_portal', _default_config['data_portal'])
            #risk_config = {**blueshift_config, **risk_config}
            data_portal_config = merge_json_recursive(blueshift_config, data_portal_config)
        else:
            data_portal_config = config.get('data_portal', _default_config['data_portal'])
        self.data_portal = data_portal_config
        
        self.user_space = config.get('workspace', _default_config['workspace'])
        self.alerts = config.get('alerts', _default_config['alerts'])
        self.defaults = config.get('defaults', _default_config['defaults'])
        self.oneclick = config.get('oneclick', {'enabled':False})
        self.smart_orders = config.get('smart-orders', {'enabled':False})
        self.channels = config.get('channels', _default_config['channels'])
        self.recovery = config.get('error_handling', _default_config['error_handling'])
        self.env_vars = config.get('environment', _default_config['environment'])
        self.resource = config.get('resource', _default_config['resource'])
        self.datasets = config.get('datasets', _default_config['datasets'])

        self._update_env_vars()

    def get_broker_details(self, broker=None) -> dict[str, Any]:
        if broker is None:
            if 'broker' not in self.defaults:
                return {}
            broker = self.defaults['broker']

        if broker not in self.brokers:
            return {}

        return self.brokers[broker]

    def _update_env_vars(self):
        for var in self.env_vars:
            value = self.env_vars[var]
            if value:
                os_environ[var] = value

    def __str__(self) -> str:
        return "Blueshift Config [{}]".format(self._root)

    def __repr__(self) -> str:
        return self.__str__()


class BlueshiftConfigWrapper:
    '''
        A wrapper object for Blueshift Configuration object to make
        access to it global.
    '''
    def __init__(self, config:BlueshiftConfig|None=None, algo_user:str|None=None):
        self.registry:dict[str|None, BlueshiftConfig] = {}
        if config:
            self.registry[algo_user] = config

    def get_config(self, algo_user:str|None=None, config_file:str|None=None, *args, **kwargs) -> BlueshiftConfig:
        if algo_user not in self.registry:
            config = BlueshiftConfig(config_file, *args, **kwargs)
            self.registry[algo_user] = config
            
        return self.registry[algo_user]

    def register_config(self, config:BlueshiftConfig, algo_user:str|None=None):
        if not isinstance(config, BlueshiftConfig):
            msg = f'Illegal input, expected config type, '
            msg += f'got {type(config)}.'
            raise ValidationError(msg)
            
        self.registry[algo_user] = config

global_config_wrapper = BlueshiftConfigWrapper()
register_config = global_config_wrapper.register_config
get_config = global_config_wrapper.get_config
