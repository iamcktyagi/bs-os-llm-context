from __future__ import annotations
from typing import TYPE_CHECKING, Callable, cast, Any, Literal
from os.path import isabs, join, isfile, basename, dirname, exists
from os import getcwd as os_getcwd
from os import getenv as os_getenv
from shutil import copy as shutil_copy
from io import StringIO
import gc
import weakref

import json
from sys import stdin as sys_stdin
import warnings

from blueshift.config import (
        BLUESHIFT_BACKTEST_BROKER_NAME, BLUESHIFT_LIVE_BROKER_NAME,
        ALLOW_USER_IMPORT, SEND_POSITION_DATA, 
        SEND_TRADE_DATA, register_config)
from blueshift.config.config import BlueshiftConfig
from blueshift.config.defaults import (
        blueshift_root, blueshift_run_path, blueshift_source_path, 
        get_config_broker_details, get_config_resource, 
        get_config_channel, get_config_env_vars)

from blueshift.calendar import get_calendar, make_consistent_tz
from blueshift.lib.common.functions import str2bool, create_message_envelope
from blueshift.lib.common.types import MsgQStringIO, PubStream
from blueshift.lib.common.constants import Frequency, CCY, Currency
from blueshift.lib.common.platform import print_msg
from blueshift.lib.common.enums import (
        ExecutionMode, AlgoMode as MODE, AlgoMessageType as MessageType,
        AlgoStatus, TerminalAlgoStatuses, BlueshiftCallbackAction)
from blueshift.lib.exceptions import (
        InitializationError, BlueshiftException, TerminationError,
        AuthenticationError, NotValidStrategy, BlueshiftWarning)
from blueshift.lib.trades.utils import create_position

from blueshift.interfaces.trading.broker import (
    list_brokers, broker_factory, paper_broker_factory, ILiveBroker, IBacktestBroker)
from blueshift.interfaces.logger import BlueshiftLogger
from blueshift.interfaces.context import IContext
from blueshift.interfaces.trading.broker import get_broker

from ..alerts import get_alert_manager, BlueshiftAlertManager

if TYPE_CHECKING:
    import pandas as pd
    from blueshift.core.algorithm.algorithm import TradingAlgorithm
    from blueshift.calendar import TradingCalendar
    from blueshift.interfaces.trading.broker import IBroker
    from blueshift.lib.clocks._clock import TradingClock
else:
    import blueshift.lib.common.lazy_pandas as pd

CMD_ARGS_FILE = 'cmd.blueshift'

_LIBRARY_CACHE = {}

class TradingEnvironment:
    '''
        The builder class of underlying objects - calendar, broker tuple,
        configuration and alert manager.
    '''

    def __init__(self, name:str|None, *args, **kwargs):
        self._initialized:bool = False
        self._is_finalized:bool = False
        self._final_status_sent:bool = False
        self.created_at:pd.Timestamp = pd.Timestamp.now(tz='Etc/UTC')
        self.algo_code:str|None = None
        self.root:str|None = None
        self.pwd:str|None = None
        self.config = None
        self.security = None
        self.ccy:Currency|None = None
        self._algo:weakref.ReferenceType[TradingAlgorithm]|None = None
        self.benchmark:str|None = None
        self.timeout:int = 0
        self.error_context:str|None = None
        self._close_fds:bool = False
        self.theme:str = 'light'

        # set True if external data is being used
        # For external data, cannot ensure forward-looking bias protection
        self.external_data:bool = False
        
        # for copying broker
        self._broker_name = None
        self._sim_name = None
        self._brkr_dict = {}
        self._sim_dict = {}
        
        self.compress_output:bool = kwargs.pop('compress_output', True)
        self.api_key:str|None = kwargs.pop('api_key', None)
        self.api_secret:str|None = kwargs.pop('secret_key', None)
        self.access_platform:str|None = kwargs.pop('platform', None)
        self.email:str|None = kwargs.pop('email', None)
        self.phone:str|None = kwargs.pop('phone', None)
        self.theme:str = kwargs.pop('theme', 'light')
        self.code_checker:str = kwargs.pop('code_checker', 'no-checker')
        self._callbacks:dict[str, list[Callable]] = {}

        try:
            alert_manager = kwargs.pop('alert_manager', get_alert_manager())

            if not alert_manager or not isinstance(alert_manager, BlueshiftAlertManager):
                msg = f'Failed to create environment, missing or invalid alert manager specified.'
                raise InitializationError(msg)
                
            self.alert_manager = alert_manager
            self._publisher = self.alert_manager.publisher
            self.server_mode:bool = kwargs.pop('server',self.alert_manager.server_mode)
            self.publish:bool = kwargs.pop('publish',self.alert_manager.publish)
            self.redirect:bool = kwargs.pop('redirect',self.alert_manager.redirect)
            self.platform:str = self.alert_manager.platform
            self.restart:bool = kwargs.pop("restart", False)
            self.verbose:bool = kwargs.pop("verbose", self.alert_manager.verbose)
            self.algo_id:str|None = kwargs.pop("algo_id", None)
            self.algo_name:str|None = kwargs.pop("algo_name", None)
            self.algo_moniker:str|None = kwargs.pop("algo_moniker", None)
            self.algo_user:str|None = kwargs.pop("algo_user", None)
            self.burn_in:bool = kwargs.pop("burn_in", False)
            self.quick:bool = kwargs.pop("quick", False)

            self._create(name, *args, **kwargs)
            self.current_context:str = self.name
        except AuthenticationError as e:
            msg = f'Please re-authenticate.'
            self.finalize(None, msg=msg)
            if self.server_mode:
                self.blueshift_callback(
                        action=BlueshiftCallbackAction.BROKER_AUTH.value,
                        msg=msg,
                        status=AlgoStatus.ERRORED)
            self._log_error(str(e))
            msg = f'failed to create environment:{str(e)}.'
            raise InitializationError(msg)
        except BlueshiftException as e:
            self._log_error(str(e))
            msg = f'failed to create environment:{str(e)}.'
            self.finalize(None, msg=msg)
            raise InitializationError(msg)
        
    def _handle_redirect(self):
        self.stderr = None
        self.stdout = None

        if self.redirect:
            if self.mode != MODE.BACKTEST:
                if not self.alert_manager.publisher:
                    msg = f'Cannot redirect stream, no publisher defined.'
                    self.logger.error(msg)
                try:
                    publisher = self.alert_manager.publisher
                    self.stdout = MsgQStringIO(
                            publisher=publisher, topic=self.name,
                            logger=self.logger)
                    self.stderr = self.stdout
                except Exception as e:
                    msg = f'Failed to redirect to stream: {str(e)}'
                    self.logger.error(msg)
            elif self.quick:
                    if not self.alert_manager.publisher:
                        msg = f'Cannot redirect stream, no publisher defined.'
                        self.logger.error(msg)
                    try:
                        publisher = self.alert_manager.publisher
                        if publisher:
                            self.stdout = PubStream(publisher=publisher, topic=self.name)
                            self.stderr = self.stdout
                    except Exception as e:
                        msg = f'Failed to redirect to quick baktest stream: {str(e)}'
                        self.logger.error(msg)
            else:
                try:
                    stdout = join(blueshift_run_path(
                            self.name),'output.log')
                    self.stdout = open(stdout, 'a')
                    stderr = join(blueshift_run_path(
                            self.name),'error.log')
                    self.stderr = open(stderr, 'a')
                    self._close_fds = True
                except Exception as e:
                    msg = f'Failed to redirect to files: {str(e)}'
                    self.logger.error(msg)
            
    def __del__(self):
        if self._close_fds:
            try:
                if self.stdout:
                    self.stdout.flush()
                    self.stdout.close()
            except Exception:
                pass
            try:
                if self.stderr:
                    self.stderr.flush()
                    self.stderr.close()
            except Exception:
                pass
            finally:
                self._close_fds = False
                
        del self._algo
        
    @property
    def algo(self):
        if self._algo:
            return self._algo()
    
    @algo.setter
    def algo(self, algo):
        if algo:
            self._algo = weakref.ref(algo)
        
    @property
    def callbacks(self):
        return self._callbacks
    
    def register_callback(self, func, ctx=None):
        ctx = ctx or self.name
        if ctx not in self._callbacks:
            self._callbacks[ctx] = []
        self._callbacks[ctx].append(func)

    def _handle_restart_options(self, kwargs):
        last_kwargs = self._load_command_args(kwargs)
        # things we can override if specified
        overrides = ['config_file', 'start_date', 'end_date',
                     'security_config', 'algo_file','algo', 
                     'execution_mode']
        # no overrides for these parameters
        no_overrides = ['initial_capital', 'mode', 'currency',
                        'broker','parameters']

        for key in no_overrides:
            if key in last_kwargs:kwargs.pop(key, None)
        for key in overrides:
            if key in kwargs and kwargs[key] is None:kwargs.pop(key, None)
            if key in kwargs and kwargs[key] == '':kwargs.pop(key, None)

        kwargs = {**last_kwargs, **kwargs}
        if 'mode' in kwargs and kwargs['mode'] == 'backtest':
            msg = 'restart in backtest not supported.'
            raise InitializationError(msg)
            
        if 'no_sec' in last_kwargs and last_kwargs['no_sec']:
            kwargs['no_sec'] = True
        
        return kwargs
    
    def __setattr__(self, key, value):
        allowed_keys = ('current_context','error_context')
        if key.startswith('_') or key in allowed_keys:
            return super().__setattr__(key, value)
        
        if hasattr(self,'_TradingEnvironment__lock_instance') and self.__lock_instance:
            raise AttributeError(f'Readonly instance, cannot set attribute.')
            
        return super().__setattr__(key, value)
    
    def lock_attrs(self):
        self.__lock_instance = True

    def __str__(self):
        mode = self.mode.name
        exec_mode = self.execution_mode.name
        return f"Blueshift Environment [name:{self.name}, mode:{mode}({exec_mode})]"

    def __repr__(self):
        return self.__str__()

    def _save_command_args(self, kwargs):
        #To convert date to string so that json can be save into file
        do_not_save_list = set(['user_config'])
        for key in do_not_save_list:
            kwargs.pop(key, None)
        
        if 'start_date' in kwargs and kwargs['start_date']:
            sdt = kwargs['start_date']
            sdt=pd.Timestamp(sdt)
            kwargs['start_date'] = sdt.strftime('%Y-%m-%d')
                        
        if 'end_date' in kwargs and kwargs['end_date']:
            edt = kwargs['end_date']
            edt=pd.Timestamp(edt)
            kwargs['end_date'] = edt.strftime('%Y-%m-%d')
        
        path = join(blueshift_run_path(self.name), CMD_ARGS_FILE)
        
        try:
            with open(path, 'w') as fp:
                json.dump(kwargs, fp)
        except Exception:
            msg = 'failed to save restart options.'
            print_msg(msg, _type="error", platform=self.platform)
            
    def _load_command_args(self, kwargs):
        if self.name is None:
            msg = 'name cannot be missing for restart.'
            raise InitializationError(msg)
            
        if not exists(blueshift_run_path(self.name, create=False)):
            msg = 'restart data not found.'
            raise InitializationError(msg)
        
        path = join(blueshift_run_path(self.name), CMD_ARGS_FILE)
        try:
            with open(path) as fp:
                cmd = json.load(fp)
                if 'parameters' in cmd and cmd['parameters']:
                    try:
                        cmd['parameters'] = json.loads(cmd['parameters'])
                    except Exception:
                        pass
                return cmd
        except Exception:
            msg = 'failed to load restart data.'
            raise InitializationError(msg)
    
    def _print_info(self, msg, nl=True):
        self.alert_manager.print_info(
                msg, name=self.name, level="info")
        
    def print(self, msg, _type:Literal['info','error','warning','success','info2']='info', msg_type=MessageType.STATUS_MSG):
        if msg_type not in (MessageType.STATUS_MSG, MessageType.STDOUT, MessageType.PLATFORM):
            raise ValueError(f'illegal message type {msg_type}')

        if self.publish and self._publisher:
            lvl = _type if _type != 'info2' else 'info'
            packet = create_message_envelope(self.name, msg_type, msg=msg, level=lvl)
            self._publisher.send_to_topic(self.name, packet)
        else:
            print_msg(
                    msg, _type=_type, platform=self.platform, nl=True)
        
    def _log_error(self, msg):
        try:
            if self.logger:
                self.logger.error(msg)
            elif self.alert_manager:
                self.alert_manager.logger.error(msg)
        except Exception:
            print_msg(msg, level='error')
        
            
    def _log_warning(self, msg):
        try:
            if self.logger:
                self.logger.warning(msg)
            elif self.alert_manager:
                self.alert_manager.logger.error(msg)
        except Exception:
            print_msg(msg, level='warning')

    def _create(self, name, *args, **kwargs):
        '''
            Function to create a trading environment with all objects
            necessary to run the algo.
        '''
        import uuid
        if not self.restart:
            if name is None:
                self.name = str(uuid.uuid4())
            else:
                self.name = str(name)
                
        mode = kwargs.pop('run_mode', None)
        mode = mode or kwargs.pop('mode', MODE.BACKTEST)
            
        try:
            if isinstance(mode, str):
                mode=mode.upper()
            mode = MODE(mode)
        except Exception:
            msg = f'Got illegal mode {mode}.'
            raise InitializationError(msg)
        else:
            if mode not in self.alert_manager.allowed_modes:
                msg = f'Mode {mode.value} not supported in this run started '
                msg += f'with mode {self.alert_manager.mode.value}.'
                raise InitializationError(msg)
            self.mode = mode
        
        if self.restart:
            if self.mode in (MODE.EXECUTION, MODE.BACKTEST):
                msg = f'Restart not allowed in mode {self.mode}.'
                raise InitializationError(msg=msg)
            kwargs = self._handle_restart_options(kwargs)
        
        # copy args before they are popped, save after the 
        # create is successful
        self.kwargs_to_save = kwargs.copy()
        
        # init cap None means auto compute required capital in paper or backtests
        self.init_capital = kwargs.pop("initial_capital", None)
        if self.init_capital == 0:
            self.init_capital = None
            msg = f'Missing capital input, will add capital automatically '
            msg = f'as and when necessary to execute trades.'
            warnings.warn(msg, BlueshiftWarning)

        if self.init_capital and self.init_capital < 0:
            msg = f'Starting capital, if specified, must be positive.'
            self._log_error(msg)
            raise InitializationError(msg)
        
        ccy = kwargs.pop("currency","LOCAL")
        if isinstance(ccy, CCY): # type: ignore
            self.ccy = ccy
        elif ccy:
            try:
                self.ccy = CCY[ccy.upper()] # type: ignore
            except Exception:
                msg = f'currency {ccy} is not supported.'
                raise InitializationError(msg)
            
        frequency = kwargs.pop('frequency', 'minute')
        try:
            self.frequency = Frequency(frequency)
        except Exception:
            msg = f'frequency {frequency} is not valid.'
            raise InitializationError(msg)
        
        security_config = kwargs.pop("security_config", 'security-check.json')
        self.benchmark = kwargs.pop("benchmark", None)
        
        timeout = kwargs.pop("timeout", 0)
        if not timeout:
            timeout = get_config_resource("timeout")
        if not timeout:
            timeout = 0
        try:
            self.timeout = int(timeout) # type: ignore
        except Exception:
            msg = f'Illegal timeout value {timeout}.'
            raise InitializationError(msg)
        
        
        params:dict|str = kwargs.pop("parameters", {})
        initial_state:dict|str = kwargs.pop("initial_state", {})
        self.set_algo_params(params)

        # create logger
        try:
            self.create_logger()
        except BaseException as e:
            msg = f'failed to create logger:{str(e)}'
            raise InitializationError(msg)
            
        # add info for server mode run
        msg = f'Creating env for algo run.'
        if self.server_mode:
            SERVER_NAME = os_getenv('BLUESHIFT_RESOURCE_NAME', '')
            SESSION_ID = os_getenv('BLUESHIFT_SESSION_ID','')
            if SERVER_NAME or SESSION_ID:
                msg += f' Running in {SERVER_NAME} and session {SESSION_ID}.'
        self.logger.info(msg)
            
        # we have got the mode, alert manager and logger set, call redirect
        self._handle_redirect()
        
        # create run specific config
        try:
            user_config = kwargs.pop("user_config", {})
            self.create_config(user_config)
        except BaseException as e:
            msg = f'failed to create config:{str(e)}'
            raise InitializationError(msg)
        
        # if we are here with redirect, we are printing to 0MQ
        if self.verbose:
            msg = f"Reticulating spline, "
            msg = msg + "setting up algorithm environment..."
            self._print_info(msg, nl=False)
        
        if 'no_sec' in kwargs and not kwargs['no_sec']:
            self.get_security_config(security_config)

        if self.verbose:
            self._print_info("Done.")
            msg = "Integrating volatility over z-axis..."
            self._print_info(msg, nl=False)

        if self.verbose:
            self._print_info("Done.")
            msg = "Configuring lunchtime ..."
            self._print_info(msg, nl=False)

        algo_file = kwargs.pop("algo_file", None)
        algo = kwargs.pop("algo", None)
        self.get_algo_file(algo, algo_file)

        if self.verbose:
            self._print_info("Done.")
            msg = "Tokenizing gravity..."
            self._print_info(msg, nl=False)
        
        # create the broker based on config details.
        # TODO: patch to enable oneclick on platform without server mode
        oneclick_env = str2bool(os_getenv(
                'BLUESHIFT_ALLOW_ONECLICK', False))
        execution_mode =  kwargs.pop(
                "execution_mode", ExecutionMode.AUTO)
        try:
            if isinstance(execution_mode, str):
                execution_mode = ExecutionMode(execution_mode.upper())
            # assert isinstance(execution_mode, ExecutionMode)
            if not isinstance(execution_mode, ExecutionMode):
                raise ValueError(f'illegal execution mode')
        except Exception:
            msg = f'Illegal execution mode {execution_mode}.'
            raise InitializationError(msg)
        if execution_mode == ExecutionMode.ONECLICK and \
            not self.server_mode and not oneclick_env:
            msg = f'Oneclick execution cannot be supported in this interface.'
            raise InitializationError(msg)
        self.execution_mode = execution_mode

        if self.verbose:
            self._print_info("Done.")
            msg = "Evaluating gene expression..."
            self._print_info(msg)
        
        broker_name = kwargs.pop('broker', None)
        if not broker_name:
            if self.mode == MODE.BACKTEST:
                broker_name = BLUESHIFT_BACKTEST_BROKER_NAME
            else:
                broker_name = BLUESHIFT_LIVE_BROKER_NAME
        self.create_broker(broker_name=broker_name)
        self.broker.initialize()

        calendar_name = kwargs.pop('trading_calendar', None)
        if not calendar_name:
            calendar_name = kwargs.pop('calendar', None)
        self.trading_calendar = self.create_calendar(calendar_name=calendar_name)
        
        self.start_dt, self.end_dt = self._get_date_range()
        
        msg = f"Broker connection is now ready."
        self.logger.platform(msg)
        self.create_clock()
        self.create_initial_state(initial_state)
        self.set_env_vars()
        
        # finally save the kwargs for restart
        if not self.restart:
            self._save_command_args(self.kwargs_to_save)
        
    def _get_date_range(self, **kwargs) -> tuple[pd.Timestamp, pd.Timestamp]:
        start_dt = kwargs.pop("start_date", None)
        end_dt = kwargs.pop("end_date", None)

        try:
            if start_dt:
                start_dt = make_consistent_tz(start_dt, tz=self.trading_calendar.tz)
        except Exception:
            raise InitializationError(f'start date is not a valid date, got {start_dt}')

        try:
            if end_dt:
                end_dt = make_consistent_tz(end_dt, tz=self.trading_calendar.tz)
        except Exception:
            raise InitializationError(f'end date is not a valid date, got {end_dt}')
            
        # check valid date range and capital for backtest
        if self.mode == MODE.BACKTEST:
            if pd.isna(start_dt) or pd.isna(end_dt):
                msg = f'missing start or end date, got start {start_dt} and end {end_dt}.'
                raise InitializationError(msg)
            
            start_date = end_date = pd.NaT
            try:
                start_date = make_consistent_tz(
                    self.broker.store.metadata['start_date'], tz=self.trading_calendar.tz)
                end_date = make_consistent_tz(
                    self.broker.store.metadata['end_date'], tz=self.trading_calendar.tz)
            except Exception:
                pass

            if not pd.isna(end_date) and end_dt > end_date:
                msg = f'Backtest end date must be before or on {end_date} '
                msg += f', will set end date to {end_date}.'
                self.end_dt = end_date
                self._log_warning(msg)
            
            if not pd.isna(start_date) and start_dt < start_date:
                msg = f'Backtest start date must be after or on {start_date} '
                msg += f', will set start date to {start_date}.'
                self.start_dt = start_date
                self._log_warning(msg)

            if start_dt > end_dt:
                msg = f'Backtest start date must be after end date '
                raise InitializationError(msg)
            
            start_dt = self.trading_calendar.bump_previous(start_dt)
            end_dt = self.trading_calendar.bump_forward(end_dt)
        else:
            if pd.isna(start_dt):
                start_dt = pd.Timestamp.now(tz=self.trading_calendar.tz).normalize()

            if pd.isna(end_dt):
                end_dt = pd.Timestamp.now(tz=self.trading_calendar.tz).normalize()
            
            start_dt = self.trading_calendar.bump_previous(start_dt)
            end_dt = self.trading_calendar.bump_forward(end_dt)

            max_runtime = float(get_config_resource('runtime')) # type: ignore
            if (end_dt - start_dt).days > max_runtime:
                msg = f'Execution run period cannot be greater than {max_runtime}.'
                self._log_error(msg)
                raise InitializationError(msg)
            
        return start_dt, end_dt

    def create_logger(self, tz=None):
        '''
            create and register the alert manager.
        '''
        if self.name != self.alert_manager.name:
            logger = BlueshiftLogger(self.name, verbose=self.verbose, tz=tz)
        else:
            logger = self.alert_manager.logger
        self.logger = logger
        
    def create_config(self, user_config=None):
        if user_config is None:
            user_config = {}
            
        try:
            kwargs = {}
            if user_config:
                kwargs['user_config'] = user_config
            config = BlueshiftConfig(self.alert_manager.config.root,**kwargs)
            register_config(config, self.algo_user)
            self.config = config
        except Exception as e:
            msg = f"failed to create configuration: {str(e)}."
            self._log_error(msg)
            self.finalize(None, msg=msg)
            raise InitializationError(msg)

    def get_security_config(self, security_config):
        """ extract security configuration for input course execution"""
        if not security_config:
            msg="Missing security configuration."
            self._log_error(msg)
            raise InitializationError(msg)
        try:
            if isfile(security_config):
                # absolute path
                with open(security_config) as fp:
                    self.security = json.load(fp)
            elif isfile(join(blueshift_root(),security_config)):
                # relative path to blueshift root
                with open(join(blueshift_root(),security_config)) as fp:
                    self.security = json.load(fp)
            else:
                # passed as text
                self.security = json.loads(security_config)
        except Exception:
            # this version requires a security config
            msg = f"failed to read security configuration."
            msg = msg + " Does not exist or not in json format."
            self._log_error(msg)
            self.finalize(None, msg=msg)
            raise InitializationError(msg)

    def get_algo_file(self, algo, algo_file):
        '''
            Search the current directory, else go the user config
            directory (code) and search.
        '''
        if not algo and not algo_file and not self.server_mode:
            # do not wait for user input in server mode
            target = join(blueshift_run_path(self.name),'algo.py')
            self.print('reading strategy from input', _type='info', msg_type=MessageType.STDOUT)
            with open(target, 'w') as fp:
                for line in sys_stdin:
                    fp.write(line)
            self.algo_file = target
            if ALLOW_USER_IMPORT:
                self.pwd = dirname(target)
                self.root = self.pwd
            self.kwargs_to_save['algo_file'] = target
            self.kwargs_to_save['algo'] = None
            return
        
        if algo:
            if isinstance(algo, str) and not algo.endswith('.py'):
                # check if we have valid Python code
                if len(algo[:128].split()) == 1:
                    # check if we have a registerd strategy
                    from blueshift.interfaces.code.strategy.strategy import get_strategy
                    strategy = get_strategy(algo)
                    if strategy:
                        try:
                            algo = strategy.to_python()
                        except Exception:
                            msg = f'failed to fetch strategy code for {algo}'
                            raise NotValidStrategy(msg)
                    else:
                        # assume a file, add the .py extension automatically
                        algo = algo.strip() + '.py'
                else:
                    try:
                        compile(algo, '<string>', 'exec')
                    except Exception:
                        msg = f'invalid input, expected a valid strategy'
                        raise NotValidStrategy(msg)
                
                if not algo.endswith('.py'):
                    # we assume this to be the algo source in string
                    target = join(blueshift_run_path(self.name),'algo.py')
                    source = StringIO(algo)
                    
                    with open(target, 'w') as fp:
                        for line in source:
                            fp.write(line)
                    
                    self.algo_file = target
                    self.algo_code = algo
                    if ALLOW_USER_IMPORT:
                        self.pwd = dirname(target)
                        self.root = self.pwd
                    self.kwargs_to_save['algo_file'] = target
                    self.kwargs_to_save['algo'] = None
                    return
                
            algo = str(algo)
            if isfile(algo):
                algo_file = algo
            elif isfile(join(blueshift_source_path(), algo)):
                algo_file = algo
            else:
                msg = f'algo {algo} does not exists or invalid'
                raise InitializationError(msg)
        
        if not algo_file:
            msg="Missing algo file."
            self._log_error(msg)
            raise InitializationError(msg)
            
        algo_file = str(algo_file)
        if isfile(algo_file):
            if isabs(algo_file):
                if ALLOW_USER_IMPORT:
                    self.root = dirname(algo_file)
            else:
                pwd = os_getcwd()
                if ALLOW_USER_IMPORT:
                    self.root = join(pwd, dirname(algo_file))
            self.pwd = self.root
        if not isabs(algo_file) and not isfile(algo_file):
            code_dir = blueshift_source_path()
            user_root = dirname(code_dir)
            user_code_dir = basename(code_dir)
            algo_file = join(user_root, user_code_dir, algo_file)
            if ALLOW_USER_IMPORT:
                self.root = join(user_root, user_code_dir)
                self.pwd = dirname(algo_file)
                if self.root not in self.pwd:
                    self.root = self.pwd

        if not isfile(algo_file):
            msg=f"Algo file {algo_file} does not exists."
            self._log_error(msg)
            raise InitializationError(msg)
            
        self.algo_file = algo_file
        target = join(blueshift_run_path(self.name),'algo.py')
        if algo_file != target:
            shutil_copy(algo_file, target)

    def create_calendar(self, calendar_name=None) -> TradingCalendar:
        '''
            get the calendar, defaults to broker calendar.
        '''
        if self.broker is None:
            msg = f'broker not initialized.'
            self._log_error(msg)
            raise InitializationError(msg)
        
        if calendar_name:
            trading_calendar = get_calendar(calendar_name)
            if not trading_calendar:
                msg = f'calendar {calendar_name} is not registered.'
                self._log_error(msg)
                raise InitializationError(msg)
            return trading_calendar
        else:
            return self.broker.calendar
            
    def create_clock(self):
        if self.mode == MODE.BACKTEST:
            from blueshift.lib.clocks._clock import SimulationClock
            sessions = self.trading_calendar.sessions(
                    self.start_dt, self.end_dt)
            if len(sessions) == 0:
                msg = f'No trading sessions between start and end dates.'
                self._log_error(msg)
                raise InitializationError(msg)
                
            clock = SimulationClock(
                trading_calendar=self.trading_calendar, 
                emit_frequency=int(self.frequency.minute_bars),
                start_dt=self.start_dt, 
                end_dt=self.end_dt)
        else:
            from blueshift.lib.clocks.realtime import RealtimeClock
            clock = RealtimeClock(
                trading_calendar=self.trading_calendar, 
                emit_frequency=int(self.frequency.minute_bars),
                start_dt=self.start_dt, 
                end_dt=self.end_dt,
                logger=self.logger)
            
        self.clock = clock
        
    def _handle_library_cache(self, brkr_dict):
        if self.mode != MODE.BACKTEST:
            return
        
        if not self.quick:
            return
        
        if 'library' not in brkr_dict:
            return
        
        root = brkr_dict['library']
        if not isinstance(root, str):
            return
        
        if root in _LIBRARY_CACHE:
            library = _LIBRARY_CACHE[root]
            library.reset()
            brkr_dict['library'] = library
            
        return root
    
    def _update_library_cache(self, root, lib):
        if self.mode != MODE.BACKTEST:
            return
        
        if not self.quick:
            return
        
        if not lib or not root or not isinstance(root, str):
            return
        
        # we want to keep only 1 lib at a time to reduce memory footprint
        for key in list(_LIBRARY_CACHE.keys()):
            item = _LIBRARY_CACHE.pop(key)
            del item
        
        _LIBRARY_CACHE[root] = lib
        gc.collect()

    def create_broker(self, broker_name):
        '''
            Create the broker object based on name mapping to a
            particular broker module under brokers. Alternatively, 
            the broker details can be passed as json object.
        '''
        if not broker_name:
            msg = f'broker name missing.'
            self._log_error(msg)
            raise InitializationError(msg)
            
        if self.logger:
            msg = f'Setting up connection to the broker.'
            self.logger.platform(msg)
            
        brkr_dict = get_config_broker_details(broker_name, self.algo_user)
        if not brkr_dict:
            try:
                brkr_dict = json.loads(broker_name)
            except Exception:
                pass
            else:
                if not isinstance(brkr_dict, dict):
                    brkr_dict = {}
            
        if not brkr_dict:
            msg = f'broker {broker_name} not in config or illegal json input.'
            self._log_error(msg)
            raise InitializationError(msg)
            
        options = { 
                   'initial_capital':self.init_capital,
                   'frequency':self.frequency}
        if self.ccy:
            options['ccy']:self.ccy # type: ignore
            
        brkr_dict = {**brkr_dict, **options}
        if not brkr_dict:
            msg = f'broker {broker_name} not in config.'
            self._log_error(msg)
            raise InitializationError(msg)
        
        name = brkr_dict.pop("name", None)
        if not name:
            msg = f'broker name missing.'
            self._log_error(msg)
            raise InitializationError(msg)
            
        # trigger broker registrations, for live and baccktests they are done via plugins.
        # for paper, combine a live and a backtester using paper_broker_factory
            
        if name not in list_brokers():
            msg = f'broker {name} not implemented.'
            self._log_error(msg)
            raise InitializationError(msg)
            
        benchmark = brkr_dict.pop("benchmark", None)
        if benchmark and not self.benchmark:
            # set broker benchmark if not already set.
            self.benchmark = benchmark
            
        if self.logger and 'logger' not in brkr_dict:
            brkr_dict['logger'] = self.logger
            
        if self.mode == MODE.BACKTEST:
            # no proxy or shared mode in backtest
            brkr_dict.pop('proxy', None)
            brkr_dict.pop('host', None)
            brkr_dict.pop('shared', None)
        else:
            if_shared = 'shared' in brkr_dict and brkr_dict['shared']
            
            if 'data_portal' in brkr_dict:
                if isinstance(brkr_dict['data_portal'], str):
                    portal = brkr_dict['data_portal']
                    portal = self.alert_manager.get_data_portal(portal)
                    
                    try:
                        portal.reset_connection_abort() # type: ignore
                    except Exception:
                        pass
                    
                    brkr_dict['data_portal'] = portal
            
            if self.server_mode or if_shared:
                brkr_dict['shared'] = True
                brkr_dict['collection'] = self.alert_manager.shared_brokers
                brkr_dict['algo'] = self.name
                brkr_dict['shared_logger'] = self.alert_manager.logger
                    
            brkr_dict['algo_user'] = self.algo_user
            if 'shared' in brkr_dict:
                self.logger.info(f'Algo run using shared broker')
            elif 'proxy' in brkr_dict:
                self.logger.info(f'Algo run using proxy broker')
        if self.mode == MODE.PAPER:
            simulator = brkr_dict.pop('simulator', None)
            if not simulator:
                msg = f'broker {broker_name} does not support paper simulation.'
                self._log_error(msg)
                raise InitializationError(msg)
            
            # this must always be broker name
            sim_dict = get_config_broker_details(simulator)
            if not sim_dict:
                msg = f'Simulator {simulator} not in config.'
                self._log_error(msg)
                raise InitializationError(msg)
            
            sim_name = sim_dict.pop("name", None)
            if not sim_name:
                msg = f'Missing simulation broker name.'
                self._log_error(msg)
                raise InitializationError(msg)
                
            if sim_name not in list_brokers():
                msg = f'Simulation broker {sim_name} not implemented.'
                self._log_error(msg)
                raise InitializationError(msg)
            
            sim_dict['simulator'] = True
            sim_dict['end_date'] = self.end_dt
            sim_dict = {**sim_dict, **options}
            broker = broker_factory(name=name, **brkr_dict)
            # save a copy of the input configs
            self._broker_name = name
            self._sim_name = sim_name
            self._brkr_dict = brkr_dict.copy()
            self._sim_dict = sim_dict.copy()
            simulator = broker_factory(name=sim_name, **sim_dict)
            broker = cast(ILiveBroker, broker)
            simulator = cast(IBacktestBroker, simulator)
            cls = get_broker('paper')
            self.broker = paper_broker_factory(broker, simulator, self.init_capital, self.frequency)
        else:
            self._broker_name = name
            self._brkr_dict = brkr_dict.copy()
            lib_root = self._handle_library_cache(brkr_dict)
            self.broker = broker_factory(name=name, **brkr_dict)
            if self.mode == MODE.BACKTEST:
                self._update_library_cache(lib_root, self.broker.library)
            
        if self.ccy is None:
            self.ccy = self.broker.ccy
            
        if self.mode not in self.broker.supported_modes:
            msg = f'broker {broker_name} does not support run mode {self.mode.name}.'
            self._log_error(msg)
            raise InitializationError(msg)
            
        self.logger.tz = self.broker.tz
        self.broker.logger = self.logger
            
    def create_broker_copy(self, init_cap:float|None=None):
        if not self.broker:
            msg = f'Cannot copy broker, not created yet.'
            raise InitializationError(msg)
        
        if self.mode == MODE.PAPER:
            # copy only the simulator, not the broker
            fx_support = str2bool(self._brkr_dict.get('fx_support'))
            sim_dict = self._sim_dict.copy()
            init_cap = init_cap or self.init_capital
            sim_dict['initial_capital'] = init_cap
            simulator = broker_factory(name=self._sim_name, **sim_dict)
            self.broker = cast(ILiveBroker, self.broker)
            simulator = cast(IBacktestBroker, simulator)
            cls = get_broker('paper')
            return paper_broker_factory(self.broker, simulator, self.init_capital, self.frequency)
        else:
            # copy the broker
            brkr_dict = self._brkr_dict.copy()
            if init_cap is not None:
                brkr_dict['initial_capital'] = init_cap
            lib_root = self._handle_library_cache(brkr_dict)
            broker = broker_factory(name=self._broker_name, **brkr_dict)
            if self.mode == MODE.BACKTEST:
                self._update_library_cache(lib_root, self.broker.library)
            return broker

    def set_algo_params(self, params):
        if params:
            try:
                if isfile(params): # type: ignore
                    # absolute path
                    with open(params) as fp: # type: ignore
                        self.parameters:dict[str, Any] = json.load(fp)
                elif isfile(join(blueshift_root(),params)): # type: ignore
                    # relative path to blueshift root
                    with open(join(blueshift_root(),params)) as fp: # type: ignore
                        self.parameters:dict[str, Any] = json.load(fp)
                else:
                    # passed as text
                    params = json.loads(params) # type: ignore
                    if isinstance(params, str):
                        # must be a dict
                        params = json.loads(params)
                    self.parameters:dict[str, Any] = params
            except Exception as e:
                if isinstance(e, TerminationError):
                    raise
                # this version requires a security config
                msg = f"failed to read algo parameters."
                msg = msg + f" Does not exist or not in json format: {str(e)}"
                self._log_error(msg)
                raise InitializationError(msg)
        else:
            self.parameters:dict[str, Any] = {}
            
    def create_initial_state(self, initial_state:dict|str):
        if self.restart or self.mode == MODE.EXECUTION:
            # no externally defined initial state for restarts or 
            # in the execution mode.
            self.initial_state = {}
            return
        
        if not initial_state:
            self.initial_state = {}
            return
        
        
        state = initial_state
        self.initial_state = {}
        try:
            if isfile(state): # type: ignore
                # absolute path
                with open(state) as fp: # type: ignore
                    self.initial_state = json.load(fp)
            elif isfile(join(blueshift_root(),state)): # type: ignore
                # relative path to blueshift root
                with open(join(blueshift_root(),state)) as fp: # type: ignore
                    self.initial_state = json.load(fp)
            else:
                # passed as text
                state = json.loads(state) # type: ignore
                if isinstance(state, str):
                    # must be a dict
                    state = json.loads(state)
                self.initial_state = state
        except Exception as e:
            if isinstance(e, TerminationError):
                raise
            # this version requires a security config
            msg = f"failed to read algo initial state."
            msg = msg + f" Does not exist or not in json format: {str(e)}"
            self._log_error(msg)
            raise InitializationError(msg)
                
        if not self.initial_state:
            self.initial_state = {}
            return
        
        # extract initial state
        self.initial_state = cast(dict, self.initial_state)
        if 'positions' in self.initial_state and self.initial_state['positions']:
            # create the initial positions
            if not self.broker:
                msg = f'Cannot read initial positions, broker not initialied.'
                raise InitializationError(msg)
            asset_finder = self.broker
            positions = {}
            for item in self.initial_state['positions']:
                item['asset_finder'] = asset_finder
                item['tz'] = self.trading_calendar.tz
                if 'product_type' not in item:
                    item['product_type'] = self.broker.supported_products[0]
                pos = create_position(**item)
                if pos.product_type not in self.broker.supported_products:
                    msg = f"Product type {pos.product_type} is not supported"
                    msg += " by the broker."
                    raise InitializationError(msg)
                    
                if pos.asset in positions:
                    positions[pos.asset].add_to_position(pos)
                else:
                    positions[pos.asset] = pos
                
            self.initial_state['positions'] = positions
    
    def set_env_vars(self):
        envs:dict = get_config_env_vars() # type: ignore
        
        if not self.api_key:
            self.api_key = envs.get('BLUESHIFT_ACCESS_KEY_ID')
        if not self.api_secret:
            self.api_secret = envs.get('BLUESHIFT_SECRET_ACCESS_KEY')
        if not self.access_platform:
            self.access_platform = envs.get('BLUESHIFT_PLATFORM')
        if not self.email:
            self.email = envs.get('BLUESHIFT_EMAIL')
        if not self.phone:
            self.phone = envs.get('BLUESHIFT_PHONE')
            
    def get_current_error_context(self):
        return self.error_context
            
    def finalize(self, timestamp, *args, **kwargs):
        if self._is_finalized:
            return
        
        try:
            if self.broker:
                self.broker.finalize()
        except Exception as e:
            self.logger.error(f'failed to finalize the algo broker: {str(e)}.')
        
        if self._close_fds:
            try:
                if self.stdout:
                    self.stdout.flush()
                    self.stdout.close()
            except Exception:
                pass
            try:
                if self.stderr:
                    self.stderr.flush()
                    self.stderr.close()
            except Exception:
                pass
            finally:
                self._close_fds = False
        
        try:
            status = AlgoStatus.ERRORED.value
            if self.algo:
                try:
                    status = self.algo.status.value
                except Exception as e:
                    if self.logger:
                        self.logger.error(f'failed to fetch algo status: {str(e)}.')
                    
                
            # send callback on exit
            msg = kwargs.pop('msg', '')
            if self.algo:
                msg = msg or self.algo.completion_msg
                
            action = BlueshiftCallbackAction.ALGO_STATUS.value
            if self.mode==MODE.EXECUTION:
                action = BlueshiftCallbackAction.SMART_ORDER_STATUS.value
            
            try:
                self.blueshift_callback(
                        action=action, msg=msg, status=status)
            except Exception as e:
                if self.logger:
                    msg = f'Failed sending algo callback for {self.name}:{str(e)}'
                    self.logger.error(msg)
            
            try:
                if self.alert_manager.publisher:
                    packet = create_message_envelope(self.name, MessageType.ALGO_END, msg=msg)
                    self.alert_manager.publisher.send_to_topic(self.name, packet)
            except Exception as e:
                if self.logger:
                    msg = f'Failed sending status update for algo {self.name}:{str(e)}'
                    self.logger.error(msg)
            
            # remove from joblist
            try:
                if self.algo:
                    if self.logger:
                        self.logger.info(f'Removing algo {self.name}')
                    # set finalize_algo to False to avoid recursion, as 
                    # that will trigger algo finalize otherwise, which 
                    # will trigger env finalize in turn.
                    self.alert_manager.remove_algo(
                            self.algo, finalize_algo=False)
            except Exception as e:
                if self.logger:
                    msg = f'Failed removing algo {self.name}:{str(e)}'
                    self.logger.error(msg)
                
            if self.logger:
                self.logger.platform(f'Algo run {self.name} finished.')
                
                if self.name != self.alert_manager.name:
                    try:
                        self.logger.close()
                    except Exception:
                        pass
        finally:
            self._is_finalized
            
    def blueshift_callback(self, action, msg=None, status:AlgoStatus|str=AlgoStatus.DONE):
        if self._final_status_sent:
            return
            
        if not self.server_mode:
            return
        
        try:
            if isinstance(status, str):
                status = AlgoStatus(status.lower())
            else:
                status = AlgoStatus(status)
            status=status.value.lower()
        except Exception:
            msg = f'Cannot send status update, illegal status update {status}.'
            self.logger.error(msg)
            return
            
        if self.logger:
            self.logger.platform(f'Algo reported status {status}:{msg}')
            
        perf={}
        positions = {}
        trades = {}
        intervene = False
        intervention_msg = ''
        has_open_positions = False
        has_open_orders = False
        
        try:
            if self.algo:
                intervene = self.algo.manual_intervention
                intervention_msg = self.algo.manual_intervention_msg
                perf = self.algo.context.performance
                self.algo.context.blotter
                
                if self.algo.mode !=MODE.BACKTEST:
                    context = cast(IContext, self.algo.context)
                    positions = context.blotter.get_open_positions()
                    
                    if positions:
                        has_open_positions = True
                    
                    if SEND_POSITION_DATA:
                        positions = {asset.exchange_ticker:pos.to_json_summary() for asset,pos in positions.items()}
                    else:
                        positions = {}
                    
                    trades = context.blotter.get_orders()
                    has_open_orders = any(
                            [not o.is_final() for oid,o in trades.items()])
                    
                    if SEND_TRADE_DATA:
                        trades = {str(oid):order.to_json_summary() for oid,order in trades.items()}
                    else:
                        trades = {}
                    
        except Exception:
            pass
            
        payload = {
                'name':self.name,
                'created_at':str(self.created_at),
                'initial_capital':self.init_capital,
                'end_date':str(self.end_dt),
                'algo':self.algo_name,
                'status':status,
                'has_open_positions':has_open_positions,
                'positions':positions,
                'has_open_orders':has_open_orders,
                'trades':trades,
                'intervention':intervene,
                'intervention_msg':intervention_msg,
                }
        
        try:
            json.dumps(self.parameters)
        except Exception:
            pass
        else:
            payload['details'] = self.parameters
        
        try:
            self.alert_manager.blueshift_callback(
                    self.name, action=action, msg=msg, status=status, 
                    performance=perf, payload=payload)
            if status in TerminalAlgoStatuses:
                self._final_status_sent = True
        except TerminationError:
            raise
        except Exception as e:
            msg = f'Failed to send status update: {str(e)}.'
            self.logger.error(msg)

    def user_callback(self, data, msg=None, status:AlgoStatus|str=AlgoStatus.RUNNING):
        if not self.server_mode:
            return
        
        try:
            if isinstance(status, str):
                status = AlgoStatus(status.lower())
            else:
                status = AlgoStatus(status)
            status=status.value.lower()
        except Exception:
            msg = f'Cannot send user update, illegal status {status}.'
            self.logger.error(msg)
            return
            
        perf={}
        positions = {}
        trades = {}
        intervene = False
        intervention_msg = ''
        
        try:
            if self.algo:
                context = cast(IContext, self.algo.context)
                intervene = self.algo.manual_intervention
                intervention_msg = self.algo.manual_intervention_msg
                perf = self.algo.context.performance
                positions = context.blotter.get_open_positions()
                positions = {asset.exchange_ticker:pos.to_json_summary() for asset,pos in positions.items()}
                trades = context.blotter.get_orders()
                trades = {str(oid):order.to_json_summary() for oid,order in trades.items()}
        except Exception:
            pass
            
        payload = {
                'name':self.name,
                'created_at':str(self.created_at),
                'initial_capital':self.init_capital,
                'end_date':str(self.end_dt),
                'algo':self.algo_name,
                'status':status,
                'positions':positions,
                'trades':trades,
                'intervention':intervene,
                'intervention_msg':intervention_msg,
                'data':data,
                }
        
        try:
            self.alert_manager.blueshift_callback(
                    self.name, 
                    action=BlueshiftCallbackAction.EXTERNAL_EVENT.value, 
                    msg=msg, status=status, 
                    performance=perf, payload=payload)
        except TerminationError:
            raise
        except Exception as e:
            msg = f'Failed to send user update: {str(e)}.'
            self.logger.error(msg)

            
