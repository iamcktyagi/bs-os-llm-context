from __future__ import annotations
from typing import TYPE_CHECKING, Callable, cast, Literal
import asyncio
import json
import os
import sys
import io
import traceback as tb
from contextlib import closing
from importlib import import_module
import time
from concurrent.futures import ThreadPoolExecutor

from blueshift.config.config import BlueshiftConfig, register_config, BLUESHIFT_DIR, BLUESHIFT_CONFIG
from blueshift.config.defaults import blueshift_dir
from blueshift.config import BLUESHIFT_PATH, SITE_PACKAGE_PATH, BLUESHIFT_PRODUCT
from blueshift.config import (
        get_config_recovery, get_config_channel, get_config_alerts, get_blueshift_server)

from blueshift.lib.exceptions import (
        ExceptionHandling, TerminationError, UserDefinedException,  BrokerError, InitializationError, 
        AuthenticationError, SubContextTerminate, UserStopError, BlueshiftException, get_clean_err_msg)
from blueshift.lib.common.sentinels import noops
from blueshift.lib.common.types import BlueshiftMsgLogHandler
from blueshift.lib.common.enums import (
        AlgoMode as MODE, 
        AlgoMessageType as MessageType, 
        BlueshiftCallbackAction, AlgoStatus)
from blueshift.lib.common.platform import print_exception, print_msg, get_exception
from blueshift.lib.common.functions import create_message_envelope

from blueshift.interfaces.data.data_portal import DataPortal
from blueshift.interfaces.trading.broker import broker_factory
from blueshift.interfaces.logger import BlueshiftLogger
from blueshift.interfaces.data.source import data_source_factory
from blueshift.interfaces.data.ingestor import ingestor_factory, StreamingIngestor
from blueshift.interfaces.nostreaming import NoPublisher
from blueshift.interfaces.streaming import get_publisher
from blueshift.interfaces.trading.broker import get_broker_collection, IBrokerCollection
from blueshift.interfaces.callbacks import server_callback_factory, CallBackResult

from blueshift.lib.serialize.json import convert_nan, BlueshiftJSONEncoder

from blueshift.providers import *

if TYPE_CHECKING:
    from blueshift.core.algorithm.algorithm import TradingAlgorithm
    from blueshift.core.utils.environment import TradingEnvironment


def _zmq_parser(msg:str|dict) -> tuple[str, str]|tuple[str, dict]:
    if not isinstance(msg, dict):
        return 'info', msg
    
    if 'event' in msg:
        # structure is {'event':'name','payload':{...}}
        event:str = msg.pop('event')

        if 'payload' in msg:
            payload:dict = msg['payload']
            return event, payload
        
        return event, msg
    
    return 'DATA', msg

_known_threads:dict[str,str] = {
        'SocketIOBgTask':'streaming',
        'CommandChannelListener':'algo-service',
        'algo_threadpool':'algo-service',
        'AsyncStreamingSourceHandler':'streaming',
        'StreamingSourceHandler':'streaming',
        'MainThread':'algo-service',
        }

def _map_threads() -> tuple[dict[str,int], set[str]]:
    import threading
    threads = threading.enumerate()
    stats = {v:0 for k,v in _known_threads.items()}
    stats['others'] = 0
    others:set[str] = set()
    
    for th in threads:
        found = False
        for k in _known_threads:
            if k in th.name:
                stats[_known_threads[k]] += 1
                found = True
                break
        if not found:
            stats['others'] += 1
            others.add(th.name)
        
    return stats, others

class BlueshiftAlertManager:
    """ Alert Manager

        Class to handle all blueshift alert handling. The alert
        manager should be called in a try/except block, passing
        any necessary arguments. The manager, based on the user
        specified rules, will decide how to handle the exception,
        including re-starts if provisioned. It also initiates the
        pub-sub for performance packets as well as external command
        channel to control a live algorithm.

        Note:
            It also provides a callback list is available for any object
            to register a callback with alert manager. All such callbacks
            will be called (in order of registration) once the shutdown
            process is initiated. This is a way to enable any object a
            method to persist itself, for example.

            see also :ref:`alerts configuration <alertsconf>`.

        Args:
            ``name(str, in kwargs)``: a name for the current run


    """
    MAX_RETRIES_LIVE = 5
    MAX_RETRIES_BACKTEST = 20

    ERROR_TYPES = [UserDefinedException, BrokerError]

    ERROR_RULES_MAP:dict[str, ExceptionHandling] = {"warn":ExceptionHandling.WARN,
                      "stop":ExceptionHandling.TERMINATE,
                      "re_start":ExceptionHandling.RECOVER,
                      "ignore":ExceptionHandling.IGNORE,
                      "log":ExceptionHandling.LOG}

    def __init__(self, name:str, max_workers:int|None, *args, **kwargs):
        '''
            Set up the error rules and logger. The logger does the
            message dispatch. The error rules decide the error
            response. The callback list is available for any object
            to register a callback with alert manager. All such
            callbacks will be called (in order of registration) once
            the shutdown process is initiated. This is a way to
            enable any object a method to persist itself, for
            example.
        '''
        self._create(name, max_workers, *args, **kwargs)
            
    def __del__(self):
        try:
            self._server_callback.close()
        except Exception:
            pass

    def _create(self, name:str, max_workers:int|None, *args, **kwargs):
        if not name:
            msg = f'Failed to create alert manager - missing name.'
            raise InitializationError(msg)
            
        self._get_event_loop()
        self._allowed_modes:set[MODE] = set()
        self._is_finalized:bool = False
        self._stopping:bool = False
        self.name = name
        self.server_mode = cast(bool, kwargs.pop('server', False))
        self._command_loop = kwargs.pop('command_loop', None)
        self._monitor_task = None

        if self.server_mode:
            self._callback_url:str|None = None
            if 'callback_url' in kwargs and kwargs['callback_url']:
                self._callback_url = str(kwargs['callback_url'])
            self._callback_url = self.get_blueshift_callback_url(
                self._callback_url or cast(str, get_config_channel('callback_url')))
            DEPLOYMENT_AUTH = os.getenv('BLUESHIFT_DEPLOYMENT_AUTH', None)
            self._server_callback = server_callback_factory('rest-api',self._callback_url, DEPLOYMENT_AUTH)
        else:
            self._server_callback = server_callback_factory('no-callback','')
        
        mode = kwargs.pop('mode', MODE.BACKTEST)
        try:
            if isinstance(mode, str):
                mode = MODE(mode.upper())
            #assert isinstance(mode, MODE)
            if not isinstance(mode, MODE):
                raise ValueError(f'illegal mode')
        except Exception:
            msg = f'Illegal mode {mode}.'
            raise InitializationError(msg)
        self.mode = mode
        
        if self.mode == MODE.BACKTEST:
            self._allowed_modes = set([MODE.BACKTEST])
        else:
            self._allowed_modes = set([MODE.PAPER, MODE.LIVE, MODE.EXECUTION])
        
        self.redirect = cast(bool, kwargs.pop("redirect", False))
        self.publish = cast(bool, kwargs.pop("publish", False))
        self.verbose = cast(bool, kwargs.pop("verbose", True))
        self.pre_processing:list = kwargs.pop('pre_processing', [])
        self.post_processing:list = kwargs.pop('post_processing', [])
        self._stream = None
        self._stdout = None
        self._stderr = None
        self.platform = str(kwargs.pop('platform', 'blueshift'))
        
        config_file = kwargs.pop('config_file', None)
        user_config = kwargs.pop('user_config', None)
        self.create_config(
                config_file=config_file, user_config=user_config)
        
        logger:BlueshiftLogger|None = kwargs.pop('logger', None)
        if logger and isinstance(logger, BlueshiftLogger):
            self.logger = logger
        else:
            self.logger = BlueshiftLogger(self.name, verbose=self.verbose)
            
        self._jobs:dict[str,TradingAlgorithm] = {}

        self.error_rules = {}
        self.callbacks = {self.name:[]}

        self._set_error_handling()
        self._retry_counts = {}
        
        cmd_channel = kwargs.get("command_channel", None)
        cmd_channel = cmd_channel or get_config_channel('cmd_addr')
        self.cmd_channel = self._handle_channel(cmd_channel)
        self._shared_brokers = get_broker_collection(logger=self.logger)
        
        self.publisher = NoPublisher()
        zmq_channel = kwargs.get("zmq_channel", None)
        message_channel = kwargs.get("message_channel", None)
        health_channel = kwargs.get("health_channel", None)
        self._set_up_subscriber(self.name, zmq_channel)
        self._set_up_publisher(self.name, message_channel)
        self._set_up_health_listener(health_channel)
        
        self._max_retry = self.MAX_RETRIES_BACKTEST if \
            self.mode==MODE.BACKTEST else self.MAX_RETRIES_LIVE
            
        self._data_portals:dict[str, DataPortal] = {}
        
        if max_workers:
            max_workers = min(200, max(max_workers, 5))
        else:
            max_workers = None
        self._executor = ThreadPoolExecutor(
                max_workers=max_workers, thread_name_prefix='algo_threadpool')
            
        msg = f'Set up alert manager {self} with retry limits {self._max_retry}.'
            
        self.logger.info(msg)
        register_alert_manager(self)

    def __str__(self):
        return f"Blueshift Alert Manager[{self.name}]"

    def __repr__(self):
        return self.__str__()
    
    @property
    def streamer(self) -> StreamingIngestor:
        return self._streamer # type: ignore
    
    @property
    def jobs(self) -> dict[str, TradingAlgorithm]:
        return self._jobs
    
    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        if self._loop and not self._loop.is_closed():
            return self._loop
        
        return self._get_event_loop()
    
    @property
    def shared_brokers(self) -> IBrokerCollection:
        return self._shared_brokers
    
    @property
    def executor(self) -> ThreadPoolExecutor:
        return self._executor
    
    @property
    def allowed_modes(self) -> set[MODE]:
        return self._allowed_modes
    
    @property
    def config(self) -> BlueshiftConfig:
        return self._config
    
    def get_data_portal(self, name) -> DataPortal|None:
        if name in self._data_portals:
            return self._data_portals.get(name)
        # try creating
        return self._create_data_portal(name)
    
    def _create_data_portal(self, name):
        config = self.config.data_portal.get(name)
        if not config:
            msg = f'failed to create data portal {name}:missing config.'
            raise InitializationError(msg)
        
        try:
            config = config.copy()
            if 'shared' in config and config['shared']:
                raise ValueError(f'Shared method not supported for data portal.')
            elif 'proxy' in config and config['proxy']:
                raise ValueError(f'Proxy method not supported for data portal.')
            config['logger'] = self.logger
            portal = broker_factory(**config)
            self._data_portals[name] = portal
            return portal
        except Exception as e:
            msg = f'failed to create data portal {name}:{str(e)}.'
            raise InitializationError(msg)
    
    def print_info(self, msg:str, name:str|None=None, 
                   level:Literal['info','error','warning','success','info2']='info', msg_type:MessageType=MessageType.STATUS_MSG):
        if msg_type not in (MessageType.STATUS_MSG, MessageType.PLATFORM, MessageType.STDOUT):
            raise ValueError(f'invalid message type {msg_type}')
        lvl = level if level != 'info2' else 'info'
        
        if self.publish and self.publisher:
            if name:
                packet = create_message_envelope(name, msg_type, msg=msg, level=lvl)
                self.publisher.send_to_topic(name, packet)
            else:
                packet = create_message_envelope(self.name, msg_type, msg=msg, level=lvl)
                self.publisher.send_to_topic(self.name, packet)
        else:
            print_msg(msg, level=level)
            
    def is_stopping(self) -> bool:
        return self._stopping
    
    def get_env(self, name:str) -> TradingEnvironment|None:
        job = self._jobs.get(name)
        if job:
            return job.env
    
    def get_logger(self, name:str|None=None) -> BlueshiftLogger:
        if name is None:
            return self.logger
        
        job = self._jobs.get(name)
        if job:
            return job.logger
        else:
            return self.logger
        
    def _get_event_loop(self):
        try:
            self._loop = asyncio.get_running_loop()
        except Exception:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop
             
        self._loop.set_exception_handler(self.handle_loop_exception)
        return self._loop
    
    def get_algo(self, name:str) -> TradingAlgorithm|None:
        return self._jobs.get(name)
    
    def register_algo(self, job:TradingAlgorithm):
        '''
            register a job (algo).
        '''
        if self._is_finalized:
            msg = f'This alert manager is already finalized.'
            raise TerminationError(msg)
        
        if self.publisher:
            alert_rules = get_config_alerts()
            handler = BlueshiftMsgLogHandler(
                    job.name,
                    self.publisher,
                    job.logger.handler_obj.formatter,
                    alert_rules)
            job.logger.add_handler('msg', handler)
        
        self._jobs[job.name] = job
        self.reset_error_tracker(job.name)
        
        if self.publish and self.publisher:
            packet = create_message_envelope(self.name, MessageType.STDOUT, display='no')
            self.publisher.send_to_topic(self.name, packet)
        
    def remove_algo(self, job:TradingAlgorithm|str, finalize_algo:bool=True):
        if self._is_finalized:
            msg = f'This alert manager is already finalized.'
            raise TerminationError(msg)
            
        name = None
        if isinstance(job, str):
            name = job
        else:
            try:
                name = job.name
            except Exception:
                pass
            
        if not name or name not in self._jobs:
            msg = f'No algo named {name} found, nothing to remove.'
            self.logger.info(msg)
            return
        
        self._retry_counts.pop(name, None)
        algo = self._jobs.pop(name, None)
        
        if algo:
            try:
                if not algo.is_finalized and finalize_algo:
                    msg = f'Algo {algo.name} was removed.'
                    try:
                        algo.finalize(None, msg=msg)
                    except Exception:
                        pass
                        
                    self.send_server_status_update(
                            algo.name, 
                            algo.completion_msg,
                            status = algo.status.value,
                            )
            except Exception as e:
                msg = f'Failed in finalizing and status update for {name}:{str(e)}.'
                self.logger.info(msg)
                
            self.shared_brokers.remove_algo(name)

    def register_callback(self, algo:TradingAlgorithm, func:Callable, ctx=None):
        '''
            register a callback for a job.
        '''
        if not algo:
            return
        
        if algo.name not in self._jobs:
            msg = f'No job named {algo.name} found, cannot register callback.'
            self.logger.warning(msg)
            return
        
        algo.env.register_callback(func, ctx)
    
    def register_handler(self, algo:TradingAlgorithm, event:str, func:Callable):
        '''
            register a handler for the subscriber channel for 
            a given run name.
        '''
        if not self.server_mode:
            # nothing to register with
            return
        
        if not algo:
            return
        
        if algo.name not in self._jobs:
            msg = f'No job named {algo.name} found, cannot register event handlers.'
            self.logger.warning(msg)
            return
        
        self.ensure_connected()
        self._streamer = cast(StreamingIngestor, self._streamer)
        self._streamer.source.subscribe(algo.name)
        self._streamer.register(event, func, topic=algo.name)
        
    def ensure_connected(self, timeout:int|None=None):
        if self._streamer:
            self._streamer = cast(StreamingIngestor, self._streamer)
            self._streamer.ensure_connected(timeout=timeout)

    def _set_error_handling(self):
        self.error_rules[UserDefinedException] = self.ERROR_RULES_MAP.get(
                get_config_recovery('user_error'), None)
        self.error_rules[BrokerError] = self.ERROR_RULES_MAP.get(
                get_config_recovery('broker_error'), None)

    @staticmethod
    def _classify_errors(obj, obj_types):
        for obj_type in obj_types:
            if isinstance(obj, obj_type):
                return obj_type
        return None
    
    def reset_error_tracker(self, algo_name:str):
        if algo_name:
            self._retry_counts[algo_name] = {}
    
    def _check_and_upgrade_handling(self, algo, e, handling, logger):
        if handling.value > ExceptionHandling.WARN.value:
            # nothing to upgrade
            return handling
        
        try:
            if not algo or not algo.name:
                # we should not be here
                return ExceptionHandling.TERMINATE
        except Exception:
            return ExceptionHandling.TERMINATE
            
        if algo.name not in self._retry_counts:
            self.reset_error_tracker(algo.name)
        retry_counts = self._retry_counts[algo.name]
        
        klass = e.__class__.__name__
        if klass not in retry_counts:
            retry_counts[klass] = 0
        retry_counts[klass] += 1
        
        if retry_counts[klass] > self._max_retry:
            msg = f'Too many errors of type {klass}. Will upgrade to terminate.'
            logger.error(msg)
            return ExceptionHandling.TERMINATE
        
        return handling

    def handle_error(self, algo:TradingAlgorithm, e:Exception, module:str, *args, **kwargs):
        """
            Method to handle error, based on handling info within the
            error object, as well as user configuration.

            If the exception is from a shutdown command API call, it logs
            the operation and does graceful shutdown with OS level exit
            status 0. Else shut down exits with status 1.

            Args:
                ``algo (TradingAlgorithm)``: Job which raised the error.
                    
                ``e (Exception)``: Exception object to handle

                ``module (str)``: Module raising the request for this

            Returns:
                None.

        """
        error_type = self._classify_errors(e, self.ERROR_TYPES)
        err_ctx = kwargs.pop('err_ctx', None)
        err_handler = kwargs.pop('err_handler', noops)
        logger = algo.logger
        
        if err_ctx and err_ctx == algo.name:
            err_ctx = None
        
        if err_ctx:
            module = err_ctx

        if not error_type and hasattr(e, 'handling') is False:
            # unknown error or unknown handling
            logger.error(str(e),module)
            if err_ctx:
                err_handler(err_ctx)
            else:
                try:
                    msg = kwargs.pop('msg', f'error in strategy:{str(e)}.')
                    if msg:kwargs['msg']=get_clean_err_msg(msg)
                    logger.error(msg)
                    err_handler(msg)
                except Exception as e:
                    logger.error(f'Failed to run error routine:{str(e)}.')
                algo.set_algo_status(AlgoStatus.ERRORED)
                self.graceful_exit(algo, *args, **kwargs)
            return

        handling = ExceptionHandling.TERMINATE
        if isinstance(e, BlueshiftException):
            handling = e.handling or ExceptionHandling.TERMINATE
        else:
            handling = self.error_rules.get(error_type, None)
            handling = handling or ExceptionHandling.TERMINATE
        
        try:
            if not isinstance(e, SubContextTerminate):
                handling = self._check_and_upgrade_handling(
                        algo, e, handling, logger)
        except Exception:
            msg = f'Failed to check for error handling upgrade:{str(e)}.'
            logger.error(msg)
        
        if handling == ExceptionHandling.IGNORE:
            pass
        elif isinstance(e, TerminationError):
            algo.set_algo_status(AlgoStatus.ERRORED)
            raise e
        elif isinstance(e, UserStopError):
            algo.set_algo_status(AlgoStatus.CANCELLED)
            raise e
        elif handling == ExceptionHandling.LOG:
            msg = kwargs.pop('msg',f'error in strategy:{str(e)}.')
            logger.info(msg,module, *args, **kwargs)
        elif handling == ExceptionHandling.WARN:
            msg = kwargs.pop('msg',f'error in strategy:{str(e)}.')
            logger.warning(msg,module, *args, **kwargs)
        elif handling == ExceptionHandling.RECOVER:
            raise NotImplementedError(f'algo run recovery not implemented.')
        elif handling == ExceptionHandling.TERMINATE:
            # exit from current context
            msg = kwargs.pop('msg', f'error in strategy:{str(e)}.')
            logger.error(msg, module, *args, **kwargs)
            kwargs.pop('mode', None)
            if msg:kwargs['msg']=get_clean_err_msg(msg)
            
            if err_ctx:
                err_handler(err_ctx)
                return
            else:
                algo.set_algo_status(AlgoStatus.ERRORED)
                try:
                    err_handler(msg)
                except Exception as e:
                    logger.error(f'Failed to run error routine:{str(e)}.')
                self.graceful_exit(algo, *args, **kwargs)
        elif handling == ExceptionHandling.CRITICAL:
            # exit from all contexts
            msg = kwargs.pop('msg', f'error in strategy:{str(e)}.')
            logger.error(str(e),module, *args, **kwargs)
            kwargs.pop('mode', None)
            if msg:kwargs['msg']=get_clean_err_msg(msg)
            
            try:
                err_handler(err_ctx)
            except Exception as e:
                logger.error(f'Failed to run error routine:{str(e)}.')
            self.graceful_exit(algo, *args, **kwargs)
        else:
            # same treatment as TERMINATE
            msg = kwargs.pop('msg', f'error in strategy:{str(e)}.')
            logger.error(str(e),module, *args, **kwargs)
            kwargs.pop('mode', None)
            if msg:kwargs['msg']=get_clean_err_msg(msg)
            
            if err_ctx:
                err_handler(err_ctx)
                return
            else:
                try:
                    err_handler(msg)
                except Exception as e:
                    logger.error(f'Failed to run error routine:{str(e)}.')
                self.graceful_exit(algo, *args, **kwargs)

    def graceful_exit(self, algo:TradingAlgorithm|None, *args, **kwargs):
        """
            Gracefully exit the algo run, try to execute all registerd
            callbacks if possible, ignoring any if it fails.
        """
        # this function should run in the main thread
        if self._is_finalized:
            return
        
        exit_msg = kwargs.pop('msg', f'Shutting down {BLUESHIFT_PRODUCT} gracefully.')
        exit_msg = get_clean_err_msg(exit_msg)
        
        if algo:
            try:
                job = self._jobs.get(algo.name)
                if not job:
                    msg = f'No job named {algo.name}, cannot execute graceful exit.'
                    self.logger.warning(msg)
                    return
            except Exception:
                pass
            shutdown_msg = f'Initiated graceful exit for {algo.name}:{exit_msg}'
            logger = algo.logger
            logger.platform(shutdown_msg,f'{algo.name}')
            algo.set_algo_status(AlgoStatus.ERRORED)
            raise TerminationError(shutdown_msg)
        
        # graceful exit of the current command handler itself
        if self._stopping:
            return
        
        finalize = kwargs.pop('finalize', True)
        self._stopping = True
        shutdown_msg = f'Initiated graceful exit for {BLUESHIFT_PRODUCT} engine.'
        self.logger.platform(shutdown_msg)
        
        names = list(self._jobs.keys())
        for name in names:
            try:
                msg = f'Killing algo {name} on {BLUESHIFT_PRODUCT} graceful exit.'
                algo = self._jobs[name]
                algo.kill(msg=msg)
            except Exception as e:
                msg = f'failed killing algo {name} on exit:{str(e)}.'
                self.logger.error(msg)
        
        if self._command_loop:
            self._command_loop.stop(exit_msg)
        
        try:
            if self.publish and self.publisher:
                packet = create_message_envelope(self.name, MessageType.ALGO_END, msg=shutdown_msg)
                self.publisher.send_to_topic(self.name, packet)
            self.logger.platform(f'Shutting down {BLUESHIFT_PRODUCT} gracefully.')
            # we do not call finalize here. It is up to the caller who 
            # created the alert manager to finalize it as well.
        except Exception:
            pass
        
        if finalize:
            try:
                self.finalize()
            except Exception:
                pass
            
    @classmethod
    def run_extensions(cls, extensions:list[str]):
        """ load and execute specified blueshift extensions. """
        ext=None

        try:
            for ext in extensions:
                if ext.endswith('.py'):
                    if not os.path.isfile(ext) and not os.path.isabs(ext):
                        ext = blueshift_dir(ext, create=False)
                    if not os.path.isfile(ext):
                        raise OSError('file not found.')
                    namespace = {}
                    with open(ext) as fp:
                        src = fp.read()
                        exec(compile(src, ext, 'exec'), namespace, namespace) #nosec
                else:
                    import_module(ext)
        except Exception as e:
            if ext:
                msg = f'failed to load extension {ext}:{str(e)}'
            else:
                msg = f'failed to load extensions:{str(e)}'
            raise BlueshiftException(msg)

    def run_callbacks(self, algo:TradingAlgorithm|None, *args, **kwargs):
        ctx = kwargs.pop('ctx', None)
        if algo:
            job = self._jobs.get(algo.name)
            if not job:
                msg = f'No job named {algo.name}, cannot run callbacks.'
                self.logger.warning(msg)
                return
            if ctx:
                callbacks = reversed(algo.env.callbacks.pop(ctx,[]))
            else:
                callbacks = []
                for ctx in job.env.callbacks:
                   callbacks.extend(reversed(algo.env.callbacks.pop(ctx,[])))
            logger = algo.logger
        else:
            callbacks = []
            for name, job in self._jobs.items():
                for ctx in job.env.callbacks:
                    callbacks.extend(reversed(job.env.callbacks.pop(ctx,[])))
            logger = self.logger
        
        for callback in callbacks:
            try:
                if callable(callback):
                    callback(*args, **kwargs)
            except BaseException as e:
                if hasattr(callback, '__name__'):
                    msg = f"failed with {callback.__name__}:{str(e)}"
                else:
                    msg = f"failed with {callback}:{str(e)}"
                logger.error(msg,'Exit callbacks')
                continue
            
    def _handle_channel(self, channel):
        if channel is None:
            return channel
        try:
            addr,port = channel.split(':')
        except Exception:
            try:
                port = int(channel)
            except Exception:
                msg = f'channel definition must be <ip>:<port> or <port>.'
                msg = f' Got {channel}.'
                raise InitializationError(msg)
            else:
                return f'127.0.0.1:{port}'
        else:
            try:
                port = int(port)
            except Exception:
                msg = f'channel definition must be <ip>:<port> or <port>.'
                msg = f' Got {channel}.'
                raise InitializationError(msg)
            
            return channel
            
    def _set_up_subscriber(self, topic, zmq_channel=None):
        if not self.server_mode:
            # no sub channel in non-server mode
            source = data_source_factory('no-source', logger=self.logger)
        else:
            try:
                zmq_channel = self._handle_channel(zmq_channel)
                if not zmq_channel:
                    addr, port = get_config_channel('zmq_addr').split(':') # type: ignore
                else:
                    addr, port = zmq_channel.split(':')
                
                url = f'{addr}:{port}'
                source = data_source_factory(
                    'zmq',
                    url,  # url must be in the host:port format
                    topic=topic, 
                    parser=_zmq_parser,
                    logger=self.logger,
                    )
            except Exception:
                msg = f'zmq is required in server mode.'
                raise InitializationError(msg)
        
        self._streamer = ingestor_factory('streaming', source)
        self._streamer = cast(StreamingIngestor, self._streamer)
        self._streamer.connect()
            
        # required for ensure_connected decorator
        setattr(self, 'connect', self._streamer.connect)
        setattr(self, 'disconnect', self._streamer.disconnect)
        setattr(self, 'reconnect', self._streamer.reconnect)
        setattr(self, 'is_connected', self._streamer.is_connected)

    def _set_up_publisher(self, name, message_channel=None):
        if not self.publish and not self.server_mode:
            return
        
        message_channel = self._handle_channel(message_channel)
        try:
            msg_type = get_config_channel('msg_type')
            if not msg_type:
                msg_type = 'zmq'
        except Exception:
            msg_type = 'zmq'

        if msg_type and msg_type != 'zmq':
            msg = f'Cannot create Blueshift publisher, unknown type {msg_type}'
            raise InitializationError(msg)

        try:
            if not message_channel:
                addr, port = get_config_channel('msg_addr').split(':') # type: ignore
            else:
                addr, port = message_channel.split(':')
                
            bind = not self.server_mode
            self.publisher = get_publisher(host=addr, port=port, topic=name, bind=bind)
        except ImportError:
            msg = f'Cannot create Blueshift publisher - zmq not installed.'
            self.logger.warning(msg)
            self.publisher = NoPublisher()
        except Exception as e:
            msg = f'Cannot create Blueshift publisher:{str(e)}.'
            self.logger.warning(msg)
            self.publisher = NoPublisher()

        # now add msg logger
        alert_rules = get_config_alerts()
        handler = BlueshiftMsgLogHandler(
                self.name,
                self.publisher,
                self.logger.handler_obj.formatter,
                alert_rules)

        self.logger.add_handler('msg', handler)

    def _set_up_health_listener(self, health_channel=None):
        # this is deprecated
        return
        
    def get_blueshift_callback_url(self, url:str|None) -> str|None:
        if not url:
            return
        
        if url.startswith('http'):
            return url
        else:
            url = url.lstrip('/')
        
        # preprend the blueshift server url
        server = get_blueshift_server()
        if not server:
            return
        
        server = cast(str, server)
        if not server.endswith('/'):
            server = server + '/'
            
        route = os.getenv('BLUESHIFT_BASE_ROUTE','')
        if route:
            route = route.strip('/')
            server = server + route + '/'
        
        url = server + url
        return url

    def blueshift_callback(self, topic:str, action:str|None=None, msg:str|None=None, status:str='done', 
                           performance:dict|None=None, payload:dict|None=None, service=False):
        if not self.server_mode:
            return
        
        if performance is None:
            performance = {}
        if payload is None:
            payload = {}
        
        data:dict = {"action":str(action),"status":status}
        
        if performance:
            try:
                convert_nan(performance)
                performance = json.loads(json.dumps(performance, cls=BlueshiftJSONEncoder))
            except Exception:
                performance = {}
            data['performance'] = performance
        
        if payload:
            try:
                convert_nan(payload)
                payload = json.loads(json.dumps(payload, cls=BlueshiftJSONEncoder))
            except Exception:
                performance = {}
            data['payload'] = payload
        
        callback_msg = ''
        try:
            source = 'algo' if not service else 'service'
            packet = create_message_envelope(
                topic, MessageType.CALLBACK, msg=msg, data=data, source=source)
            r, callback_msg = self._server_callback.callback(topic, packet)
            if r == CallBackResult.UNREACHABLE:
                raise ValueError(f'callback server unreachable:{callback_msg}')
        except Exception as e:
            if self.logger:
                msg = f'Failed in blueshift server callback:{str(e)}.'
                self.logger.error(msg)
            if not self._stopping:
                msg = f'Fatal error in {BLUESHIFT_PRODUCT}, failed '
                msg += f'to update server, will exit all algos.'
                self.logger.error(msg)
                return self.graceful_exit(None, msg=msg)
            else:
                msg = f'Successfully posted callback update topic {topic}, action {action}'
                self.logger.info(msg)
                return    
        
        if r == CallBackResult.ERROR:
            # this algo is not recognized by the server, kill it
            msg = f'Server reports error for algo {topic}:{callback_msg}'
            raise TerminationError(msg)
        elif r == CallBackResult.FAILED:
            if self.logger:
                msg = f'Failed in blueshift server callback for algo {topic}:{callback_msg}'
                self.logger.error(msg)
                return
        if self.logger:
            msg = f'Successfully posted callback action {action}@{topic}.'
            self.logger.info(msg)
            
    def send_server_status_update(self, name:str, msg:str, status:str, smart_order:bool=False,
                                  service:bool=False):
        """ Send status update to blueshift server in server mode. """ 
        if not self.server_mode:
            return
        
        try:
            perf = {}
            if name in self._jobs:
                algo = self._jobs.get(name)
                if algo:
                    smart_order = algo.mode == MODE.EXECUTION
                    try:
                        perf = algo.context.performance
                    except Exception:
                        pass
                
            action = BlueshiftCallbackAction.ALGO_STATUS.value
            if service:
                action = BlueshiftCallbackAction.SERVICE_STATUS.value
            elif smart_order:
                action = BlueshiftCallbackAction.SMART_ORDER_STATUS.value
                
            self.blueshift_callback(
                    name, 
                    action=action,
                    msg=msg, status=status, performance=perf, service=service)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            err_msg = get_exception(exc_type, exc_value, exc_traceback)
            msg = f'Failed sending status for {name}:{str(e)}.'
            msg = msg + ' \n' + err_msg
            self.logger.error(msg)
            
    def create_config(self, config_file:str|None, user_config:str=''):
        '''
            create the config object from the config file, default
            is .blueshift_config.json in the home directory.
        '''
        if not config_file:
            config_file = os.environ.get("BLUESHIFT_CONFIG_FILE", None)
        if not config_file:
            config_file = os.path.join(
                    os.path.expanduser(BLUESHIFT_DIR), BLUESHIFT_CONFIG)


        config = BlueshiftConfig(config_file, user_config=user_config)
        self._config = config
        # register the root config with user None
        register_config(config)
        
    
    @classmethod
    def _log_or_print_error(cls, exc_type, exc_value, exc_traceback, err_ctx, algo, 
                        generic=False):
        if_print = algo.env.quick and algo.mode == MODE.BACKTEST
        skip_paths = [BLUESHIFT_PATH,SITE_PACKAGE_PATH]
        
        with closing(io.StringIO()) as error_string:
            if not generic:
                print_exception(exc_type, exc_value, exc_traceback, skip_paths, 
                          file=error_string)
                msg = f'Error in strategy'
            else:
                tb.print_exception(
                        exc_type, exc_value, exc_traceback, file=error_string)
                msg = f'Fatal error in strategy'
            
            if err_ctx:
                msg = f'{msg} {err_ctx}:'
            else:
                msg = f'{msg}:'
            msg += f'{error_string.getvalue()}'
            
            if if_print:
                algo.env.print(msg,_type='error', msg_type=MessageType.PLATFORM)
            else:
                algo.log_info(msg)
                
        return if_print
    
    def handle_subcontext_error(self, algo:TradingAlgorithm, e:Exception, ctx:str):
        """
            Handle sub-strategy errors. First we print or log the error details.
            Then if we have an alert manager defined, invoke its handle_error 
            method with the `remove_strategy` as the error handler. Otherwise
            we call `remove_strategy` directly.
        """ 
        if isinstance(e, SubContextTerminate):
            # the subcontext was "terminated", do nothing
            algo.log_warning(f'Subcontext terminated:{str(e)}.')
            return
        
        exc_type, exc_value, exc_traceback = sys.exc_info()
        generic = False if isinstance(e, BlueshiftException) else True
        
        if_print = self._log_or_print_error(
                exc_type, exc_value, exc_traceback, ctx, algo, 
                generic=generic)
        
        msg = f"Error in strategy {ctx}: {str(e)}."
        if not if_print:
            msg += f' See log for more details.'
            algo.log_error(msg)
            
        err_handler = lambda x:algo.remove_strategy(x,error=msg)
        self.handle_error(
                algo, e,ctx, mode=algo.mode, timestamp=algo.context.timestamp,
                msg=str(e), err_ctx=ctx,
                err_handler=err_handler)
            
        return msg
    
    def handle_execution_timeout(self, algo:TradingAlgorithm):
        """ Exit on timeout """
        msg = f"{algo.name} run timed-out."
        algo.log_error(msg)
        algo.set_algo_status(AlgoStatus.ERRORED)
        return msg
    
    def handle_backtest_generic_error(self, algo:TradingAlgorithm, e:Exception):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        err_ctx = algo.env.get_current_error_context()
        if_print = self._log_or_print_error(
                exc_type, exc_value, exc_traceback, err_ctx, algo, 
                generic=True)
        
        if not err_ctx or err_ctx == algo.name:
            # error in the main context
            msg = f"Fatal error in strategy, will exit: {str(e)}."
            if not if_print:
                msg += f' See log for more details.'
                algo.log_error(msg)
                
            algo.set_algo_status(AlgoStatus.ERRORED)
            raise TerminationError(msg)
        else:
            # error in a sub-context. The events `on_error` and `finalize` is 
            # called by the `remove_strategy` and the main blotter, respectively.
            msg = f"Fatal error in strategy {err_ctx}, will exit: {str(e)}."
            if not if_print:
                msg += f' See log for more details.'
            algo.log_error(msg)
            algo.remove_strategy(err_ctx, error=msg)
            
        return msg
    
    async def handle_asyc_cancelled(self, algo:TradingAlgorithm):
        """ Exit on asyncio.CancelledError """
        msg = f"Algo {algo.name} run cancelled with error."
        algo.log_error(msg)
        await asyncio.sleep(0.5)
        algo.set_algo_status(AlgoStatus.ERRORED)
        return msg
    
    async def handle_async_exception(self, algo:TradingAlgorithm, e:Exception, source:str='algo'):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        err_ctx = algo.env.get_current_error_context()
        if_print = self._log_or_print_error(
                exc_type, exc_value, exc_traceback, err_ctx, algo, 
                generic=True)
        
        if not err_ctx or err_ctx == algo.name:
            # error in the main context
            msg = f"Fatal error in strategy({source}), will exit: {str(e)}."
            if not if_print:
                msg += f' See log for more details.'
                algo.log_error(msg)
            
            algo.set_algo_status(AlgoStatus.ERRORED)
            await asyncio.sleep(0.5)
            raise TerminationError(msg)
        else:
            # error in a sub-context. The events `on_error` and `finalize` is 
            # called by the `remove_strategy` and the main blotter, respectively.
            msg = f"Fatal error in strategy {err_ctx}|{source}, will exit: {str(e)}."
            if not if_print:
                msg += f' See log for more details.'
            algo.log_error(msg)
            algo.remove_strategy(err_ctx, error=msg)
            
        return msg
    
    def handle_backtest_blueshift_error(self, algo:TradingAlgorithm, e:Exception):
        if isinstance(e, AuthenticationError) and algo.env.server_mode:
            # callback to local blueshift server
            algo.env.blueshift_callback(
                    action=BlueshiftCallbackAction.BROKER_AUTH.value,
                    status=AlgoStatus.ERRORED.value,
                    msg=f'Please re-authenticate')
        
        exc_type, exc_value, exc_traceback = sys.exc_info()
        err_ctx = algo.env.get_current_error_context()
        if_print = self._log_or_print_error(
                exc_type, exc_value, exc_traceback, err_ctx, algo, 
                generic=False)
        
        if err_ctx and err_ctx != algo.name:
            # error in a sub-context
            module=err_ctx
            msg = f"Error in strategy {err_ctx}: {str(e)}."
            if not if_print:
                msg += f' See log for more details.'
            err_handler = lambda x:algo.remove_strategy(x,error=msg)
        else:
            # error in the main context
            module='algorithm'
            msg = f"Error in strategy: {str(e)}."
            if not if_print:
                msg += f' See log for more details.'
            err_handler = lambda x:algo.on_error(msg)
        
        self.handle_error(
                algo, e, module, mode=algo.mode, 
                timestamp=algo.context.timestamp, 
                msg=msg, err_ctx=err_ctx, 
                err_handler=err_handler)
        
        return msg
    
    async def handle_blueshift_async_exception(self, algo:TradingAlgorithm, e:Exception):
        if isinstance(e, AuthenticationError) and algo.env.server_mode:
            # callback to local blueshift server
            algo.env.blueshift_callback(
                    BlueshiftCallbackAction.BROKER_AUTH.value,
                    status=AlgoStatus.ERRORED.value,
                    msg=f'Please re-authenticate')
        
        exc_type, exc_value, exc_traceback = sys.exc_info()
            
        err_ctx = algo.env.get_current_error_context()
        if_print = self._log_or_print_error(
                exc_type, exc_value, exc_traceback, err_ctx, algo, 
                generic=False)
        
        if err_ctx and err_ctx != algo.name:
            # error in a sub-context
            module=err_ctx
            msg = f"Error in strategy {err_ctx}: {str(e)}."
            if not if_print:
                msg += f' See log for more details.'
            err_handler = lambda x:algo.remove_strategy(x,error=msg)
        else:
            module='algorithm'
            msg = f"Error in strategy: {str(e)}."
            if not if_print:
                msg += f' See log for more details.'
            err_handler = lambda x:algo.on_error(msg)
        
        self.handle_error(
                algo, e, module, mode=algo.mode, 
                timestamp=algo.context.timestamp, 
                msg=msg, err_ctx=err_ctx, 
                err_handler=err_handler)
        
        return msg
    
    def handle_loop_exception(self, loop:asyncio.AbstractEventLoop, context:dict):
        exp = context.get('exception')
        if not exp:
            return
        
        exc_type, exc_value, exc_traceback = (type(exp), exp, exp.__traceback__)
        msg = f'Algo event loop encountered an error: {str(exp)}.'
        err_msg = get_exception(exc_type, exc_value, exc_traceback)
        msg = msg + ' \n' + err_msg
        
        algo = getattr(exp, 'algo', None)
        if algo:
            algo.log_error(msg)
            algo.stop(msg=f'Fatal error in event loop, will exit algo.')
        else:
            # do not exit on generic exceptions
            self.logger.error(msg)
    
    def initialize(self, *args, **kwargs):
        if self.pre_processing:
            try:
                self.run_extensions(self.pre_processing)
            except Exception as e:
                msg = f'Failed running pre processing extension(s):{str(e)}.'
                self.logger.error(msg)
                
        self.send_server_status_update(
                self.name, 
                msg=f'Started {BLUESHIFT_PRODUCT} instance {self.name}',
                status='start', service=True)
    
    def finalize(self, *args, **kwargs):
        if self._is_finalized:
            return
        
        self.stop_monitor()
        
        names = list(self._jobs.keys())
        for name in names:
            try:
                self._jobs[name].finalize(None)
            except Exception:
                pass
            
        if self.post_processing:
            try:
                self.run_extensions(self.post_processing)
            except Exception as e:
                msg = f'Failed running ppost processing extension(s):{str(e)}.'
                self.logger.error(msg)
                
        self.send_server_status_update(
                self.name, 
                msg=f'Exiting {BLUESHIFT_PRODUCT} run {self.name}',
                status='exit', service=True)
        
        self.logger.info(f'Successfully finalized the alert manager for {self.name}.')
                
        if not self.server_mode:
            # else closed by the event loop
            try:
                self.logger.close()
            except Exception:
                pass
            
        try:
            self._server_callback.close()
        except Exception:
            pass
        
        try:
            if self.publisher:
                self.publisher.close(forced=True)
        except Exception:
            pass
        finally:
            self._is_finalized = True
            
    def start_monitor(self, frequency:int=5, delay:float=1, threshold:float=2):
        if not self.loop:
            msg = f'Skipping event loop monitoring as no event loop found.'
            self.logger.warning(msg)
            return
        
        self.stop_monitor() # stop anything running already
        coro = self._monitor(
                frequency=frequency, delay=delay, threshold=threshold)
        self._monitor_task = asyncio.ensure_future(coro, loop=self.loop)
        
    def stop_monitor(self):
        if self._monitor_task:
            try:
                self._monitor_task.cancel()
                self._monitor_task.exception()
            except Exception:
                pass
            
    async def _monitor(self, frequency:int=5, delay:float=1, threshold:float=1):
        """ check asyncio loop call later actual delay. """        
        if self.loop and self.loop.is_running():
            self.logger.warning(f'Starting event loop monitor.')
            while not self._stopping:
                try:
                    await asyncio.sleep(frequency)
                    self.loop.call_later(
                            delay, self._check_delay, time.time(), delay, 
                            threshold)
                except Exception as e:
                    self.logger.error(f'Error in event loop monitor: {str(e)}.')
                    break
                
    def _check_delay(self, start, delay, threshold):
        elapsed = time.time() - start
        
        if elapsed - delay > threshold:
            msg = f'Event loop is sluggish: expected {delay}, actual {elapsed}.'
            self.logger.warning(msg)
            
            stats, others = _map_threads()
            msg = f'Current threads {stats}/ other threads -> {others}.'
            self.logger.warning(msg)

class AlertManagerWrapper:
    '''
        A wrapper class for alert manager to make access global.
    '''

    def __init__(self, alert_manager:BlueshiftAlertManager|None=None):
        if alert_manager:
            self.instance = alert_manager
        else:
            self.instance = None

    def get_alert_manager(self) -> BlueshiftAlertManager:
        """ retrieve the current (last) registered alert manager. """
        if self.instance and isinstance(self.instance, BlueshiftAlertManager):
            return self.instance
        
        raise BlueshiftException(f'no alert manager registered')

    def register_alert_manager(self, alert_manager:BlueshiftAlertManager):
        """ register (set) the current alert manager. """
        if isinstance(alert_manager, BlueshiftAlertManager):
            self.instance = alert_manager
            return
        raise BlueshiftException(f'not a valid Blueshift alert manager')

global_alert_manager_wrapper = AlertManagerWrapper()
register_alert_manager = global_alert_manager_wrapper.register_alert_manager

get_alert_manager = global_alert_manager_wrapper.get_alert_manager
