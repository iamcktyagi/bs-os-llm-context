from __future__ import annotations
from typing import Any, TYPE_CHECKING, cast
from abc import ABC, abstractmethod
import queue
import uuid
import json
import asyncio
from enum import Enum
from collections import namedtuple
from functools import partial
import sys
import gc

from blueshift.config import (
        BLUESHIFT_PRODUCT, BLUESHIFT_BACKTEST_INIT_SCRIPT, 
        BLUESHIFT_LIVE_INIT_SCRIPT, BLUESHIFT_LIVE_BROKER_NAME,
        BLUESHIFT_BACKTEST_BROKER_NAME, BLUESHIFT_PRIMING_RUN)
from blueshift.lib.common.platform import get_exception
from blueshift.lib.common.enums import AlgoStatus
from blueshift.core import create_alert_manager, algo_factory
from blueshift.core.alerts.signals_handlers import BlueshiftInterruptHandler
from blueshift.lib.exceptions import InitializationError, BlueshiftException, ValidationError
from .command import CommandProcessor

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop
    from blueshift.core.algorithm.algorithm import TradingAlgorithm

class Command(Enum):
    START='start'
    RUN='run'
    PAUSE='pause'
    RESUME='resume'
    CANCEL='cancel'
    STOP='stop'
    KILL='kill'
    PING='ping'
    BREAK='break'
    LIST='list'
    
JobCommand = namedtuple('namedtuple',['cmd','name','params'])

_prime_backtest = """
def initialize(context):
    pass
"""
_prime_backtest = BLUESHIFT_BACKTEST_INIT_SCRIPT or _prime_backtest
_prime_backtest = _prime_backtest.replace('\\n','\n').strip('"').strip("'")

_prime_paper_run = """
from blueshift.api import quit
def initialize(context):
    quit()
"""
_prime_paper_run = BLUESHIFT_LIVE_INIT_SCRIPT or _prime_paper_run
_prime_paper_run = _prime_paper_run.replace('\\n','\n').strip('"').strip("'")

class ICommandLoop(ABC):
    """ Abstract interface for command handling loop. """
    def __init__(self, name:str, maxsize:int|None, *args, **kwargs):
        self._termination_msg:str|None = None
        self._wants_to_quit:bool = False
        # for algo started already and receieve a kill or cancel command
        self._command_cache:dict[str, Command|None] = {}
        # for algo in the queue yet to process
        self.pending_start:set[str] = set()
        self.removed_algos:set[str] = set()
        # current algo being processed for running
#        self._current_algo = None
        # for finalize idempotency
        self._is_finalized:bool = False
        
        try:
            self._alert_manager = create_alert_manager(
                    name, command_loop=self, maxsize=maxsize, *args, **kwargs)
            
            host = kwargs.pop('host',None)
            port = kwargs.pop('post',None)
            if not host or not port:
                if self._alert_manager.cmd_channel:
                    addr, pp = self._alert_manager.cmd_channel.split(':')
                    if not host:host=addr
                    if not port:port = pp
            
            host = host or '127.0.0.1'
            port = port or 9000
            port = int(port)
            
            timeout = int(kwargs.pop('timeout', 5))
            max_retries = int(kwargs.pop('max_retries', 5))
            
            self._ALLOWED_CONTROL_COMMANDS:list[Command] = [
                Command.CANCEL, Command.STOP, Command.KILL, Command.PAUSE, Command.RESUME]
            
            self._command_processor = CommandProcessor(
                    host=host, port=port, handler=self.command_handler,
                    timeout=timeout, EOM=Command.BREAK.value, 
                    max_retries=max_retries, 
                    logger=self._alert_manager.logger)
            self.initialize()
        except Exception as e:
            msg = f'Failed to start {BLUESHIFT_PRODUCT} engine {name}: {str(e)}.'
            raise InitializationError(msg)
        
    def initialize(self):
        self._alert_manager.initialize()
    
    def finalize(self, msg:str|None=None):
        # this should run in the main thread
        if self._is_finalized:
            return
        
        msg = msg or 'Exiting {BLUESHIFT_PRODUCT}, will stop all running algos.'
        
        try:
            if not self.alert_manager.is_stopping():
                for name in self.pending_start:
                    try:
                        msgg = f'Exiting {BLUESHIFT_PRODUCT}, will cancel algo {name}.'
                        status = AlgoStatus.ERRORED.value
                        self.alert_manager.send_server_status_update(
                                name, msgg, status)
                    except Exception as e:
                        msgg = f'Failed to send status for algo {name}:{str(e)}.'
                        self.logger.error(msgg)
                
                self.alert_manager.graceful_exit(None, msg=msg)
        except Exception:
            pass
        finally:
            self.logger.info(f'finalizing alert manager.......')
            try:
                self.alert_manager.finalize()
            except Exception:
                pass
            self._is_finalized = True
        
        msg = f'Successfully stopped the service {self}.'
        self.logger.info(msg)
        
        self.logger.info(f'Successfully shut down the algo event loop.')
        try:
            self.logger.close()
        except Exception:
            pass
        
    def validate_algo_name(self, name:str):
        if self.algo_exists(name):
            msg = f'Algo name must be unique, {name} already exists.'
            raise ValidationError(msg)
            
    def algo_exists(self, name:str) -> bool:
        if self.alert_manager.get_algo(name):
            return True
        if name in self.pending_start:
            return True
        if name in self.removed_algos:
            return True
        return False
        
    def create_algo(self, cmd:Command, name:str, params:dict[str, Any]) -> TradingAlgorithm|None:
        gc.collect()
        if name in self.removed_algos:
            self.removed_algos.remove(name)
            return
        
        if self.alert_manager.get_algo(name):
            msg = f'Cannot create algo {name}. An algo with '
            msg += f'the same name exists, name must be unique.'
            raise BlueshiftException(msg)
        
        self.logger.info(f'Setting up algo run for {name}.')
        params['alert_manager'] = self.alert_manager
        
        # add to the name to the command cache
        if name not in self._command_cache:
            self._command_cache[name] = None
            
        algo = algo_factory(name, **params)
        
        # remove from pending start just before returning
        if name in self.pending_start:
            self.pending_start.remove(name)
        
        return algo
    
    def handle_cancel(self, name, cmd, cache=False, remove=False, msg=None,
                      raise_exception=True):
        if not self.algo_exists(name):
            msg = f'Cannot complete request {cmd.value} for algo {name}: '
            msg += f'No algo found.'
            if raise_exception:
                raise BlueshiftException(msg)
            else:
                self.logger.error(msg)
                return
            
        algo = self.alert_manager.get_algo(name)
            
        if cmd not in self._ALLOWED_CONTROL_COMMANDS:
            msg = f'Cannot complete request, command {cmd} not supported.'
            
            if raise_exception:
                raise BlueshiftException(msg)
            else:
                self.logger.error(msg)
                return
        
        if not msg:
            msg = f'{cmd.value.capitalize()} request received for algo {name}.'
            
        self.logger.info(msg)
        
        if cmd==Command.STOP:
            if algo:
                # if algo is created/ running, call stop
                algo.stop(msg=msg)
                self.logger.info(f'Initiated stop for algo {name}')
            elif name in self.pending_start:
                # if it is yet to be created, cancel it right away
                self.pending_start.remove(name)
                # put in removed algos list to  check the job from queue
                self.removed_algos.add(name)
                # send server update
                self.alert_manager.send_server_status_update(
                        name, msg=msg, status=AlgoStatus.STOPPED.value)
            elif cache:
                # else cache the command
                self._command_cache[name] = cmd
        elif cmd==Command.PAUSE:
            if algo:
                algo.pause(msg=msg)
                self.logger.info(f'Initiated pause for algo {name}')
            elif cache:
                self._command_cache[name] = cmd
        elif cmd==Command.RESUME:
            if algo:
                algo.resume(msg=msg)
                self.logger.info(f'Initiated resume for algo {name}')
            elif cache:
                self._command_cache[name] = cmd
        elif cmd==Command.CANCEL:
            if algo:
                algo.cancel(msg=msg)
                self.logger.info(f'Initiated cancel for algo {name}')
            elif name in self.pending_start:
                self.pending_start.remove(name)
                self.removed_algos.add(name)
                self.alert_manager.send_server_status_update(
                        name, msg=msg, status=AlgoStatus.CANCELLED.value)
            elif cache:
                self._command_cache[name] = cmd
        elif cmd==Command.KILL:
            if algo:
                algo.kill(msg=msg)
                self.logger.info(f'Initiated kill for algo {name}')
            elif name in self.pending_start:
                self.pending_start.remove(name)
                self.removed_algos.add(name)
                self.alert_manager.send_server_status_update(
                        name, msg=msg, status=AlgoStatus.KILLED.value)
            elif cache:
                self._command_cache[name] = cmd
        else:
            msg = f'Unknown request {cmd} received for algo {name}.'
            if raise_exception:
                self.logger.error(msg)
                raise BlueshiftException(msg)
            else:
                self.logger.error(msg)
            
        if remove and algo:
            self.alert_manager.remove_algo(algo)
            
        return msg
    
    @classmethod
    def process_command(cls, command:str) -> JobCommand:
        if isinstance(command, str) and command.lower() in ('stop','ping','break'):
            cmd = command.lower()
            name, params = None, {}
        else:
            try:
                command = json.loads(command)
                if not isinstance(command, dict):
                    raise ValueError(f'expected a dictionary, got {type(command)}.')
                name = command.get('name')
                cmd = command.get('cmd')
                params = command.get('params',{})
            except Exception as e:
                msg = f'Illegal command {command}:{str(e)}.'
                raise ValueError(msg)
        
        if cmd is None:
            raise ValueError(f'Missing input command.')
            
        try:
            cmd = Command(cmd.lower())
        except Exception:
            raise ValueError(f'Illegal input command {cmd}.')
            
        return JobCommand(cmd, name, params)
    
    def command_handler(self, command:str):
        name = cmd = None
        try:
            msg = 'ok'
            job_cmd = self.process_command(command)
            cmd, name, params = job_cmd.cmd, job_cmd.name, job_cmd.params
            
            if cmd==Command.PING:
                if self._command_processor.poll():
                    return 'pong'
                return 'dead'
            
            if cmd==Command.BREAK:
                return 'done'
            
            if cmd==Command.START or cmd==Command.RUN:
                if not name:
                    name = str(uuid.uuid4())
                    
                try:
                    self.validate_algo_name(name)
                except ValidationError as e:
                    msg = f'Cannot create algo {name}:{str(e)}.'
                    raise BlueshiftException(msg)
                
                self.logger.info(f'Received command to create algo {name}.')
                job_cmd = JobCommand(Command.START, name, params)
                gc.collect()
                self.start_algo(job_cmd)
                msg = f'Successfully queued algo {name}.'
                self.logger.info(msg)
                return {"status":"Ok","results": msg}
            elif cmd==Command.STOP and name is None:
                msg = f'{BLUESHIFT_PRODUCT} received stop command.'
                self.logger.info(msg)
                self.stop(msg=msg)
                return {"status":"Ok","results": f'Service {self} stop initiated.'}
            else:
                if not name:
                    msg = f'Missing algo name for command {cmd.value}.'
                    raise BlueshiftException(msg)
                    
                if name in self.pending_start and cmd in (
                        Command.PAUSE, Command.RESUME):
                    msg = f'Cannot {cmd.value} algo {name}, yet to start.'
                    return {"status":"Error","results": msg}
                    
                msg = self.handle_cancel(name, cmd, cache=True)
                return {"status":"Ok","results": msg}
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            err_msg = get_exception(exc_type, exc_value, exc_traceback)
            self.logger.error(err_msg)
            
            if not name:
                msg = f'Failed to process input command {cmd}: {str(e)}.'
            else:
                msg = f'Failed to process input command {cmd} for '
                msg += f'algo {name}: {str(e)}.'
            self.logger.error(msg)
            return {"status":"Error","results": msg}
        
    def __str__(self):
        return f'CommandLoop ({self.name})'
    
    def __repr__(self):
        return self.__str__()
    
    @property
    def name(self):
        return self.alert_manager.name
    
    @property
    def host(self):
        return self._command_processor.host
    
    @property
    def port(self):
        return self._command_processor.port
    
    @property
    def logger(self):
        return self.alert_manager.logger
    
    @property
    def alert_manager(self):
        return self._alert_manager
    
    @property
    def command_processor(self):
        return self._command_processor
    
    @abstractmethod
    def start(self):
        raise NotImplementedError
        
    @abstractmethod
    def stop(self, msg=None):
        raise NotImplementedError
    
    @abstractmethod
    def signal_handler(self, *args, **kwargs):
        raise NotImplementedError
        
    @abstractmethod
    def start_algo(self, command):
        raise NotImplementedError
        
class BusyLoop(ICommandLoop):
    """
        Command loop for live runs, using queue.
    """
    def __init__(self, name:str, *args, **kwargs):
        maxsize = kwargs.pop('queue_size', 10)
        self._queue:queue.Queue[JobCommand] = queue.Queue(maxsize=maxsize)
        super().__init__(name, maxsize=maxsize, *args, **kwargs)
        self._ALLOWED_CONTROL_COMMANDS:list[Command] = [
            Command.CANCEL, Command.STOP, Command.KILL]
        
    def __str__(self):
        return f'CommandLoop (busy|{self.name})'
        
    def start(self):
        if self._wants_to_quit:
            self.logger.info(f'The service {self}@{self.host}:{self.port} is exiting.')
            return
        
        self._command_processor.start(logger=self.logger)
        self.logger.info(f'Starting the service {self}@{self.host}:{self.port}.')
        
        try:
            self._termination_msg = None
            with BlueshiftInterruptHandler(
                    self.alert_manager, handler=self.signal_handler):
                self._wants_to_quit = False
                self._run_forever()
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            err_msg = get_exception(exc_type, exc_value, exc_traceback)
            self.logger.error(err_msg)
            
            msg = f'Failed in algo event loop: {str(e)}.'
            self.logger.error(msg)
        finally:
            try:
                self.finalize(self._termination_msg)
            except Exception as e:
                msg = f'Failed to finalize the algo event loop:{str(e)}.'
                self.logger.error(msg)
            
    def stop(self, msg:str|None=None):
        if self._wants_to_quit:
            # already called
            return
        
        msg = f'{BLUESHIFT_PRODUCT} received stop command, will exit.'
        self.logger.info(msg)
        self._wants_to_quit = True
        self._command_processor.stop()
        
    def signal_handler(self, *args, **kwargs):
        msg = kwargs.pop('msg','Interrupt received, {BLUESHIFT_PRODUCT} will exit.')
        self.stop(msg=msg)
        
    def start_algo(self, command:JobCommand):
        try:
            self._queue.put(command, block=True, timeout=0.5)
            self.pending_start.add(command.name)
            self._command_cache[command.name] = None
        except queue.Full:
            msg = f'Cannot create algo {command.name}. Max allowed '
            msg += f'jobs already queued.'
            raise BlueshiftException(msg)
        
    def _run_forever(self):
        print_wait_msg = True
        while not self._wants_to_quit:
            try:
                if print_wait_msg:
                    # print only once for each job pick
                    self.logger.info(f'Listening for new command@{self.host}:{self.port}')
                command = self._queue.get(block=True, timeout=0.5)
            except queue.Empty:
                print_wait_msg = False
                continue
            else:
                print_wait_msg = True
                name = None
                try:
                    cmd, name, params = command.cmd, command.name, command.params
                    if cmd != Command.START:
                        continue
                    algo = self.create_algo(cmd, name, params)
                    if not algo:
                        continue
                except Exception as e:
                    msg = f'Failed to create algo {name}:{str(e)}.'
                    self.logger.error(msg)
                    if name:
                        self.alert_manager.send_server_status_update(
                            name, msg=msg, status=AlgoStatus.ERRORED.value)
                else:
                    # check if the command cache has some cancellation
                    cached_cmd = self._command_cache.pop(name, None)
                    
                    if cached_cmd:
                        # non-empty cache commands, means we received a 
                        # control command before we could start the run
                        try:
                            cached_cmd = Command(cached_cmd)
                            if cached_cmd in self._ALLOWED_CONTROL_COMMANDS:
                                msg = f'Got {cached_cmd} command for {algo}, '
                                msg += f'will skip launch.'
                                self.logger.info(msg)
                                self.handle_cancel(
                                        name, cached_cmd, remove=True, 
                                        raise_exception=False)
                                continue
                            else:
                                msg = f'Got unsupported {cached_cmd} command '
                                msg += f'for {algo}, will contune.'
                                self.logger.warning(msg)
                        except Exception as e:
                            msg = f'Failed handling command {cached_cmd}:{str(e)}.'
                            self.logger.error(msg)
                            
                    if self._wants_to_quit:
                        # check again before the algo run
                        msg = f'Exiting {BLUESHIFT_PRODUCT}, will cancel algo {name}.'
                        self.handle_cancel(name, Command.KILL, remove=True)
                        break
                    
                    try:
                        if name in self.removed_algos:
                            self.removed_algos.remove(name)
                            try:
                                algo.finalize(None)
                            except Exception:
                                pass
                            continue
                        
                        self.logger.info(f'Starting algo run for {algo}.')
#                        self._current_algo = algo
                        algo.run()
                        self.logger.info(f'Finished algo run for {algo}.')
                    except Exception as e:
                        msg = f'Failed to run algo {name}:{str(e)}.'
                        self.logger.error(msg)
                    finally:
#                        self._current_algo = None
                        self._command_cache.pop(name, None)
                        if algo:
                            del algo
        
class SingleJobBusyLoop(BusyLoop):
    """
        Command loop for live runs, using queue. Only a single job 
        can be scheduled at a time. A second job will cancel other(s).
    """
    def initialize(self):
        if BLUESHIFT_BACKTEST_BROKER_NAME:
            try:
                # create a dummy algo to prime the imports
                algo = algo_factory(
                        BLUESHIFT_PRIMING_RUN, **{'quick':True,
                                      'initial_capital':100,
                                      'server':False,
                                      'publish':False,
                                      'redirect':False,
                                      'algo':_prime_backtest})
                algo.run()
            except Exception:
                pass
        
        super().initialize()
        
    def create_algo(self, cmd, name, params):
        gc.collect()
        if name in self.removed_algos:
            self.removed_algos.remove(name)
            return
        
        if self.alert_manager.get_algo(name):
            msg = f'Cannot create algo {name}. An algo with '
            msg += f'the same name exists, name must be unique.'
            raise BlueshiftException(msg)
        
        self.logger.info(f'Setting up algo run for {name}.')
        params['alert_manager'] = self.alert_manager
        
        try:
            current_jobs = list(self.alert_manager.jobs.keys())
            for job_name in current_jobs:
                algo = self.alert_manager.get_algo(job_name)
                if algo:
                    msg = f'Cancelling job {job_name} as a new request is received.'
                    self.logger.warning(msg)
                    algo.cancel(msg)
        except Exception as e:
            raise type(e)(f'Error cancelling existing algos:{str(e)}') from e
        
        # add to the name to the command cache
        if name not in self._command_cache:
            self._command_cache[name] = None
            
        algo = algo_factory(name, **params)
        
        # remove from pending start just before returning
        if name in self.pending_start:
            self.pending_start.remove(name)
            
        return algo
    
    def start_algo(self, command):
        """ Cancel any existing job. """
        try:
            current_jobs = list(self.alert_manager.jobs.keys())
            for job_name in current_jobs:
                algo = self.alert_manager.get_algo(job_name)
                if algo:
                    msg = f'Cancelling job {job_name} as a new request is received.'
                    self.logger.warning(msg)
                    algo.cancel(msg)
        except Exception as e:
            raise type(e)(f'Error cancelling existing algos:{str(e)}') from e
            
        try:
            self._queue.put(command, block=True, timeout=0.5)
            self.pending_start.add(command.name)
            self._command_cache[command.name] = None
        except queue.Full:
            msg = f'Cannot create algo {command.name}. Max allowed '
            msg += f'jobs already queued.'
            raise BlueshiftException(msg)
    
    
class AsyncLoop(ICommandLoop):
    """
        Command loop for live runs, using asyncio.
    """
    def __init__(self, name, *args, **kwargs):
        maxsize = kwargs.pop('queue_size', 20)
        self.maxsize = int(maxsize)
        self._pending_tasks = []
        self._loop:AbstractEventLoop|None = None
        super().__init__(name, maxsize=maxsize, *args, **kwargs)
        self._loop = self.alert_manager.loop
        self._ALLOWED_CONTROL_COMMANDS:list[Command] = [
            Command.CANCEL, Command.STOP, Command.KILL, Command.PAUSE, Command.RESUME]
        
    def __str__(self):
        return f'CommandLoop (async|{self.name})'
    
    def initialize(self):
        if BLUESHIFT_LIVE_BROKER_NAME:
            try:
                # create a dummy algo to prime the imports and the broker
                algo = algo_factory(
                        BLUESHIFT_PRIMING_RUN, **{'mode':'paper',
                                      'initial_capital':100,
                                      'server':False,
                                      'publish':False,
                                      'redirect':False,
                                      'no_sec':False,
                                      'algo':_prime_paper_run})
                algo.run()
            except Exception as e:
                self.logger.info(f'Priming run failed: {str(e)}.')
            else:
                self.logger.info(f'Priming run complete.')
        
        super().initialize()
    
    def start(self):
        if self._loop is None:
            raise ValidationError(f'cannot start, no eventloop.')
        
        self._command_processor.start()
        self.logger.info(f'Starting the service {self}@{self.host}:{self.port}.')
        self.alert_manager.start_monitor()
        
        try:
            self._termination_msg = None
            with BlueshiftInterruptHandler(
                    self.alert_manager, handler=self.signal_handler):
                if not self._loop.is_running():
                    self.logger.info(f'Listening for new command@{self.host}:{self.port}')
                    self._loop.run_forever()
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            err_msg = get_exception(exc_type, exc_value, exc_traceback)
            self.logger.error(err_msg)
            
            msg = f'Failed in algo event loop: {str(e)}.'
            self.logger.error(msg)
        finally:
            try:
                self._finalize_loop()
                self.finalize(self._termination_msg)
            except Exception as e:
                msg = f'Failed to finalize the algo event loop:{str(e)}.'
                self.logger.error(msg)
        
    def _finalize_loop(self):
        if self._loop is None:
            return
        
        # see https://github.com/python/cpython/blob/3.10/Lib/asyncio/runners.py
        try:
            # eat the cancelled tasks
            if self._pending_tasks:
                self._loop.run_until_complete(
                        asyncio.gather(*self._pending_tasks, return_exceptions=True))
        except BaseException:
            pass
        finally:
            # eat the exceptions now
            if self._pending_tasks:
                for t in self._pending_tasks:
                    if not t.cancelled():
                        try:
                            t.exception()
                        except Exception:
                            pass
            try:
                self.alert_manager.executor.shutdown(wait=True)
                self._loop.run_until_complete(self._loop.shutdown_asyncgens())
                self._loop.run_until_complete(self._loop.shutdown_default_executor())
            except Exception as e:
                msg = f'Failed to shutdown async gens and executor(s):{str(e)}'
                self.logger.error(msg)
            finally:
                asyncio.set_event_loop(None)
                self._loop.close()
        
            
    def _stop(self, msg:str|None=None):
        if not msg:
            msg = f'Exiting {BLUESHIFT_PRODUCT}, will cancel all running algo '
            
        # stop the command processor first
        self._command_processor.stop()
        
        if self._loop and self._loop.is_running:
            try:
                self._pending_tasks = []
                # stop the loop
                self._loop.stop()
                # cancel pending tasks
                for t in asyncio.all_tasks():
                    if not t.done():
                        t.cancel()
                        self._pending_tasks.append(t)
            except Exception:
                pass
            
        self.logger.info(f'Initiated stop for the service {self}.')
        self._termination_msg = msg
        
    def stop(self, msg:str|None=None):
        if self._wants_to_quit:
            # already called
            return
        
        msg = f'{BLUESHIFT_PRODUCT} received stop command, will exit.'
        self.logger.info(msg)
        self._wants_to_quit = True

        if self._loop:
            _stop = partial(self._stop, msg=msg)
            self._loop.call_soon_threadsafe(_stop)
        
    def signal_handler(self, *args, **kwargs):
        msg = kwargs.pop('msg','Interrupt received, {BLUESHIFT_PRODUCT} will exit.')
        self.stop(msg=msg)
        
    def start_algo(self, command:JobCommand):
        if self._loop is None:
            raise ValidationError(f'cannot start algo, no eventloop.')
        
        if len(self.alert_manager.jobs) > self.maxsize:
            msg = f'Cannot create algo {command.name}. Max allowed '
            msg += f'jobs already running.'
            raise BlueshiftException(msg)
            
        def launch(command):
            name = cmd = None
            try:
                cmd, name, params = command.cmd, command.name, command.params
                if cmd != Command.START:
                    return
                algo = self.create_algo(cmd, name, params)
                if not algo:
                    return
            except Exception as e:
                msg = f'Failed to create algo {name}:{str(e)}.'
                self.logger.error(msg)
                if name:
                    self.alert_manager.send_server_status_update(
                            name, msg=msg, status=AlgoStatus.ERRORED.value)
            else:
                # check if the command cache has some cancellation
                cached_cmd = self._command_cache.pop(name, None)
                if cached_cmd:
                    # non-empty cache commands, means we received a 
                    # control command before we could start the run
                    try:
                        cached_cmd = Command(cached_cmd)
                        if cached_cmd in self._ALLOWED_CONTROL_COMMANDS:
                            msg = f'Got {cached_cmd} command for {algo}, '
                            msg += f'will skip launch.'
                            self.logger.warning(msg)
                            self.handle_cancel(name, cached_cmd, remove=True)
                            return
                        else:
                            msg = f'Got unsupported {cached_cmd} command '
                            msg += f'for {algo}, will contune.'
                            self.logger.warning(msg)
                    except Exception as e:
                        msg = f'Failed handling command {cached_cmd}:{str(e)}.'
                        self.logger.error(msg)
            
                try:
                    # do not trigger run_until_complete, return immediately
                    if name in self.removed_algos:
                        algo.log_error(f'Algo was stopped before it could start.')
                        self.removed_algos.remove(name)
                        try:
                            algo.finalize(None)
                        except Exception:
                            pass
                        return
                    
                    self.logger.info(f'Starting algo run for {algo}.')
                    algo.run(skip_wait=True)
                    self.logger.info(f'Started algo run for {algo}.')
                    del algo
                except Exception as e:
                    msg = f'Failed to run algo {name}:{str(e)}.'
                    self.logger.error(msg)
                    
        self.pending_start.add(command.name)
        self._command_cache[command.name] = None
        self._loop.call_soon_threadsafe(launch, command)
        self.logger.info(f'Listening for new command@{self.host}:{self.port}')