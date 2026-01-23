from __future__ import annotations
from typing import TYPE_CHECKING, cast
import os.path as os_path

from blueshift.config import blueshift_run_path, blueshift_reset_run_path
from blueshift.interfaces.streaming import StreamingPublisher
from blueshift.interfaces.nostreaming import NoPublisher
from blueshift.lib.common.ctx_mgrs import MessageBrokerCtxManager, StdioRedirect
from blueshift.lib.common.types import MsgQStringIO, PubStream
from blueshift.lib.common.enums import AlgoMode
from blueshift.lib.exceptions import BlueshiftException

from .alerts import BlueshiftAlertManager, get_alert_manager

from .algorithm import AlgoContext, TradingAlgorithm
from .utils.environment import TradingEnvironment

from blueshift.providers import *
from blueshift.finance import *

def create_alert_manager(name:str, maxsize:int|None, *args, **kwargs) -> BlueshiftAlertManager:
    """ create and register the alert manager. A single alert 
        manager should be created for all jobs.
    """
    return BlueshiftAlertManager(name, maxsize, *args, **kwargs)
    
    
def algo_factory(name:str, *args, **kwargs) -> TradingAlgorithm:
    """ 
        Create an algo from inputs. Use this function to create an algo. Note: 
        a wide range of parameters is accepted and dependong on the algo run mode, 
        and some parameters are required while some others are optionals.

        Example:
        .. code-block:: python
            from blueshift.core import create_alert_manager, algo_factory
            from blueshift.core.alerts.signal_handlers import BlueshiftInterruptHandler

            params = {
                'algo_file':'path/to/your/strategy.py',
                'initial_capital':None,
                'mode':'live',
                'broker':'zerodha', # must be implemented and in your .blueshift_config.json
            }

            name = 'live_test'
            mgr = create_alert_manager(name, maxsize=None, **params)
            algo = algo_factory(name, **params)

            mgr.initialize()
            with BlueshiftInterruptHandler(mgr):
                algo.run()
    """
    env:TradingEnvironment|None = None
    alert_manager = get_alert_manager()
    restart = cast(bool, kwargs.get('restart'))
        
    if not alert_manager:
        if not restart:
            blueshift_reset_run_path(name)
        try:
            env = TradingEnvironment(name, *args, **kwargs)
            context = AlgoContext(env=env)
            return TradingAlgorithm(env=env, context=context)
        except Exception as e:
            msg = f'Failed to create algo: {str(e)}.'
            if env:
                env.finalize(None, msg=msg)
            raise
            
    redirect = alert_manager.redirect
    publish = alert_manager.publish
    publisher_handle = alert_manager.publisher
    stream = None
    err_stream = None
    backtest = alert_manager.mode == AlgoMode.BACKTEST
    quick = cast(bool, kwargs.get('quick', False))
    
    if not restart:
        ignore_log_dir:bool = False
        if name == alert_manager.name:
            ignore_log_dir = True
        blueshift_reset_run_path(name, ignore_log_dir=ignore_log_dir)
    
    with MessageBrokerCtxManager(
                publisher_handle, enabled=publish) as publisher:
        if not publisher:
            publisher = NoPublisher()
        
        publisher = cast(StreamingPublisher, publisher)
        if redirect:
            if not backtest:
                stream = MsgQStringIO(publisher=publisher, topic=name)
            elif quick:
                stream = PubStream(publisher=publisher, topic=name)
            else:
                stream = os_path.join(blueshift_run_path(name),'output.log')
                err_stream = os_path.join(blueshift_run_path(name),'error.log')
                
        with StdioRedirect(stream, err_stream=err_stream, close=False):
            try:
                env = TradingEnvironment(name, *args, **kwargs)
                context = AlgoContext(env=env)
                return TradingAlgorithm(env=env, context=context)
            except Exception as e:
                msg = f'Failed to create algo: {str(e)}.'
                if env:
                    env.finalize(None, msg=msg)
                raise BlueshiftException(f'failed to create algo:{str(e)}') from e
