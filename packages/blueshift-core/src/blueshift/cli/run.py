from __future__ import annotations
from typing import TYPE_CHECKING

import warnings
from os.path import basename, exists
from os.path import join, expanduser, isfile, isabs
from shutil import rmtree
import tarfile
from os import environ as os_environ
import json

from blueshift.lib.common.enums import AlgoMode as MODE, AlgoMessageType as MessageType

from blueshift.lib.common.platform import print_exception, print_and_log_exception, print_algo_summary
from blueshift.lib.exceptions import BlueshiftException, TerminationError, InitializationError
from blueshift.config import (
        BLUESHIFT_DIR, BLUESHIFT_CONFIG, blueshift_dir, 
        blueshift_saved_performance_path, blueshift_run_path,
        blueshift_log_path, blueshift_reset_run_path,
        BLUESHIFT_PRODUCT, get_config)

from blueshift.core.alerts.signals_handlers import BlueshiftInterruptHandler
from blueshift.core import create_alert_manager, algo_factory
from blueshift.providers import *
from blueshift.finance import *

def run_algo(name:str, *args, **kwargs):
    """ run algo from parameters """
    show_progress:bool = kwargs.pop('show_progress', False)
    output:str|None = kwargs.pop('output', None)
    
    mgr = None
    try:
        restart:bool = kwargs.get('restart', False) 
        if not restart:
            blueshift_reset_run_path(name)
        mgr = create_alert_manager(name, maxsize=None, *args, **kwargs)
        mgr.initialize()
    except Exception as e:
        msg = f'Failed to set up the algo run'
        raise InitializationError(f'{msg}:{str(e)}')
    
    try:
        msg = f'Setting up {BLUESHIFT_PRODUCT} for algo run.'
        mgr.logger.platform(msg)
        algo = algo_factory(name, *args, **kwargs)
    except Exception as e:
        msg = f'Failed to create algo instance'
        try:
            print_and_log_exception(name, mgr, msg=msg, error=e)
            mgr.finalize()
        except Exception:
            pass
        raise InitializationError(f'{msg}:{str(e)}')
        
        
    if algo.env.external_data:
        msg = f'Algo uses external data.'
        mgr.print_info(msg, name=mgr.name, level="info2", msg_type=MessageType.PLATFORM)
        mgr.logger.info(msg)
        
    # filter out numpy errors for nanmean and nanstd on empty array
    warnings.filterwarnings('ignore', message='Mean of empty slice')
    warnings.filterwarnings('ignore', message='Degrees of freedom <= 0 for slice')
    
    try:
        if algo.mode == MODE.BACKTEST:
            with BlueshiftInterruptHandler(mgr):
                perfs = algo.run(show_progress=show_progress)
                
            if output and perfs is not None:
                try:
                    perfs.to_csv(output)
                except Exception as e:
                    msg = f'Failed to save performance to output: {str(e)}.'
                    mgr.logger.error(msg)
            
            if algo.env.quick:
                try:
                    name = algo.name
                    output_dir = blueshift_saved_performance_path(name)
                    log_dir = blueshift_log_path(name)
                    rmtree(output_dir)
                    rmtree(log_dir)
                except Exception:
                    pass
            elif  algo.env.compress_output:
                name = algo.name
                fname = 'performance.tar.gz'
                target = join(blueshift_run_path(name), fname)
                output_dir = blueshift_saved_performance_path(name)
                log_dir = blueshift_log_path(name)
                
                with tarfile.open(target, "w:gz") as tar:
                    if exists(output_dir):
                        tar.add(output_dir, arcname=basename(output_dir))
                    if exists(log_dir):
                        tar.add(log_dir, arcname=basename(log_dir))
                    
                msg = 'successfully saved output to compressed directory'
                mgr.print_info(msg, name=name)
                
                try:
                    rmtree(output_dir)
                    if algo.name != mgr.name:
                        rmtree(log_dir)
                except Exception as e:
                    msg = f'failed to remove original output directories: {str(e)}.'
                    mgr.logger.error(msg)

            if algo.env.platform == 'cli':
                print_algo_summary(algo.name, perfs)
            return perfs
            
            
        elif algo.mode in (MODE.LIVE, MODE.PAPER, MODE.EXECUTION):
            with BlueshiftInterruptHandler(mgr):
                algo.run()
        else:
            msg = f"illegal mode supplied."
            mgr.logger.info(msg)
            raise InitializationError(msg)
    except Exception as e:
        msg = f'Failed to run the algo: {str(e)}.'
        try:
            print_and_log_exception(name, mgr, msg=msg, error=e)
        except Exception:
            pass
        raise TerminationError(msg)
    finally:
        mgr.finalize()
        
def list_items(kind:str|None="broker") -> list[str]:
    """ list registered brokers and calendars. """
        
    def list_datasets(config):
        path = get_config().datasets.get('library','')
        from blueshift.interfaces.data.library import ILibrary
        return ILibrary.list_libraries(path)
    
    def list_available_bundles(config):
        from blueshift.interfaces.data.ingestor import list_ingestion_bundles
        return list_ingestion_bundles()
    
    def list_available_data_sources(config):
        from blueshift.interfaces.data.source import list_data_sources
        return list_data_sources()
    
    def list_available_data_stores(config):
        from blueshift.interfaces.data.store import list_data_stores
        return list_data_stores()
    
    def list_available_ingestors(config):
        from blueshift.interfaces.data.ingestor import list_ingestors
        return list_ingestors()
    
    def list_registered_brokers(config):
        from blueshift.interfaces.trading.broker import list_brokers
        return list_brokers()
    
    def list_registered_slippage_models(config):
        from blueshift.interfaces.trading.simulations import list_slippage_models
        return list_slippage_models()
    
    def list_registered_margin_models(config):
        from blueshift.interfaces.trading.simulations import list_margin_models
        return list_margin_models()
    
    def list_registered_cost_models(config):
        from blueshift.interfaces.trading.simulations import list_cost_models
        return list_cost_models()
        
    def list_registered_calendars(config):
        from blueshift.calendar import list_calendars, register_builtin_calendars
        register_builtin_calendars()
        return list_calendars()
    
    def list_available_strategies(config):
        from blueshift.interfaces.code.strategy.strategy import list_strategies
        return list_strategies()
    
    config_file = os_environ.get("BLUESHIFT_CONFIG_FILE", None)
    
    if not config_file:
        config_file = join(expanduser(BLUESHIFT_DIR), BLUESHIFT_CONFIG)
    
    if not isfile(config_file):
        msg='missing config file {config_file}'
        raise FileNotFoundError(msg)
    
    try:
        with open(config_file) as fp:
            config = json.load(fp)
    except FileNotFoundError:
        msg='config file {config_file} not found'
        raise FileNotFoundError(msg)
    except json.JSONDecodeError as e:
        msg = f'Failed to load {BLUESHIFT_PRODUCT} config:{str(e)}.'
        raise InitializationError(msg)
    
    if kind is not None:
        kind = str(kind).replace('-','').replace('_','')
    
    if kind is None:
        return list_registered_brokers(config)
    elif kind in ('broker','brokers'):
        return list_registered_brokers(config)
    elif kind in ('calendar','calendars'):
        return list_registered_calendars(config)
    elif kind in ('costmodel','costmodels'):
        return list_registered_cost_models(config)
    elif kind in ('marginmodel','marginmodels'):
        return list_registered_margin_models(config)
    elif kind in ('slippagemodel','slippagemodels'):
        return list_registered_slippage_models(config)
    elif kind in ('dataset','datasets'):
        return list_datasets(config)
    elif kind in ('strategy','strategies'):
        return list_available_strategies(config)
    elif kind in ('algo','algos'):
        return list_available_strategies(config)
    elif kind in ('datastore','datastores'):
        return list_available_data_stores(config)
    elif kind in ('datasource','datasources'):
        return list_available_data_sources(config)
    elif kind in ('ingestor','ingestors'):
        return list_available_ingestors(config)
    elif kind in ('bundle','bundles'):
        return list_available_bundles(config)
    else:
        return []
    
def run_extensions(extensions):
    """ load and execute specified blueshift extensions. """
    from importlib import import_module

    ext = None
    try:
        for ext in extensions:
            if ext.endswith('.py'):
                if not isfile(ext) and not isabs(ext):
                    ext = blueshift_dir(ext, create=False)
                if not isfile(ext):
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

