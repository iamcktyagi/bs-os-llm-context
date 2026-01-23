from __future__ import annotations
from typing import TYPE_CHECKING, Any, cast

import click
from rich.console import Console
from os import path as os_path
from os import _exit as os_exit
from os import environ as os_environ
from os import mkdir
import json
import uuid
from importlib import import_module

from blueshift.config import (
    generate_default_config, BLUESHIFT_PRODUCT, blueshift_dir, blueshift_reset_run_path)
from blueshift.lib.common.enums import AlgoMode
from blueshift.lib.common.platform import print_msg, set_pdeathsig, print_pretty
from blueshift.lib.common.functions import (
    get_kwargs_from_click_ctx, process_extra_args, to_int_or_float)
from blueshift.lib.exceptions import BlueshiftException
from blueshift.interfaces.plugin_manager import load_plugins

from blueshift.providers import *
from blueshift.finance import *

CONTEXT_SETTINGS = dict(ignore_unknown_options=True,
                        allow_extra_args=True,
                        token_normalize_func=lambda x: x.lower())

PREPROCESSING:list[str] = []
POSTPROCESSING:list[str] = []
console = Console()

def _register_plugin_commands(group: click.Group) -> None:
    from importlib.metadata import entry_points

    for ep in entry_points(group="blueshift.commands"):
        try:
            cmd = ep.load()
            group.add_command(cmd, ep.name)
        except Exception:
            pass

def run_extensions(extensions:list[str]):
    """ load and execute specified blueshift extensions. """
    ext =''
    try:
        for ext in extensions:
            if ext.endswith('.py'):
                if not os_path.isfile(ext) and not os_path.isabs(ext):
                    ext = blueshift_dir(ext, create=False)
                if not os_path.isfile(ext):
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

@click.group()
@click.option(
    '--api-key',
    envvar="BLUESHIFT_ACCESS_KEY_ID",
    default=None,
    type=click.STRING,
    help=f'your {BLUESHIFT_PRODUCT} API key. Visit the site to generate one.'
)
@click.option(
    '--secret-key',
    envvar="BLUESHIFT_SECRET_ACCESS_KEY",
    default=None,
    type=click.STRING,
    help=f'your {BLUESHIFT_PRODUCT} secret key. Visit the site to generate one.'
)
@click.option(
    '--platform', 
    '-p',
    envvar="BLUESHIFT_PLATFORM",
    default='blueshift',
    type=click.Choice(['cli', 'blueshift', 'api', 'desktop','web','mobile']),
    help='Platform type. [blueshift, cli, api, web, desktop, mobile]'
)
@click.option(
    '-m',
    '--email',
    envvar="BLUESHIFT_EMAIL",
    default=None,
    type=click.STRING,
    help='Email for blueshift email alerts.',
    )
@click.option(
    '-n',
    '--phone',
    envvar="BLUESHIFT_PHONE",
    default=None,
    type=click.STRING,
    help='Phone number for blueshift text alerts.',
    )
@click.option(
    '--config-file',
    '-c',
    envvar="BLUESHIFT_CONFIG_FILE",
    default='~/.blueshift/.blueshift_config.json',
    type=click.Path(),
    help=f'{BLUESHIFT_PRODUCT} config file. You can generate a config template'
            ' using the `config` command.'
)
@click.option(
    '--user-config',
    default='~/.blueshift/.user_config.json',
    type=click.Path(),
    help='Optional broker config file.'
)
@click.option(
    '--extension', 
    '-e',
    multiple=True,
    help=f'{BLUESHIFT_PRODUCT} extension to load. Specify Python file or module.'
)
@click.option(
    "--plugin",
    multiple=True,
    type=click.Choice(
        ['broker', 'data', 'finance','assets','rms','oneclick','blotter','security',
         'streaming','strategy','exit_handler','algo_order_handler']),
    help="Enable a plugin. Can be used multiple times."
)
@click.option(
    '--no-plugins',
    is_flag=True,
    default=True,
    help='Disable all plugins.')
@click.option(
    '--preprocessor',
    multiple=True,
    help='Preprocessing scripts to run. Specify Python file or module.'
)
@click.option(
    '--postprocessor',
    multiple=True,
    help='Postprocessing scripts to run. Specify Python file or module.'
)
@click.pass_context
def main(ctx:click.Context, api_key:str, secret_key:str, platform:str, email:str, phone:str, 
         config_file:str, user_config:str, extension:list[str], plugin:list[str], no_plugins:bool, 
         preprocessor:list[str], postprocessor:list[str]):
    """
        Blueshift is a stand-alone as well as API connected complete
        trading system. It supports multiple assets across multiple 
        markets - both for back-testing and live trading and research.
        
        Usage:\n
            blueshift config > ~/blushift_config.json\n
            blueshift ls \n
            blueshift run --mode backtest [--data-frequency 5m --initial-capital 1000] --algo-file 'myalgo.py'\n
            blueshift ingest [options] src dest\n
            blueshift --help
    """
    ctx.obj = {'config':config_file,
               'user_config':user_config,
               'api_key': api_key,
               'secret_key': secret_key,
               'platform': platform,
               'email': email,
               'phone': phone,
               }
    
    if not no_plugins:
        for item in plugin:
            try:
                group = f'blueshift.plugins.{item}'

                for entrypoint in load_plugins(group, load=False):
                    try:
                        entrypoint.load()
                    except Exception as e:
                        print_msg(
                            f'Failed load plugin {entrypoint.name}: {str(e)}',level='error', 
                            console=console)
            except Exception as e:
                print_msg(
                    f'{BLUESHIFT_PRODUCT} failed load plugins: {str(e)}', level='error', console=console)
    
    try:
        run_extensions(extension)
        
        for item in preprocessor:
            PREPROCESSING.append(item)
        
        for item in postprocessor:
            POSTPROCESSING.append(item)
    except Exception as e:
        print_msg(f'{BLUESHIFT_PRODUCT} failed to start: {str(e)}',level='error', console=console)
        os_exit(1)

_register_plugin_commands(main)

@main.command(context_settings=CONTEXT_SETTINGS)
@click.argument('item', required=False)
@click.option(
    '--entry',
    default=None,
    type=click.STRING,
    help='Entry name to update.')
@click.option(
    '--save',
    is_flag=True,
    default=False,
    help='Save output to configuration file.')
@click.pass_context
def config(ctx, item, entry, save):
    """
        Create a template for configuration, or update existing.
    """
    config_file = os_path.expanduser(ctx.obj['config'])
    root = os_path.dirname(config_file)

    config:dict[str, Any] = {}
    if os_path.exists(config_file):
        with open(config_file) as fp:
            config:dict[str, Any] = json.load(fp)
    else:
        default_config = generate_default_config()
        config:dict[str, Any] = json.loads(cast(str, default_config))

    if item is None:
        try:
            # update the dict with supplied parameters.
            config['owner'] = os_environ.get('USERNAME')
            config['api_key'] = ctx.obj.get('api_key', None)
            config['workspace']['root'] = root
            
            if not save:
                print_pretty(config, console=console)
            else:
                # create all directories in root if they do not exists already
                if not os_path.exists(root): mkdir(root)
                for d in config['workspace']:
                    if d=='root':
                        continue
                    else:
                        full_path = os_path.join(root, config['workspace'][d])
                        if not os_path.exists(full_path): mkdir(full_path)
                    
                with open(config_file, 'w') as fp:
                    json.dump(config, fp)
        except Exception as e:
            msg = f'failed to generate blueshift config: {str(e)}'
            print_msg(msg, level="error", console=console)
            os_exit(1)
    else:   
        if entry is None:
            msg = 'entry name to update not defined'
            print_msg(msg, level="error", console=console)
            os_exit(1)
        
        try:
            if entry in config:
                segment = config[entry]
            else:
                segment = config
            
            try:
                new = json.loads(item)
            except Exception:
                try:
                    new = to_int_or_float(item)
                except Exception:
                    new = item
                
            segment[entry] = new

            if not save:
                print_pretty(config, console=console)
            if save:
                with open(config_file, 'w') as fp:
                    json.dump(config, fp)
        except Exception as e:
            msg = f'failed to update blueshift config:{str}'
            print_msg(msg, level="error", console=console)
            os_exit(1)
    
@main.command(context_settings=CONTEXT_SETTINGS)
@click.argument('kind', required=False)
@click.pass_context
def ls(ctx, kind:str|None=None):
    """
        List the supported brokers or calendars.
    """
    from .run import list_items
    
    platform = ctx.obj['platform']
    print_list = list_items(kind)

    if print_list is None:
        msg = "no match found for arguments."
        console.print(msg)
        return
    
    if not print_list:
        msg = "no registered item found."
        print_msg(msg, level="info", console=console)
    else:
        print_pretty(print_list, console=console)

@main.command(context_settings=CONTEXT_SETTINGS)
@click.option(
    '-s',
    '--start-date',
    default=None,
    type=click.STRING,
    help='Start date for algo run.',
    )
@click.option(
    '-e',
    '--end-date',
    default=None,
    type=click.STRING,
    help='End date for algo run.',
    )
@click.option(
    '-c',
    '--initial-capital',
    default=None,
    type=click.FLOAT,
    help='Initial capital for this algo.',
    )
@click.option(
    '-a',
    '--algo-file',
    default=None,
    type=click.Path(file_okay=True, writable=True),
    help='Algo script file or module path.',
    )
@click.option(
    '--security-config',
    default='security-check.json',
    type=click.STRING,
    help='Security configuration or path to it (json format).',
    )
@click.option(
    '-m',
    '--run-mode',
    default='backtest',
    type=click.Choice(['backtest', 'live', 'paper','execution']),
    help='Run mode [backtest, live].',
    )
@click.option(
    '-x',
    '--execution-mode',
    default='auto',
    type=click.Choice(['auto', 'oneclick']),
    help='Execution mode [auto, oneclick].',
    )
@click.option(
    '-b',
    '--broker',
    default=None,
    type=click.STRING,
    help='Choose the broker to run this strategy.',
    )
@click.option(
    '--currency',
    default="LOCAL",
    type=click.STRING,
    help='Currency of denomination for trading account.',
    )
@click.option(
    '-n',
    '--name',
    default=None,
    help='Name of this run',
    )
@click.option(
    '-f',
    '--frequency',
    default='minute',
    help='Clock frequency.',
    )
@click.option(
    '--output',
    default=None,
    type=click.Path(file_okay=True, writable=True),
    help='Output stream to write to',
    )
@click.option(
    '--benchmark',
    default=None,
    type=click.STRING,
    help='Specify the benchmark to use',
    )
@click.option(
    '--quick/--full',
    default=False,
    help='Run in quick mode with less analytics.')
@click.option(
    '--show-progress/--no-progress',
    default=False,
    help='Turn on/ off the progress bar. [show-progress/no-progress]')
@click.option(
    '--publish/--no-publish',
    default=False,
    help='Turn on/ off streaming results. [publish/no-publish]')
@click.option(
    '--restart/--no-restart',
    default=False,
    help='Turn on/ off restart capability. [restart/no-restart]')
@click.option(
    '--compress-output/--no-compress-output',
    default=True,
    help='Turn on/ off output compression (tar.gz)')
@click.option(
    '--redirect/--no-redirect',
    default=False,
    help='Turn on/ off standard streams redirection. [redirect/no-redirect]')
@click.option(
    '--verbose/--silent',
    default=False,
    help='Turn on/ off verbosity. [verbose/silent]')
@click.option(
    '--no-sec',
    is_flag=True,
    default=False,
    help='Turn off code security check.')
@click.option(
    '--timeout',
    default=0,
    type=click.INT,
    help='Time-out for the run.')
@click.option(
    '-p',
    '--parameters',
    default=None,
    help='Parameters for the algo in json format.',
    )
@click.option(
    '--initial-state',
    default=None,
    help='Initial state for the algo.',
    )
@click.option(
    '--command-channel',
    default=None,
    type=click.STRING,
    help='Command channel host and port.',
    )
@click.option(
    '--message-channel',
    default=None,
    type=click.STRING,
    help='Message channel host and port.',
    )
@click.option(
    '--health-channel',
    default=None,
    type=click.STRING,
    help='Health channel host and port.',
    )
@click.option(
    '--zmq-channel',
    default=None,
    type=click.STRING,
    help='ZMQ channel host and port for interactive actions.',
    )
@click.option(
    '--server',
    is_flag=True,
    default=False,
    help='Turn on the server mode.')
@click.option(
    '--algo-id',
    default=None,
    help='Algo ID to track order field "placed_by".',
    )
@click.option(
    '--algo-name',
    default=None,
    help='Algo name to search the algo class for vlass based strategies',
    )
@click.option(
    '--algo-moniker',
    default=None,
    help='The underlying algo name or identifier',
    )
@click.option(
    '--algo-user',
    default=None,
    help='Algo User ID to track order field "user".',
    )
@click.option(
    '--burn-in',
    is_flag=True,
    default=False,
    help='Turn on backtesting switching to live mode.')
@click.option(
    '--theme',
    default='light',
    type=click.STRING,
    help='Specify the theme to use (light|dark) for charts output',
    )
@click.argument(
    'algo', 
    default='',
    required=False,
    type=click.STRING)
@click.pass_context
def run(ctx, start_date, end_date, initial_capital, algo_file,
        security_config, run_mode, execution_mode, broker, currency, name, 
        frequency, output, benchmark, quick, show_progress, publish, restart, 
        compress_output, redirect, verbose, no_sec, timeout, parameters,
        initial_state, command_channel, message_channel, health_channel, 
        zmq_channel, server, algo_id, algo_name, algo_moniker, algo_user, burn_in, 
        theme, algo):
    """
        Run a strategy with given inputs.
    """
    # exit if parent dies as a safety feature
    set_pdeathsig()
    
    from .run import run_algo
    
    if not algo:
        algo = None
    
    # set name if not restart
    if not name and restart:
        msg = f'Must specify a name for restart.'
        print_msg(msg, level="error", console=console)
        os_exit(1)
    if not name:
        name = str(uuid.uuid4())
    
    exit_status = 0
    try:
        kwargs = get_kwargs_from_click_ctx(ctx)
        configfile = os_path.expanduser(ctx.obj['config'])
        user_config = ctx.obj['user_config']
        api_key = ctx.obj['api_key']
        secret_key = ctx.obj['secret_key']
        platform = ctx.obj['platform']
        email = ctx.obj['email']
        phone = ctx.obj['phone']
        kwargs['no_sec'] = no_sec
        
        run_algo(
                name,
                config_file=configfile,
                user_config=user_config,
                algo_file=algo_file,
                start_date=start_date, 
                end_date=end_date,
                initial_capital=initial_capital,
                frequency=frequency,
                mode=run_mode,
                execution_mode=execution_mode,
                broker=broker,
                currency=currency, 
                restart=restart, 
                benchmark=benchmark,
                quick=quick,
                timeout=timeout,
                parameters=parameters,
                initial_state=initial_state,
                publish=publish,
                compress_output=compress_output,
                redirect=redirect, 
                security_config=security_config, 
                verbose=verbose,
                command_channel=command_channel, 
                message_channel=message_channel, 
                health_channel=health_channel,
                zmq_channel=zmq_channel,
                pre_processing=PREPROCESSING,
                post_processing=POSTPROCESSING,
                server=server,
                algo_id=algo_id,
                algo_name=algo_name,
                algo_moniker=algo_moniker,
                algo_user=algo_user,
                burn_in=burn_in,
                theme=theme,
                algo=algo,
                api_key=api_key,
                secret_key=secret_key,
                platform=platform,
                email=email,
                phone=phone,
                show_progress=show_progress,
                output=output,
                **kwargs)
    except Exception as e:
        exit_status = 1
        print_msg(f'Algo run for {name} failed:{str(e)}', level='error', console=console)
    finally:
        os_exit(exit_status)
        
@main.command(context_settings=CONTEXT_SETTINGS)
@click.option(
    '-m',
    '--mode',
    default='backtest',
    type=click.Choice(['backtest', 'quickbacktest','realtime']),
    help='Run mode [backtest, live].',
    )
@click.option(
    '-q',
    '--queue-size',
    default=5,
    type=click.INT,
    help='Max allowed concurrent jobs.',
    )
@click.option(
    '-n',
    '--name',
    default=None,
    help='Name of this server',
    )
@click.option(
    '--verbose/--silent',
    default=False,
    help='Turn on/ off verbosity. [verbose/silent]')
@click.option(
    '--command-channel',
    default=None,
    type=click.STRING,
    help='Command channel host and port.',
    )
@click.option(
    '--message-channel',
    default=None,
    type=click.STRING,
    help='Message channel host and port.',
    )
@click.option(
    '--health-channel',
    default=None,
    type=click.STRING,
    help='Health channel host and port.',
    )
@click.option(
    '--zmq-channel',
    default=None,
    type=click.STRING,
    help='ZMQ channel host and port for interactive actions.',
    )
@click.option(
    '--callback-url',
    default=None,
    type=click.STRING,
    help='Callback url for status updates.',
    )
@click.pass_context
def start(ctx, mode, queue_size, name, verbose, command_channel, message_channel, health_channel, 
          zmq_channel, callback_url):
    """
        Start blueshift in the server mode.
    """
    publish = server = True
    redirect = True
    
    quick = False    
    if mode == 'quickbacktest':
        quick = True
    
    if mode in ('backtest', 'quickbacktest', 'research'):
        mode = AlgoMode.BACKTEST
    else:
        mode = AlgoMode.LIVE
    
    # exit if parent dies as a safety feature
    set_pdeathsig()
    
    from .eventloop import eventloop_factory
    
    # set name
    if not name:
        name = str(uuid.uuid4())
    
    exit_status = 0
    try:
        kwargs = get_kwargs_from_click_ctx(ctx)
        configfile = os_path.expanduser(ctx.obj['config'])
        user_config = ctx.obj['user_config']
        platform = ctx.obj['platform']
        exit_status = 0

        if callback_url:
            kwargs['callback_url'] = callback_url
        
        try:
            blueshift_reset_run_path(name)
        except Exception as e:
            msg = f'Failed to reset run directory:{str(e)}.'
            raise BlueshiftException(msg)
        
        loop = eventloop_factory(
                name,
                quick,
                queue_size=queue_size,
                config_file=configfile,
                user_config=user_config,
                mode=mode,
                publish=publish,
                redirect=redirect,
                verbose=verbose,
                command_channel=command_channel, 
                message_channel=message_channel, 
                health_channel=health_channel,
                zmq_channel=zmq_channel,
                server=server,
                platform=platform,
                pre_processing=PREPROCESSING,
                post_processing=POSTPROCESSING,
                **kwargs)
        
        loop.start()
    except Exception as e:
        exit_status = 1
        print_msg(
            f'Failed to create {BLUESHIFT_PRODUCT} algo service {name}:{str(e)}',level='error',
            console=console)
    finally:
        os_exit(exit_status)

@main.command(context_settings=CONTEXT_SETTINGS)
@click.argument(
        'bundle', 
        required=True,
        type=click.STRING,)
@click.argument(
        'src', 
        required=True,
        type=click.Path(file_okay=True, writable=False),)
@click.argument(
        'dest', 
        required=True,
        type=click.Path(file_okay=False, writable=True),)
@click.option(
    '-f',
    '--frequency',
    default='daily',
    type=click.Choice(['daily', 'minute','Q','A']),
    help='Ingestion data frequency [daily, minute].',
    )
@click.option(
    '-s',
    '--start-date',
    default=None,
    type=click.STRING,
    help='Start date for ingestion.',
    )
@click.option(
    '-e',
    '--end-date',
    default=None,
    type=click.STRING,
    help='End date for ingestion.',
    )
@click.option(
    '-c',
    '--chunksize',
    default=None,
    help='Chunksize to write ,a number (of years) or ["m","q","y"].',
    )
@click.option(
    '-v', 
    '--verbose', 
    count=True,
    help='Set verbosity level.')
@click.option(
    '--dry-run',
    is_flag=True,
    default=False,
    help='Dry run only (no data write).')
@click.option(
    '--report',
    is_flag=True,
    default=False,
    help='Save an ingestion report in the current directory.')
@click.option(
    '--overwrite',
    is_flag=True,
    default=False,
    help='Overwrite existing data (if any).')
@click.option(
    '--archive',
    is_flag=True,
    default=False,
    help='Archive ingested data.')
@click.option(
    '--show-progress',
    is_flag=True,
    default=False,
    help='Show a progress bar.')
@click.pass_context
def ingest(ctx, bundle, src, dest, frequency, start_date, end_date, 
           chunksize, verbose, dry_run, report, overwrite, archive,
           show_progress):
    """
        Ingest data for a bundle.
    """
    from blueshift.interfaces.data.ingestor import get_ingestion_bundle
    
    platform = ctx.obj['platform']
    obj = get_ingestion_bundle(bundle)
    
    if not obj:
        msg = f'ingestion bundle {bundle} not registerd'
        print_msg(msg, level="error", console=console)
        os_exit(1)
        
    kwargs = process_extra_args(**get_kwargs_from_click_ctx(ctx))
    
    try:
        if PREPROCESSING:
            run_extensions(PREPROCESSING)
            
        obj.ingest(src, dest, frequency=frequency, 
                   start_date=start_date, end_date=end_date, 
                   chunksize=chunksize, verbose=verbose, 
                   dry_run=dry_run, report=report, overwrite=overwrite, 
                   archive=archive, show_progress=show_progress, 
                   **kwargs)
        
        if POSTPROCESSING:
            run_extensions(POSTPROCESSING)
    except Exception as e:
        print_msg(f'Ingestion failed: {str(e)}', level='error', console=console)
        raise

