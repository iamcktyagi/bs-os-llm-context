from __future__ import annotations
from typing import TYPE_CHECKING, Literal
import os
import sys
import asyncio
import re
import signal

from blueshift.lib.exceptions import BlueshiftException
from blueshift.lib.common.enums import AlgoMessageType
from blueshift.lib.common.functions import create_message_envelope

if TYPE_CHECKING:
    import rich
    from blueshift.core.utils.environment import TradingEnvironment
    from blueshift.core.alerts.alert import BlueshiftAlertManager

def sizeof(obj, seen=None):
    """Recursively finds size of objects"""
    from sys import getsizeof as sys_getsizeof

    size = sys_getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    
    try:
        if isinstance(obj, dict):
            size += sum([sizeof(v, seen) for v in obj.values()])
            size += sum([sizeof(k, seen) for k in obj.keys()])
        elif hasattr(obj, '__dict__'):
            size += sizeof(obj.__dict__, seen)
        elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
            size += sum([sizeof(i, seen) for i in obj])
    except Exception:
        # get some approximate results at least
        pass
    return size

def set_traceback_for_warnings():
    import traceback
    import warnings
    import sys
    
    def warn_with_traceback(message, category, filename, lineno, file=None, line=None):
        log = file if hasattr(file,'write') else sys.stderr
        traceback.print_stack(file=log)
        log.write(warnings.formatwarning(message, category, filename, lineno, line)) # type: ignore
    
    warnings.showwarning = warn_with_traceback

def check_csv_encoding(path):
    """ search for appropriate encoding for a csv file. """
    import pandas as pd

    def check_df(encoding=None):
        try:
            reader = pd.read_csv(path, chunksize=100, encoding=encoding)
            df = next(reader)
            if len(df.dropna()) > 0:
                return True
            return False
        except Exception:
            return False
    
    encodings = set(['utf_7','utf_8','utf_8_sig','utf_16','utf_16_le',
                     'utf_16_be','utf_32','utf_32_le','utf_32_be'])
    
    if not os.path.exists(path):
        raise ValueError('input file not found.')
    if not os.path.isfile(path):
        raise ValueError('input not a file.')
    
    if check_df():
        return None
    
    for encoding in encodings:
        if check_df(encoding):
            return encoding
        
    raise ValueError('could not find a suitable enconding.')

def get_all_dirs(path, root):
    """ 
        for a given root directory and module full path, recursively 
        search for all directory names starting from the root location
        till the module location.
    """
    if root not in path:
        raise ValueError("invalid path.")
        
    if root == path:
        return []

    dir_list = []
    while True:
        path, node = os.path.split(path)
        dir_list.append(node)
        if path == root:
            break

    return [item for item in reversed(dir_list)]

def directory_details(path, extn):
    """ List total file count and size of a directory recursively. """
    size = 0
    count = 0
    for root, dirs, files in os.walk(path):
        for file in files:
            dest = os.path.join(root, file)
            if dest.endswith(extn):
                size = size + os.path.getsize(dest)
                count = count + 1
    return count, size


def ensure_directory(path):
    """ Ensure a directory exists (else create it). """
    import errno

    if os.path.isdir(path):
        return
    try:
        os.makedirs(path)
        return
    except OSError as e:
        if e.errno == errno.EEXIST:
            return
    
    msg = f"directory {path} not found and failed to create."
    raise OSError(msg)
    
def _build_cmdtuple(path, cmdtuples):
    for f in os.listdir(path):
        real_f = os.path.join(path, f)
        if os.path.isdir(real_f) and not os.path.islink(real_f):
            _build_cmdtuple(real_f, cmdtuples)
        else:
            cmdtuples.append((os.remove, real_f))
    cmdtuples.append((os.rmdir, path))
    
def remove_tree(path, verbose=False, dry_run=False):
    """ 
        Replacement for distutils remove_tree (without dry_run and 
        verbose support) and with exception raise - from distuitl github. 
    """
    if verbose >= 1:
        print(f"removing {path} (and everything under it).")
        
    if dry_run:
        return
        
    # first get the commands
    cmdtuples = []
        
    # then execute them and raise error if any
    _build_cmdtuple(path, cmdtuples)
    
    for cmd in cmdtuples:
        cmd[0](cmd[1])
        
def copy_file(path):
    pass
    
def copy_tree(path):
    pass
    
def touch(path, file=None):
    """ create a file, mimicking unix `touch` command. """
    if file:
        path = os.path.join(path, file)
        
    with open(path, 'a'):
        pass
    
def sanitaize_pathnames(s):
    """ remove possible path name information from strings """

    s = s.split('://')
    if len(s) > 1:
        s = s[-1]
    else:
        s = s[0]

    old_s = s
    s = s.split('/')
    if len(s) > 1:
        s = s[0] + s[-1]
    else:
        s = old_s

    return s

def get_exception(exc_type, exc_value, exc_traceback):
    from contextlib import closing
    import io
    import traceback as tb
    
    msg = ''
    with closing(io.StringIO()) as error_string:
        tb.print_exception(
                exc_type, exc_value, exc_traceback, file=error_string)
        
        msg = f'Error: {error_string.getvalue()}.'
        
    return msg

def print_exception(exc_type, exc_value, exc_traceback, pathnames=[],
                    msg=None, file=sys.stderr):
    """
        Print the most recent exception with tracebacks.
    """
    from rich.console import Console
    from rich.traceback import Traceback
    
    console = Console(file=file)
    
    if not msg:
        msg = "Error in strategy - Traceback (most recent call last):"
    
    console.print(msg)
    tb = Traceback.from_exception(exc_type, exc_value, exc_traceback)
    console.print(tb)

def print_msg(msg, level:Literal['info','error','warning','success','info2']='info', *args, **kwargs):
    """ print with ansi colour escape codes based on platform """
    from rich.console import Console

    styles = {
        "info": "[bold cyan]",
        "info2": "[bold cyan]",
        "warning": "[bold yellow]",
        "error": "[bold red]",
        "success": "[bold green]",
    }

    if level:
        style = styles.get(level.lower(),'')
    else:
        style = None

    if style:
        msg = f"{style}{level.upper():<5}[/] {msg}"

    console = kwargs.pop('console', None)
    if not console:
        file = kwargs.pop('file', None)
        if file:
            console = Console(file=file)
        else:
            console = Console()
        
    console.print(msg)

def print_and_log_exception(algo:str|None, alert_manager:BlueshiftAlertManager, msg:str='', 
                            error:Exception|None=None):
    """ Pretty print exception. """
    import sys
    
    publish = alert_manager.publish
    algo_name = algo if algo is not None else ''
    msg = msg if msg else 'Unexpected error in algo {algo}'
    err_msg = ''

    if error:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        err_msg = get_exception(exc_type, exc_value, exc_traceback)
        err_msg = f'{msg}:{err_msg}'
        print_exception(exc_type, exc_value, exc_traceback, msg=msg)
    else:
        err_msg = f'{msg}:{err_msg}'
        print_msg(err_msg, level='error')

    if alert_manager.logger:
        if error:
            if not isinstance(error, BlueshiftException):
                alert_manager.logger.error(msg)
            else:
                alert_manager.logger.error(err_msg)

    if publish and alert_manager.publisher and algo:
        err_msg = f'{msg}:{error}' if error else msg
        packet = create_message_envelope(algo, AlgoMessageType.PLATFORM, msg = err_msg, level='error')
        alert_manager.publisher.send_to_topic(algo, packet)

def print_pretty(items, **kwargs):
    from rich.pretty import Pretty
    if not 'console' in kwargs:
        from rich.console import Console
        console = Console()
    else:
        console:rich.console.Console = kwargs.pop('console')

    console.print(Pretty(items))

def print_algo_summary(name, perfs, **kwargs):
    from rich.table import Table
    
    if not 'console' in kwargs:
        from rich.console import Console
        console = Console()
    else:
        console:rich.console.Console = kwargs.pop('console')

    final = perfs.tail(1).iloc[0]
    if console.is_terminal:
        table = Table(title=f"{name}| Backtest performance summary")
        table.add_column("Metric")
        table.add_column("Value")

        table.add_row('start_dt', str(perfs.index[0].date()))
        table.add_row('end_dt', str(perfs.index[-1].date()))

        metrics = [
            'cumulative_returns',
            'volatility',
            'drawdown', 
            'cagr',
            'sharpe_ratio',
            'alpha',
            'beta',
            'sortino_ratio',
            'calmar_ratio',
            'omega_ratio',
            'stability',
            'tail_ratio',
            'value_at_risk'
            ]

        for col, val in final.items():
            if col not in metrics:
                continue
            table.add_row(col, str(round(val,4)))

        console.print(table)
    else:
        print(final)
    
def check_platform(platforms='console'):
    """ 
        Check if current platform is of the given type. Options can be 
        combined using `|` to specify multiple platform types.
    """
    def if_notebook():
        import sys
        return 'ipykernel' in sys.modules
    
    def if_ipython():
        import sys
        return 'IPython' in sys.modules
    
    def if_docker():
        import os
        return os.path.exists('/.dockerenv') or os.path.exists('/.dockerinit')
    
    platforms = platforms.split('|')
    rets = None
    for p in platforms:
        if p.lower() == 'notebook' or p.lower == 'jupyter':
            rets = rets or if_notebook()
        elif p.lower() == 'ipython':
            rets = rets or if_ipython()
        elif p.lower() == 'docker':
            rets = rets or if_docker()
        elif p.lower() == 'console' or p.lower() == 'terminal':
            rets = rets or (not if_ipython()) and (not if_docker())
        else:
            raise ValueError(f'unsupported platform type {p}')
            
    return rets

def if_notebook():
    """ check if the current platform is a notebook or similar """
    import sys
    return 'ipykernel' in sys.modules

def if_ipython():
    """ check if the current platform is running IPython """
    import sys
    return 'IPython' in sys.modules

def if_docker():
    """ check if the current platform is a docker container """
    import os
    return os.path.exists('/.dockerenv') or os.path.exists('/.dockerinit')

def is_platform(platforms='console'):
    """ 
        Check if current platform is of the given type. Options can be 
        combined using `|` to specify multiple platform types.
    """
    def if_notebook():
        import sys
        return 'ipykernel' in sys.modules
    
    def if_ipython():
        import sys
        return 'IPython' in sys.modules
    
    def if_docker():
        import os
        return os.path.exists('/.dockerenv') or os.path.exists('/.dockerinit')
    
    platforms = platforms.split('|')
    rets = None
    for p in platforms:
        if p.lower() == 'notebook' or p.lower == 'jupyter':
            rets = rets or if_notebook()
        elif p.lower() == 'ipython':
            rets = rets or if_ipython()
        elif p.lower() == 'docker':
            rets = rets or if_docker()
        elif p.lower() == 'console' or p.lower() == 'terminal':
            rets = rets or (not if_ipython()) and (not if_docker())
        else:
            raise ValueError(f'unsupported platform type {p}')
            
    return rets

def is_windows():
    if 'win' in sys.platform:
        return True
    return False

IF_NOTEBOOK = if_notebook()
IF_IPYTHON = if_ipython()
IF_DOCKER = if_docker()
IF_WINDOWS = is_windows()
        
def get_host_name():
    # do cat /etc/hostname
    try:
        import socket
        return socket.gethostname()
    except Exception:
        pass

def get_host_ip():
    try:
        import socket
        host = get_host_name()
        return socket.gethostbyname(host) # type: ignore
    except Exception:
        pass
    
def get_current_pid():
    try:
        import os
        return os.getpid()
    except Exception:
        pass

def ping_latency(host="google.com", n=1):
    from platform import system as platform_system
    from subprocess import run as subprocess_run
    from subprocess import PIPE as subprocess_PIPE

    pattern = r"Average = (\d+\S+)"
    param = '-n' if platform_system().lower()=='windows' else '-c'
    command = ['ping', param, str(n), host]
    
    try:
        res = subprocess_run(
                command, stdout=subprocess_PIPE, 
                stderr=subprocess_PIPE)
    except Exception:
        return -1
    
    if res.stdout:
        try:
            latency = re.findall(pattern, res.stdout.decode())[0]
            splits = latency.split("ms")
        except Exception:
            return -1
        
        if len(splits) > 1:
            return int(splits[0])
        else:
            return -1
    return -1

def check_system_health(check_resource=False):
    try:
        from psutil import cpu_percent as psutil_cpu_percent
        from psutil import virtual_memory as psutil_virtual_memory
    except ImportError:
        raise ImportError(f'psutils, which is reqiured for this feature, is not installed.')

    cpu_threshold = 100
    mem_threshold = 90    # trigger a gc at this point
    mem_threshold2 = 95   # trigger health check failure at this point
    status = False
    
    try:
        cpu = psutil_cpu_percent()
    except Exception:
        cpu = -1
    
    try:
        mem = psutil_virtual_memory()
        mem = round(mem.used/mem.total*100,1)
    except Exception:
        mem = -1
    
    # TODO: ping is turned off in this version.
    # for stand-alone mode enable this.
    #ping = ping_latency()
    ping = 0
    
    if cpu < 0 or mem < 0 or ping < 0:
        status = False
    elif cpu > cpu_threshold or mem > mem_threshold:
        status = False
    else:
        status = True
    
    if check_resource:
        return {'status':status,'cpu':cpu, 'mem':mem, 'ping':ping}
    
    if mem < mem_threshold2:
        status = True
    
    return {'status':status,'cpu':cpu, 'mem':mem, 'ping':ping}

def set_pdeathsig(signum=signal.SIGTERM):
    if os.name == 'posix':
        try:
            import prctl
            prctl.set_pdeathsig(signum)
            return True
        except Exception:
            return False
        
def set_resource_limits(cpu=None, data=None, rss=None):
    if not os.name == 'posix':
        return
    
    try:
        import resource
    except ImportError:
        return
    
    # no core dump
    resource.setrlimit(resource.RLIMIT_CORE,(0,0))
    
    if cpu:
        resource.setrlimit(resource.RLIMIT_CPU,(cpu,cpu))
        
    if data:
        resource.setrlimit(resource.RLIMIT_DATA,(data,data))
        
    if rss:
        resource.setrlimit(resource.RLIMIT_RSS,(rss,rss))

def forced_shutdown():
    """ carry out a forced shutdown."""
    from sys import exit as sys_exit
    pending_tasks = [task for task in asyncio.all_tasks()\
                     if not task.done()]
    if pending_tasks:
        for task in pending_tasks:
            task.cancel()
        
    sys_exit(1)
        
def kill_windows_process(pid):
    import subprocess
    return subprocess.call(
            ['taskkill', '/F', '/T', '/PID', str(pid)])