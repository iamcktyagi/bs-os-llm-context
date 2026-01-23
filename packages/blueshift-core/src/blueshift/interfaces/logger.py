from __future__ import annotations
from typing import TYPE_CHECKING
import logging
from sys import stderr as sys_stderr
from os import path as os_path
from functools import partial

from blueshift.lib.common.enums import AlgoMode as MODE
from blueshift.config import (blueshift_log_path, 
                               get_config_alerts, BLUESHIFT_PRODUCT)

if TYPE_CHECKING:
    import pandas as pd
else:
    import blueshift.lib.common.lazy_pandas as pd


PLATFORM_LEVELV_NUM = 150
logging.addLevelName(PLATFORM_LEVELV_NUM, "PLATFORM")
MAX_LOG_NUM = 2000

def platform_log(self, message, *args, **kwargs):
    if self.isEnabledFor(PLATFORM_LEVELV_NUM):
        self._log(PLATFORM_LEVELV_NUM, message, args, **kwargs)


class BlueshiftLogHandlers:
    """
        Defines log handlers and sets their levels based on user config.
        It also adds the formatter for logged messages.
        
        See also: :ref:alerts configuration <alertsconf>`.
        
        Args:
            name (str, in kwargs): Name of the current run
            
        Returns:
            None.
    """
    
    LOG_DEST = ['log', 'console', 'email', 'msg', 'websocket']
    
    def __init__(self, *args, **kwargs):
        name = kwargs.get("name","blueshift")
        log_path = blueshift_log_path(name)
        self.log_root = os_path.dirname(log_path)
        self.log_dir = os_path.basename(log_path)
        
        timestamp = pd.Timestamp.now().normalize()
        logfile = (str(timestamp)+".log").strip().\
                        replace(' ','_').replace(':','-')
        self.log_file = os_path.join(self.log_root, self.log_dir,
                                     logfile)
        
        self.handlers = {}

        try:
            from rich.logging import RichHandler # type: ignore
            self.handlers['console'] = RichHandler(
                markup=True,  # Allows using markup like [bold]...[/bold]
                rich_tracebacks=True, # Pretty and readable tracebacks
                )
        except ImportError:
            self.handlers['console'] = logging.StreamHandler(sys_stderr)
        
        self.handlers['log'] = logging.FileHandler(self.log_file)
        self.handlers['msg'] = None
        self.handlers['email'] = None
        self.handlers['websocket'] = None
        
        prefix = f'{BLUESHIFT_PRODUCT} Alert({name})'
        formatstr = f'{prefix}[%(myasctime)s](%(levelname)s): %(message)s'
        formatstr = logging.Formatter(formatstr)
        self.formatter = formatstr
        
        for key in self.handlers:
            if self.handlers[key]:
                self.handlers[key].setFormatter(formatstr)
            
        # set up alert levels according to the rules in config
        alert_rules = get_config_alerts()
        
        # set level for errors
        for dest in alert_rules['error']:
            if self.handlers[dest]:
                self.handlers[dest].setLevel(logging.ERROR)
        # set level for warning
        for dest in alert_rules['warning']:
            if self.handlers[dest]:
                self.handlers[dest].setLevel(logging.WARNING)
        # info handler setting
        info_set = set([*alert_rules['platform_msg'],
                    *alert_rules['log']])
        for dest in info_set:
            if self.handlers[dest]:
                self.handlers[dest].setLevel(logging.INFO)
                
        verbose = kwargs.pop('verbose', False)
        if verbose:
            self.handlers['log'].setLevel(logging.DEBUG)
                
    def __str__(self):
        return "Blueshift Log Handler"
    
    def __repr__(self):
        return self.__str__()
        

class BlueshiftLogger(logging.Logger):
    """ Blueshift Logger class
    
        This class handles the logging duty. It is a part of the alert 
        manager object and is set up to know where to direct different 
        message types.
        
        Note:
            In the logging functions, supply a ``timestamp`` argument to 
            mark a specific timestamp for a message. Else the current 
            system time will be used.
            
            See also: :ref:alerts configuration <alertsconf>`.
            
        Args:
            ``name (kwargs)``: the name of the current run
        
    """
    
    def __init__(self, name:str, *args, **kwargs):
        self._name = name
        self._create(*args, **kwargs)
        self._log_count = 0
        
    def __eq__(self, other):
        if isinstance(other, BlueshiftLogger):
            return self.name == other.name
        return False
    
    def __hash__(self):
        return hash(self.name)
                
    def _create(self, *args, **kwargs):
        self._logger = logging.getLogger(self.name)
        self._logger.platform = partial(platform_log, self._logger) # type: ignore
        
        self.handler_obj = BlueshiftLogHandlers(name=self._name, *args, **kwargs)
        self.handlers_dict = self.handler_obj.handlers
        
        self._levels = {}
        for tag, handler in self.handlers_dict.items():
            self.add_handler(tag, handler)
                
        self._logger.setLevel(logging.INFO)
        
        self.tz = kwargs.get('tz', None)
        
    def add_handler(self, tag:str, handler:logging.Handler):
        if handler:
            self._logger.addHandler(handler)
            self._levels[tag] = handler.level
            
    @property
    def name(self): # type: ignore -> this is a pyi issue from the logging module
        return self._name
        
    def __str__(self):
        return f"Blueshift Logger[{self.name}]"
    
    def __repr__(self):
        return self.__str__()
    
    def _pretty_msg(self, msg, module=None, *args, **kwargs) -> str:
        mode = kwargs.get("mode", None)
        trace = kwargs.pop("trace", None)
        if mode == MODE.BACKTEST:
            if module==None:
                return f"{str(msg)}"
            return f"{str(msg)}"
        
        if module is not None:
            return f"{str(msg)}"
        
        if trace:
            try:
                fname, lineno, modname, sinfo = self._logger.findCaller()
            except Exception:
                msg=f"in unknown:{str(msg)}"
            else:
                msg=f"in {modname}:{str(msg)}"
        else:
            return f"{str(msg)}"
            
        return msg
    
    def debug(self, msg, module=None, *args, **kwargs):
        asctime = None
        msg = self._pretty_msg(msg, module, *args, **kwargs)
        
        mode = kwargs.pop('mode', None)
        if mode == MODE.BACKTEST:
            asctime = str(kwargs.pop('timestamp', None))
        
        if not asctime:
            asctime = str(pd.Timestamp.now(tz=self.tz))
        self._logger.debug(msg, extra={'myasctime':asctime})
    
    def info(self, msg, module=None, *args, **kwargs):
        if self._log_count > MAX_LOG_NUM:
            return
        
        asctime = None
        msg = self._pretty_msg(msg, module, *args, **kwargs)
        
        mode = kwargs.pop('mode', None)
        if mode == MODE.BACKTEST:
            self._log_count += 1
            asctime = str(kwargs.pop('timestamp', None))
        
        if not asctime:
            asctime = str(pd.Timestamp.now(tz=self.tz))
        self._logger.info(msg, extra={'myasctime':asctime})
        
    def platform(self, msg, module=None, *args, **kwargs):
        asctime = None
        msg = self._pretty_msg(msg, module, *args, **kwargs)
        
        mode = kwargs.pop('mode', None)
        if mode == MODE.BACKTEST:
            asctime = str(kwargs.pop('timestamp', None))
        
        if not asctime:
            asctime = str(pd.Timestamp.now(tz=self.tz))
        self._logger.platform(msg, extra={'myasctime':asctime}) # type: ignore
        
    def warning(self, msg, module=None, *args, **kwargs):
        if self._log_count > MAX_LOG_NUM:
            return
        
        asctime = None
        msg = self._pretty_msg(msg, module, *args, **kwargs)
        mode = kwargs.pop('mode', None)
        if mode == MODE.BACKTEST:
            self._log_count += 1
            asctime = kwargs.pop('timestamp', None)
            
        if not asctime:
            asctime = str(pd.Timestamp.now(tz=self.tz))
        self._logger.warning(msg, extra={'myasctime':asctime})
        
    def warn(self, msg, module=None, *args, **kwargs):
        self.warning(msg, module, *args, **kwargs)
        
    def error(self, msg, module=None, *args, **kwargs):
        asctime = None
        msg = self._pretty_msg(msg, module, *args, **kwargs)
        
        mode = kwargs.pop('mode', None)
        if mode == MODE.BACKTEST:
            asctime = kwargs.pop('timestamp', None)
            
        if not asctime:
            asctime = str(pd.Timestamp.now(tz=self.tz))
            
        self._logger.error(msg, extra={'myasctime':asctime})
        
    def daily_log(self):
        pass
    
    def set_levels(self, levels={}):
        try:
            level = int(levels)
        except Exception:
            for key in levels:
                if key not in self.handlers_dict:
                    continue
                if self.handlers_dict[key]:
                    self.handlers_dict[key].setLevel(levels[key])
        else:
            for key in self.handlers_dict:
                if self.handlers_dict[key]:
                    self.handlers_dict[key].setLevel(level)
    
    def restore_levels(self):
        for key in self.handlers_dict:
            if self.handlers_dict[key]:
                self.handlers_dict[key].setLevel(self._levels[key])
    
    def close(self):
        for tag, handler in self.handlers_dict.items():
            if handler:
                try:
                    self._logger.removeHandler(handler)
                    handler.close()
                except Exception:
                    pass

_logger_registry:dict[str, BlueshiftLogger] = {}

def register_logger(name:str, logger:BlueshiftLogger):
    _logger_registry[name] = logger

def get_logger(name:str=BLUESHIFT_PRODUCT) -> BlueshiftLogger:
    if name in _logger_registry:
        return _logger_registry[name]
    
    logger = BlueshiftLogger(name)
    register_logger(name, logger)
    return logger

__all__ = ['BlueshiftLogger', 'register_logger','get_logger']