from __future__ import annotations
from typing import Type
from types import CodeType
from abc import ABC, abstractmethod
import logging
from io import TextIOBase

from ...plugin_manager import load_plugins

class CodeChecker(ABC):
    """ interface code scanning, transformation and security check.

    Args:
        pwd (str): current working directory.
        root (str): root directory for user workspace.
        config (dict, optional): a config object (dictionary from json).
        config_file (str, optional): configuration (json) file location.
        dry_run (bool): Do not import non-user modules.
    """

    def __init__(self, pwd:str, root:str, config:dict|None=None, config_file:str|None=None,
                 dry_run:bool=False, no_sec:bool=False, mode:str="algo", max_print:int=0,
                 stdout:TextIOBase|None=None, stderr:TextIOBase|None=None, 
                 logger:logging.Logger|None=None, *args, **kwargs):
        pass
        
    @abstractmethod
    def compile_(self, src:str, fname:str, mode:str='exec', flags:int=0, dont_inherit:bool=False, 
                 optimize:int=-1, *args, **kwargs) -> CodeType:
        """  compile with AST scanning and regex and other rules

        Args:
            src (str): Source code to compile.
            fname(str): File name for the source code.

        Returns:
            compile code object if successful, raise exception otherwise.
        """
        raise NotImplementedError
                    

    @abstractmethod
    def exec_(self, code:CodeType, namespace:dict={}, *args, **kwargs):
        """  exec with restricted builtins

        Args:
            code (obj): Code object from `compile_`.
            namespace(dict): Namespace of the exec.

        Returns:
            None. The objects after execution are available in `namespace`
        """
        raise NotImplementedError

    @property
    def workspace(self):
        raise NotImplementedError

class NoCodeChecker(CodeChecker):
    """ base implementation without any checks. """
    def compile_(self, src:str, fname:str, mode:str='exec', flags:int=0, dont_inherit:bool=False, 
                 optimize:int=-1, *args, **kwargs) -> CodeType:
        return compile(src, fname, mode, flags=flags, dont_inherit=dont_inherit, 
                       optimize=optimize, *args, **kwargs)
    
    def exec_(self, code:CodeType, namespace:dict={}, *args, **kwargs):
        return exec(code, namespace) # nosec
    

_code_registry:dict[str, Type[CodeChecker]] = {}

def register_code_checker(name: str, cls: Type[CodeChecker]):
    """Register a new code checker type."""
    _code_registry[name] = cls

def code_checker_factory(code_checker_type: str, *args, **kwargs) -> CodeChecker:
    """ create code checker instance by type name. """
    from inspect import getfullargspec

    cls = _code_registry.get(code_checker_type)
    if cls is None:
        try:
            load_plugins('blueshift.plugins.security')
        except Exception:
            pass

    if cls is None:
        raise NotImplementedError(f"Unknown code checker type: {code_checker_type}")
    
    specs = getfullargspec(cls.__init__)

    if specs.varkw:
        kw = kwargs.copy()
    else:
        args_specs = specs.args
        kw = {}
        for key in kwargs:
            if key in args_specs:
                kw[key] = kwargs[key]

    return cls(*args, **kw)

def list_code_checkers() -> list[str]:
    """ registered code checker class names. """
    return list(_code_registry)

register_code_checker('no-checker', NoCodeChecker)

__all__ = [
    'CodeChecker',
    'NoCodeChecker',
    'register_code_checker',
    'code_checker_factory',
    'list_code_checkers',
    ]

