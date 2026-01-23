from __future__ import annotations
import sys
import importlib
from types import ModuleType
from typing import TYPE_CHECKING

_LAZY_MODULE_NAME = 'pandas'

class LazyModule(ModuleType):
    """
    A lazy loader for a Python module.
    The module is imported only when one of its attributes is accessed.
    """
    def __init__(self, name, module_name):
        super().__init__(name)
        self._module_name = module_name
        if TYPE_CHECKING:
            self._module = importlib.import_module(self._module_name)
        else:
            self._module = None

    def __getattr__(self, name):
        """
        Load the module and return the requested attribute.
        """
        if name.startswith('__'):
            try:
                return self.__dict__[name]
            except KeyError:
                # try from the proxy target
                pass
        
        if self._module is None:
            self._module = importlib.import_module(self._module_name)
        return getattr(self._module, name)
    
    def __dir__(self):
        """
        Provide autocompletion and `dir()` support.
        """
        if self._module is None:
            self._module = importlib.import_module(_LAZY_MODULE_NAME)
        return dir(self._module)

lazy = LazyModule(__name__, 'pandas')
lazy.__dict__.update({
    "__file__": __file__,
    "__package__": __package__,
    "__spec__": __spec__,
})

sys.modules[__name__] = lazy

