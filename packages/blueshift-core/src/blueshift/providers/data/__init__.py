from blueshift.interfaces.data.library import set_builtin_library_loader

def install_library(name:str):
    if name=='default' or name is None:
        from .library import Library

set_builtin_library_loader(install_library)