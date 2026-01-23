from blueshift.interfaces.trading.oneclick import set_builtin_oneclick_loader

def register_oneclick(name:str):
    if name=='default' or name is None:
        from .oneclick import OneClickService

set_builtin_oneclick_loader(register_oneclick)