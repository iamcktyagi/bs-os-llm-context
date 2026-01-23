from blueshift.interfaces.trading.rms import set_builtin_rms_loader

def install_rms(name:str):
    if name is None or name in ('NSE','BSE', 'NFO', 'BFO','IND'):
        from .nse_rms import NSERMS

set_builtin_rms_loader(install_rms)