from blueshift.interfaces.data.adjustments import set_builtin_adj_handler_loader

def adj_installer(source_type=None):
    if source_type is None or source_type=='no-adj':
        from .noadjustment import NoAdjustments

set_builtin_adj_handler_loader(adj_installer)