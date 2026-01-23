from blueshift.interfaces.trading.blotter import set_builtin_blotter_loader

def install_blotters(blotter_type=None):
    if blotter_type is None or blotter_type=='backtest':
        from .blotters.backtest_blotter import BacktestBlotter

    if blotter_type is None or blotter_type=='paper':
        from .blotters.paper_blotter import PaperBlotter

    if blotter_type is None or blotter_type=='live':
        from .blotters.live_blotter import LiveBlotter

    if blotter_type is None or blotter_type=='execution':
        from .blotters.execution_blotter import ExecutionBlotter

set_builtin_blotter_loader(install_blotters)