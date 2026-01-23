from blueshift.interfaces.trading.broker import set_builtin_brokers_loader

def install_brokers(broker_type):
    backtesters = [
        'backtest',
        'india-eq-broker',
        'nse', 
        'bse',
        'india-eq-paper',
        'nse-paper',
        'bse-paper',
        'regT',
        'us-cash',
        'crypto',
    ]

    if broker_type is None or broker_type in backtesters:
        from .backtester import BacktestBroker

    from .paper import PaperBroker
    from .shared import SharedBroker, SharedBrokerClient, SharedBrokerCollection

set_builtin_brokers_loader(install_brokers)