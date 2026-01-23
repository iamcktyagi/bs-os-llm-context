from blueshift.interfaces.data.store import set_builtin_data_store_loader

def store_installer(store_type=None):
    if store_type is None or store_type=='dataframe':
        from .dataframe_store import DataFrameStore
    
    if store_type is None or store_type=='broker':
        from .broker_store import BrokerStore

set_builtin_data_store_loader(store_installer)