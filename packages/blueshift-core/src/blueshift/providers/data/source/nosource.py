import logging

from blueshift.lib.common.enums import ConnectionStatus
from blueshift.interfaces.data.source import DataSource, DataSourceType, register_data_source
from blueshift.interfaces.logger import get_logger

class NoSource(DataSource):
    def __init__(self, *args, **kwargs):
        self._type = DataSourceType.UNDEFINED
        self._root = None
        self._frequency = None
        self._metadata = None
        self._exception = None
        self._logger:logging.Logger = kwargs.pop('logger', get_logger())
    
    @property
    def timeout(self):
        return 1
    
    @property
    def logger(self):
        return self._logger
        
    @property
    def frequency(self):
        raise NotImplementedError
        
    @property
    def root(self):
        return self._root
        
    @property
    def connected(self) -> bool:
        return True
    
    @property
    def connection_status(self):
        return ConnectionStatus.ESTABLISHED
        
    @property
    def metadata(self) -> dict:
        return {}
    
    def connect(self, *args, **kwargs):
        pass
    
    async def aconnect(self, *args, **kwargs):
        raise NotImplementedError
        
    def close(self, *args, **kwargs):
        pass
    
    async def aclose(self, *args, **kwargs):
        raise NotImplementedError
        
    def add_masterdata(self, *args, **kwargs):
        pass
        
    def add_metadata(self, *args, **kwargs):
        pass
        
    def __iter__(self):
        raise StopAsyncIteration
    
    def __aiiter__(self):
        raise NotImplementedError
    
register_data_source('no-source', NoSource)