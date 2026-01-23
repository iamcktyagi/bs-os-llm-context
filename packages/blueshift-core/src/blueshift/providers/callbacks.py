import httpx

from blueshift.interfaces.callbacks import BlueshiftCallback, register_server_callback
from blueshift.interfaces.logger import get_logger
from blueshift.lib.common.enums import CallBackResult

class NoCallback(BlueshiftCallback):
    def connect(self):
        pass

    def close(self):
        pass

    def callback(self, name, data):
        return CallBackResult.SUCCESS, 'success'

class BlueshiftRestAPICallback(BlueshiftCallback):
    def __init__(self, url, auth=None, **kwargs):
        transport = httpx.HTTPTransport(retries=3)
        self._session = httpx.Client(transport=transport)

        self.url = url
        headers = {
            'User-Agent': 'blueshift',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            }
        
        if auth:
            headers['X-Authorization'] = auth

        self.headers = headers
        self.logger = kwargs.pop('logger', None) or get_logger()

    def connect(self):
        if not self._session:
            transport = httpx.HTTPTransport(retries=5)
            self._session = httpx.Client(transport=transport)

    def close(self):
        try:
            if self._session:
                self._session.close()
        except Exception:
            pass

    def callback(self, name, data):
        try:
            self.logger.info(f'sending status update to {self.url}')
            r = self._session.post(self.url, headers=self.headers, json=data)
        except Exception as e:
            msg = f'Failed in blueshift server callback:{str(e)}.'
            self.logger.error(msg)
            return CallBackResult.UNREACHABLE, str(e)
        
        if r.status_code > 399 and r.status_code < 499:
            # this algo is not recognized by the server, kill it
            msg = f'Server reports error for algo {name}:{r.text}'
            return CallBackResult.ERROR, str(r.text)
        elif r.status_code != 200:
            msg = f'Failed in blueshift server callback: {r.text}'
            self.logger.error(msg)
            return CallBackResult.FAILED, str(r.text)
            
        
        msg = f'Successfully posted callback for {name}.'
        self.logger.info(msg)
        return CallBackResult.SUCCESS, 'success'
    
register_server_callback('no-callback', NoCallback)
register_server_callback('rest-api', BlueshiftRestAPICallback)