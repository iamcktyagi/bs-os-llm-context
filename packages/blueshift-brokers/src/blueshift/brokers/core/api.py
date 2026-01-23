from __future__ import annotations
from typing import TYPE_CHECKING, cast, Any
import time
from functools import wraps
from httpx import Response

from blueshift.lib.common.decorators import api_rate_limit
from blueshift.lib.common.constants import Frequency
from blueshift.lib.trades._order import Order
from blueshift.lib.trades._position import Position
from blueshift.interfaces.assets._assets import MarketData, Asset
from blueshift.lib.exceptions import (
    InitializationError, ValidationError, APIException, APIError, AuthenticationError)
from blueshift.providers.trading.broker.utils.api import BrokerAPI

if TYPE_CHECKING:
    import pandas as pd
    from threading import Event
    from blueshift.interfaces.logger import BlueshiftLogger
    from .config.config import APIBrokerConfig
    from .config.api import APIEndPoint
    from .broker import RestAPIBroker
else:
    import blueshift.lib.common.lazy_pandas as pd

MAX_PAGE_FETCH = 50
LATENCY_UPDATE_DELAY = 5

def validate_session(func):
    @wraps(func)
    def wrapper(self:BrokerAPIMixin, *args, **kwargs):
        validate = kwargs.get('validate', True)

        if validate:
            if not self.initialized:
                self.initialize()
            ctx = {'api':self}
            self.config.broker.credentials.validate(**ctx)
        return func(self, *args, **kwargs)
    return wrapper

class BrokerAPIMixin(BrokerAPI):
    """
        api mixin client class for broker's REST API.
    """
    def __init__(self, config:APIBrokerConfig, broker:RestAPIBroker|None=None, logger:BlueshiftLogger|None=None, event:Event|None=None, 
                 **kwargs):
        self.initialized = False
        self.config = config
        self.logger = logger or config.logger
        self.broker = broker
        
        agent = self.config.broker.options.agent
        timeout = self.config.api.timeout
        proxies = self.config.api.proxies
        verify_cert = self.config.api.verify_cert
        lazy = self.config.broker.options.lazy_session
        
        super(BrokerAPIMixin, self).__init__(event, self.logger, timeout, agent, proxies, verify_cert)
        
        self._requests = api_rate_limit(
                max_count=self.config.api.rate_limit, 
                time_period=self.config.api.rate_limit_period, 
                tracker=self, logger=self.logger)(self._requests)

        required_endpoints = ['get_orders', 'place_order', 'cancel_order', 'get_account']
        data_endpoints = ['get_history','get_history_multi']
        
        missing = [ep for ep in required_endpoints if ep not in self.config.api.endpoints]
        if missing:
            msg = f"Missing required endpoints in configuration: {', '.join(missing)}. "
            msg += f'Got implementations for {list(self.config.api.endpoints)}'
            raise ValidationError(msg)
        
        if not any([True if ep in self.config.api.endpoints else False for ep in data_endpoints]):
            msg = f'one of "get_history" or "get_history_multi" must be defined.'
            raise ValidationError(msg)
        
        self._last_latency_check_dt = None
        self._last_latency_check = None

        if not lazy:
            self.initialize()

    def is_valid_session(self):
        try:
            ctx = {'api':self}
            self.config.broker.credentials.validate(**ctx)
        except Exception as e:
            self.logger.error(f'session validation failed: {str(e)}')
            return False
        else:
            return True
            
    def _paginated_request(self, ep:APIEndPoint, token, data, **ctx):
        MAX_PAGES = min(self.config.broker.options.max_page_fetch, MAX_PAGE_FETCH)
        results = list(data)
        page_count = 0
        
        while token and page_count < MAX_PAGES:
            ctx['next_page_token'] = token
            
            try:
                url, _, verb, headers, params, body, json_params = ep.request.resolve(**ctx)
            except Exception as e:
                raise APIException(f'failed to resolve endpoint {ep.name} for pagination:{str(e)}') from e
        
            if not verb:
                raise APIException(f'unexpected error -> method undefined for paginated request for {ep.name}')
            
            get_data = delete_data = None
            if verb.upper() == 'GET' and body:
                get_data = body
            elif verb.upper() == 'DELETE' and body:
                delete_data = body
            elif verb.upper() in ('POST', 'PATCH', 'PUT'):
                params = body

            logger = ctx.get('logger', self.logger)

            success, status_code, resp = self._requests(
                url, headers, params, verb, json_params, None, get_data, delete_data,
                timeout=ep.timeout, logger=logger)
            
            text = ''
            if isinstance(resp, str):
                text = resp
                resp = {}

            ctx['success'] = success
            ctx['status_code'] = status_code
            ctx['response'] = resp
            ctx['text'] = text

            ep.error_handler.resolve(**ctx)
            if not success:
                raise APIError(f'paginated request failed for endpoint {ep.name}')

            try:
                new_data, token = ep.response.resolve(**ctx)
            except Exception as e:
                raise APIException(f'failed to parse data from API for {ep.name}:{str(e)}') from e
            
            if isinstance(new_data, list):
                results.extend(new_data)
            
            page_count += 1
            
        return results
    
    def raw_request(self, url, verb, headers, params, body, json_params, timeout=None, 
                    logger=None, data_file=False) -> tuple[bool, int, str|dict[str, Any]]:
        get_data = delete_data = None

        if verb.upper() == 'GET' and body:
            get_data = body
        elif verb.upper() == 'DELETE' and body:
            delete_data = body
        elif verb.upper() in ('POST', 'PATCH', 'PUT'):
            params = body

        logger = logger or self.logger
        timeout = timeout or self._timeout

        success, status_code, resp = self._requests(
            url, headers, params, verb, json_params, None, get_data, delete_data,
            timeout=timeout, logger=logger, data_file=data_file)

        return success, status_code, resp
    
    def download(self, endpoint:str|APIEndPoint, **context) -> Response:
        if isinstance(endpoint, str):
            if endpoint not in self.config.api.endpoints:
                raise NotImplementedError(f'endpoint {endpoint} is not available.')
            ep = self.config.api.endpoints[endpoint]
        else:
            ep = endpoint

        ctx = context.copy()
        ctx['broker'] = ctx.get('broker') or self.broker
        ctx['api'] = self
        ctx['credentials'] = self.config.broker.credentials
        ctx['mappings'] = self.config.broker.mappings
        ctx['config'] = self.config

        try:
            url, custom, verb, headers, params, body, json_params = ep.request.resolve(**ctx)
        except Exception as e:
            raise APIException(f'failed to resolve endpoint {endpoint}:{str(e)}') from e

        if custom:
            ctx['url'] = url
            ctx['verb'] = verb
            ctx['headers'] = headers
            ctx['params'] = params
            ctx['body'] = body
            ctx['json_params'] = json_params
            ctx['logger'] = context.get('logger', self.logger)
            try:
                return custom.resolve(**ctx)
            except Exception as e:
                raise APIException(f'failed to run custom API call for {ep.name}:{str(e)}') from e
        
        if not verb:
            raise APIException(f'no custom API function or method defined for {ep.name}')
        
        get_data = delete_data = None
        if verb.upper() == 'GET' and body:
            get_data = body
        elif verb.upper() == 'DELETE' and body:
            delete_data = body
        elif verb.upper() in ('POST', 'PATCH', 'PUT'):
            params = body
        
        logger = ctx.get('logger', self.logger)
        success, status_code, resp = self._requests(
            url, headers, params, verb, json_params, {}, get_data, delete_data, timeout=ep.timeout, 
            logger=logger, data_file=True)
        
        text = ''
        if isinstance(resp, str):
            text = resp
            resp = {}

        ctx['success'] = success
        ctx['status_code'] = status_code
        ctx['response'] = resp
        ctx['text'] = text

        ep.error_handler.resolve(**ctx)
        if not success:
            raise APIError(f'request failed for endpoint {ep.name}')
        
        return cast(Response, resp)
    
    @validate_session
    def request(self, endpoint:str|APIEndPoint, **context):
        if isinstance(endpoint, str):
            if endpoint not in self.config.api.endpoints:
                raise NotImplementedError(f'endpoint {endpoint} is not available.')
            ep = self.config.api.endpoints[endpoint]
        else:
            ep = endpoint

        check_latency = context.pop('check_latency', True)
        ctx = context.copy()
        ctx['broker'] = ctx.get('broker') or self.broker
        ctx['api'] = self
        ctx['credentials'] = self.config.broker.credentials
        ctx['mappings'] = self.config.broker.mappings
        ctx['config'] = self.config
        ctx['endpoint'] = ep.name

        try:
            url, custom, verb, headers, params, body, json_params = ep.request.resolve(**ctx)
        except Exception as e:
            raise APIException(f'failed to resolve endpoint {endpoint}:{str(e)}') from e
        
        if custom:
            ctx['url'] = url
            ctx['verb'] = verb
            ctx['headers'] = headers
            ctx['params'] = params
            ctx['body'] = body
            ctx['json_params'] = json_params
            ctx['logger'] = context.get('logger', self.logger)
            try:
                return custom.resolve(**ctx)
            except Exception as e:
                raise APIException(f'failed to run custom API call for {ep.name}:{str(e)}') from e
        
        if ep.hooks and 'before_send' in ep.hooks:
            hk = ep.hooks['before_send']
            try:
                new_ctx = {'url':url, 'verb':verb, 'headers':headers, 'query':params, 'body':body,
                           'json':json_params}
                new_ctx = {**ctx, **new_ctx}
                url, verb, headers, params, body, json_params = hk.resolve(**new_ctx)
            except Exception as e:
                raise APIException(f'failed to resolve before send hook for endpoint {endpoint}:{str(e)}') from e
        
        if not verb:
            raise APIException(f'no custom API function or method defined for {ep.name}')
        
        get_data = delete_data = None
        if verb.upper() == 'GET' and body:
            get_data = body
        elif verb.upper() == 'DELETE' and body:
            delete_data = body
        elif verb.upper() in ('POST', 'PATCH', 'PUT'):
            params = body

        logger = ctx.get('logger', self.logger)

        t1 = 0
        if check_latency:
            t1 = time.monotonic()
        
        success, status_code, resp = self._requests(
            url, headers, params, verb, json_params, None, get_data, delete_data,
            timeout=ep.timeout, logger=logger)
        
        if check_latency and t1:
            t2 = time.monotonic()
            ts = round((t2- t1)*1000, 2)
            if self._last_latency_check is None:
                self._last_latency_check = ts
            else:
                # exponential smoothing
                self._last_latency_check = 0.8*self._last_latency_check + 0.2*ts
            self._last_latency_check_dt = t2
        
        text = ''
        if isinstance(resp, str):
            text = resp
            resp = {}
        
        ctx['success'] = success
        ctx['status_code'] = status_code
        ctx['response'] = resp
        ctx['text'] = text

        if ep.hooks and 'after_response' in ep.hooks:
            hk = ep.hooks['after_response']
            try:
                hk.resolve(**ctx)
            except Exception as e:
                raise APIException(f'failed to resolve before send hook for endpoint {endpoint}:{str(e)}') from e

        ep.error_handler.resolve(**ctx)
        if not success:
            raise APIError(f'request failed for endpoint {ep.name}, status {status_code}, response {resp}')
        
        try:
            data, token = ep.response.resolve(**ctx)
        except Exception as e:
            raise APIException(f'failed to parse data from API for {endpoint}:{str(e)}') from e
        
        if not token:
            return data
        
        if not isinstance(data, list):
            raise APIException(f'paginated fetch must return a list in response.')
        
        return self._paginated_request(ep, token, data, **context)
        
    def initialize(self) -> None:
        """ 
            this method must initialize the api session, including setting 
            of user id, token and token expiries etc. It should raise 
            AuthenticationError, ValidationError or InitializationError. 
            This method does not return any value.
        """
        endpoint = 'session'
        ctx = {'logger':self.logger, 'credentials':self.config.broker.credentials, 'validate':False}

        try:
            self.request(endpoint, **ctx)
        except NotImplementedError:
            self.initialized = True
        except (ValidationError, InitializationError):
            raise
        except Exception as e:
            raise AuthenticationError(f'failed to initialize API session:{str(e)}') from e
        
        self.initialized = True
        
    def finalize(self) -> None:
        """ 
            finalize the api handler. It must call the super ``close`` method 
            to close the API session object.
        """
        self.close()
        
    def request_get_latency(self, logger=None) -> float:
        """ 
            get api latency. this maybe used to check if host is reachable
            before placing a post request like new order. Should return -1 if 
            the api call fails else returns the time latency in ms.
        """
        if self._last_latency_check_dt is not None and self._last_latency_check is not None:
            if time.monotonic() - self._last_latency_check_dt < LATENCY_UPDATE_DELAY:
                return self._last_latency_check

        t1 = time.monotonic()
        try:
            self.request_get_account(logger=logger)
        except Exception:
            return -1
        else:
            t2 = time.monotonic()
            ts = round((t2 - t1)*1000, 2)
            if self._last_latency_check is None:
                self._last_latency_check = ts
            else:
                # exponential smoothing
                self._last_latency_check = 0.8*self._last_latency_check + 0.2*ts

            self._last_latency_check_dt = t2
            return ts
            
    def request_get_account(self, logger=None, **kwargs) -> list[dict]:
        """ 
            get an account funds and margin details. Returns a list of 
            account data dict (that can be used to create account objects).
        """
        endpoint = 'get_account'
        ctx = {'logger':logger}
        data = self.request(endpoint, **ctx)

        if not isinstance(data, list):
            if not isinstance(data, dict):
                raise APIException(f'expected a dict.')
            return [data]
        else:
            return data
     
    def request_get_orders(self, logger=None, **kwargs)-> list[dict]:
        """ returns a list of order data ."""
        endpoint = 'get_orders'
        ctx = {'logger':logger}
        data = self.request(endpoint, **ctx)

        if not isinstance(data, list):
            if not isinstance(data, dict):
                raise APIException(f'expected a dict.')
            return [data]
        else:
            return data
        
    def request_get_order_by_id(self, order_id, logger=None, **kwargs)-> dict:
        """ Get an order data by id."""
        endpoint = 'get_order'
        ctx = {'order_id':order_id, 'logger':logger}
        data = self.request(endpoint, **ctx)

        if not isinstance(data, dict):
            raise APIException(f'expected a dict.')
        return data
        
    def request_place_trade(self, order:Order, algo_id=None, logger=None, **kwargs) -> str:
        """ place an order. Raises OrderError, PriceOutOfRange."""
        endpoint = 'place_order'
        ctx = {'order':order, 'logger':logger}
        data = self.request(endpoint, **ctx)

        if not isinstance(data, dict) or 'order_id' not in data:
            raise APIException(f'expected a dict with `order_id` field.')
        return data['order_id']
    
    def request_cancel_order(self, order:Order, logger=None, **kwargs) -> str:
        """ cancel an order by id. Raises OrderError, OrderAlreadyProcessed."""
        endpoint = 'cancel_order'
        ctx = {'order':order, 'logger':logger}
        data = self.request(endpoint, **ctx)

        if not isinstance(data, dict) or 'order_id' not in data:
            raise APIException(f'expected a dict with `order_id` field.')
        return data['order_id']
    
    def request_update_order(self, order:Order, quantity:float|None=None, price:float|None=None, 
                             logger=None, **kwargs) -> str:
        """ Update an open order. Raises OrderError, OrderAlreadyProcessed."""
        endpoint = 'update_order'
        ctx = {'order':order, 'quantity':quantity, 'price':price, 'logger':logger}
        data = self.request(endpoint, **ctx)

        if not isinstance(data, dict) or 'order_id' not in data:
            raise APIException(f'expected a dict with `order_id` field.')
        return data['order_id']
    
    def request_unwind_by_asset(self, asset:Asset):
        raise NotImplementedError
            
    def request_get_positions(self, logger=None, **kwargs) -> list[dict]:
        """ Fetch all open position."""
        endpoint = 'get_positions'
        ctx = {'logger':logger}

        try:
            data = self.request(endpoint, **ctx)
        except NotImplementedError:
            raise NotImplementedError(f'positions API not implemented')

        if not isinstance(data, list):
            if not isinstance(data, dict):
                raise APIException(f'expected a dict.')
            return [data]
        else:
            return data
            
    def request_get_trading_margins(self, orders:list[Order], positions:list[Position], 
                                    logger=None, **kwargs) -> tuple[bool, float]:
        """ 
            Calculate required margin given the orders, and the positions. 
            The first value is True, if the margin calculated is incremental,
            else it must be false.
        """
        endpoint = 'get_margins'
        ctx = {'orders':orders, 'positions':positions, 'logger':logger}

        try:
            data = self.request(endpoint, **ctx)
        except NotImplementedError:
            raise NotImplementedError(f'margins API not implemented')

        if not isinstance(data, dict):
            raise APIException(f'expected a dict.')
        elif 'incremental' not in data or 'margins' not in data:
            raise APIException(f'return dict must have keys "incremental" and "margins".')
        
        return data['incremental'], data['margins']
    
    def request_get_charge(self, order:Order, logger=None, **kwargs) -> tuple[float, float]:
        """ 
            calculate charges for an order. Returns a tuple of brokerage 
            and other charges.
        """
        endpoint = 'get_charges'
        ctx = {'order':order, 'logger':logger}

        try:
            data = self.request(endpoint, **ctx)
        except NotImplementedError:
            return 0, 0

        if not isinstance(data, dict):
            raise APIException(f'expected a dict.')
        elif 'commission' not in data or 'charges' not in data:
            raise APIException(f'return dict must have keys "commission" and "charges".')
        
        return data['commission'], data['charges']
             
    def request_get_quote(self, asset:Asset, logger=None, **kwargs) -> dict:
        """ get quotes. """
        endpoint = 'get_quote'
        ctx = {'asset':asset, 'logger':logger}

        try:
            data = self.request(endpoint, **ctx)
        except NotImplementedError:
            raise NotImplementedError(f'quotes API not implemented')


        if not isinstance(data, dict):
            raise APIException(f'expected a dict.')
        return data
            
    def request_get_history(self, asset:MarketData, freq:Frequency, 
                            from_dt:pd.Timestamp, to_dt:pd.Timestamp, 
                            nbars:int, logger=None, **kwargs) -> pd.DataFrame:
        """ get historical data for a single instruments. """
        endpoint = 'get_history'
        ctx = {
            'asset':asset,
            'freq':freq, 
            'from_dt':from_dt, 
            'to_dt':to_dt, 
            'nbars':nbars,
            'logger':logger
        }
        
        data = self.request(endpoint, **ctx)

        if not isinstance(data, pd.DataFrame):
            raise APIException(f'expected a dataframe.')
        
        return data
        
    def request_get_history_multi(self, assets:list[MarketData], freq:Frequency, 
                            from_dt:pd.Timestamp, to_dt:pd.Timestamp, 
                            nbars:int, logger=None, **kwargs) -> dict[str,pd.DataFrame]:
        """ get historical data for multiple instruments in a single API call. """
        if self.config.broker.options.multi_assets_data_query:
            endpoint = 'get_history_multi'
            ctx = {
                'asset':assets,
                'freq':freq, 
                'from_dt':from_dt, 
                'to_dt':to_dt, 
                'nbars':nbars,
                'logger':logger
            }
            
            try:
                data = self.request(endpoint, **ctx)
            except NotImplementedError:
                pass
            else:
                if not isinstance(data, dict):
                    raise APIException(f'expected a dict of dataframes')
                return data

        res = {}  
        for asset in assets:
            data = self.request_get_history(asset, freq, from_dt, to_dt, nbars, logger)
            if not isinstance(data, pd.DataFrame):
                raise APIException(f'{asset}:expected a dataframe')
            res[asset.exchange_ticker] = data

        return res
            
        
        

