from os import getenv
import time
import httpx # type: ignore -> optional dependency
from urllib.request import getproxies
from httpx import HTTPTransport # type: ignore -> optional dependency
import threading
import logging

from blueshift.config import (
        BLUESHIFT_DISABLE_IPV6, BLUESHIFT_DEBUG_MODE, ELAPSED_TIME_THRESHOLD)
from blueshift.lib.exceptions import (IllegalRequest,
                                                ServerError,
                                                APIError,
                                                AuthenticationError,
                                                TooManyRequests,
                                                APIRetryError,
                                                TerminationError,
                                                ExceptionHandling)

from blueshift.lib.common.decorators import retry_on_error
from blueshift.interfaces.logger import get_logger, BlueshiftLogger


def get_broker_proxy():
    broker_proxies = {}
    
    if getenv('BLUESHIFT_BROKER_HTTP_PROXY'):
        broker_proxies['http'] = getenv('BLUESHIFT_BROKER_HTTP_PROXY')
        
    if getenv('BLUESHIFT_BROKER_HTTPS_PROXY'):
        broker_proxies['https'] = getenv('BLUESHIFT_BROKER_HTTPS_PROXY')
        
    return broker_proxies

class BrokerAPI:
    """
        HTTP based api mixin client class for REST API using httpx.
    """

    def __init__(self, event:threading.Event|None=None, logger:BlueshiftLogger|None=None,
                 timeout:int=10, agent:str="blueshift", proxies:dict={}, verify_cert:bool=True, 
                 max_connections:int=50, max_keepalive_connections:int=50, keepalive_expiry:int=2 * 60):
        
        # Configure connection pool settings
        limits = httpx.Limits(
            max_keepalive_connections=max_keepalive_connections,
            max_connections=max_connections,         
            keepalive_expiry=keepalive_expiry
        )
        
        # Create proxy mounts based on scheme
        self._proxy_mounts = {}
        system_proxies = getproxies()
        broker_proxies = get_broker_proxy()
        self._proxies  = {**proxies, **broker_proxies, **system_proxies}
        for scheme, proxy_url in self._proxies.items():
            # Normalize scheme format
            if scheme == 'http':
                scheme_key = 'http://'
            elif scheme == 'https':
                scheme_key = 'https://'
            else:
                continue

            # Create HTTPTransport with proxy configuration
            self._proxy_mounts[scheme_key] = HTTPTransport(
                proxy=proxy_url,
                verify=verify_cert,
                local_address= '0.0.0.0' if BLUESHIFT_DISABLE_IPV6 else '::' # nosec
            )

        # Create httpx client with connection pooling
        self._client = httpx.Client(
            limits=limits,
            timeout=httpx.Timeout(timeout),
            verify=verify_cert,
            headers={
                'User-Agent': agent,
                'Accept': 'application/json',
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            mounts=self._proxy_mounts,
            trust_env=False  # Disable automatic env proxy detection
        )
        
        self._agent = agent
        self._timeout = timeout
        self._last_error_response = None
        self._reset_on_error_flag = True
        self._verify_cert = verify_cert
        self._logger = logger or get_logger()
        
    def __del__(self):
        try:
            self._client.close()
        except Exception:
            pass

    def close(self):
        try:
            self._client.close()
        except Exception:
            pass

    @property
    def logger(self) -> BlueshiftLogger:
        return self._logger
    
    @logger.setter
    def logger(self, value:BlueshiftLogger):
        self._logger = value

    @retry_on_error(retry=3, sleep=2, exception=APIRetryError, inc=2)
    def _requests(self, endpoint, headers={}, parameters=None, verb="get",
                  json=None, request_headers=None, get_data=None,
                  delete_data=None, timeout=None, debug=False, 
                  logger=None, data_file=False):
        """ handle http requests using httpx."""

        if not timeout:
            timeout = self._timeout
            
        logger = logger or self._logger
        debug = debug or BLUESHIFT_DEBUG_MODE
        
        if request_headers is None:
            request_headers = {
                    'User-Agent': self._agent,
                    'Accept': 'application/json',
                    'Content-Type': 'application/x-www-form-urlencoded'
                    }
        if not isinstance(request_headers, dict):
            raise IllegalRequest(f'invalid request header, expected a dict')
        request_headers = {**request_headers, **headers}

        if verb.lower() not in ["get", "post", "delete", "patch", "put"]:
            msg = f"Invalid HTTP request type {verb}"
            raise IllegalRequest(msg)
            
        if debug:
            self.print_debug(
                    verb, endpoint, parameters, get_data, json, logger=logger)
            
        response = None
        t1 = time.time()
        
        try:
            # Prepare request parameters
            request_kwargs = {
                'headers': request_headers,
                'timeout': timeout
            }

            if data_file:
                if verb.lower() != "get":
                    raise IllegalRequest(f'data file download must be get request')
                
                with httpx.Client(
                    mounts=self._proxy_mounts, timeout=httpx.Timeout(timeout), 
                    verify=self._verify_cert, headers=request_headers) as client:
                    response = client.get(endpoint, params=parameters)
            elif verb.lower() == "get":
                if parameters:
                    request_kwargs['params'] = parameters
                if get_data:
                    request_kwargs['data'] = get_data
                
                # httpx get call don't support data parameter
                if request_kwargs.get('data'):    
                    response = self._client.request(verb, endpoint, **request_kwargs)
                else:
                    response = self._client.get(endpoint, **request_kwargs)
                
            elif verb.lower() == "post":
                if parameters:
                    request_kwargs['data'] = parameters
                if json:
                    request_kwargs['json'] = json
                response = self._client.post(endpoint, **request_kwargs)
                
            elif verb.lower() == "delete":
                if parameters:
                    request_kwargs['params'] = parameters
                if delete_data:
                    request_kwargs['data'] = delete_data
                if json:
                    request_kwargs['json'] = json

                # httpx delete call don't support data and json parameter
                if request_kwargs.get('data') or request_kwargs.get('json'):
                    response = self._client.request(verb, endpoint, **request_kwargs)
                else:
                    response = self._client.delete(endpoint, **request_kwargs)
                
            elif verb.lower() == "patch":
                if parameters:
                    request_kwargs['data'] = parameters
                if json:
                    request_kwargs['json'] = json
                response = self._client.patch(endpoint, **request_kwargs)
                
            elif verb.lower() == "put":
                if parameters:
                    request_kwargs['data'] = parameters
                if json:
                    request_kwargs['json'] = json
                response = self._client.put(endpoint, **request_kwargs)
            else:
                raise ValueError(f'unsupported http verb {verb}')
        except TerminationError:
            raise
        except httpx.TimeoutException as e:
            self.print_debug(
                    verb, endpoint, parameters, get_data, json, response, 
                    logger, error=f'endpoint not reachable:{str(e)}')
            msg = f"Failed to connect to the server in api handler at "
            msg += f'{endpoint}. Seems endpoint not reachable:{str(e)}'
            
            if verb.lower() == "post":
                raise APIError(msg)
            else:
                raise APIRetryError(msg)
        except httpx.ConnectError as e:
            self.print_debug(
                    verb, endpoint, parameters, get_data, json, response, 
                    logger, error='connection error')
            msg = f"Failed to connect to the server in api handler at "
            msg += f'{endpoint}. Seems internet connection is down:{str(e)}'
            error = APIError(msg)
            error.handling = ExceptionHandling.TERMINATE
            raise error
        except Exception as e:
            self.print_debug(
                    verb, endpoint, parameters, get_data, json, response, 
                    logger, error=e)
            msg = f"error in API for {endpoint} -> {type(e)}:{str(e)}"
            if verb.lower() == "post":
                # we do not want to retry "unsafe" post!
                raise APIError(msg)
            else:
                # let's give it a retry!
                raise APIRetryError(msg)

        status = response.status_code
        success = True if status < 300 else False
        
        if success:
            if data_file:
               return success, status, response
            
            if 'content-type' not in response.headers or "json" not in response.headers["content-type"]:
                self._set_last_error_response(response)
                self.print_debug(
                        verb, endpoint, parameters, get_data, json, response, 
                        logger, error=f'illegal response {response.text}')
                msg = f"illegal response type from server at {endpoint}. \n"
                msg += f'headers:{response.headers} \n'
                msg += f'response: {response.text}'
                raise ServerError(msg)
        elif status == 401:
            self._set_last_error_response(response)
            self.print_debug(
                    verb, endpoint, parameters, get_data, json, response, 
                    logger, error=f'authentication error {response.text}')
            msg = f'Server reports authentication: {response.text}.'
            raise AuthenticationError(msg)
        elif status == 429:
            self._set_last_error_response(response)
            self.print_debug(
                    verb, endpoint, parameters, get_data, json, response, 
                    logger, error=f'too many requests {response.text}')
            msg = f'Server reports too many requests: {response.text}'
            raise TooManyRequests(msg)
        else:
            # passively return blank data for the rest
            self.print_debug(
                    verb, endpoint, parameters, get_data, json, response, 
                    logger, error=f'status {status}, response {response.text}')

        data = None
        try:
            data = response.json()
        except TerminationError:
            raise
        except Exception as e:
            self.print_debug(
                    verb, endpoint, parameters, get_data, json, response, 
                    logger, error=e)
            if not success:
                data = response.content.decode("utf8")
                self._set_last_error_response(response)
                try:
                    if "<html>" in data.lower():
                        logger.warning(f"Got html response from server, expected JSON, see log for details")
                        logger.info(f"Got html response from API server: {data}")
                    else:
                        logger.warning(f"Got response from server with data {data}:{e}")
                except Exception:
                    logger.warning(f"Got response from server with data {data}:{e}")
                
            return success, status, data
        else:
            self._reset_on_error_flag = True
            try:
                t2 = time.time()
                elapsed = t2-t1
                if elapsed > ELAPSED_TIME_THRESHOLD:
                    t = response.elapsed.total_seconds()
                    msg = f'API request for {verb.lower()}@{endpoint} '
                    msg += f'took {round(t,2)}s for first response and '
                    msg += f'{round(elapsed,2)}s to complete.'
                    logger.info(msg)
            except Exception:
                pass
            return success, status, data

    def _set_last_error_response(self, response):
        self._last_error_response = response
        
    def print_debug(self, verb, endpoint, parameters, data, json, resp=None,
                    logger=None, error=None):
        logger = logger or self._logger
        if_print = BLUESHIFT_DEBUG_MODE or error
        
        if if_print:
            if error:
                msg = f'Error in API request for {verb.lower()}@{endpoint} with params '
                msg += f'{parameters}, data {data}, json {json}. Error: {error}'
            else:
                msg = f'API request for {verb.lower()}@{endpoint} with params '
                msg += f'{parameters}, data {data}, json {json}'
            
            logger.info(msg)
            
            if resp:
                try:
                    msg = f'Got response {resp.json()}.'
                    logger.info(msg)
                except Exception:
                    try:
                         msg = f'Got response {resp.text}.'
                         logger.info(msg)
                    except Exception:
                        try:
                            msg = f'Got response {resp.content.decode("utf8")}.'
                            logger.info(msg)
                        except Exception:
                            msg = f'Got response {resp}.'
                            logger.info(msg)
            
