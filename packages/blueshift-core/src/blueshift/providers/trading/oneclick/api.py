from __future__ import annotations
from typing import Any, Literal
import httpx
from urllib.parse import urljoin
from enum import Enum
from os import getenv as os_getenv

from blueshift.lib.exceptions import (raise_exception, ExceptionHandling, RetryError, APIError, 
        ValidationError, TerminationError)
from blueshift.lib.common.decorators import retry_on_error


def oneclick_post_params(data:dict[str, Any]) -> dict[str,Any]:
    name = data.pop('name')
    params = {
        "executionHash": name,
        "notificationHash": data["id"],
        "timeout": data.pop("timeout", None),
        "orderDetails": data
    }
    return params

def oneclick_put_params(data:dict[str, Any]) -> dict[str, Any]:
    name = data.pop('name')
    params = {
        "executionHash": name,
        "notificationHash": data["notificationId"],
        "type": data['type'],
        "status": data['status'],
        "msg": data['msg']
    }
    return params

class OneClickServiceAPI:
    """
        httpx based api class for one-click service.
    """
    def __init__(self, server_url:str,
                basic_auth:bool=True,
                api_username:str|None=None,
                api_password:str|None=None,
                proxies:dict={},
                agent:str="oneclick",
                verify_cert:bool=True):
        self._server_url = server_url
        self._api_username = api_username
        self._api_password = api_password
        self._proxies = proxies
        self._agent = agent
        self._verify_cert = verify_cert
        self._auth_required = basic_auth
        self._basic_auth = basic_auth

    def request_post(self, data:dict[str, Any]) -> dict[str, Any]|str|None:
        params = oneclick_post_params(data)
        success, code, res = self._requests(
                self._server_url,
                parameters=params,
                verb="post")
        if success:
            return res
        else:
            raise_exception(None, str(res), APIError,
                            module="oneclick", func="request_post",
                            handling=ExceptionHandling.WARN)

    def request_put(self, data:dict[str,Any], throwOnly5xx:bool=False) -> dict[str, Any]|str|None:
        params = oneclick_put_params(data)
        success, code, res = self._requests(
                self._server_url,
                parameters=params,
                verb="put")

        if throwOnly5xx and code < 500:
            return
        elif success:
            return res
        else:
            raise_exception(None, str(res), APIError,
                            module="oneclick", func="request_put",
                            handling=ExceptionHandling.WARN)

    def request_get(self, id:str) -> dict[str, Any]|str|None:
        success, code, res = self._requests(
                urljoin(self._server_url, id),
                verb="get")
        if success:
            return res
        else:
            raise_exception(None, str(res), APIError,
                            module="oneclick", func="request_get",
                            handling=ExceptionHandling.WARN)

    @retry_on_error(retry=3, sleep=2, inc=2)
    def _requests(self, endpoint:str, headers:dict[str,str]={}, parameters:dict[str,Any]|None=None, 
                  verb:Literal['get','post','put','patch','delete']="get"
                  ) -> tuple[bool, int, dict[str,Any]|str]:
        """ handle http requests."""
        request_headers = {
                'User-Agent': self._agent,
                'Accept': 'application/json',
                "Content-Type" : "application/json",
                }
        
        DEPLOYMENT_AUTH = os_getenv('BLUESHIFT_DEPLOYMENT_AUTH', '')
        if DEPLOYMENT_AUTH:
            request_headers['X-Authorization'] = DEPLOYMENT_AUTH
        
        request_headers = {**request_headers, **headers}
        
        if self._basic_auth and not (self._api_password and self._api_username):
            msg = "authentication missing"
            handling = ExceptionHandling.TERMINATE
            raise APIError(msg=msg, handling=handling)
        
        # Prepare request parameters
        request_kwargs = {
            'headers': request_headers,
            'timeout': httpx.Timeout(10.0)
        }
        
        # Add authentication if required
        if self._basic_auth:
            request_kwargs['auth'] = (self._api_username, self._api_password)
        
        # Add proxies if configured
        if self._proxies:
            request_kwargs['proxies'] = self._proxies
        
        response = None
        try:
            with httpx.Client(verify=self._verify_cert) as client:
                if verb.lower() == "post":
                    response = client.post(endpoint, json=parameters, **request_kwargs)
                elif verb.lower() == "put":
                    response = client.put(endpoint, json=parameters, **request_kwargs)
                elif verb.lower() == "get":
                    response = client.get(endpoint, params=parameters, **request_kwargs)
                elif verb.lower() == "patch":
                    response = client.patch(endpoint, params=parameters, **request_kwargs)
                elif verb.lower() == "delete":
                    response = client.delete(endpoint, params=parameters, **request_kwargs)
                else:
                    msg = f"Invalid HTTP request type {verb}"
                    raise ValidationError(msg) 
                
                response.raise_for_status()  # Raise exception for bad status codes
        except httpx.HTTPStatusError as e:
            # Handle HTTP status errors (4xx, 5xx)
            response = e.response
            status = response.status_code
            success = False
            try:
                data = response.json()
                return success, status, data
            except Exception:
                err = response.text
                return success, status, err
        except httpx.RequestError as e:
            # Handle network/connection errors
            msg = f"{type(e)}:{str(e)}"
            if verb.lower() == "post":
                # we do not want to retry "unsafe" post!
                raise_exception(None, msg, APIError,
                                module="oneclick", func="request_post",
                                handling=ExceptionHandling.WARN)
                return # type: ignore -> this is never reached
            else:
                # let's give it a retry!
                raise RetryError(msg)
        except Exception as e:
            if isinstance(e, TerminationError):
                raise
            msg = f"{type(e)}:{str(e)}"
            if verb.lower() == "post":
                # we do not want to retry "unsafe" post!
                raise_exception(None, msg, APIError,
                                module="oneclick", func="request_post",
                                handling=ExceptionHandling.WARN)
                return # type: ignore -> this is never reached
            else:
                # let's give it a retry!
                raise RetryError(msg)

        status = response.status_code
        success = True if status < 300 else False
        
        # Check content type
        if "content-type" not in response.headers or "json" not in response.headers["content-type"]:
            raise_exception(None, f"illegal response type from server. {response}", APIError,
                            module="oneclick", func="-request",
                            handling=ExceptionHandling.WARN)
        
        data:dict[str,Any] = {}
        err:str = ''
        try:
            data = response.json()
        except Exception as e:
            if isinstance(e, TerminationError):
                raise
            if not success:
                err = response.text
            return success, status, err
        else:
            return success, status, data

