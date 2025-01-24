import logging
from pathlib import Path
from types import TracebackType
from typing import Dict, Literal, Optional, Type
from urllib.parse import urljoin

from httpx import Cookies, Client, AsyncClient, Response, RequestError

from src.error import async_retry


class RequestHandler:
    def __init__(
        self, user: str, password: str, base_url: str, download_folder: Path
    ) -> None:
        self.user = user
        self.password = password

        self.base_url = base_url
        self.download_folder = download_folder

        self.cookies = Cookies()
        self.client = Client()
        self.async_client = AsyncClient()

        self.headers = dict()
        self.client.headers = dict()
        self.async_client.headers = dict()

    def update_cookies(self, cookies: Cookies) -> None:
        self.cookies.update(cookies)
        self.client.cookies.update(cookies)
        self.async_client.cookies.update(cookies)

    def set_cookie(self, name: str, value: str) -> None:
        self.cookies.set(name, value)
        self.client.cookies.set(name, value)
        self.async_client.cookies.set(name, value)

    def clear_cookies(self) -> None:
        self.cookies.clear()
        self.client.cookies.clear()
        self.async_client.cookies.clear()

    def update_headers(self, headers: Dict[str, str]) -> None:
        self.headers.update(headers)
        self.client.headers.update(headers)
        self.async_client.headers.update(headers)

    def set_header(self, name: str, value: str) -> None:
        self.headers[name] = value
        self.client.headers[name] = value
        self.async_client.headers[name] = value

    def _handle_response(
        self,
        response: Response,
        method: str,
        path: str,
        update_cookies: bool,
    ) -> Optional[Response]:
        if response.status_code != 200:
            logging.warning(
                f"FAILURE - {method.upper()} {response.status_code} to {path!r}"
            )
            return None

        if update_cookies:
            logging.debug(f"Updating cookies with response from {path!r}")
            self.update_cookies(response.cookies)

        logging.debug(f"{method.upper()} {response.status_code} to {path!r}")
        return response

    def request(
        self,
        method: Literal["get", "post"],
        path: str,
        headers: Optional[Dict[str, str]] = None,
        json: Optional[Dict[str, str]] = None,
        data: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, str]] = None,
        update_cookies: bool = False,
        timeout: int = 60,
    ) -> Optional[Response]:
        url = urljoin(self.base_url, path)
        try:
            response = self.client.request(
                method=method,
                url=url,
                json=json,
                data=data,
                headers=headers,
                params=params,
                timeout=timeout,
            )
        except RequestError as e:
            logging.error(f"FAILURE - Request to {path!r} failed: {e}")
            return None

        return self._handle_response(response, method, path, update_cookies)

    @async_retry(exceptions=(RequestError,), tries=5, delay=5, backoff=5)
    async def async_request(
        self,
        method: Literal["get", "post"],
        path: str,
        headers: Optional[Dict[str, str]] = None,
        json: Optional[Dict[str, str]] = None,
        data: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, str]] = None,
        update_cookies: bool = False,
        timeout: int = 60,
    ) -> Optional[Response]:
        url = urljoin(self.base_url, path)
        try:
            response = await self.async_client.request(
                method=method,
                url=url,
                json=json,
                data=data,
                headers=headers,
                params=params,
                timeout=timeout,
            )
        except RequestError as e:
            logging.error(f"FAILURE - Request to {path!r} failed: {e}")
            return None

        return self._handle_response(response, method, path, update_cookies)

    def __enter__(self) -> "RequestHandler":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ):
        if exc_val is not None or exc_type is not None or exc_tb is not None:
            pass
        self.client.close()
