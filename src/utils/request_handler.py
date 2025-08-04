from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from urllib.parse import urljoin

from httpx import Client, Cookies, RequestError

if TYPE_CHECKING:
    from pathlib import Path
    from types import TracebackType
    from typing import Literal, Type, TYPE_CHECKING
    from httpx import Response, Headers


logger = logging.getLogger("DAMU")


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

        self.headers: dict[str, str] = dict()
        self.client.headers = dict()

    def update_cookies(self, cookies: Cookies) -> None:
        self.cookies.update(cookies)
        self.client.cookies.update(cookies)

    def set_cookie(self, name: str, value: str) -> None:
        self.cookies.set(name, value)
        self.client.cookies.set(name, value)

    def clear_cookies(self) -> None:
        self.cookies.clear()
        self.client.cookies.clear()

    def update_headers(self, headers: dict[str, str]) -> None:
        self.headers.update(headers)
        self.client.headers.update(headers)

    def set_header(self, name: str, value: str) -> None:
        self.headers[name] = value
        self.client.headers[name] = value

    def _handle_response(
        self, response: Response, method: str, path: str, update_cookies: bool
    ) -> Response | None:
        if response.is_error:
            logger.warning(
                f"FAILURE - {method.upper()} {response.status_code} to {path!r}"
            )
            return None

        if update_cookies:
            logger.debug(f"Updating cookies with response from {path!r}")
            self.update_cookies(response.cookies)

        logger.debug(f"{method.upper()} {response.status_code} to {path!r}")
        return response

    def request(
        self,
        method: Literal["get", "post"],
        path: str,
        headers: dict[str, str] | Headers | None = None,
        json: dict[str, int | float | str] | None = None,
        data: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        update_cookies: bool = False,
        timeout: int = 60,
    ) -> Response | None:
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
        except (RequestError, RuntimeError) as e:
            logger.error(f"FAILURE - Request to {path!r} failed: {e}")
            return None

        return self._handle_response(response, method, path, update_cookies)

    def __enter__(self) -> RequestHandler:
        self.cookies = Cookies()
        self.client = Client()

        self.headers = dict()
        self.client.headers = dict()
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if exc_val is not None or exc_type is not None or exc_tb is not None:
            pass
        self.client.close()
