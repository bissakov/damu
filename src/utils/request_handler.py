import logging
from pathlib import Path
from types import TracebackType
from typing import Dict, Literal, Optional, Type
from urllib.parse import urljoin

import requests
from requests.cookies import RequestsCookieJar


class RequestHandler:
    def __init__(
        self, user: str, password: str, base_url: str, download_folder: Path
    ) -> None:
        self.user = user
        self.password = password
        self.session = requests.Session()
        self.session.cookies = RequestsCookieJar()
        self.base_url = base_url
        self.download_root_folder = download_folder

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
    ) -> Optional[requests.Response]:
        url = urljoin(self.base_url, path)
        try:
            response = self.session.request(
                method=method,
                url=url,
                json=json,
                data=data,
                headers=headers,
                params=params,
                timeout=timeout,
            )
        except requests.RequestException as e:
            logging.error(f"FAILURE - Request to {path!r} failed: {e}")
            return None

        status_code = response.status_code
        if status_code != 200:
            logging.warning(f"FAILURE - {method.upper()} {status_code} to {path!r}")
            return None

        if update_cookies:
            logging.debug(f"Updating cookies with response from {path!r}")
            self.session.cookies.update(response.cookies)

        logging.debug(f"SUCCESS - {method.upper()} {status_code} to {path!r}")

        if not response:
            logging.warning(
                f"FAILURE - {response=} {method.upper()} {status_code} to {path!r}"
            )
            return None
        return response

    def __enter__(self) -> "EDO":
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ):
        if exc_val is not None or exc_type is not None or exc_tb is not None:
            pass
        self.session.close()
