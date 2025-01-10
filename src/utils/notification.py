import io
import logging
import urllib.parse
from typing import cast

import PIL.Image as Image
import PIL.ImageGrab as ImageGrab
import requests
import requests.adapters
from requests import HTTPError
from requests.exceptions import SSLError

from src.error import retry
from src.utils.custom_list import CustomList
from src.utils.utils import get_from_env


class Messages(CustomList[str]):
    pass


class TelegramAPI:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.mount("http://", requests.adapters.HTTPAdapter(max_retries=5))
        self.token, self.chat_id = get_from_env("TOKEN"), get_from_env("CHAT_ID")
        self.api_url = f"https://api.telegram.org/bot{self.token}/"

        self.pending_messages: Messages = Messages()

    def reload_session(self) -> None:
        self.session = requests.Session()
        self.session.mount("http://", requests.adapters.HTTPAdapter(max_retries=5))

    @retry(
        exceptions=(
            requests.exceptions.ConnectionError,
            requests.exceptions.SSLError,
            requests.exceptions.HTTPError,
        ),
        tries=5,
        delay=5,
        backoff=5,
    )
    def send_message(
        self,
        message: str | None = None,
        media: Image.Image | None = None,
        use_session: bool = True,
        use_md: bool = False,
    ) -> bool:
        send_data: dict[str, str | None] = {"chat_id": self.chat_id}

        if use_md:
            send_data["parse_mode"] = "MarkdownV2"

        files = None

        pending_message = str(self.pending_messages)
        if pending_message:
            message = f"{pending_message}\n{message}"

        if media is None:
            url = urllib.parse.urljoin(self.api_url, "sendMessage")
            send_data["text"] = message
        else:
            url = urllib.parse.urljoin(self.api_url, "sendPhoto")

            image_stream = io.BytesIO()
            if media is None:
                media = ImageGrab.grab()
            media.save(image_stream, format="JPEG", optimize=True)
            image_stream.seek(0)
            raw_io_base_stream = cast(io.RawIOBase, image_stream)
            buffered_reader = io.BufferedReader(raw_io_base_stream)

            files = {"photo": buffered_reader}

            send_data["caption"] = message

        status_code = 0

        try:
            if use_session:
                response = self.session.post(
                    url, data=send_data, files=files, verify=False
                )
            else:
                response = requests.post(url, data=send_data, files=files, verify=False)

            data = "" if not hasattr(response, "json") else response.json()
            status_code = response.status_code
            logging.info(f"{status_code=}")
            logging.info(f"{data=}")
            response.raise_for_status()

            if status_code == 200:
                self.pending_messages.clear()
                return True

            return False
        except (SSLError, HTTPError) as err:
            if status_code == 429:
                self.pending_messages.append(message)

            logging.exception(err)
            return False

    def send_image(
        self, media: Image.Image | None = None, use_session: bool = True
    ) -> bool:
        try:
            send_data = {"chat_id": self.chat_id}

            url = urllib.parse.urljoin(self.api_url, "sendPhoto")

            image_stream = io.BytesIO()
            if media is None:
                media = ImageGrab.grab()
            media.save(image_stream, format="JPEG", optimize=True)
            image_stream.seek(0)
            raw_io_base_stream = cast(io.RawIOBase, image_stream)
            buffered_reader = io.BufferedReader(raw_io_base_stream)

            files = {"photo": buffered_reader}

            if use_session:
                response = self.session.post(url, data=send_data, files=files)
            else:
                response = requests.post(url, data=send_data, files=files)

            data = "" if not hasattr(response, "json") else response.json()
            logging.info(f"{response.status_code=}")
            logging.info(f"{data=}")
            response.raise_for_status()
            return response.status_code == 200
        except requests.exceptions.ConnectionError as exc:
            logging.exception(exc)
            return False
