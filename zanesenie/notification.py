import email.utils
import io
import logging
import os
import shutil
import smtplib
import traceback
import urllib.parse
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from functools import wraps
from pathlib import Path
from typing import Callable, cast

import PIL.Image as Image
import PIL.ImageGrab as ImageGrab
import requests
import requests.adapters
from requests import HTTPError
from requests.exceptions import SSLError

from zanesenie.data import Job


class TelegramAPI:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.mount(
            "http://", requests.adapters.HTTPAdapter(max_retries=5)
        )
        self.token, self.chat_id = os.environ["TOKEN"], os.environ["CHAT_ID"]
        self.api_url = f"https://api.telegram.org/bot{self.token}/"

        self.pending_messages: list[str] = []

    def reload_session(self) -> None:
        self.session = requests.Session()
        self.session.mount(
            "http://", requests.adapters.HTTPAdapter(max_retries=5)
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

        pending_message = "\n".join(self.pending_messages)
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
                response = requests.post(
                    url, data=send_data, files=files, verify=False
                )

            data = "" if not hasattr(response, "json") else response.json()
            status_code = response.status_code
            logging.info(f"{status_code=}")
            logging.info(f"{data=}")
            response.raise_for_status()

            if status_code == 200:
                self.pending_messages = []
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

    def send_with_retry(
        self,
        message: str,
    ) -> bool:
        retry = 0
        while retry < 5:
            try:
                use_session = retry < 5
                success = self.send_message(message, use_session)
                return success
            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.SSLError,
                requests.exceptions.HTTPError,
            ) as e:
                self.reload_session()
                logging.exception(e)
                logging.warning(f"{e} intercepted. Retry {retry + 1}/10")
                retry += 1

        return False


def handle_error(func: Callable[..., any]) -> Callable[..., any]:
    @wraps(func)
    def wrapper(*args, **kwargs) -> any:
        bot: TelegramAPI | None = kwargs.get("bot")

        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt as error:
            raise error
        except (Exception, BaseException) as error:
            logging.exception(error)
            error_traceback = traceback.format_exc()

            developer = os.getenv("DEVELOPER")
            if developer:
                error_traceback = f"@{developer} {error_traceback}"

            if bot:
                bot.send_message(
                    message=error_traceback, media=ImageGrab.grab()
                )
            raise error

    return wrapper


def attach_file(
    msg: MIMEMultipart, file_path: Path, file_name: str | None = None
) -> None:
    if not file_name:
        file_name = file_path.name
    with open(file_path, "rb") as f:
        file_part = MIMEApplication(f.read())
    file_part.add_header(
        "Content-Disposition", "attachment", filename=file_name
    )
    msg.attach(file_part)


def send_mail(job: Job, is_empty: bool) -> bool:
    mail_info = job.mail_info
    t_range = job.t_range
    job_name = job.job_type.name

    recipients_lst: list[str] = list(
        filter(bool, mail_info.recipients.split(";"))
    )

    msg = MIMEMultipart()
    msg["From"] = mail_info.sender
    msg["To"] = mail_info.recipients
    msg["Date"] = email.utils.formatdate(localtime=True)
    msg["Subject"] = mail_info.subject

    body = mail_info.subject

    if is_empty:
        body += f"\n\nНет приказов на период с {t_range.start.short} по {t_range.end.short}."
    else:
        body += f"\n\nПриказы на период с {t_range.start.short} по {t_range.end.short}."

        attach_file(msg, mail_info.report_path)

        archive_name = f"screenshots_{t_range.end.short}"
        original_zip_path = shutil.make_archive(
            base_name=archive_name,
            format="zip",
            root_dir=mail_info.screenshot_folder,
        )
        archive_name = f"{archive_name}.zip"
        screenshot_archive_path = mail_info.screenshot_folder / archive_name
        shutil.move(original_zip_path, screenshot_archive_path)

        attach_file(msg, screenshot_archive_path, archive_name)

    logging.info(f"{body=}")
    msg.attach(MIMEText(body, "html", "utf-8"))

    try:
        with smtplib.SMTP(mail_info.server, 25) as smtp:
            response = smtp.sendmail(
                mail_info.sender, recipients_lst, msg.as_string()
            )
            if response:
                logging.error(
                    f"{job_name} - Failed to send email to the following recipients:"
                )
                for recipient, error in response.items():
                    logging.error(f"{recipient}: {error}")
                return False
            else:
                logging.info(f"{job_name} - Email sent successfully.")
                return True
    except smtplib.SMTPException as e:
        logging.error(f"{job_name} - Failed to send email: {e}")
        return False
