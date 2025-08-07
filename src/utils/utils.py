from __future__ import annotations

import io
import logging
import os
import re
import shutil
import zipfile
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING
from urllib.parse import urljoin

import httpx
import pandas as pd
import psutil
import pytz

if TYPE_CHECKING:
    from typing import Any, BinaryIO, Literal
    from pathlib import Path
    from collections.abc import Callable

logger = logging.getLogger("DAMU")


class TelegramAPI:
    def __init__(self, process_name: Literal["sve", "zan"]) -> None:
        self.client = httpx.Client()
        self.token, self.chat_id = os.environ["TOKEN"], os.environ["CHAT_ID"]
        self.api_url = f"https://api.telegram.org/bot{self.token}/"

        self.pending_messages: list[str] = []

        self.process_name = process_name

    def reload_session(self) -> None:
        self.client.close()
        self.client = httpx.Client()

    def send_message(
        self,
        message: str | None = None,
        use_session: bool = True,
        use_md: bool = False,
    ) -> bool:
        send_data: dict[str, str | None] = {"chat_id": self.chat_id}

        if use_md:
            send_data["parse_mode"] = "MarkdownV2"

        pending_message = "\n".join(self.pending_messages)
        if pending_message:
            message = f"{pending_message}\n{message}"

        url = urljoin(self.api_url, "sendMessage")
        send_data["text"] = f"{self.process_name.upper()} - {message}"

        status_code = 0

        try:
            if use_session:
                response = self.client.post(url, data=send_data, timeout=10)
            else:
                response = httpx.post(url, data=send_data, timeout=10)

            data = "" if not hasattr(response, "json") else response.json()
            status_code = response.status_code
            logger.debug(f"{status_code=}, {data=}")
            response.raise_for_status()

            if status_code == 200:
                self.pending_messages = []
                return True

            return False
        except httpx.HTTPError as err:
            if status_code == 429 and message:
                self.pending_messages.append(message)

            logger.exception(err)
            return False

    def send_with_retry(self, message: str) -> bool:
        retry = 0
        while retry < 5:
            try:
                use_session = retry < 5
                success = self.send_message(message, use_session)
                return success
            except httpx.HTTPError as e:
                self.reload_session()
                logger.exception(e)
                logger.warning(f"{e} intercepted. Retry {retry + 1}/10")
                retry += 1

        return False


def kill_all_processes(proc_name: str) -> None:
    for proc in psutil.process_iter():
        try:
            if proc_name in proc.name():
                proc.terminate()
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            continue


def normalize_value(value: str) -> str:
    encoding_pairs = [("ibm437", "cp866"), ("cp65001", "ibm866")]

    last_exception = None
    for src_enc, dest_enc in encoding_pairs:
        try:
            return value.encode(src_enc).decode(dest_enc)
        except UnicodeError as err:
            last_exception = err

    print(f"Failed to normalize: {value!r}")
    raise ValueError from last_exception


def safe_extract(
    archive_path: Path,
    documents_folder: Path,
    check_format: bool = True,
    normalize_name: bool = True,
) -> None:
    try:
        archive = zipfile.ZipFile(archive_path, "r")
    except zipfile.BadZipfile as err:
        logger.error(f"{archive_path.as_posix()!r} - {err!r}")
        return

    with archive:
        for file in archive.namelist():
            file_name = os.path.basename(file)

            if check_format and file_name.lower().endswith("docx"):
                continue

            if normalize_name:
                normalized_file_name = normalize_value(file_name)
                normalized_file_name = re.sub(r"\s+", " ", normalized_file_name)
                normalized_file_name = normalized_file_name.replace(
                    "?", ""
                ).strip()
                extract_path = documents_folder / normalized_file_name
            else:
                extract_path = documents_folder / file_name

            if extract_path.exists():
                continue

            try:
                with (
                    archive.open(file) as source,
                    open(extract_path, "wb") as dest,
                ):
                    dest.write(source.read())
            except OSError as err:
                logger.error(err)
                raise err


def compare(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    if (
        (df1.empty and df2.empty)
        or (df1.empty and not df2.empty)
        or (df2.empty and not df1.empty)
    ):
        return True

    if len(df1) != len(df2):
        return False

    return next(
        (
            False
            for idx in df1.index[~df1["total"]]
            if not df1.loc[idx].equals(df2.loc[idx])
        ),
        True,
    )


def save_to_bytes(write_func: Callable[[BinaryIO], Any]) -> bytes:
    with io.BytesIO() as buffer_io:
        write_func(buffer_io)
        data = buffer_io.getvalue()

    return data


def days360(
    start_date: date | datetime | pd.Timestamp,
    end_date: date | datetime | pd.Timestamp,
    method: bool = False,
) -> int:
    d1, m1, y1 = start_date.day, start_date.month, start_date.year
    d2, m2, y2 = end_date.day, end_date.month, end_date.year

    if method:
        if d1 == 31:
            d1 = 30
        if d2 == 31:
            d2 = 30
    else:
        if d1 == 31:
            d1 = 30
        if d2 == 31 and d1 == 30:
            d2 = 30

    return (y2 - y1) * 360 + (m2 - m1) * 30 + (d2 - d1)


def humanize_timedelta(seconds: int | float) -> str:
    td = timedelta(seconds=int(seconds))
    return str(td)


def is_tomorrow(tomorrow: date) -> bool:
    return datetime.now(pytz.timezone("Asia/Almaty")).date() >= tomorrow


def delete_leftovers(
    download_folder: Path, today: date, max_days: int = 14
) -> None:
    for folder in download_folder.parent.iterdir():
        if not folder.is_dir():
            continue

        if not any(folder.iterdir()):
            logger.info(f"Deleting empty {folder.name!r} folder")
            folder.rmdir()
            continue

        try:
            run_date = date.fromisoformat(folder.name)
        except ValueError:
            continue
        delta = (today - run_date).days
        if delta <= max_days:
            continue

        logger.info(f"Deleting {folder.name!r} folder. {delta} > {max_days}")
        shutil.rmtree(folder)


# def dump_data(
#     db: DatabaseManager,
#     bank_mapping: dict[str, dict[str, str]],
#     contract_cls: object(),
# ) -> None:
#     temp_folder = Path("temp2")
#     temp_folder.mkdir(exist_ok=True)
#     contracts = list(contract_cls.iter_contracts(db, bank_mapping))
#     for c in contracts:
#         assert isinstance(c.macro_path, Path)
#         assert isinstance(c.document_pdf_path, Path)
#         assert isinstance(c.protocol_pdf_path, Path)
#
#         c_folder = temp_folder / c.contract_id
#         c_folder.mkdir(exist_ok=True)
#
#         shutil.copyfile(c.document_path, c_folder / c.document_path.name)
#         shutil.copyfile(c.macro_path, c_folder / c.macro_path.name)
#         shutil.copyfile(
#             c.document_pdf_path, c_folder / c.document_pdf_path.name
#         )
#         shutil.copyfile(
#             c.protocol_pdf_path, c_folder / c.protocol_pdf_path.name
#         )
#
#     contracts = list(contract_cls.iter_contracts(db, bank_mapping))
#     df = pd.DataFrame(contracts)
#
#     df.drop(
#         [
#             "contract_id",
#             "macro_path",
#             "document_path",
#             "document_pdf_path",
#             "protocol_pdf_path",
#             "category",
#         ],
#         axis=1,
#         inplace=True,
#     )
#
#     df["protocol_date"] = pd.to_datetime(df["protocol_date"], format="%d%m%Y")
#     df["vypiska_date"] = pd.to_datetime(df["vypiska_date"], format="%d%m%Y")
#     df["decision_date"] = pd.to_datetime(df["decision_date"], format="%d%m%Y")
#     df["ds_date"] = pd.to_datetime(df["ds_date"], format="%d%m%Y")
#     df["dbz_date"] = pd.to_datetime(df["dbz_date"], format="%d%m%Y")
#     df["start_date"] = pd.to_datetime(df["start_date"], format="%d%m%Y")
#     df["end_date"] = pd.to_datetime(df["end_date"], format="%d%m%Y")
#
#     df.rename(
#         columns={
#             "contragent": "Контрагент",
#             "project": "Название проекта",
#             "bank": "Банк/Лизинг",
#             "credit_purpose": "Цель кредитования",
#             "repayment_procedure": "Вид погашения",
#             "loan_amount": "Сумма кредита",
#             "subsid_amount": "Сумма субсидирования",
#             "investment_amount": "Сумма инвестирования",
#             "pos_amount": "Сумма на ПОС",
#             "protocol_date": "Дата протокола",
#             "vypiska_date": "Дата выписки",
#             "decision_date": "Дата решения",
#             "settlement_date": "Дата расчета",
#             "iban": "IBAN",
#             "ds_id": "№ДС",
#             "ds_date": "Дата ДС",
#             "dbz_id": "№ДБЗ",
#             "dbz_date": "Дата ДБЗ",
#             "start_date": "Дата начала",
#             "end_date": "Дата окончания",
#             "protocol_id": "Номер протокола",
#             "sed_number": "Номер СЭД",
#         }
#     )
#
#     df.to_excel("Отчет.xlsx", index=False)
