import logging
import os
import re
import zipfile
from pathlib import Path
from typing import Union

import pandas as pd
import psutil
from bs4 import BeautifulSoup, Tag

from src.error import HTMLElementNotFound


def kill_all_processes(proc_name: str) -> None:
    for proc in psutil.process_iter():
        try:
            if proc_name in proc.name():
                proc.terminate()
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            continue


def get_from_env(key: str) -> str:
    value = os.getenv(key)

    if value is None:
        error_msg = f"{key} not set in the environment variables"
        logging.error(error_msg)
        raise EnvironmentError(error_msg)
    return value


def select_one(root: Union[BeautifulSoup, Tag], selector: str) -> Tag:
    match = root.select(selector)
    if not match:
        warning_msg = f"{selector=} was not found..."
        logging.warning(warning_msg)
        raise HTMLElementNotFound(warning_msg)

    result = match[0]
    return result


def safe_extract(archive_path: Path, documents_folder: Path) -> None:
    try:
        archive = zipfile.ZipFile(archive_path, "r")
    except zipfile.BadZipfile as err:
        logging.error(f"{archive_path.as_posix()!r} - {err!r}")
        return

    with archive:
        for file in archive.namelist():
            file_path = Path(file)
            file_name = file_path.name
            if not file_name.endswith(".docx") and not file_name.endswith(".DOCX"):
                continue

            normalized_file_name = file_name.encode("ibm437").decode("cp866")
            normalized_file_name = re.sub(r"\s+", " ", normalized_file_name)
            normalized_file_name = normalized_file_name.replace("?", "").strip()
            extract_path = documents_folder / normalized_file_name

            try:
                with archive.open(file) as source, open(extract_path, "wb") as dest:
                    dest.write(source.read())
            except OSError as err:
                logging.error(err)
                raise err


def normalize_text(text: str) -> str:
    new_text = text.lower().strip().split("/")[0]
    new_text = new_text.replace("i", "і")
    new_text = re.sub(r"\s{2,}", " ", new_text)
    new_text = re.sub(r"[^\w\s/№]", "", new_text)
    return new_text


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
