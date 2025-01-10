import logging
import os
import re
import zipfile
from pathlib import Path
from typing import Union

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
        warning_msg = f"WARNING - {selector=} was not found..."
        logging.warning(warning_msg)
        raise HTMLElementNotFound(warning_msg)

    result = match[0]
    return result


def safe_extract(archive_path: Path, extract_folder: Path) -> None:
    with zipfile.ZipFile(archive_path, "r") as archive:
        for file in archive.namelist():
            file_path = Path(file)
            file_name = file_path.name
            if not file_name.endswith(".docx"):
                continue

            normalized_file_name = file_name.encode("ibm437").decode("cp866")
            normalized_file_name = re.sub(r"\s+", " ", normalized_file_name)
            normalized_file_name = normalized_file_name.replace("?", "").strip()
            extract_path = extract_folder / normalized_file_name

            try:
                with archive.open(file) as source, open(extract_path, "wb") as dest:
                    dest.write(source.read())
            except OSError as err:
                logging.error(err)
                raise err
