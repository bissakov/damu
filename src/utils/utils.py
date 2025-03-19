import io
import logging
import re
import zipfile
import zlib
from pathlib import Path
from typing import Any, BinaryIO, Callable, Union

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


def select_one(root: Union[BeautifulSoup, Tag], selector: str) -> Tag:
    match = root.select(selector)
    if not match:
        warning_msg = f"{selector=} was not found..."
        logging.warning(warning_msg)
        raise HTMLElementNotFound(warning_msg)

    result = match[0]
    return result


def normalize_value(value: str):
    encoding_pairs = [
        ("ibm437", "cp866"),
        ("cp65001", "ibm866"),
    ]

    last_exception = None
    for src_enc, dest_enc in encoding_pairs:
        try:
            return value.encode(src_enc).decode(dest_enc)
        except UnicodeError as err:
            last_exception = err

    print(f"Failed to normalize: {value!r}")
    raise last_exception


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

            if file_name.lower().endswith("docx"):
                continue

            normalized_file_name = normalize_value(file_name)
            normalized_file_name = re.sub(r"\s+", " ", normalized_file_name)
            normalized_file_name = normalized_file_name.replace("?", "").strip()
            extract_path = documents_folder / normalized_file_name

            if extract_path.exists():
                continue

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


def save_to_bytes(
    write_func: Callable[[BinaryIO], Any], compress: bool = True
) -> bytes:
    with io.BytesIO() as buffer_io:
        write_func(buffer_io)
        data = buffer_io.getvalue()

    return zlib.compress(data) if compress else data
