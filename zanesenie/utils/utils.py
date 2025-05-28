import logging
import os
import secrets
import string
from datetime import datetime, timedelta
from pathlib import Path
from typing import Generator, cast

import pandas as pd
import psutil
from pandas import Series

from zanesenie.data import Job, Order


def iterate_datetime(
    start: datetime, end: datetime, step: timedelta | None = None
) -> Generator[datetime, None, None]:
    if step is None:
        step = timedelta(days=1)

    current = start
    while current <= end:
        yield current
        current += step


def kill_all_processes(proc_name: str) -> None:
    for proc in psutil.process_iter():
        try:
            if proc_name in proc.name():
                proc.terminate()
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            continue


def construct_screenshot_path(
    screenshot_folder: Path,
    fullname: str,
    order_number: str,
    date: str,
    img_format: str = ".jpeg",
) -> str:
    fullname = fullname.replace(" ", "_")
    order_number = order_number.replace(" ", "")
    screenshot_path = screenshot_folder / f"{fullname}_{order_number}_{date}"
    screenshot_path = screenshot_path.with_suffix(img_format)
    return screenshot_path.as_posix()


def df_construct_screenshot_path(
    employee_fullname: pd.Series,
    order_number: pd.Series,
    today: str,
    screenshot_folder: str,
    img_format: str = ".jpeg",
) -> pd.Series:
    df = pd.DataFrame()
    df.loc[:, "employee_fullname"] = employee_fullname.str.replace(" ", "_")
    df.loc[:, "order_number"] = order_number.str.replace(" ", "")
    df.loc[:, "screenshot_name"] = (
        df["employee_fullname"]
        + "_"
        + df["order_number"]
        + "_"
        + today
        + img_format
    )
    df.loc[:, "screenshot_path"] = (
        screenshot_folder + os.sep + df["screenshot_name"]
    )
    return df["screenshot_path"]


def create_report(report_file_path: Path) -> None:
    if report_file_path.exists():
        return

    df = pd.DataFrame(
        {
            "Дата": [],
            "Сотрудник": [],
            "Операция": [],
            "Номер приказа": [],
            "Статус": [],
        }
    )
    df.to_excel(report_file_path, index=False)


def update_report(
    order: Order,
    job: Job,
    operation: str,
    status: str,
) -> None:
    df = pd.read_excel(
        job.registry.report_path,
        dtype={
            "Дата": str,
            "Сотрудник": str,
            "Операция": str,
            "Номер приказа": str,
            "Статус": str,
        },
        engine="calamine",
    )

    match_filter = cast(
        Series,
        (
            (df["Дата"] == job.t_range.end.short)
            & (df["Сотрудник"] == order.employee_fullname)
            & (df["Операция"] == operation)
            & (df["Номер приказа"] == order.order_number)
        ),
    )

    if not match_filter.any():
        new_row = {
            "Дата": job.t_range.end.short,
            "Сотрудник": order.employee_fullname,
            "Операция": operation,
            "Номер приказа": order.order_number,
            "Статус": status,
        }
        df.loc[len(df)] = new_row
        df.to_excel(job.registry.report_path, index=False)


def generate_password(
    length: int = 12,
    min_digits: int = 1,
    min_low_letters: int = 1,
    min_up_letters: int = 1,
    min_punctuations: int = 1,
) -> str:
    def random_chars(char_set: str, min_count: int) -> str:
        return "".join(secrets.choice(char_set) for _ in range(min_count))

    digits = random_chars(string.digits, min_digits)
    low_letters = random_chars(string.ascii_lowercase, min_low_letters)
    up_letters = random_chars(string.ascii_uppercase, min_up_letters)
    punctuations = random_chars(string.punctuation, min_punctuations)

    required_length = (
        min_digits + min_low_letters + min_up_letters + min_punctuations
    )
    remaining_length = max(0, length - required_length)
    all_characters = string.ascii_letters + string.digits
    remaining_chars = random_chars(all_characters, remaining_length)

    password_chars = list(
        digits + low_letters + up_letters + punctuations + remaining_chars
    )
    secrets.SystemRandom().shuffle(password_chars)

    password = "".join(password_chars)
    return password
