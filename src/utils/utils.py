import io
import logging
import re
import zipfile
from datetime import date, datetime
from pathlib import Path
from typing import Any, BinaryIO, Callable, Dict, Union

import pandas as pd


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


def compare(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    if (df1.empty and df2.empty) or (df1.empty and not df2.empty) or (df2.empty and not df1.empty):
        return True

    if len(df1) != len(df2):
        return False

    return next(
        (False for idx in df1.index[~df1["total"]] if not df1.loc[idx].equals(df2.loc[idx])),
        True,
    )


def save_to_bytes(write_func: Callable[[BinaryIO], Any]) -> bytes:
    with io.BytesIO() as buffer_io:
        write_func(buffer_io)
        data = buffer_io.getvalue()

    return data


def days360(
    start_date: Union[date, datetime, pd.Timestamp],
    end_date: Union[date, datetime, pd.Timestamp],
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


def get_column_mapping() -> Dict[str, str]:
    return {
        "debt_repayment_date": "Дата погашения основного долга",
        "principal_debt_balance": "Сумма остатка основного долга",
        "principal_debt_repayment_amount": "Сумма погашения основного долга",
        "agency_fee_amount": "Сумма вознаграждения, оплачиваемая финансовым агентством",
        "recipient_fee_amount": "Сумма вознаграждения, оплачиваемая Получателем",
        "total_accrued_fee_amount": "Итого сумма начисленного вознаграждения",
        "day_count": "Кол-во дней",
        "rate": "Ставка вознаграждения",
        "day_year_count": "Кол-во дней в году",
        "subsidy_sum": "Сумма рассчитанной субсидии",
        "bank_excel_diff": "Разница между расчетом Банка и Excel",
        "check_total": 'Проверка корректности столбца "Итого начисленного вознаграждения"',
        "ratio": "Соотношение суммы субсидий на итоговую сумму начисленного вознаграждения",
        "difference2": "Разница между субсидируемой и несубсидируемой частями",
        "principal_balance_check": "Проверка корректности остатка основного долга после произведенного погашения",
    }
