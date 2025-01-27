import json
import logging
import re
from contextlib import suppress
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from docx import Document
from docx.document import Document as DocumentObject
from docx.table import Table
from docx2python import docx2python
from docx2python.docx_output import DocxContent
from pandas._libs import OutOfBoundsDatetime

from src.error import (
    ContractsNofFoundError,
    DataFrameInequalityError,
    DateNotFoundError,
    ExcesssiveTableCountError,
    InterestRateMismatchError,
    InvalidColumnCount,
    MismatchError,
    TableNotFound,
    format_error,
)
from src.subsidy import (
    InterestRate,
    ParseContract,
    ProtocolID,
)
from src.utils.collections import find, index
from src.utils.db_manager import DatabaseManager
from src.utils.my_types import IterableResult, Result
from src.utils.office import Office, OfficeType
from src.utils.utils import compare, safe_extract


@dataclass
class RegexPatterns:
    months: Dict[str, str]
    file_name: re.Pattern = re.compile(
        r"((дог\w*.?.суб\w*.?)|(дс))",
        re.IGNORECASE,
    )
    file_contents: re.Pattern = re.compile(
        r"((бір бөлігін субсидиялау туралы)|(договор субсидирования))",
        re.IGNORECASE,
    )
    wrong_contents: re.Pattern = re.compile(r"дополнительное соглашение", re.IGNORECASE)
    protocol_id: re.Pattern = re.compile(r"№?.?(\d{6})")
    iban: re.Pattern = re.compile(r"коды?:?.+?(KZ[0-9A-Z]{18})", re.IGNORECASE)
    primary_column: re.Pattern = re.compile(
        r"((дата *погашени\w+ *основно\w+ *долга)|(негізгі *борышты *өтеу))",
        re.IGNORECASE,
    )
    secondary_column: re.Pattern = re.compile(
        r"((сумма *остатка *основного *долга)|(негізгі *борыш\w* *қалды\w* *сомасы))",
        re.IGNORECASE,
    )
    alpha_letters: re.Pattern = re.compile(r"[а-яәғқңөұүһі]", re.IGNORECASE)
    kz_letters: re.Pattern = re.compile(r"[әғқңөұүһі]", re.IGNORECASE)
    float_number_full: re.Pattern = re.compile(r"^[\d ., ]+$")
    float_number: re.Pattern = re.compile(r"([\d ., ]+)")
    number: re.Pattern = re.compile(r"(\d+)")
    start_date: re.Pattern = re.compile(r"^9\.")
    end_date1: re.Pattern = re.compile(r"^18\.")
    end_date2: re.Pattern = re.compile(r"^30\.")
    complex_date: re.Pattern = re.compile(
        r"(((\d{2,}) +(\w+) +(\w+) +(\w+))|(\d+.\d+.\d+))"
    )
    whitespace: re.Pattern = re.compile(r"\s+")
    date_separator: re.Pattern = re.compile(r"[. /-]")
    interest_dates: re.Pattern = re.compile(r"«?(\d{2,})»? (\w+) «?(\d+)»? (\w+)")
    date: re.Pattern = re.compile(r"(\d+\.\d+\.\d+)")
    interest_rates1: re.Pattern = re.compile(r"([\d,.]+) ?%? ?\(")
    interest_rates2: re.Pattern = re.compile(r"([\d,.]+) ?%? ?\w")
    interest_rate_para: re.Pattern = re.compile(r"6\.(.+?)7\. ", re.DOTALL)


class Backend(Enum):
    PythonDocx = 0
    Docx2Python = 1


class SubsidyDocument:
    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path
        self.doc: Optional[DocumentObject] = None
        self.paragraphs: List[str] = []
        self.is_subsidy = False

        self.docx_content: Optional[DocxContent] = None

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(file_path={self.file_path.as_posix()}, is_subsidy={self.is_subsidy})"

    def is_subsidy_file(self, patterns: RegexPatterns) -> bool:
        file_name = self.file_path.name.lower()
        if not file_name.endswith("docx") or file_name.startswith("~$"):
            return False

        self.doc, err = self.open_document()
        if err:
            raise err

        self.paragraphs = [
            text
            for para in self.doc.paragraphs
            if (text := patterns.whitespace.sub(" ", para.text).strip())
        ]

        first_n_paras = "\n".join(self.paragraphs[0:10])
        self.is_subsidy = (
            patterns.file_contents.search(first_n_paras) is not None
            and patterns.wrong_contents.search(first_n_paras) is None
        )
        return self.is_subsidy

    @staticmethod
    def recover_document(file_path: Path) -> Path:
        logging.info(f"Recovering corrupted document: {file_path}")

        og_file_path = file_path.with_name(f"og_{file_path.name}")
        if og_file_path.exists():
            og_file_path.unlink()
        file_path.rename(og_file_path)

        copy_file_path = file_path.parent / f"copy_{file_path.name}"
        with Office(file_path=og_file_path, office_type=OfficeType.WordType) as word:
            word.save_as(copy_file_path, 16)

        og_file_path.unlink()
        copy_file_path.rename(file_path)
        return file_path

    def open_document(
        self, backend: Backend = Backend.PythonDocx
    ) -> Result[Union[DocumentObject, DocxContent]]:
        match backend:
            case Backend.PythonDocx:
                try:
                    return Document(str(self.file_path)), None
                except Exception:
                    logging.warning(
                        f"Failed to open document {self.file_path}. Attempting recovery..."
                    )
                    self.file_path = self.recover_document(self.file_path)

                    try:
                        return Document(str(self.file_path)), None
                    except KeyError as err:
                        logging.error(
                            f"Failed to open document even after recovery: {err}"
                        )
                        return None, err
            case Backend.Docx2Python:
                return docx2python(self.file_path), None


class TableParser:
    def __init__(
        self,
        document: SubsidyDocument,
        patterns: RegexPatterns,
    ) -> None:
        self.document = document
        self.patterns = patterns

        self.human_readable = {
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
            "difference": "Разница между расчетом Банка и Excel",
            "check_total": 'Проверка корректности столбца "Итого начисленного вознаграждения"',
            "ratio": "Соотношение суммы субсидий на итоговую сумму начисленного вознаграждения",
            "difference2": "Разница между субсидируемой и несубсидируемой частями",
            "check2": "Проверка корректности остатка основного долга после произведенного погашения",
        }

        self.expected_columns = [
            "debt_repayment_date",
            "principal_debt_balance",
            "principal_debt_repayment_amount",
            "agency_fee_amount",
            "recipient_fee_amount",
            "total_accrued_fee_amount",
        ]

    @staticmethod
    def parse_table(table: Table, filter_empty: bool = False) -> List[List[str]]:
        table_data = []
        for row in table.rows:
            row_data = []
            for cell in row.cells:
                text = cell.text.strip().replace("\n", "")
                if filter_empty and not text:
                    continue
                row_data.append(text)
            if any(row_data):
                table_data.append(row_data)
        return table_data

    def find_tables(self) -> List[List[List[str]]]:
        tables: List[List[List[str]]] = []
        for idx, table in enumerate(self.document.doc.tables):
            parsed_table = self.parse_table(table)

            try:
                if len(parsed_table[0]) == 2:
                    continue
            except IndexError:
                pass

            try:
                secondary_column = " ".join(row[1] for row in parsed_table)
            except IndexError:
                continue

            if self.patterns.secondary_column.search(
                secondary_column
            ) or self.patterns.primary_column.search(secondary_column):
                if len(parsed_table) == 1:
                    next_table = self.parse_table(self.document.doc.tables[idx + 1])
                    parsed_table.extend(next_table)

                tables.append(parsed_table)
                continue

            try:
                next_column = " ".join(row[2] for row in parsed_table)
            except IndexError:
                continue

            if self.patterns.secondary_column.search(
                next_column
            ) or self.patterns.primary_column.search(next_column):
                if len(parsed_table) == 1:
                    next_table = self.parse_table(self.document.doc.tables[idx + 1])
                    parsed_table.extend(next_table)

                tables.append(parsed_table)
                continue

        return tables

    def get_total_row_idx(self, df: pd.DataFrame) -> int:
        keywords = {"итого", "жиыны", "барлығы", "жиынтығы"}
        for idx in range(len(df) - 1, -1, -1):
            row = df.iloc[idx]
            for value in row:
                if isinstance(value, str) and any(
                    keyword in value.lower() for keyword in keywords
                ):
                    return idx

        col = df.columns[-1]
        value = df.loc[len(df) - 1, col]
        if not self.patterns.float_number_full.search(value):
            for idx in range(len(df) - 2, -1, -1):
                value = df.loc[idx, col]
                if self.patterns.float_number_full.search(value):
                    return idx

        return len(df) - 1

    def validate_totals(self, df: pd.DataFrame, value_columns: List[str]) -> List[str]:
        value_columns.remove("principal_debt_balance")
        is_total_row = df["total"]
        mismatches = []
        for column in value_columns:
            sum_regular_rows = df.loc[~is_total_row, column].sum()
            sum_total_row = df.loc[is_total_row, column].sum()

            if not np.isclose(sum_regular_rows, sum_total_row):
                human_readable_column = self.human_readable.get(column)
                message = (
                    f"{human_readable_column!r}: {sum_regular_rows} != {sum_total_row}"
                )
                mismatches.append(message)

        return mismatches

    def clean_dataframe(self, original_df: pd.DataFrame) -> Result[pd.DataFrame]:
        if original_df.empty:
            return original_df, None

        df: pd.DataFrame = original_df.copy()

        total_row_idx = self.get_total_row_idx(df)
        df = df.iloc[: total_row_idx + 1]

        if len(df.columns) > 6:
            df = df.loc[:, ~df.T.duplicated()]

        df.dropna(axis=1, how="all", inplace=True)

        # noinspection PyUnresolvedReferences
        df = df.loc[:, ~(df == "").all()]

        if df.loc[0, df.columns[0]].isdigit():
            # noinspection PyUnresolvedReferences
            if (
                df.loc[0 : len(df) // 2, df.columns[0]].astype(int).diff().loc[1:]
                == 1.0
            ).all():
                df.drop(df.columns[0], axis=1, inplace=True)

        if len(df.columns) == 6:
            df.columns = self.expected_columns
        else:
            return None, InvalidColumnCount(
                f"Expected 6 columns - {len(df.columns)} found..."
            )

        df["total"] = False

        if "№" in df.columns:
            df.drop("№", axis=1, inplace=True)

        if df.iloc[0]["debt_repayment_date"] == "1":
            df = df.iloc[1:]
            df.reset_index(inplace=True, drop=True)

        df.loc[total_row_idx, "total"] = True
        df.loc[total_row_idx, "debt_repayment_date"] = None
        df.loc[total_row_idx, "principal_debt_balance"] = None

        df.dropna(axis=1, how="all", inplace=True)

        try:
            df.loc[:, "debt_repayment_date"] = pd.to_datetime(
                df.loc[:, "debt_repayment_date"], dayfirst=True, format="mixed"
            )
        except (OutOfBoundsDatetime, ValueError) as err:
            return None, err

        columns_to_process = [
            "principal_debt_balance",
            "principal_debt_repayment_amount",
            "agency_fee_amount",
            "recipient_fee_amount",
            "total_accrued_fee_amount",
        ]

        df[columns_to_process] = (
            df[columns_to_process]
            .replace({"-": "0", "": "0", "[  ]+": "", ",": "."}, regex=True)
            .astype(float)
        )

        df = df.where(pd.notna(df), None)
        df.reset_index(inplace=True, drop=True)

        if mismatches := self.validate_totals(df, value_columns=columns_to_process):
            return None, MismatchError("; ".join(mismatches))

        return df, None

    def parse_tables(
        self, contract: ParseContract
    ) -> IterableResult[List[pd.DataFrame]]:
        tables = self.find_tables()
        table_count = len(tables)

        if not tables:
            return [], TableNotFound(
                self.document.file_path.name,
                contract.contract_id,
                target="График погашения",
            )

        if table_count < 1 or table_count > 2:
            return [], ExcesssiveTableCountError(
                self.document.file_path.name, contract.contract_id, table_count
            )

        logging.debug(f"PARSE - found {table_count} table")

        err = None
        dfs = []
        for table in tables:
            data_start_row_idx = index(
                items=table,
                condition=lambda row: not any(
                    self.patterns.alpha_letters.search(cell) is not None for cell in row
                ),
            )

            data_start_row_idx2 = index(
                items=table,
                condition=lambda row: any(
                    len(cell) > 1 and not self.patterns.alpha_letters.search(cell)
                    for cell in row
                ),
            )

            if data_start_row_idx != data_start_row_idx2:
                logging.info(f"{data_start_row_idx=}, {data_start_row_idx2=}")
                data_start_row_idx = data_start_row_idx2

            df = pd.DataFrame(table[data_start_row_idx:])
            df, err = self.clean_dataframe(df)
            if err:
                break
            dfs.append(df)

        return dfs, err


class SubsidyParser:
    def __init__(
        self,
        document: SubsidyDocument,
        contract: ParseContract,
        interest_rates: List[InterestRate],
        protocol_ids: List[ProtocolID],
        patterns: RegexPatterns,
    ) -> None:
        self.contract = contract
        self.interest_rates = interest_rates
        self.protocol_ids = protocol_ids
        self.patterns = patterns
        self.document = document

        self.table_parser = TableParser(document=self.document, patterns=self.patterns)

    def find_protocol_ids(self) -> List[ProtocolID]:
        protocol_ids: List[ProtocolID] = []
        termin_para_idx = index(
            self.document.paragraphs, condition=lambda p: "ермин" in p
        )

        if not termin_para_idx:
            logging.error(f"EDO - no protocol ids found...")
            return protocol_ids

        text = [
            x
            for x in "".join(self.document.paragraphs[:termin_para_idx]).split(";")
            if x
        ][-1]

        protocol_ids = [
            ProtocolID(protocol_id=pid, contract_id=self.contract.contract_id)
            for pid in self.patterns.protocol_id.findall(text)
        ]
        return protocol_ids

    def find_ibans(self) -> List[str]:
        ibans: List[str] = self.patterns.iban.findall("".join(self.document.paragraphs))
        return ibans

    def find_subsidy_date(self, pat: re.Pattern) -> Result[date]:
        para = find(
            self.document.paragraphs, condition=lambda p: pat.search(p) is not None
        )

        if not para or (isinstance(para, str) and len(para) < 30):
            self.document.docx_content, err = self.document.open_document(
                backend=Backend.Docx2Python
            )
            if err:
                return None, err

            new_pat = re.compile(pat.pattern.replace(r"\.", "[.)]"))
            para = ""
            for l in self.document.docx_content.text.split("\n"):
                line = l.strip()
                if not line:
                    continue
                if new_pat.search(line):
                    para += " " + line

        if "ислам" in para:
            return None, DateNotFoundError(
                self.document.file_path.name, self.contract.contract_id, para
            )

        para = (
            para.replace('"', "")
            .replace("«", "")
            .replace("»", "")
            .replace("-ін", " ін")
            .replace("г.", " г.")
            .replace("ж.", " ж.")
            .replace("года", " года")
            .replace("жыл", " жыл")
        )

        for month in self.patterns.months.keys():
            para = para.replace(month, f" {month}")

        date_str = (
            match.group(1)
            if (match := self.patterns.complex_date.search(para))
            else None
        )

        if not isinstance(date_str, str):
            return None, DateNotFoundError(
                self.document.file_path.name, self.contract.contract_id, para
            )

        date_str = date_str.replace("-", ".").replace("/", ".")

        with suppress(ValueError):
            if len(date_str) == 10:
                res = datetime.strptime(date_str, "%d.%m.%Y").date()
                return res, None
            elif len(date_str) == 8:
                res = datetime.strptime(date_str, "%d.%m.%y").date()
                return res, None

        items: Tuple[str, ...] = tuple(
            item
            for item in self.patterns.date_separator.split(date_str)
            if item
            and all(not item.startswith(word) for word in {"год", "жыл"})
            and (item.isdigit() or len(item) > 1)
        )

        if len(items) != 3:
            return None, DateNotFoundError(
                self.document.file_path.name, self.contract.contract_id, para
            )

        if len(items[0]) == 2:
            day, month, year = items
        else:
            year, day, month = items

        if not day.isdigit():
            day, month = month, day

        if not month.isdigit():
            month = month[0:3]
            month_num = self.patterns.months.get(month)
        else:
            month_num = month

        if month_num is None:
            month_num = month

        if not year.isdigit():
            year_match = self.patterns.number.search(year)
            if year_match:
                year = year_match.group(1)

        fmt = "%d.%m.%Y" if len(year) == 4 else "%d.%m.%y"

        try:
            res = datetime.strptime(f"{day}.{month_num}.{year}", fmt).date()
            return res, None
        except ValueError as err:
            return None, err

    def find_subsidy_loan_amount(self) -> Optional[float]:
        for table in self.document.doc.tables:
            parsed_table = self.table_parser.parse_table(table, filter_empty=True)
            row_count = len(parsed_table)
            if not (
                7 <= row_count <= 9
                and sum(1 for row in parsed_table if len(row) == 2) > row_count // 2
            ):
                continue

            row_idx = index(
                parsed_table,
                condition=lambda row: row[0].startswith("Сумма"),
                default=3,
            )

            amount_str = parsed_table[row_idx][1]
            match = self.patterns.float_number.search(amount_str)
            if not match:
                continue

            match_str = match.group(1)
            match_str = match_str.replace(" ", "").replace(",", ".").replace(" ", "")
            amount = float(match_str)
            return amount

    def find_interest_rate(self) -> IterableResult[List[InterestRate]]:
        para_idx = index(
            self.document.paragraphs,
            condition=lambda p: p.startswith("6. ") and "," in p,
        )

        if not para_idx:
            return [], IndexError(f"Could not find paragraph...")

        match = self.patterns.interest_rate_para.search(
            "\n".join(self.document.paragraphs[para_idx:])
        )

        if not match:
            return [], InterestRateMismatchError(f"Could not find text...")

        text = match.group(1).strip()

        blacklist = {"ал жылдық", "ал сыйақы", "ал қалған", "а остальную"}
        blacklist_re = re.compile(
            r"((ал жылдық)|(ал сыйақы)|(ал қалған)|(а остальную))"
        )
        blacklist_count = len(blacklist_re.findall(text))
        rate_count = len(self.patterns.interest_rates1.findall(text)) or len(
            self.patterns.interest_rates2.findall(text)
        )
        enterpreneur_rate_after_subsidy = blacklist_count == 1 and rate_count > 3
        if text.count("\n") > 1 and not (blacklist_count == 1 and rate_count == 3):
            parts = [blacklist_re.split(part)[0] for part in text.split("\n")]
        else:
            parts = [part.lower() for part in re.split(r"[,;][ -]", text)]
        print("\n".join(parts))
        interest_rates = []
        nominal_rate = 0.0
        for part in parts:
            if enterpreneur_rate_after_subsidy and len(interest_rates) == (
                (rate_count - 1) // 2
            ):
                break

            if (
                any(part.startswith(phrase) for phrase in blacklist)
                or "получатель" in part
            ):
                continue

            rates = [
                rate.replace(",", ".")
                for rate in (
                    self.patterns.interest_rates1.findall(part)
                    or self.patterns.interest_rates2.findall(part)
                )
            ]
            if not rates:
                continue
            rate = float(rates[0])

            if nominal_rate == 0.0:
                nominal_rate = rate
                continue

            interest_rate = InterestRate(
                rate=rate, contract_id=self.contract.contract_id
            )

            dates_str = self.patterns.date.findall(part)
            try:
                dates = [datetime.strptime(dt, "%d.%m.%Y") for dt in dates_str]
            except ValueError:
                dates_str = [
                    tuple(d for d in dates_str if "год" not in d and "жыл" not in d)
                ]
                dates = []
                for date_str in dates_str:
                    if len(date_str[0]) == 2:
                        day, month_str, year = tuple(date_str)
                    else:
                        year, day, month_str = tuple(date_str)

                    month = self.patterns.months.get(month_str)
                    if not month:
                        return [], ValueError(f"Month {month_str!r} not found...")

                    dates.append(datetime.strptime(f"{day}.{month}.{year}", "%d.%m.%Y"))

            if len(dates) == 2:
                interest_rate.start_date = dates[0].date()
                interest_rate.end_date = dates[1].date()
            interest_rates.append(interest_rate)

        print("\n".join([repr(rate) for rate in interest_rates]))

        return interest_rates, None

        # END
        #
        # dates = self.patterns.date.findall(text)
        # rates = [
        #     rate.replace(",", ".")
        #     for rate in self.patterns.interest_rates.findall(text)
        # ]
        #
        # dates = [datetime.strptime(date, "%d.%m.%Y") for date in dates]
        #
        # if not dates and len(rates) > 3 and (len(rates) - 1) % 3 == 0:
        #     dates_str = [
        #         tuple(d for d in dates if "год" not in d and "жыл" not in d)
        #         for dates in self.patterns.interest_dates.findall(text)
        #     ]
        #
        #     for date_str in dates_str:
        #         if len(date_str[0]) == 2:
        #             day, month_str, year = tuple(date_str)
        #         else:
        #             year, day, month_str = tuple(date_str)
        #
        #         month = self.patterns.months.get(month_str)
        #         if not month:
        #             raise ValueError(f"Month {month_str!r} not found...")
        #
        #         dates.append(datetime.strptime(f"{day}.{month}.{year}", "%d.%m.%Y"))
        #
        # subsidized_dates = [dates[i] for i in range(0, len(dates), 2)]
        # entrepreneur_dates = [dates[i] for i in range(1, len(dates), 2)]
        #
        # if not is_progressive(subsidized_dates) or not is_progressive(
        #     entrepreneur_dates
        # ):
        #     subsidized_dates = [dates[i] for i in range(0, len(dates) // 2)]
        #     entrepreneur_dates = [dates[i] for i in range(len(dates) // 2, len(dates))]
        #     if not is_progressive(subsidized_dates) or not is_progressive(
        #         entrepreneur_dates
        #     ):
        #         raise InterestRateMismatchError(
        #             f"Could not generate dates in a progressive manner {dates!r}"
        #         )
        #
        # nominal_rate = float(rates[0])
        # subsidized_rates = [float(rates[i]) for i in range(1, len(rates), 2)]
        # entrepreneur_rates = [float(rates[i]) for i in range(2, len(rates), 2)]
        #
        # if not subsidized_rates or not entrepreneur_rates:
        #     raise InterestRateMismatchError(
        #         f"Could not find subsidy or nominal rates in {text!r}"
        #     )
        #
        # interest_rates: List[InterestRate] = []
        #
        # try:
        #     for (
        #         subsidized_rate,
        #         entrepreneur_rate,
        #     ) in itertools.zip_longest(subsidized_rates, entrepreneur_rates):
        #         if subsidized_rate + entrepreneur_rate == nominal_rate:
        #             interest_rate = InterestRate(rate=subsidized_rate)
        #             interest_rates.append(interest_rate)
        #             continue
        #
        #         err_msg = (
        #             f"{subsidized_rate=} + {entrepreneur_rate=} != {nominal_rate=}"
        #         )
        #         raise InterestRateMismatchError(err_msg)
        # except InterestRateMismatchError as err:
        #     interest_rates.clear()
        #     subsidized_rates = [float(rates[i]) for i in range(1, len(rates) // 2 + 1)]
        #     entrepreneur_rates = [
        #         float(rates[i]) for i in range(len(rates) // 2 + 1, len(rates))
        #     ]
        #
        #     for subsidized_rate, entrepreneur_rate in zip(
        #         subsidized_rates, entrepreneur_rates
        #     ):
        #         if subsidized_rate + entrepreneur_rate == nominal_rate:
        #             interest_rate = InterestRate(rate=subsidized_rate)
        #             interest_rates.append(interest_rate)
        #             continue
        #
        #         err_msg = f"({err}) or ({subsidized_rate=} + {entrepreneur_rate=} != {nominal_rate=})"
        #         raise InterestRateMismatchError(err_msg)
        #
        # if dates:
        #     for i in range(0, len(dates), 2):
        #         interest_rates[i // 2].start_date = dates[i]
        #         interest_rates[i // 2].end_date = dates[i + 1]
        # try:
        #     are_dates_progressive = is_progressive(interest_rates)
        # except TypeError as e:
        #     raise e
        #
        # if not are_dates_progressive:
        #     raise InterestRateMismatchError(
        #         f"Dates are not progressive {interest_rates=}"
        #     )

    def parse_document(
        self, todo: Dict[str, bool]
    ) -> IterableResult[List[pd.DataFrame]]:
        if todo.get("protocol_ids") is True:
            self.protocol_ids.extend(
                pid
                for pid in self.find_protocol_ids()
                if pid not in [p.protocol_id for p in self.protocol_ids]
            )

            if not self.protocol_ids:
                logging.error("PARSE - WARNING - protocols not found")
            else:
                self.protocol_ids[-1].newest = True
                logging.debug(f"PARSE - found {len(self.protocol_ids)} protocols")

        if todo.get("iban") is True:
            ibans = self.find_ibans()
            if len(set(ibans)) > 1:
                logging.error(
                    f"PARSE - Different IBAN codes found in the document: {ibans}"
                )
            if ibans:
                self.contract.iban = ibans[0]

            if not self.contract.iban:
                logging.error("PARSE - WARNING - IBAN not found")
            else:
                logging.debug(f"PARSE - iban={self.contract.iban!r}")

        if todo.get("start_date") is True:
            self.contract.start_date, err = self.find_subsidy_date(
                self.patterns.start_date
            )
            logging.debug(f"PARSE - start_date={self.contract.start_date}")
            if err:
                return [], err

        if todo.get("end_date") is True:
            self.contract.end_date, err = self.find_subsidy_date(
                self.patterns.end_date1
            )
            if not self.contract.end_date:
                self.contract.end_date, err = self.find_subsidy_date(
                    self.patterns.end_date2
                )

            logging.debug(f"PARSE - end_date={self.contract.end_date}")
            if err:
                return [], err

        # interest_rates, err = self.find_interest_rate()
        # if err:
        #     return [], err

        if todo.get("interest_rates") is True:
            self.interest_rates.extend(
                [
                    InterestRate(
                        rate=10.0,
                        start_date=self.contract.start_date,
                        end_date=self.contract.end_date,
                        contract_id=self.contract.contract_id,
                    )
                ]
            )

        if todo.get("loan_amount") is True:
            self.contract.loan_amount = self.find_subsidy_loan_amount()
            if not self.contract.loan_amount:
                logging.error("PARSE - WARNING - loan_amount=None")
            else:
                logging.debug(f"PARSE - loan_amount={self.contract.loan_amount!r}")

        dfs, err = self.table_parser.parse_tables(self.contract)

        return dfs, err


def parse_documents(db: DatabaseManager, months_json_path: Path) -> None:
    with months_json_path.open("r", encoding="utf-8") as f:
        months = json.load(f)
    patterns = RegexPatterns(months=months)
    del months

    contracts = db.execute(
        "SELECT id, save_folder FROM edo_contracts WHERE DATE(date_modified) = ?",
        (date.today().isoformat(),),
    )

    count = len(contracts)
    for idx, (contract_id, save_folder) in enumerate(contracts, start=1):
        logging.info(f"PARSE - {idx:02}/{count} - {contract_id}")

        parse_contract = ParseContract(contract_id=contract_id)
        interest_rates: List[InterestRate] = []
        protocol_ids: List[ProtocolID] = []

        todo = {
            "protocol_ids": not protocol_ids,
            "iban": parse_contract.iban is None,
            "start_date": parse_contract.start_date is None,
            "end_date": parse_contract.end_date is None,
            "interest_rates": not interest_rates,
            "loan_amount": parse_contract.loan_amount is None,
        }

        # if not any(todo.values()):
        #     continue

        save_folder = Path(save_folder)
        documents_folder = save_folder / "documents"
        safe_extract(save_folder / "contract.zip", documents_folder=documents_folder)

        documents: List[SubsidyDocument] = [
            doc
            for file_path in documents_folder.iterdir()
            if (doc := SubsidyDocument(file_path)).is_subsidy_file(patterns)
        ]
        document_count = len(documents)

        if documents:
            dfs: List[pd.DataFrame] = []
            for jdx, document in enumerate(documents, start=1):
                parser = SubsidyParser(
                    document=document,
                    contract=parse_contract,
                    interest_rates=interest_rates,
                    protocol_ids=protocol_ids,
                    patterns=patterns,
                )

                if document_count > 1:
                    logging.info(
                        f"PARSE - {jdx}/{len(documents)} - {document.file_path.name!r}"
                    )
                doc_dfs, err = parser.parse_document(todo)
                if err:
                    logging.error(f"{err!r}")
                    parse_contract.error = format_error(err)

                dfs.extend(doc_dfs)

            if len(dfs) == 2 and not compare(dfs[0], dfs[1]):
                err = DataFrameInequalityError("DataFrames not equal to each other")
                parse_contract.error = format_error(err)
        else:
            err = ContractsNofFoundError(
                f"EDO - {contract_id} does not have subsidy contracts..."
            )
            logging.error(err)
            parse_contract.error = format_error(err)

        parse_contract.save(db)

        for rate in interest_rates:
            rate.save(db)

        for protocol_id in protocol_ids:
            protocol_id.save(db)

        # if any(todo.values()):
        #     contract.save(db)
