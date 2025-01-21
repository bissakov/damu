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
from memory_profiler import profile

from src.error import (
    ExcesssiveTableCountError,
    InterestRateMismatchError,
    InvalidColumnCount,
    MismatchError,
    ParseError,
    TableNotFound,
)
from src.subsidy import SubsidyContract, contract_count, iter_contracts
from src.utils.collections import find, index
from src.utils.office import Office, OfficeType
from src.utils.utils import compare, safe_extract

pattern1 = re.compile(r'[«"](.+)[»"]')
pattern2 = re.compile(r'[«"](.+?)[»"]')
quotes = re.compile(r'[«»"\']')
unwanted = re.compile(r'[«"] .+ [»"]')
clean_counterparty = re.compile(r"\d+, +(.+)")


def nested_contents(string: str, pat: re.Pattern = pattern1) -> str:
    match = pat.search(string)
    if match:
        contents = match.group(1)
        if unwanted.search(contents):
            return nested_contents(string, pattern2)

        if len(quotes.findall(contents)) >= 2:
            return nested_contents(contents)
        else:
            return contents


class BankType(Enum):
    Altyn: "Altyn Bank"
    AstanaMotorsFinance: "ASTANA MOTORS FINANCE"
    RBK: "RBK Bank"
    Bereke: "Bereke Bank"
    Jusan: "Jusan Bank"
    Forte: "Forte Bank"
    BCK: "Bank Center Credit"
    Eurasia: "Eurasia"
    Ijarah: "Ijarah"
    Halyk: "Halyk"
    Nurbank: "Nurbank"
    Freedom: "Freedom"


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
    protocol_id: re.Pattern = re.compile(r"№.?(\d{6})")
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
    interest_rates: re.Pattern = re.compile(r"([\d,.]+) ?%? ?\(")
    interest_rate_para: re.Pattern = re.compile(r"6\.(.+?)7\. ", re.DOTALL)


class SubsidyDocument:
    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path
        self.doc: Optional[DocumentObject] = None
        self.paragraphs: List[str] = []
        self.is_subsidy = False

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(file_path={self.file_path.as_posix()}, is_subsidy={self.is_subsidy})"

    def is_subsidy_file(self, patterns: RegexPatterns) -> bool:
        file_name = self.file_path.name.lower()
        if not file_name.endswith("docx") or file_name.startswith("~$"):
            return False

        self.doc = self.open_document()
        self.paragraphs = [
            text
            for para in self.doc.paragraphs
            if (text := patterns.whitespace.sub(" ", para.text).strip())
        ]

        self.is_subsidy = (
            patterns.file_contents.search("\n".join(self.paragraphs[0:10])) is not None
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

    def open_document(self) -> DocumentObject:
        try:
            return Document(str(self.file_path))
        except Exception:
            logging.warning(
                f"Failed to open document {self.file_path}. Attempting recovery..."
            )
            self.file_path = self.recover_document(self.file_path)

            try:
                return Document(str(self.file_path))
            except Exception as e:
                logging.error(f"Failed to open document even after recovery: {e}")
                raise


@dataclass
class InterestRate:
    rate: float
    start_date: Optional[Union[datetime, str]] = None
    end_date: Optional[Union[datetime, str]] = None

    def __lt__(self, other: "InterestRate") -> bool:
        if not isinstance(other, InterestRate):
            return False

        return self.start_date < other.start_date and self.end_date < other.end_date

    def __le__(self, other: "InterestRate") -> bool:
        if not isinstance(other, InterestRate):
            return False

        return self.start_date <= other.start_date and self.end_date <= other.end_date

    def __gt__(self, other: "InterestRate") -> bool:
        if not isinstance(other, InterestRate):
            return False

        return self.start_date > other.start_date and self.end_date > other.end_date

    def __ge__(self, other: "InterestRate") -> bool:
        if not isinstance(other, InterestRate):
            return False

        return self.start_date >= other.start_date and self.end_date >= other.end_date


class TableParser:
    def __init__(
        self,
        document: SubsidyDocument,
        contract: SubsidyContract,
        patterns: RegexPatterns,
    ) -> None:
        self.document = document
        self.contract = contract
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

    def clean_dataframe(self, original_df: pd.DataFrame) -> pd.DataFrame:
        if original_df.empty:
            return original_df

        df: pd.DataFrame = original_df.copy()

        total_row_idx = self.get_total_row_idx(df)
        df = df.iloc[: total_row_idx + 1]

        if len(df.columns) > 6:
            df = df.loc[:, ~df.T.duplicated()]

        df.dropna(axis=1, how="all", inplace=True)

        # noinspection PyUnresolvedReferences
        df = df.loc[:, ~(df == "").all()]

        if df.loc[0, df.columns[0]].isdigit():
            if (
                df.loc[0 : len(df) // 2, df.columns[0]].astype(int).diff().loc[1:]
                == 1.0
            ).all():
                df.drop(df.columns[0], axis=1, inplace=True)

        if len(df.columns) == 6:
            df.columns = self.expected_columns
        else:
            raise InvalidColumnCount(f"Expected 6 columns - {len(df.columns)} found...")

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

        df.loc[:, "debt_repayment_date"] = pd.to_datetime(
            df.loc[:, "debt_repayment_date"], dayfirst=True, format="mixed"
        )

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
            raise MismatchError("; ".join(mismatches))

        return df

    def parse_tables(self) -> List[pd.DataFrame]:
        tables = self.find_tables()
        table_count = len(tables)

        if not tables:
            raise TableNotFound(
                self.document.file_path.name,
                self.contract.contract_id,
                target="График погашения",
            )

        if table_count < 1 or table_count > 2:
            raise ExcesssiveTableCountError(
                self.document.file_path.name, self.contract.contract_id, table_count
            )

        logging.info(f"PARSE - SUCCESS - found {table_count} table")

        dfs = []
        for table in tables:
            data_start_row_idx = index(
                items=table,
                condition=lambda row: not any(
                    self.patterns.alpha_letters.search(cell) is not None for cell in row
                ),
            )

            df = pd.DataFrame(table[data_start_row_idx:])
            df = self.clean_dataframe(df)
            dfs.append(df)

        return dfs


class SubsidyParser:
    def __init__(
        self,
        document: SubsidyDocument,
        contract: SubsidyContract,
        patterns: RegexPatterns,
    ) -> None:
        self.contract = contract
        self.patterns = patterns
        self.document = document

        self.table_parser = TableParser(
            contract=self.contract, document=self.document, patterns=self.patterns
        )

    def find_protocol_ids(self) -> List[str]:
        protocol_ids: List[str] = []
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

        protocol_ids = self.patterns.protocol_id.findall(text)
        return protocol_ids

    def find_ibans(self) -> List[str]:
        ibans: List[str] = self.patterns.iban.findall("".join(self.document.paragraphs))
        return ibans

    def find_subsidy_date(self, pat: re.Pattern) -> Optional[date]:
        para = find(
            self.document.paragraphs, condition=lambda p: pat.search(p) is not None
        )

        if "ислам" in para:
            return None

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
            return None

        date_str = date_str.replace("-", ".").replace("/", ".")

        with suppress(ValueError):
            if len(date_str) == 10:
                return datetime.strptime(date_str, "%d.%m.%Y")
            elif len(date_str) == 8:
                return datetime.strptime(date_str, "%d.%m.%y")

        items: Tuple[str, ...] = tuple(
            item
            for item in self.patterns.date_separator.split(date_str)
            if item
            and all(not item.startswith(word) for word in {"год", "жыл"})
            and (item.isdigit() or len(item) > 1)
        )

        if len(items) != 3:
            return None

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
        return datetime.strptime(f"{day}.{month_num}.{year}", fmt).date()

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

    def find_interest_rate(self) -> str:
        para_idx = index(
            self.document.paragraphs,
            condition=lambda p: p.startswith("6. ") and "," in p,
        )

        if not para_idx:
            logging.info("text=None")

        match = self.patterns.interest_rate_para.search(
            "\n".join(self.document.paragraphs[para_idx:])
        )

        if not match:
            raise InterestRateMismatchError(f"Could not find text...")

        text = match.group(1).strip()

        # START

        blacklist = {"ал жылдық", "ал сыйақы", "ал қалған", "а остальную"}
        blacklist_re = re.compile(
            r"((ал жылдық)|(ал сыйақы)|(ал қалған)|(а остальную))"
        )
        blacklist_count = len(blacklist_re.findall(text))
        rate_count = len(self.patterns.interest_rates.findall(text))
        enterpreneur_rate_after_subsidy = blacklist_count == 1 and rate_count > 3
        if enterpreneur_rate_after_subsidy:
            pass

        if text.count("\n") > 1:
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
                for rate in self.patterns.interest_rates.findall(part)
            ]
            if not rates:
                continue
            rate = float(rates[0])

            if nominal_rate == 0.0:
                nominal_rate = rate
                continue

            interest_rate = InterestRate(rate=rate)

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
                        raise ValueError(f"Month {month_str!r} not found...")

                    dates.append(datetime.strptime(f"{day}.{month}.{year}", "%d.%m.%Y"))

            if len(dates) == 2:
                interest_rate.start_date = dates[0]
                interest_rate.end_date = dates[1]
            interest_rates.append(interest_rate)

        print("\n".join([repr(rate) for rate in interest_rates]))
        pass

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

        logging.info(f"{interest_rates=}")
        return text

    def parse_document(self) -> List[pd.DataFrame]:
        self.contract.protocol_ids.extend(
            pid
            for pid in self.find_protocol_ids()
            if pid not in self.contract.protocol_ids
        )

        if not self.contract.protocol_ids:
            logging.error("PARSE - WARNING - protocols not found")
        else:
            logging.debug(
                f"PARSE - SUCCESS - found {len(self.contract.protocol_ids)} protocols"
            )

        # interest_rate = self.find_interest_rate()

        self.contract.ibans.extend(self.find_ibans())

        if not self.contract.ibans:
            logging.error("PARSE - WARNING - IBAN not found")
        else:
            logging.debug(f"PARSE - SUCCESS - found {len(self.contract.ibans)} IBAN")

            if len(set(self.contract.ibans)) > 1:
                logging.error(
                    f"PARSE - Different IBAN codes found in the document: {self.contract.ibans}"
                )

        self.contract.start_date = self.find_subsidy_date(self.patterns.start_date)
        self.contract.end_date = self.find_subsidy_date(self.patterns.end_date1)
        if not self.contract.end_date:
            self.contract.end_date = self.find_subsidy_date(self.patterns.end_date2)
        logging.debug(
            f"PARSE - start_date={self.contract.start_date}, end_date={self.contract.end_date}"
        )

        self.contract.loan_amount = self.find_subsidy_loan_amount()
        if not self.contract.loan_amount:
            logging.error("PARSE - WARNING - loan_amount=None")
        else:
            logging.debug(
                f"PARSE - SUCCESS - loan_amount={self.contract.loan_amount!r}"
            )

        dfs = self.table_parser.parse_tables()

        return dfs


def parse_documents(download_folder: Path, patterns: RegexPatterns) -> None:
    count = contract_count(download_folder)
    for idx, contract in enumerate(iter_contracts(download_folder), start=1):
        if not contract:
            logging.warning(f"PARSE - {idx:02}/{count} - not found...")
            continue

        logging.info(f"PARSE - {idx:02}/{count} - {contract.contract_id}")
        save_folder = Path(contract.save_folder)
        documents_folder = save_folder / "documents"

        if not any(documents_folder.iterdir()):
            safe_extract(save_folder / "contract.zip", extract_folder=documents_folder)

        documents: List[SubsidyDocument] = [
            doc
            for file_path in documents_folder.iterdir()
            if (doc := SubsidyDocument(file_path)).is_subsidy_file(patterns)
        ]

        if not documents:
            logging.error(
                f"EDO - {contract.contract_id} does not have subsidy contracts..."
            )

        contract.protocol_ids.clear()
        contract.ibans.clear()
        contract.data.clear()

        dfs: List[pd.DataFrame] = []
        for jdx, document in enumerate(documents, start=1):
            parser = SubsidyParser(document, contract, patterns)

            logging.info(
                f"PARSE - {jdx}/{len(documents)} - {document.file_path.name!r}"
            )
            try:
                doc_dfs = parser.parse_document()
            except (ParseError, IndexError, ValueError) as err:
                logging.error(f"{err!r}")
                continue

            dfs.extend(doc_dfs)

        if len(dfs) == 2 and not compare(dfs[0], dfs[1]):
            logging.error("DataFrames not equal to each other")

        contract.save()

    # data = []
    #
    # for contract in contracts:
    #     counterparty = clean_counterparty.search(contract.counterparty).group(1)
    #     counterparty = re.sub(r"\s{2,}", " ", counterparty)
    #     counterparty = counterparty.replace("''", '"')
    #
    #     if 'Дочерняя компания АО "Банк ЦентрКредит"' in counterparty:
    #         pass
    #
    #     bank = nested_contents(counterparty)
    #     bank = (
    #         bank.replace("\\", "")
    #         .lower()
    #         .replace("ý", "u")
    #         .replace(" ", "")
    #         .replace('"', "")
    #     )
    #
    #     data.append({"counterparty": counterparty, "bank": bank})
    #
    # df = pd.DataFrame(data)
    # df.sort_values(by="bank", inplace=True)
    # df.reset_index(inplace=True, drop=True)
    #
    # pd.set_option("display.max_rows", None)
    # # pd.set_option("display.max_columns", None)
    #
    # df.to_excel(
    #     r"D:\Work\python_rpa\damu\downloads\2025-01-15\a37e6624-e810-4890-8d97-675979f000f4\test.xlsx",
    #     index=False,
    # )
