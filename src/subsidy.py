import io
import json
import zlib
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime
from typing import Dict, Optional, Union, cast

import pandas as pd
from pandas import Timestamp
from pandas._typing import WriteBuffer

from src.utils.db_manager import DatabaseManager


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if is_dataclass(obj):
            return asdict(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


def date_to_str(dt: Optional[date]) -> Optional[str]:
    if isinstance(dt, (datetime, Timestamp)):
        return dt.date().isoformat()
    elif isinstance(dt, date):
        return dt.isoformat()
    else:
        return None


def str_to_date(dt: Union[str, Timestamp]) -> Optional[Timestamp]:
    return pd.to_datetime(dt) if isinstance(dt, str) else None


@dataclass(slots=True)
class InterestRate:
    contract_id: str
    subsid_term: int
    nominal_rate: float
    rate_one_two_three_year: float
    rate_four_year: float
    rate_five_year: float
    rate_six_seven_year: float
    rate_fee_one_two_three_year: float
    rate_fee_four_year: float
    rate_fee_five_year: float
    rate_fee_six_seven_year: float
    start_date_one_two_three_year: Optional[Timestamp] = None
    end_date_one_two_three_year: Optional[Timestamp] = None
    start_date_four_year: Optional[Timestamp] = None
    end_date_four_year: Optional[Timestamp] = None
    start_date_five_year: Optional[Timestamp] = None
    end_date_five_year: Optional[Timestamp] = None
    start_date_six_seven_year: Optional[Timestamp] = None
    end_date_six_seven_year: Optional[Timestamp] = None

    def __post_init__(self) -> None:
        self.nominal_rate /= 100.0
        self.rate_one_two_three_year /= 100.0
        self.rate_four_year /= 100.0
        self.rate_five_year /= 100.0
        self.rate_six_seven_year /= 100.0
        self.rate_fee_one_two_three_year /= 100.0
        self.rate_fee_four_year /= 100.0
        self.rate_fee_five_year /= 100.0
        self.rate_fee_six_seven_year /= 100.0

    def to_json(self) -> Dict[str, Union[str, float, None]]:
        return {
            "id": self.contract_id,
            "subsid_term": self.subsid_term,
            "nominal_rate": self.nominal_rate,
            "rate_one_two_three_year": self.rate_one_two_three_year,
            "rate_four_year": self.rate_four_year,
            "rate_five_year": self.rate_five_year,
            "rate_six_seven_year": self.rate_six_seven_year,
            "rate_fee_one_two_three_year": self.rate_fee_one_two_three_year,
            "rate_fee_four_year": self.rate_fee_four_year,
            "rate_fee_five_year": self.rate_fee_five_year,
            "rate_fee_six_seven_year": self.rate_fee_six_seven_year,
            "start_date_one_two_three_year": date_to_str(self.start_date_one_two_three_year),
            "end_date_one_two_three_year": date_to_str(self.end_date_one_two_three_year),
            "start_date_four_year": date_to_str(self.start_date_four_year),
            "end_date_four_year": date_to_str(self.end_date_four_year),
            "start_date_five_year": date_to_str(self.start_date_five_year),
            "end_date_five_year": date_to_str(self.end_date_five_year),
            "start_date_six_seven_year": date_to_str(self.start_date_six_seven_year),
            "end_date_six_seven_year": date_to_str(self.end_date_six_seven_year),
        }

    def save(self, db: DatabaseManager) -> None:
        query = """
        INSERT OR REPLACE INTO interest_rates
            (
                id,
                subsid_term,
                nominal_rate,
                rate_one_two_three_year,
                rate_four_year,
                rate_five_year,
                rate_six_seven_year,
                rate_fee_one_two_three_year,
                rate_fee_four_year,
                rate_fee_five_year,
                rate_fee_six_seven_year,
                start_date_one_two_three_year,
                end_date_one_two_three_year,
                start_date_four_year,
                end_date_four_year,
                start_date_five_year,
                end_date_five_year,
                start_date_six_seven_year,
                end_date_six_seven_year
            )
        VALUES
            (
                :id,
                :subsid_term,
                :nominal_rate,
                :rate_one_two_three_year,
                :rate_four_year,
                :rate_five_year,
                :rate_six_seven_year,
                :rate_fee_one_two_three_year,
                :rate_fee_four_year,
                :rate_fee_five_year,
                :rate_fee_six_seven_year,
                :start_date_one_two_three_year,
                :end_date_one_two_three_year,
                :start_date_four_year,
                :end_date_four_year,
                :start_date_five_year,
                :end_date_five_year,
                :start_date_six_seven_year,
                :end_date_six_seven_year
            )
        """
        db.execute(query, self.to_json())


@dataclass(slots=True)
class Error:
    contract_id: str
    traceback: Optional[str] = None
    human_readable: Optional[str] = None

    def to_json(self) -> Dict[str, Union[str, float, None]]:
        return {
            "id": self.contract_id,
            "traceback": self.traceback,
            "human_readable": self.human_readable,
        }

    def save(self, db: DatabaseManager) -> None:
        error_exists = db.execute("SELECT id FROM errors WHERE id = ? LIMIT 1", (self.contract_id,))
        if not error_exists:
            query = """
                INSERT OR REPLACE INTO errors
                (id, traceback, human_readable)
                VALUES
                (:id, :traceback, :human_readable)
            """
        else:
            query = """
                UPDATE errors
                SET id = :id,
                    traceback = :traceback,
                    human_readable = :human_readable
                WHERE id = :id
            """

        db.execute(query, self.to_json())

    def get_human_readable(self) -> Optional[str]:
        trc = self.traceback
        if trc is None:
            return None

        if "ContractsNofFoundError" in trc:
            human_readable = (
                "Не найден договор субсидирования (файл .docx) в списке вложенных файлов."
            )
        elif "DateNotFoundError" in trc:
            human_readable = (
                "Не удалсь найти либо не удалось обработать дату начала или завершения ДС."
            )
        elif "InvalidColumnCount" in trc or "TableNotFound" in trc:
            human_readable = (
                "Таблица погашения нестандартного вида, не удалось обработать таблицу. "
                "Возможные причины - смещеннные строки/колонки, "
                "неравназначное кол-во именных колонок и колонок данных."
            )
        elif "MismatchError" in trc:
            human_readable = "Расхождения между строчными и итоговыми данными в таблице погашения."
        elif "ExcesssiveTableCountError" in trc:
            human_readable = "Найдено неверное кол-во таблиц погашений - меньше 1, но больше 2."
        elif "DataFrameInequalityError" in trc:
            human_readable = "Казахские и русские версии таблиц погашений не равны друг другу."
        elif "BankNotSupportedError" in trc or "Договор Исламского банка" in trc:
            human_readable = "Данный банк не поддерживается на данный момент."
        elif "Protocol IDs not found" in trc:
            human_readable = "Номера протоколов не найдены в договоре субсидирования."
        elif "IBANs not found" in trc:
            human_readable = "IBAN коды не найдены в договоре субсидирования."
        elif "IBANs are different" in trc:
            human_readable = (
                "Расхождения между IBAN кодами в казахской и русской версиях графика погашения."
            )
        elif "CRMNotFoundError" in trc:
            human_readable = "Не удалось найти проект по протоколу в CRM."
        elif "VypiskaDownloadError" in trc:
            human_readable = "Не удалось скачать выписку из CRM."
        elif "TypeError" in trc and "vypiska_date" in trc:
            human_readable = "Не удалось получить дату протокола из CRM."
        elif "ValueError" in trc and "repayment_procedure=None" in trc:
            human_readable = "Не удалось получить порядок погашения из CRM."
        else:
            human_readable = "Неизвестная ошибка."

        return human_readable


@dataclass(slots=True)
class EdoContract:
    contract_id: str
    ds_id: str
    ds_date: date
    contragent: str
    sed_number: str

    def to_json(self) -> Dict[str, Union[str, float, None]]:
        return {
            "id": self.contract_id,
            "ds_id": self.ds_id,
            "ds_date": self.ds_date,
            "contragent": self.contragent,
            "sed_number": self.sed_number,
        }

    def save(self, db: DatabaseManager) -> None:
        contract_exists = db.execute(
            "SELECT id FROM contracts WHERE id = ? LIMIT 1", (self.contract_id,)
        )
        if not contract_exists:
            query = """
                INSERT OR REPLACE INTO contracts
                (id, ds_id, ds_date, contragent, sed_number)
                VALUES
                (:id, :ds_id, :ds_date, :contragent, :sed_number)
            """
        else:
            query = """
                UPDATE contracts
                SET id = :id,
                    ds_id = :ds_id,
                    ds_date = :ds_date,
                    contragent = :contragent,
                    sed_number = :sed_number
                WHERE id = :id
            """

        db.execute(query, self.to_json())


@dataclass(slots=True)
class ParseContract:
    contract_id: str
    protocol_id: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    loan_amount: Optional[float] = None
    iban: Optional[str] = None
    df: Optional[pd.DataFrame] = None
    dbz_id: Optional[str] = None
    dbz_date: Optional[date] = None
    file_name: Optional[str] = None
    settlement_date: Optional[int] = None
    error: Optional[Error] = None

    def __hash__(self) -> int:
        return hash((self.contract_id,))

    def to_json(self) -> Dict[str, Union[str, float, None]]:
        if self.df is None:
            df_blob = None
        else:
            buffer = io.BytesIO()
            self.df.to_parquet(cast(WriteBuffer[bytes], buffer), engine="fastparquet")
            df_blob = zlib.compress(buffer.getvalue())

        return {
            "id": self.contract_id,
            "protocol_id": self.protocol_id,
            "start_date": date_to_str(self.start_date),
            "end_date": date_to_str(self.end_date),
            "loan_amount": self.loan_amount,
            "iban": self.iban,
            "df": df_blob if self.df is not None else None,
            "dbz_id": self.dbz_id,
            "dbz_date": date_to_str(self.dbz_date),
            "file_name": self.file_name,
            "settlement_date": self.settlement_date,
        }

    def save(self, db: DatabaseManager) -> None:
        query = """
            UPDATE contracts
            SET protocol_id = :protocol_id,
                start_date = :start_date,
                end_date = :end_date,
                loan_amount = :loan_amount,
                iban = :iban,
                df = :df,
                dbz_id = :dbz_id,
                dbz_date = :dbz_date,
                file_name = :file_name,
                settlement_date = :settlement_date,
                modified = CURRENT_TIMESTAMP
            WHERE id = :id
        """
        db.execute(query, self.to_json())


@dataclass(slots=True)
class Bank:
    contract_id: str
    bank_id: str
    bank: str
    year_count: Optional[int]

    def __hash__(self) -> int:
        return hash((self.bank_id,))

    def to_json(self) -> Dict[str, Union[str, float, None]]:
        return {
            "id": self.contract_id,
            "bank_id": self.bank_id,
            "bank": self.bank,
            "year_count": self.year_count,
        }

    def save(self, db: DatabaseManager) -> None:
        query = """
            UPDATE contracts
            SET bank_id = :bank_id,
                bank = :bank,
                year_count = :year_count
            WHERE id = :id
        """
        db.execute(query, self.to_json())


@dataclass(slots=True)
class CrmContract:
    contract_id: str
    project_id: Optional[str] = None
    project: Optional[str] = None
    customer: Optional[str] = None
    customer_id: Optional[str] = None
    bank_id: Optional[str] = None
    subsid_amount: Optional[float] = None
    investment_amount: Optional[float] = None
    pos_amount: Optional[float] = None
    vypiska_date: Optional[date] = None
    credit_purpose: Optional[str] = None
    repayment_procedure: Optional[str] = None
    request_number: Optional[int] = None
    protocol_date: Optional[date] = None
    decision_date: Optional[date] = None
    dbz_id: Optional[str] = None
    dbz_date: Optional[date] = None
    error: Optional[Error] = None

    def __hash__(self) -> int:
        return hash((self.contract_id,))

    def to_json(self) -> Dict[str, Union[str, float, None]]:
        return {
            "id": self.contract_id,
            "project_id": self.project_id,
            "project": self.project,
            "customer": self.customer,
            "customer_id": self.customer_id,
            "bank_id": self.bank_id,
            "subsid_amount": self.subsid_amount,
            "investment_amount": self.investment_amount,
            "pos_amount": self.pos_amount,
            "vypiska_date": date_to_str(self.vypiska_date),
            "credit_purpose": self.credit_purpose,
            "repayment_procedure": self.repayment_procedure,
            "request_number": self.request_number,
            "protocol_date": date_to_str(self.protocol_date),
            "decision_date": date_to_str(self.decision_date),
            "dbz_id": self.dbz_id,
            "dbz_date": date_to_str(self.dbz_date),
        }

    def save(self, db: DatabaseManager) -> None:
        query = """
            UPDATE contracts
            SET project_id = :project_id,
                project = :project,
                customer = :customer,
                customer_id = :customer_id,
                bank_id = :bank_id,
                subsid_amount = :subsid_amount,
                investment_amount = :investment_amount,
                pos_amount = :pos_amount,
                vypiska_date = :vypiska_date,
                credit_purpose = :credit_purpose,
                repayment_procedure = :repayment_procedure,
                request_number = :request_number,
                protocol_date = :protocol_date,
                decision_date = :decision_date,
                dbz_id = :dbz_id,
                dbz_date = :dbz_date,
                modified = CURRENT_TIMESTAMP
            WHERE id = :id
        """
        db.execute(query, self.to_json())


@dataclass(slots=True)
class SubsidyContract:
    contract_id: str
    start_date: Timestamp
    end_date: Timestamp
    loan_amount: float
    df: pd.DataFrame
    bank: str
    year_count: int
    rate_one_two_three_year: float
    rate_four_year: float
    rate_five_year: float
    rate_six_seven_year: float
    start_date_one_two_three_year: Optional[Timestamp]
    end_date_one_two_three_year: Optional[Timestamp]
    start_date_four_year: Optional[Timestamp]
    end_date_four_year: Optional[Timestamp]
    start_date_five_year: Optional[Timestamp]
    end_date_five_year: Optional[Timestamp]
    start_date_six_seven_year: Optional[Timestamp]
    end_date_six_seven_year: Optional[Timestamp]

    def print_rates(self) -> str:
        one_two_three = (
            f"1-3=(rate={self.rate_one_two_three_year}, "
            f"start={self.start_date_one_two_three_year}, "
            f"end={self.end_date_one_two_three_year})"
        )
        four = (
            f"4=(rate={self.rate_four_year}, "
            f"start={self.start_date_four_year}, "
            f"end={self.end_date_four_year})"
        )
        five = (
            f"5=(rate={self.rate_five_year}, "
            f"start={self.start_date_five_year}, "
            f"end={self.end_date_five_year})"
        )
        six_seven = (
            f"6-7=(rate={self.rate_six_seven_year}, "
            f"start={self.start_date_six_seven_year}, "
            f"end={self.end_date_six_seven_year})"
        )
        return f"start={self.start_date!r}, end={self.end_date!r}, {one_two_three}, {four}, {five}, {six_seven}"

    def __post_init__(self) -> None:
        if isinstance(self.df, bytes):
            self.df = pd.read_parquet(io.BytesIO(zlib.decompress(self.df)), engine="fastparquet")

        self.start_date = str_to_date(self.start_date)
        self.end_date = str_to_date(self.end_date)
        self.start_date_one_two_three_year = str_to_date(self.start_date_one_two_three_year)
        self.end_date_one_two_three_year = str_to_date(self.end_date_one_two_three_year)
        self.start_date_four_year = str_to_date(self.start_date_four_year)
        self.end_date_four_year = str_to_date(self.end_date_four_year)
        self.start_date_five_year = str_to_date(self.start_date_five_year)
        self.end_date_five_year = str_to_date(self.end_date_five_year)
        self.start_date_six_seven_year = str_to_date(self.start_date_six_seven_year)
        self.end_date_six_seven_year = str_to_date(self.end_date_six_seven_year)
