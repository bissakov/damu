import io
import zlib
from dataclasses import dataclass
from datetime import date, datetime
from typing import cast

import pandas as pd
from pandas import Timestamp
from pandas._typing import WriteBuffer

from utils.db_manager import DatabaseManager


def date_to_str(dt: date | None) -> str | None:
    if isinstance(dt, (datetime, Timestamp)):
        return dt.date().isoformat()
    elif isinstance(dt, date):
        return dt.isoformat()
    else:
        return None


def str_to_date(dt: str | Timestamp) -> Timestamp | None:
    return pd.to_datetime(dt) if isinstance(dt, str) else None


@dataclass(slots=True)
class InterestRate:
    contract_id: str
    subsid_term: int
    nominal_rate: int
    rate_one_two_three_year: int
    rate_four_year: int
    rate_five_year: int
    rate_six_seven_year: int
    rate_fee_one_two_three_year: int
    rate_fee_four_year: int
    rate_fee_five_year: int
    rate_fee_six_seven_year: int
    start_date_one_two_three_year: Timestamp | None = None
    end_date_one_two_three_year: Timestamp | None = None
    start_date_four_year: Timestamp | None = None
    end_date_four_year: Timestamp | None = None
    start_date_five_year: Timestamp | None = None
    end_date_five_year: Timestamp | None = None
    start_date_six_seven_year: Timestamp | None = None
    end_date_six_seven_year: Timestamp | None = None

    def to_json(self) -> dict[str, str | float | None]:
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
            "start_date_one_two_three_year": date_to_str(
                self.start_date_one_two_three_year
            ),
            "end_date_one_two_three_year": date_to_str(
                self.end_date_one_two_three_year
            ),
            "start_date_four_year": date_to_str(self.start_date_four_year),
            "end_date_four_year": date_to_str(self.end_date_four_year),
            "start_date_five_year": date_to_str(self.start_date_five_year),
            "end_date_five_year": date_to_str(self.end_date_five_year),
            "start_date_six_seven_year": date_to_str(
                self.start_date_six_seven_year
            ),
            "end_date_six_seven_year": date_to_str(
                self.end_date_six_seven_year
            ),
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
        db.request(query, self.to_json())


@dataclass(slots=True)
class Error:
    contract_id: str
    traceback: str | None = None
    error: Exception | None = None
    human_readable: str | None = None

    def to_json(self) -> dict[str, str | float | None]:
        return {
            "id": self.contract_id,
            "traceback": self.traceback,
            "human_readable": self.human_readable,
        }

    def save(self, db: DatabaseManager) -> None:
        error_exists = db.request(
            "SELECT id FROM errors WHERE id = ? LIMIT 1",
            (self.contract_id,),
            req_type="fetch_one",
        )
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

        db.request(query, self.to_json())

    def get_human_readable(self) -> str | None:
        trc = self.traceback
        if trc is None:
            return None

        if "ContractsNofFoundError" in trc:
            human_readable = "Не найден документ (файл .docx) для обработки в списке вложенных файлов."
        elif "ProtocolIDNotFoundError" in trc:
            human_readable = (
                "Номер протокола не найден во время обработки документа."
            )
        elif "LoanAmountNotFoundError" in trc:
            human_readable = "Сумма кредита не найдена в файле заявления."
        elif "JoinPDFNotFoundError" in trc:
            human_readable = (
                "PDF файл 'Заявление получателя к договору "
                "присоединения' для получения номера выписки не найден."
            )
        elif "JoinProtocolNotFoundError" in trc:
            human_readable = (
                "Номер протокола не найден в файле "
                "'Заявление получателя к договору присоединения'. "
                "Возможно скан документа невозможно прочесть роботу."
            )
        elif "DateNotFoundError" in trc:
            human_readable = "Не удалось найти либо не удалось обработать дату начала или завершения ДС."
        elif "FloatConversionError" in trc:
            human_readable = "Не удалось преобразовать значения графика погашения в числовой формат."
        elif "InvalidColumnCount" in trc or "TableNotFound" in trc:
            human_readable = (
                "Таблица погашения нестандартного вида, не удалось обработать таблицу. "
                "Возможные причины - смещеннные строки/колонки, "
                "неравназначное кол-во именных колонок и колонок данных."
            )
        elif "EmptyTableError" in trc:
            human_readable = "Отсутствуют данные в таблицe."
        elif "MismatchError" in trc:
            human_readable = (
                "Расхождения между строчными и итоговыми данными в "
                "оригинальной таблице погашения (сумма строк неравна итоговым суммам).\n"
                f"{self.error}"
            )
        elif "WrongDataInColumnError" in trc:
            human_readable = str(self.error)
        elif "ExcesssiveTableCountError" in trc:
            human_readable = "Найдено неверное кол-во таблиц погашений - меньше 1, но больше 2."
        elif "DataFrameInequalityError" in trc:
            human_readable = "Казахские и русские версии таблиц погашений не равны друг другу."
        elif (
            "BankNotSupportedError" in trc or "Договор Исламского банка" in trc
        ):
            human_readable = "Данный банк не поддерживается на данный момент."
        elif "Protocol IDs not found" in trc:
            human_readable = (
                "Номера протоколов не найдены в договоре субсидирования."
            )
        elif "IBANs not found" in trc:
            human_readable = "IBAN коды не найдены в договоре субсидирования."
        elif "IBANs are different" in trc:
            human_readable = "Расхождения между IBAN кодами в казахской и русской версиях графика погашения."
        elif "CRMNotFoundError" in trc:
            human_readable = "Не удалось найти проект по протоколу в CRM."
        elif "ProtocolDateNotInRangeError" in trc:
            human_readable = "Не согласовано. Дата первого протокола превышает 180 дней (6 месяцев)."
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
    ds_date: date | None
    dbz_id: str
    dbz_date: date | None
    sed_number: str
    contract_type: str

    def to_json(self) -> dict[str, str | float | date | None]:
        return {
            "id": self.contract_id,
            "ds_id": self.ds_id,
            "ds_date": self.ds_date,
            "dbz_iz": self.dbz_id,
            "dbz_date": self.dbz_date,
            "sed_number": self.sed_number,
            "contract_type": self.contract_type,
        }

    def save(self, db: DatabaseManager) -> None:
        contract_exists = db.request(
            "SELECT id FROM contracts WHERE id = ? LIMIT 1",
            (self.contract_id,),
            req_type="fetch_one",
        )
        if not contract_exists:
            query = """
                INSERT OR REPLACE INTO contracts
                (id, ds_id, ds_date, ds_id, dbz_date, sed_number, contract_type)
                VALUES
                (:id, :ds_id, :ds_date, :ds_id, :dbz_date, :sed_number, :contract_type)
            """
        else:
            query = """
                UPDATE contracts
                SET id = :id,
                    ds_id = :ds_id,
                    ds_date = :ds_date,
                    dbz_id = :dbz_id,
                    dbz_date = :dbz_date,
                    sed_number = :sed_number,
                    contract_type = :contract_type
                WHERE id = :id
            """

        db.request(query, self.to_json())


@dataclass(slots=True)
class ParseSubsidyContract:
    contract_id: str
    protocol_id: str | None = None
    start_date: date | None = None
    end_date: date | None = None
    loan_amount: float | None = None
    iban: str | None = None
    df: pd.DataFrame | None = None
    file_name: str | None = None
    settlement_date: int | None = None
    error: Error | None = None

    def __hash__(self) -> int:
        return hash((self.contract_id,))

    def to_json(self) -> dict[str, str | float | None]:
        if self.df is None:
            df_blob = None
        else:
            buffer = io.BytesIO()
            self.df.to_parquet(
                cast(WriteBuffer[bytes], buffer), engine="fastparquet"
            )
            df_blob = zlib.compress(buffer.getvalue())

        return {
            "id": self.contract_id,
            "protocol_id": self.protocol_id,
            "start_date": date_to_str(self.start_date),
            "end_date": date_to_str(self.end_date),
            "loan_amount": self.loan_amount,
            "iban": self.iban,
            "df": df_blob if self.df is not None else None,
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
                file_name = :file_name,
                settlement_date = :settlement_date,
                modified = CURRENT_TIMESTAMP
            WHERE id = :id
        """
        db.request(query, self.to_json())


@dataclass(slots=True)
class ParseJoinContract:
    contract_id: str
    protocol_id: str | None = None
    start_date: date | None = None
    end_date: date | None = None
    loan_amount: float | None = None
    iban: str | None = None
    df: pd.DataFrame | None = None
    file_name: str | None = None
    settlement_date: int | None = None
    error: Error | None = None

    def __hash__(self) -> int:
        return hash((self.contract_id,))

    def to_json(self) -> dict[str, str | float | bytes | None]:
        if self.df is None:
            df_blob = None
        else:
            buffer = io.BytesIO()
            self.df.to_parquet(
                cast(WriteBuffer[bytes], buffer), engine="fastparquet"
            )
            df_blob = zlib.compress(buffer.getvalue())

        return {
            "id": self.contract_id,
            "protocol_id": self.protocol_id,
            "start_date": date_to_str(self.start_date),
            "end_date": date_to_str(self.end_date),
            "loan_amount": self.loan_amount,
            "iban": self.iban,
            "df": df_blob if self.df is not None else None,
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
        db.request(query, self.to_json())


@dataclass(slots=True)
class Bank:
    contract_id: str
    bank_id: str
    bank: str
    year_count: int | None

    def __hash__(self) -> int:
        return hash((self.bank_id,))

    def to_json(self) -> dict[str, str | float | None]:
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
        db.request(query, self.to_json())


class _CrmContract:
    def __init__(self, contract_id: str) -> None:
        self.contract_id: str = contract_id
        self._project_id: str | None = None
        self._project: str | None = None
        self._customer: str | None = None
        self._customer_id: str | None = None
        self._bank_id: str | None = None
        self._subsid_amount: float | None = None
        self._investment_amount: float | None = None
        self._pos_amount: float | None = None
        self._vypiska_date: date | None = None
        self._credit_purpose: str | None = None
        self._repayment_procedure: str | None = None
        self._request_number: int | None = None
        self._protocol_date: date | None = None
        self._decision_date: date | None = None
        self._dbz_id: str | None = None
        self._dbz_date: date | None = None
        self._error: Error | None = None

    @property
    def project_id(self) -> str:
        if not self._project_id:
            raise ValueError()
        return self._project_id

    @project_id.setter
    def project_id(self, value: str) -> None:
        self._project_id = value

    @project_id.deleter
    def project_id(self) -> None:
        del self._project_id

    @property
    def project(self) -> str:
        if not self._project:
            raise ValueError()
        return self._project

    @project.setter
    def project(self, value: str) -> None:
        self._project = value

    @project.deleter
    def project(self) -> None:
        del self._project


@dataclass(slots=True)
class CrmContract:
    contract_id: str
    error: Error
    project_id: str | None = None
    project: str | None = None
    customer: str | None = None
    customer_id: str | None = None
    bank_id: str | None = None
    subsid_amount: float | None = None
    investment_amount: float | None = None
    pos_amount: float | None = None
    vypiska_date: date | None = None
    credit_purpose: str | None = None
    repayment_procedure: str | None = None
    request_number: int | None = None
    protocol_date: date | None = None
    decision_date: date | None = None
    dbz_id: str | None = None
    dbz_date: date | None = None
    contragent: str | None = None

    def __hash__(self) -> int:
        return hash((self.contract_id,))

    def to_json(self) -> dict[str, str | float | None]:
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
            "contragent": self.contragent,
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
                contragent = :contragent,
                modified = CURRENT_TIMESTAMP
            WHERE id = :id
        """
        db.request(query, self.to_json())


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
    start_date_one_two_three_year: Timestamp | None
    end_date_one_two_three_year: Timestamp | None
    start_date_four_year: Timestamp | None
    end_date_four_year: Timestamp | None
    start_date_five_year: Timestamp | None
    end_date_five_year: Timestamp | None
    start_date_six_seven_year: Timestamp | None
    end_date_six_seven_year: Timestamp | None

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
            self.df = pd.read_parquet(
                io.BytesIO(zlib.decompress(self.df)), engine="fastparquet"
            )

        self.start_date = str_to_date(self.start_date)
        self.end_date = str_to_date(self.end_date)
        self.start_date_one_two_three_year = str_to_date(
            self.start_date_one_two_three_year
        )
        self.end_date_one_two_three_year = str_to_date(
            self.end_date_one_two_three_year
        )
        self.start_date_four_year = str_to_date(self.start_date_four_year)
        self.end_date_four_year = str_to_date(self.end_date_four_year)
        self.start_date_five_year = str_to_date(self.start_date_five_year)
        self.end_date_five_year = str_to_date(self.end_date_five_year)
        self.start_date_six_seven_year = str_to_date(
            self.start_date_six_seven_year
        )
        self.end_date_six_seven_year = str_to_date(self.end_date_six_seven_year)
