import json
import pickle
from dataclasses import asdict, dataclass, field, fields, is_dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Union

from src.utils.db_manager import DatabaseManager


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if is_dataclass(obj):
            return asdict(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


@dataclass(slots=True)
class InterestRate:
    rate: float
    contract_id: str
    start_date: Optional[Union[date, str]] = None
    end_date: Optional[Union[date, str]] = None

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

    def to_json(self) -> Dict[str, Union[str, float, None]]:
        return {
            "rate": self.rate,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "contract_id": self.contract_id,
            "date_modified": datetime.now().isoformat(),
        }

    def save(self, db: DatabaseManager) -> None:
        query = """
        INSERT OR REPLACE INTO interest_rates
            (rate, start_date, end_date, contract_id, date_modified)
        VALUES
            (:rate, :start_date, :end_date, :contract_id, :date_modified)
        """
        db.execute(query, self.to_json())


@dataclass(slots=True)
class ProtocolID:
    protocol_id: str
    contract_id: str
    newest: bool = False

    def to_json(self) -> Dict[str, Union[str, float, None]]:
        return {
            "protocol_id": self.protocol_id,
            "contract_id": self.contract_id,
            "newest": self.newest,
            "date_modified": datetime.now().isoformat(),
        }

    def save(self, db: DatabaseManager) -> None:
        query = """
        INSERT OR REPLACE INTO protocol_ids
            (protocol_id, contract_id, newest, date_modified)
        VALUES
            (:protocol_id, :contract_id, :newest, :date_modified)
        """
        db.execute(query, self.to_json())


@dataclass(slots=True)
class Record:
    value: str
    display_value: str


@dataclass(slots=True)
class EdoContract:
    contract_id: str
    reg_number: str
    contract_type: str
    reg_date: date
    download_path: str
    save_folder: str

    def __hash__(self) -> int:
        return hash((self.contract_id,))

    def to_json(self) -> Dict[str, Union[str, float, None]]:
        return {
            "id": self.contract_id,
            "reg_number": self.reg_number,
            "contract_type": self.contract_type,
            "reg_date": self.reg_date,
            "download_path": self.download_path,
            "save_folder": self.save_folder,
            "date_modified": datetime.now().isoformat(),
        }

    def save(self, db: DatabaseManager) -> None:
        query = """
            INSERT OR REPLACE INTO edo_contracts
                (id, reg_number, contract_type, reg_date, 
                download_path, save_folder, date_modified)
            VALUES
                (:id, :reg_number, :contract_type, :reg_date, 
                :download_path, :save_folder, :date_modified)
        """
        db.execute(query, self.to_json())


@dataclass(slots=True)
class ParseContract:
    contract_id: str
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    loan_amount: Optional[float] = None
    iban: Optional[str] = None
    error: Optional[str] = None

    def __hash__(self) -> int:
        return hash((self.contract_id,))

    def to_json(self) -> Dict[str, Union[str, float, None]]:
        return {
            "id": self.contract_id,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "loan_amount": self.loan_amount,
            "iban": json.dumps(self.iban),
            "error": self.error,
            "date_modified": datetime.now().isoformat(),
        }

    def save(self, db: DatabaseManager) -> None:
        query = """
        INSERT OR REPLACE INTO parse_contracts
            (id, start_date, end_date, loan_amount, iban, error, date_modified)
        VALUES
            (:id, :start_date, :end_date, :loan_amount, :iban, :error, :date_modified)
        """
        db.execute(query, self.to_json())


@dataclass(slots=True)
class Bank:
    bank_id: str
    bank: str
    year_count: Optional[int]

    def __hash__(self) -> int:
        return hash((self.bank_id,))

    def to_json(self) -> Dict[str, Union[str, float, None]]:
        return {
            "bank_id": self.bank_id,
            "bank": self.bank,
            "year_count": self.year_count,
        }

    def save(self, db: DatabaseManager) -> None:
        query = """
        INSERT OR REPLACE INTO banks (bank_id, bank, year_count)
        VALUES (:bank_id, :bank, :year_count)
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
            "date_modified": datetime.now().isoformat(),
        }

    def save(self, db: DatabaseManager) -> None:
        query = """
        INSERT OR REPLACE INTO crm_contracts
            (id, project_id, project, customer, 
            customer_id, bank_id, date_modified)
        VALUES
            (:id, :project_id, :project, :customer, 
            :customer_id, :bank_id, :date_modified)
        """
        db.execute(query, self.to_json())


@dataclass(slots=True)
class SubsidyContract:
    contract_id: str
    reg_number: str
    contract_type: str
    reg_date: str
    download_path: str
    save_folder: str
    project_id: Optional[str] = None
    bank: Optional[Record] = None
    project: Optional[Record] = None
    customer: Optional[Record] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    loan_amount: Optional[float] = None
    iban: Optional[str] = None
    protocol_ids: List[str] = field(default_factory=list)
    interest_rates: List[InterestRate] = field(default_factory=list)
    error: Optional[str] = None

    def save(self, db: DatabaseManager) -> None:
        # noinspection PyTypeChecker
        json_blob = json.dumps(
            asdict(self), ensure_ascii=False, indent=2, cls=CustomJSONEncoder
        )
        contract_blob = pickle.dumps(self, protocol=pickle.HIGHEST_PROTOCOL)

        db.execute(
            """
                INSERT OR REPLACE INTO contracts
                    (id, reg_date, date_modified, json, contract, error)
                VALUES
                    (?, ?, ?, ?, ?, ?)
            """,
            (
                self.contract_id,
                self.reg_date,
                datetime.now().isoformat(),
                json_blob,
                contract_blob,
                self.error,
            ),
        )

    def __hash__(self) -> int:
        return hash((self.contract_id, self.reg_number))

    @classmethod
    def load(cls, folder: Path) -> Optional["SubsidyContract"]:
        pkl_path = folder / "contract.pkl"
        if pkl_path.exists():
            with pkl_path.open("rb") as f:
                contract: SubsidyContract = pickle.load(f)
            return contract

        json_path = folder / "contract.json"
        if json_path.exists():
            with json_path.open("r", encoding="utf-8") as f:
                contract_data: Dict[str, Any] = json.load(f)
            contract = SubsidyContract(**contract_data)
            return contract


def contract_count(db: DatabaseManager) -> int:
    query = """
        SELECT COUNT(*) FROM contracts
        WHERE DATE(date_modified) = ? AND error IS NULL
    """
    result = db.execute(query, (date.today().isoformat(),), fetch_one=True)
    return result[0] if result else 0


def iter_contracts(
    db: DatabaseManager, keys: List[str], table: str, additional_condition: str = ""
) -> Generator[SubsidyContract, None, None]:
    columns = ", ".join(keys)
    query = f"""
        SELECT {columns} FROM {table}
        WHERE DATE(date_modified) = ? {additional_condition}
    """
    contracts = db.execute(query, (date.today().isoformat(),))
    for contract_blob, json_blob in contracts:
        try:
            contract: SubsidyContract = pickle.loads(contract_blob)
        except AttributeError:
            # noinspection PyTypeChecker
            valid_fields = [f.name for f in fields(SubsidyContract)]
            contract_data = {
                key: value
                for key, value in json.loads(json_blob).items()
                if key in valid_fields
            }
            contract: SubsidyContract = SubsidyContract(**contract_data)

        yield contract
