import json
import pickle
from dataclasses import asdict, dataclass, field, fields, is_dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

from src.utils.collections import batched
from src.utils.db_manager import DatabaseManager

HEADER_MAPPING = {
    "contract_id": "contract_id",
    "Рег.№": "reg_number",
    "Тип договора": "contract_type",
    "Рег. дата": "reg_date",
    "download_path": "download_path",
}


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


@dataclass(slots=True)
class Record:
    value: str
    display_value: str


@dataclass(slots=True)
class ParseContract:
    contract_id: str
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    loan_amount: Optional[float] = None
    iban: Optional[str] = None
    protocol_ids: List[str] = field(default_factory=list)
    interest_rates: List[InterestRate] = field(default_factory=list)
    error: Optional[str] = None

    def __hash__(self) -> int:
        return hash((self.contract_id,))


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


def iter_contracts_batched(
    db: DatabaseManager, batch_size: int = 10
) -> Generator[Tuple, None, None]:
    query = """
        SELECT contract, json FROM contracts
        WHERE DATE(date_modified) = ? AND error IS NULL
    """
    contracts = db.execute(query, (date.today().isoformat(),))
    batches = batched(contracts, batch_size)

    for batch in batches:
        yield batch


def map_row_to_subsidy_contract(
    contract_id: str, download_folder: Path, row: Dict[str, str]
) -> SubsidyContract:
    kwargs = {
        HEADER_MAPPING[header]: value
        for header, value in row.items()
        if header in HEADER_MAPPING
    }
    kwargs["save_folder"] = (download_folder / contract_id).as_posix()
    return SubsidyContract(contract_id=contract_id, **kwargs)
