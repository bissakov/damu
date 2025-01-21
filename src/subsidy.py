import json
import pickle
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

HEADER_MAPPING = {
    "contract_id": "contract_id",
    "Рег.№": "reg_number",
    "Тип договора": "contract_type",
    "Состояние": "status",
    "Дата создания": "creation_date",
    "Сумма договорa числовое": "contract_amount_numeric",
    "Рег. дата": "reg_date",
    "Контрагент": "counterparty",
    "Контрагенты все участники": "all_counterparties",
    "Заемщик": "borrower",
    "download_path": "download_path",
}


@dataclass(slots=True)
class SubsidyContract:
    contract_id: str
    reg_number: str
    contract_type: str
    status: str
    creation_date: str
    contract_amount_numeric: str
    reg_date: str
    counterparty: str
    all_counterparties: str
    borrower: str
    download_path: str
    save_folder: str
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    loan_amount: Optional[float] = None
    protocol_ids: List[str] = field(default_factory=list)
    ibans: List[str] = field(default_factory=list)
    data: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, str]:
        contract = asdict(self)
        if isinstance(contract["start_date"], date):
            contract["start_date"] = self.start_date.isoformat()
        if isinstance(contract["end_date"], date):
            contract["end_date"] = self.end_date.isoformat()
        return contract

    def save(self) -> None:
        json_path = Path(self.save_folder) / "contract.json"
        pkl_path = Path(self.save_folder) / "contract.pkl"
        with json_path.open("w", encoding="utf-8") as f1, pkl_path.open("wb") as f2:
            json.dump(self.to_dict(), f1, ensure_ascii=False, indent=2)
            pickle.dump(self, f2)

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
                contract_data = json.load(f)
            contract = SubsidyContract(**contract_data)
            return contract


def contract_count(root_folder: Path) -> int:
    return sum(1 for f in root_folder.iterdir() if f.is_dir())


def iter_contracts(
    root_folder: Path,
) -> Generator[Optional[SubsidyContract], None, None]:
    for folder in root_folder.iterdir():
        if not folder.is_dir():
            continue

        contract = SubsidyContract.load(folder)
        yield contract


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
