import dataclasses
import json
import re
from pathlib import Path


@dataclasses.dataclass
class RegexPatterns:
    months: dict[str, str]
    file_name: re.Pattern = re.compile(
        r"((дог\w*.?.суб\w*.?)|(дс))",
        re.IGNORECASE,
    )
    file_contents: re.Pattern = re.compile(
        r"((бір бөлігін субсидиялау туралы)|(договор субсидирования)|(субсидиялаудың шарты))",
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
    end_dates: list[re.Pattern] = dataclasses.field(
        default_factory=lambda: [
            re.compile(r"^18\."),
            re.compile(r"^30\."),
            re.compile(r"^19\."),
        ]
    )
    complex_date: re.Pattern = re.compile(r"(((\d{2,}) +(\w+) +(\w+) +(\w+))|(\d+.\d+.\d+))")
    whitespace: re.Pattern = re.compile(r"\s+")
    date_separator: re.Pattern = re.compile(r"[. /-]")
    interest_dates: re.Pattern = re.compile(r"«?(\d{2,})»? (\w+) «?(\d+)»? (\w+)")
    date: re.Pattern = re.compile(r"(\d+\.\d+\.\d+)")
    interest_rates1: re.Pattern = re.compile(r"([\d,.]+) ?%? ?\(")
    interest_rates2: re.Pattern = re.compile(r"([\d,.]+) ?%? ?\w")
    interest_rate_para: re.Pattern = re.compile(r"6\.(.+?)7\. ", re.DOTALL)


@dataclasses.dataclass(slots=True)
class Registry:
    download_folder: Path
    resources_folder: Path | None = None
    database: Path | None = None
    schema_json_path: Path | None = None
    patterns: RegexPatterns | None = None
    mappings: dict[str, dict[str, str]] | None = None
    banks: dict[str, int | None] | None = None

    def __post_init__(self) -> None:
        self.resources_folder = Path("resources")
        self.download_folder.mkdir(parents=True, exist_ok=True)
        self.database = self.resources_folder / "database.sqlite"

        self.schema_json_path = self.resources_folder / "schemas.json"

        months_json_path = self.resources_folder / "months.json"
        with months_json_path.open("r", encoding="utf-8") as f:
            months = json.load(f)
        self.patterns = RegexPatterns(months=months)

        mappings_json_path = self.resources_folder / "mappings.json"
        with mappings_json_path.open("r", encoding="utf-8") as f:
            self.mappings = json.load(f)

        banks_json_path = self.resources_folder / "banks.json"
        with banks_json_path.open("r", encoding="utf-8") as f:
            self.banks = json.load(f)
