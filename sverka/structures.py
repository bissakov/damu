import dataclasses
import json
import re
from pathlib import Path
from typing import Pattern

MONTHS = {
    "янв": "01",
    "фев": "02",
    "мар": "03",
    "апр": "04",
    "май": "05",
    "мая": "05",
    "маю": "05",
    "мае": "05",
    "июн": "06",
    "июл": "07",
    "авг": "08",
    "сен": "09",
    "окт": "10",
    "ноя": "11",
    "дек": "12",
    "қан": "01",
    "қаң": "01",
    "ақп": "02",
    "нау": "03",
    "сәу": "04",
    "cәү": "04",
    "cәу": "04",
    "мам": "05",
    "мау": "06",
    "шіл": "07",
    "там": "08",
    "қыр": "09",
    "қаз": "10",
    "каз": "10",
    "қар": "11",
    "жел": "12",
}


RE_FILE_CONTENTS = re.compile(
    r"((бір бөлігін субсидиялау туралы)|(договор субсидирования)|(субсидиялаудың шарты))",
    re.IGNORECASE,
)
RE_WRONG_CONTENTS = re.compile(r"дополнительное соглашение", re.IGNORECASE)
RE_JOIN_CONTENTS = re.compile(r"договор\w? *присоединени\w", re.IGNORECASE)
RE_PROTOCOL_ID = re.compile(r"№?.?(\d{6})")
RE_IBAN = re.compile(r"коды?:?.+?(KZ[0-9A-Z]{18})", re.IGNORECASE)
RE_PRIMARY_COLUMN = re.compile(
    r"((дата *погашени\w+ *основно\w+ *долга)|(негізгі *борышты *өтеу))",
    re.IGNORECASE,
)
RE_SECONDARY_COLUMN = re.compile(
    r"((сумма *остатка *основного *долга)|(негізгі *борыш\w* *қалды\w* *сомасы))",
    re.IGNORECASE,
)
RE_ALPHA_LETTERS = re.compile(r"[а-яәғқңөұүһі]", re.IGNORECASE)
RE_KZ_LETTERS = re.compile(r"[әғқңөұүһі]", re.IGNORECASE)
RE_FLOAT_NUMBER_FULL = re.compile(r"^[\d ., ]+$")
RE_FLOAT_NUMBER = re.compile(r"([\d ., ]+)")
RE_NUMBER = re.compile(r"(\d+)")
RE_START_DATE = re.compile(r"^9\.")
RE_END_DATES = [
    re.compile(r"^18\."),
    re.compile(r"^30\."),
    re.compile(r"^19\."),
]
RE_JOIN_DATE_RUS = re.compile(
    r"дата ?[\w\s]+ ?субсидирования\D+ ?(\d+\.\d+\.\d+)",
    re.IGNORECASE,
)
RE_JOIN_DATE_KAZ = re.compile(
    r"күні ?субсидиялау\D+ ?(\d+\.\d+\.\d+)", re.IGNORECASE
)
RE_COMPLEX_DATE = re.compile(r"(((\d{2,}) +(\w+) +(\w+) +(\w+))|(\d+.\d+.\d+))")
RE_WHITESPACE = re.compile(r"\s+")
RE_DATE_SEPARATOR = re.compile(r"[. /-]")
RE_INTEREST_DATES = re.compile(r"«?(\d{2,})»? (\w+) «?(\d+)»? (\w+)")
RE_DATE = re.compile(r"(\d+\.\d+\.\d+)")
RE_INTEREST_RATES1 = re.compile(r"([\d,.]+) ?%? ?\(")
RE_INTEREST_RATES2 = re.compile(r"([\d,.]+) ?%? ?\w")
RE_INTEREST_RATE_PARA = re.compile(r"6\.(.+?)7\. ", re.DOTALL)
RE_JOIN_PROTOCOL_ID_RUS = re.compile(
    r"номер ?и ?дата ?решения ?уполномоченного ?органа ?финансового ?агентства ?.*?(\d{5,})",
    re.IGNORECASE,
)
RE_JOIN_PROTOCOL_ID_KAZ = re.compile(
    r"қаржы ?агенттігінің ?уәкілетті ?органы ?шешімінің ?нөмірі ?және ?күні ?.*?(\d{5,})",
    re.IGNORECASE,
)
RE_JOIN_LOAN_AMOUNT = re.compile(r"([\d., ]{6,})")
RE_JOIN_PROTOCOL_ID_OCR = re.compile(r"(\d{5,})", re.IGNORECASE)
RE_JOIN_PDF_PATH = re.compile(
    r"заявление получателя к договору присоединения", re.IGNORECASE
)


@dataclasses.dataclass
class RegexPatterns:
    file_name: Pattern[str] = re.compile(
        r"((дог\w*.?.суб\w*.?)|(дс))",
        re.IGNORECASE,
    )
    file_contents: Pattern[str] = re.compile(
        r"((бір бөлігін субсидиялау туралы)|(договор субсидирования)|(субсидиялаудың шарты))",
        re.IGNORECASE,
    )
    wrong_contents: Pattern[str] = re.compile(
        r"дополнительное соглашение", re.IGNORECASE
    )
    join_contents: Pattern[str] = re.compile(
        r"договор\w? *присоединени\w", re.IGNORECASE
    )
    protocol_id: Pattern[str] = re.compile(r"№?.?(\d{6})")
    iban: Pattern[str] = re.compile(
        r"коды?:?.+?(KZ[0-9A-Z]{18})", re.IGNORECASE
    )
    primary_column: Pattern[str] = re.compile(
        r"((дата *погашени\w+ *основно\w+ *долга)|(негізгі *борышты *өтеу))",
        re.IGNORECASE,
    )
    secondary_column: Pattern[str] = re.compile(
        r"((сумма *остатка *основного *долга)|(негізгі *борыш\w* *қалды\w* *сомасы))",
        re.IGNORECASE,
    )
    alpha_letters: Pattern[str] = re.compile(r"[а-яәғқңөұүһі]", re.IGNORECASE)
    kz_letters: Pattern[str] = re.compile(r"[әғқңөұүһі]", re.IGNORECASE)
    float_number_full: Pattern[str] = re.compile(r"^[\d ., ]+$")
    float_number: Pattern[str] = re.compile(r"([\d ., ]+)")
    number: Pattern[str] = re.compile(r"(\d+)")
    start_date: Pattern[str] = re.compile(r"^9\.")
    end_dates: list[Pattern[str]] = dataclasses.field(
        default_factory=lambda: [
            re.compile(r"^18\."),
            re.compile(r"^30\."),
            re.compile(r"^19\."),
        ]
    )
    join_date_rus: Pattern[str] = re.compile(
        r"дата ?[\w\s]+ ?субсидирования\D+ ?(\d+\.\d+\.\d+)",
        re.IGNORECASE,
    )
    join_date_kaz: Pattern[str] = re.compile(
        r"күні ?субсидиялау\D+ ?(\d+\.\d+\.\d+)", re.IGNORECASE
    )
    complex_date: Pattern[str] = re.compile(
        r"(((\d{2,}) +(\w+) +(\w+) +(\w+))|(\d+.\d+.\d+))"
    )
    whitespace: Pattern[str] = re.compile(r"\s+")
    date_separator: Pattern[str] = re.compile(r"[. /-]")
    interest_dates: Pattern[str] = re.compile(
        r"«?(\d{2,})»? (\w+) «?(\d+)»? (\w+)"
    )
    date: Pattern[str] = re.compile(r"(\d+\.\d+\.\d+)")
    interest_rates1: Pattern[str] = re.compile(r"([\d,.]+) ?%? ?\(")
    interest_rates2: Pattern[str] = re.compile(r"([\d,.]+) ?%? ?\w")
    interest_rate_para: Pattern[str] = re.compile(r"6\.(.+?)7\. ", re.DOTALL)
    join_protocol_id_rus = re.compile(
        r"номер ?и ?дата ?решения ?уполномоченного ?органа ?финансового ?агентства ?.*?(\d{5,})",
        re.IGNORECASE,
    )
    join_protocol_id_kaz = re.compile(
        r"қаржы ?агенттігінің ?уәкілетті ?органы ?шешімінің ?нөмірі ?және ?күні ?.*?(\d{5,})",
        re.IGNORECASE,
    )
    join_loan_amount = re.compile(r"([\d., ]{6,})")
    join_protocol_id_ocr = re.compile(r"(\d{5,})", re.IGNORECASE)
    join_pdf_path = re.compile(
        r"заявление получателя к договору присоединения", re.IGNORECASE
    )


class Registry:
    def __init__(self, download_folder: Path) -> None:
        self.download_folder: Path = download_folder
        self.download_folder.mkdir(parents=True, exist_ok=True)

        self.resources_folder: Path = Path("resources")
        self.database = self.resources_folder / "database.sqlite"

        self.schema_json_path = self.resources_folder / "schemas.json"

        mappings_json_path = self.resources_folder / "mappings.json"
        with mappings_json_path.open("r", encoding="utf-8") as f:
            self.mappings = json.load(f)

        banks_json_path = self.resources_folder / "banks.json"
        with banks_json_path.open("r", encoding="utf-8") as f:
            self.banks = json.load(f)
