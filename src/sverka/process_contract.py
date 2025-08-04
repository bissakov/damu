from __future__ import annotations

import dataclasses
import os
import traceback
from contextlib import suppress
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast

from sverka.crm import fetch_crm_data_one, is_first_protocol_id_valid
from sverka.error import CRMNotFoundError, ProtocolDateNotInRangeError
from sverka.macros import process_macro
from sverka.parser import parse_document
from sverka.subsidy import date_to_str
from utils.office import docx_to_pdf
from utils.utils import safe_extract

if TYPE_CHECKING:
    from logging import Logger

    from sverka.crm import CRM
    from sverka.edo import EDO
    from sverka.structures import Registry
    from utils.db_manager import DatabaseManager


def iso_to_standard(dt: str) -> str:
    if isinstance(dt, date):
        return dt.strftime("%d.%m.%Y")
    if dt[2] == "." and dt[5] == ".":
        return dt
    return datetime.fromisoformat(dt).strftime("%d.%m.%Y")


@dataclasses.dataclass(slots=True)
class _Contract:
    contract_id: str
    contract_type: str
    contragent: str
    project: str
    bank: str
    credit_purpose: str
    repayment_procedure: str
    loan_amount: float
    subsid_amount: float
    protocol_date: str
    vypiska_date: str
    decision_date: str
    settlement_date: int
    iban: str
    ds_id: str
    ds_date: str
    dbz_id: str
    dbz_date: str
    start_date: str
    end_date: str
    protocol_id: str
    sed_number: str
    document_path: Path
    macro_path: Path
    document_pdf_path: Path | None = None
    protocol_pdf_path: Path | None = None

    def __post_init__(self) -> None:
        self.protocol_date = iso_to_standard(self.protocol_date).replace(
            ".", ""
        )
        self.vypiska_date = iso_to_standard(self.vypiska_date).replace(".", "")
        self.ds_date = iso_to_standard(self.ds_date).replace(".", "")
        self.dbz_date = iso_to_standard(self.dbz_date).replace(".", "")
        self.start_date = iso_to_standard(self.start_date).replace(".", "")
        self.end_date = iso_to_standard(self.end_date).replace(".", "")
        self.decision_date = iso_to_standard(self.decision_date).replace(
            ".", ""
        )

        contract_folder = (
            "downloads/zanesenie"
            / Path(str(os.environ["today"]))
            / self.contract_id
        ).absolute()
        with suppress(FileNotFoundError):
            self.protocol_pdf_path = next(
                (contract_folder / "vypiska").iterdir(), None
            )

        document_folder = contract_folder / "documents"

        self.document_path = document_folder / Path(self.document_path)

        self.document_pdf_path = self.document_path.with_suffix(".pdf")
        if not self.document_pdf_path.exists():
            docx_to_pdf(str(self.document_path), str(self.document_pdf_path))

        assert isinstance(self.macro_path, bytes)
        macro_path = document_folder / "macro.xlsx"
        with macro_path.open("wb") as f:
            f.write(self.macro_path)

        self.macro_path = macro_path

        protocol_ids = self.protocol_id.split(";")
        if "Транш" in self.contract_type:
            self.protocol_id = protocol_ids[0]
        else:
            self.protocol_id = protocol_ids[-1]


class Contract:
    def __init__(
        self,
        contract_id: str,
        contract_type: str,
        contragent: str,
        contragent_id: str,
        project: str,
        bank: str,
        credit_purpose: str,
        repayment_procedure: str,
        loan_amount: float,
        subsid_amount: float,
        protocol_date: date,
        vypiska_date: date,
        decision_date: date,
        settlement_date: int,
        iban: str,
        ds_id: str,
        ds_date: date,
        dbz_id: str,
        dbz_date: date,
        start_date: date,
        end_date: date,
        contract_start_date: date | None,
        contract_end_date: date | None,
        protocol_id: str,
        sed_number: str,
        document_name: str,
        region: str,
    ) -> None:
        self.contract_id = contract_id
        self.contract_type = contract_type
        self.contragent = contragent
        self.contragent_id = contragent_id
        self.project = project
        self.bank = bank
        self.credit_purpose = credit_purpose
        self.repayment_procedure = repayment_procedure
        self.loan_amount = loan_amount
        self.subsid_amount = subsid_amount
        self.protocol_date = protocol_date.strftime("%d.%m.%Y").replace(".", "")
        self.vypiska_date = vypiska_date.strftime("%d.%m.%Y").replace(".", "")
        self.decision_date = decision_date.strftime("%d.%m.%Y").replace(".", "")
        self.settlement_date = settlement_date
        self.iban = iban
        self.ds_id = ds_id
        self.ds_date = ds_date.strftime("%d.%m.%Y").replace(".", "")
        self.dbz_id = dbz_id
        self.dbz_date = dbz_date.strftime("%d.%m.%Y").replace(".", "")
        self.start_date = start_date.strftime("%d.%m.%Y").replace(".", "")
        self.end_date = end_date.strftime("%d.%m.%Y").replace(".", "")

        if contract_start_date:
            self.contract_start_date = contract_start_date.strftime(
                "%d.%m.%Y"
            ).replace(".", "")
        else:
            self.contract_start_date = None

        if contract_end_date:
            self.contract_end_date = contract_end_date.strftime(
                "%d.%m.%Y"
            ).replace(".", "")
        else:
            self.contract_end_date = None

        self.protocol_id = protocol_id
        self.sed_number = sed_number
        self.region = region

        contract_folder = (
            "downloads/zanesenie"
            / Path(str(os.environ["today"]))
            / self.contract_id
        ).absolute()
        with suppress(FileNotFoundError):
            self.protocol_pdf_path = next(
                (contract_folder / "vypiska").iterdir(), None
            )

        document_folder = contract_folder / "documents"

        self.document_path = Path(f"{document_folder}/{document_name}")

        self.document_pdf_path = self.document_path.with_suffix(".pdf")
        if not self.document_pdf_path.exists():
            docx_to_pdf(str(self.document_path), str(self.document_pdf_path))

        self.macro_path = document_folder / "macro.xlsx"

        protocol_ids = self.protocol_id.split(";")
        if "Транш" in self.contract_type:
            self.protocol_id = protocol_ids[0]
        else:
            self.protocol_id = protocol_ids[-1]

    def __repr__(self) -> str:
        return (
            f"Contract(contract_id={self.contract_id!r}, contract_type={self.contract_type!r}, "
            f"contragent={self.contragent!r}, contragent_id={self.contragent_id!r}, project={self.project!r}, bank={self.bank!r}, "
            f"credit_purpose={self.credit_purpose!r}, repayment_procedure={self.repayment_procedure!r}, "
            f"loan_amount={self.loan_amount!r}, subsid_amount={self.subsid_amount!r}, "
            f"protocol_date={self.protocol_date!r}, vypiska_date={self.vypiska_date!r}, "
            f"decision_date={self.decision_date!r}, settlement_date={self.settlement_date!r}, "
            f"iban={self.iban!r}, ds_id={self.ds_id!r}, ds_date={self.ds_date!r}, dbz_id={self.dbz_id!r}, "
            f"dbz_date={self.dbz_date!r}, start_date={self.start_date!r}, end_date={self.end_date!r}, "
            f"contract_start_date={self.contract_start_date!r}, contract_end_date={self.contract_end_date!r}, "
            f"protocol_id={self.protocol_id!r}, sed_number={self.sed_number!r}, "
            f"document_path={self.document_path!r}, macro_path={self.macro_path!r}, "
            f"document_pdf_path={self.document_pdf_path!r}, protocol_pdf_path={self.protocol_pdf_path!r})"
        )


def process_contract(
    logger: Logger,
    db: DatabaseManager,
    contract_id: str,
    edo: EDO,
    crm: CRM,
    registry: Registry,
) -> tuple[Contract | None, str | None]:
    logger.info(f"Trying to find a row for {contract_id=!r}")

    save_folder = edo.download_folder / contract_id
    documents_folder = save_folder / "documents"
    documents_folder.mkdir(parents=True, exist_ok=True)
    macros_folder = save_folder / "macros"
    macros_folder.mkdir(parents=True, exist_ok=True)

    soup, basic_contract, edo_contract = edo.get_basic_contract_data(
        contract_id=contract_id, db=db
    )
    if not basic_contract:
        reply = (
            "Не найден приложенный документ по данной ссылке - "
            f"/workflow/document/view/beff8bc1-14fd-4657-86f1-55797181018f/{contract_id}"
        )
        return None, reply

    if not edo_contract.ds_date:
        reply = (
            "Не найдена дата подписания по данной ссылке - "
            f"/workflow/document/view/beff8bc1-14fd-4657-86f1-55797181018f/{contract_id}"
        )
        return None, reply

    logger.info(f"{basic_contract.contract_type=!r}")

    if basic_contract.contract_type in [
        "Дополнительное соглашение к договору субсидирования",
        "Транш к договору присоединения",
    ]:
        reply = (
            f"Не поддерживаемый тип договора - {basic_contract.contract_type}"
        )
        return None, reply

    edo.download_file(contract_id=contract_id)
    download_info = edo.get_signed_contract_url(
        contract_id=contract_id, soup=soup
    )
    download_statuses = [
        edo.download_signed_contract(url, fpath) for url, fpath in download_info
    ]
    if not all(download_statuses):
        reply = "Не удалось скачать подписанный ЭЦП договор."
        return None, reply

    safe_extract(
        save_folder / "contract.zip", documents_folder=documents_folder
    )

    parse_contract = parse_document(
        contract_id=contract_id,
        contract_type=basic_contract.contract_type,
        download_folder=registry.download_folder,
        db=db,
    )
    if parse_contract.error and parse_contract.error.traceback:
        reply = f"{parse_contract.error.human_readable}\nНе удалось обработать договор."
        return None, reply

    protocol_ids_str = cast(str, parse_contract.protocol_id)
    protocol_ids = protocol_ids_str.split(";")
    start_date = cast(date, parse_contract.start_date)
    end_date = cast(date, parse_contract.end_date)

    start_date_str = date_to_str(start_date)
    end_date_str = date_to_str(end_date)

    assert start_date_str
    assert end_date_str

    if (
        not parse_contract.contract_start_date
        or not parse_contract.contract_end_date
    ):
        logger.error(
            f"{parse_contract.contract_start_date=!r}, {parse_contract.contract_end_date=!r}"
        )
        reply = "Сроки контракта не найдены в файле договора"
        return None, reply

    response = crm.client.get(crm.base_url)
    if response.is_error:
        logger.error("CRM is not available")
        reply = "CRM на данный момент не доступен"
        return None, reply

    crm_contract = fetch_crm_data_one(
        crm=crm,
        db=db,
        contract_id=contract_id,
        protocol_ids=protocol_ids,
        start_date=start_date_str,
        end_date=end_date_str,
        registry=registry,
        dbz_id=edo_contract.dbz_id,
        dbz_date=edo_contract.dbz_date,
    )

    if crm_contract.error and crm_contract.error.traceback:
        reply = crm_contract.error.human_readable
        return None, reply

    banks = list(registry.banks.keys())
    if crm_contract.bank not in banks or crm_contract.bank_id not in banks:
        reply = f"Банк/лизинг {crm_contract.bank!r} не поддерживается."
        return None, reply

    macro = process_macro(
        contract_id=contract_id,
        db=db,
        macros_folder=macros_folder,
        documents_folder=documents_folder,
        raise_exc=False,
        skip_pretty_macro=True,
    )
    macro.error.save(db)
    macro.save(db)

    if macro.error and macro.error.traceback:
        reply = macro.error.human_readable
        if "Банк" in reply and "не поддерживается" in reply:
            reply = reply.replace("для сверки", "для занесения")
        return None, reply

    check_date = "Транш" not in edo_contract.contract_type
    if check_date and len(protocol_ids) > 2:
        try:
            is_first_protocol_id_valid(crm=crm, protocol_id=protocol_ids[0])
        except (CRMNotFoundError, ProtocolDateNotInRangeError) as err:
            logger.exception(err)
            logger.error(f"CRM - ERROR - {crm_contract.project_id=} - {err!r}")
            crm_contract.error.traceback = f"{err!r}\n{traceback.format_exc()}"
            crm_contract.error.error = err
            crm_contract.error.human_readable = (
                crm_contract.error.get_human_readable()
            )
            crm_contract.error.save(db)
            crm_contract.save(db)

            reply = crm_contract.error.human_readable
            return None, reply

    contract = Contract(
        contract_id=contract_id,
        contract_type=edo_contract.contract_type,
        contragent=crm_contract.contragent,
        contragent_id=crm_contract.customer_id,
        project=crm_contract.project,
        bank=crm_contract.bank,
        credit_purpose=crm_contract.credit_purpose,
        repayment_procedure=crm_contract.repayment_procedure,
        loan_amount=parse_contract.loan_amount,
        subsid_amount=crm_contract.subsid_amount,
        protocol_date=crm_contract.protocol_date,
        vypiska_date=crm_contract.vypiska_date,
        decision_date=crm_contract.decision_date,
        settlement_date=parse_contract.settlement_date,
        iban=parse_contract.iban,
        ds_id=edo_contract.ds_id,
        ds_date=edo_contract.ds_date,
        dbz_id=edo_contract.ds_id,
        dbz_date=edo_contract.ds_date,
        start_date=parse_contract.start_date,
        end_date=parse_contract.end_date,
        contract_start_date=parse_contract.contract_start_date,
        contract_end_date=parse_contract.contract_end_date,
        protocol_id=parse_contract.protocol_id,
        sed_number=edo_contract.sed_number,
        document_name=parse_contract.file_name,
        region=crm_contract.region,
    )

    return contract, None
