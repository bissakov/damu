from logging import Logger

from sverka.crm import CRM, fetch_crm_data_one
from sverka.edo import EDO
from sverka.macros import process_macro
from sverka.parser import parse_document
from sverka.structures import Registry
from sverka.subsidy import date_to_str
from utils.db_manager import DatabaseManager
from utils.utils import safe_extract


def process_contract(
    logger: Logger, db: DatabaseManager, contract_id: str, edo: EDO, crm: CRM, registry: Registry
) -> str:
    logger.info(f"Trying to find a row for {contract_id=!r}")

    save_folder = edo.download_folder / contract_id
    documents_folder = save_folder / "documents"
    documents_folder.mkdir(parents=True, exist_ok=True)
    macros_folder = save_folder / "macros"
    macros_folder.mkdir(parents=True, exist_ok=True)

    soup, basic_contract, _ = edo.get_basic_contract_data(contract_id=contract_id, db=db)
    if not basic_contract:
        reply = (
            "Не найден приложенный документ по данной ссылке - "
            f"/workflow/document/view/beff8bc1-14fd-4657-86f1-55797181018f/{contract_id}"
        )
        return reply

    logger.info(f"{basic_contract.contract_type=!r}")

    if basic_contract.contract_type in ["Дополнительное соглашение к договору субсидирования"]:
        reply = f"Не поддерживаемый тип договора - {basic_contract.contract_type}"
        return reply

    edo.download_file(contract_id=contract_id)
    download_info = edo.get_signed_contract_url(contract_id=contract_id, soup=soup)
    download_statuses = [edo.download_signed_contract(url, fpath) for url, fpath in download_info]
    if not all(download_statuses):
        reply = "Не удалось скачать подписанный ЭЦП договор."
        return reply

    safe_extract(save_folder / "contract.zip", documents_folder=documents_folder)

    parse_contract = parse_document(
        contract_id=contract_id,
        contract_type=basic_contract.contract_type,
        download_folder=registry.download_folder,
        db=db,
    )
    if parse_contract.error and parse_contract.error.traceback:
        reply = f"{parse_contract.error.human_readable}\nНе удалось обработать договор."
        return reply

    assert parse_contract.protocol_id
    assert parse_contract.start_date
    assert parse_contract.end_date

    with crm:
        crm_contract = fetch_crm_data_one(
            crm=crm,
            db=db,
            contract_id=contract_id,
            protocol_id=parse_contract.protocol_id,
            start_date=date_to_str(parse_contract.start_date),
            end_date=date_to_str(parse_contract.end_date),
            registry=registry,
        )

    if crm_contract.error and crm_contract.error.traceback:
        reply = f"{crm_contract.error.human_readable}\nНе удалось выгрузить данные из CRM."
        return reply

    macro = process_macro(contract_id=contract_id, db=db, macros_folder=macros_folder)
    macro.error.save(db)
    macro.save(db)

    if macro.error and macro.error.traceback:
        reply = f"Не согласовано. {macro.error.human_readable}"
        return reply

    reply = "Согласовано. Не найдено замечаний."
    return reply
