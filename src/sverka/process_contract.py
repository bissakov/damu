import traceback
from datetime import date
from logging import Logger
from typing import cast

from sverka.error import CRMNotFoundError, ProtocolDateNotInRangeError
from sverka.crm import CRM, fetch_crm_data_one, is_first_protocol_id_valid
from sverka.edo import EDO
from sverka.macros import process_macro
from sverka.parser import parse_document
from sverka.structures import Registry
from sverka.subsidy import date_to_str
from utils.db_manager import DatabaseManager
from utils.utils import safe_extract


def process_contract(
    logger: Logger,
    db: DatabaseManager,
    contract_id: str,
    edo: EDO,
    crm: CRM,
    registry: Registry,
) -> str:
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
        return reply

    if not edo_contract.ds_date:
        reply = (
            "Не найдена дата подписания по данной ссылке - "
            f"/workflow/document/view/beff8bc1-14fd-4657-86f1-55797181018f/{contract_id}"
        )
        return reply

    logger.info(f"{basic_contract.contract_type=!r}")

    if basic_contract.contract_type in [
        "Дополнительное соглашение к договору субсидирования",
        "Транш к договору присоединения",
    ]:
        reply = (
            f"Не поддерживаемый тип договора - {basic_contract.contract_type}"
        )
        return reply

    edo.download_file(contract_id=contract_id)
    download_info = edo.get_signed_contract_url(
        contract_id=contract_id, soup=soup
    )
    download_statuses = [
        edo.download_signed_contract(url, fpath) for url, fpath in download_info
    ]
    if not all(download_statuses):
        reply = "Не удалось скачать подписанный ЭЦП договор."
        return reply

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
        return reply

    protocol_ids_str = cast(str, parse_contract.protocol_id)
    protocol_ids = protocol_ids_str.split(";")
    start_date = cast(date, parse_contract.start_date)
    end_date = cast(date, parse_contract.end_date)

    start_date_str = date_to_str(start_date)
    end_date_str = date_to_str(end_date)

    assert start_date_str
    assert end_date_str

    with crm:
        response = crm.client.get(crm.base_url)
        if response.is_error:
            logger.error("CRM is not available")
            return "CRM на данный момент не доступен"

        crm_contract = fetch_crm_data_one(
            crm=crm,
            db=db,
            contract_id=contract_id,
            protocol_ids=protocol_ids,
            start_date=start_date_str,
            end_date=end_date_str,
            registry=registry,
            dbz_id=parse_contract.dbz_id,
            dbz_date=parse_contract.dbz_date,
        )

        if crm_contract.error and crm_contract.error.traceback:
            reply = crm_contract.error.human_readable
            return reply

        macro = process_macro(
            contract_id=contract_id,
            db=db,
            macros_folder=macros_folder,
            raise_exc=False,
        )
        macro.error.save(db)
        macro.save(db)

        if macro.error and macro.error.traceback:
            reply = macro.error.human_readable
            if "Банк" in reply and "не поддерживается" in reply:
                reply = reply.replace("для сверки", "для занесения")
            return reply

        check_date = "Транш" not in edo_contract.contract_type
        if check_date and len(protocol_ids) > 2:
            try:
                is_first_protocol_id_valid(crm=crm, protocol_id=protocol_ids[0])
            except (CRMNotFoundError, ProtocolDateNotInRangeError) as err:
                logger.exception(err)
                logger.error(
                    f"CRM - ERROR - {crm_contract.project_id=} - {err!r}"
                )
                crm_contract.error.traceback = (
                    f"{err!r}\n{traceback.format_exc()}"
                )
                crm_contract.error.error = err
                crm_contract.error.human_readable = (
                    crm_contract.error.get_human_readable()
                )
                crm_contract.error.save(db)
                crm_contract.save(db)

                reply = crm_contract.error.human_readable
                return reply

    reply = "Согласовано. Не найдено замечаний."
    return reply
