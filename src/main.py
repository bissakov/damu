import inspect
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import dotenv
import pytz

project_folder = Path(__file__).resolve().parent.parent
os.environ["project_folder"] = str(project_folder)
os.chdir(project_folder)
sys.path.append(str(project_folder))

from src.structures import Registry
from src.subsidy import date_to_str
from src.utils.utils import safe_extract

from src.crm import CRM, fetch_crm_data_one
from src.edo import EDO, EdoNotification
from src.parser import parse_document

from src.macros import process_macro
from src.utils.db_manager import DatabaseManager
from src.utils.logger import setup_logger

today = datetime.now(pytz.timezone("Asia/Almaty")).date()
os.environ["today"] = today.isoformat()
setup_logger(today)

logger = logging.getLogger("DAMU")


def reply_to_notification(edo: EDO, notification: EdoNotification, reply: str) -> None:
    reply = inspect.cleandoc(reply)
    logger.info(f"Notification reply - {reply!r}")
    if not edo.reply_to_notification(notification=notification, reply=reply):
        logger.error("Unable to reply to the notification...")
        raise Exception("Unable to reply to the notification...")
    if not edo.mark_as_read(notif_id=notification.notif_id):
        logger.error("Unable to mark the notification as read...")
        raise Exception("Unable to mark the notification as read...")


def process_notification(
    db: DatabaseManager, edo: EDO, crm: CRM, registry: Registry, notification: EdoNotification
) -> str:
    document_url = edo.get_attached_document_url(notification)
    contract_id = document_url.split("/")[-1]

    logger.info(f"Trying to find a row for {contract_id=!r}")

    save_folder = edo.download_folder / contract_id
    documents_folder = save_folder / "documents"
    documents_folder.mkdir(parents=True, exist_ok=True)

    soup, basic_contract = edo.get_basic_contract_data(contract_id=contract_id)

    logger.info(f"{basic_contract.contract_type=!r}")

    edo.find_contract(basic_contract_data=basic_contract, db=db)
    edo.download_file(contract_id=contract_id)
    download_info = edo.get_signed_contract_url(contract_id=contract_id, soup=soup)
    download_statuses = [edo.download_signed_contract(url, fpath) for url, fpath in download_info]
    if not all(download_statuses):
        reply = (
            f"Тип договора - {basic_contract.contract_type!r}\n"
            f"Не удалось скачать подписанный ЭЦП договор.\n\n"
            "Список раннее проделанной успешной работы:\n"
            "1) Найдена страница прикрепленного договора\n"
            "2) Скачаны все прикрепленные файлы, без подписанного договора"
        )
        return reply

    safe_extract(save_folder / "contract.zip", documents_folder=documents_folder)

    parse_contract = parse_document(
        contract_id=contract_id,
        contract_type=basic_contract.contract_type,
        download_folder=registry.download_folder,
        patterns=registry.patterns,
        db=db,
    )
    if parse_contract.error.traceback:
        reply = (
            f"Тип договора - {basic_contract.contract_type!r}\n"
            f"{parse_contract.error.human_readable}\n\n"
            f"Не удалось обработать договор {basic_contract.contract_type!r}.\n"
            "Список раннее проделанной успешной работы:\n"
            "1) Найдена страница прикрепленного договора\n"
            "2) Скачаны все прикрепленные файлы, включая подписанный договор.\n"
            "3) Обработан ДС, выгружены данные, проверены графики погашения в двух языках, сверены IBAN кода"
        )
        return reply

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

    if crm_contract.error.traceback:
        reply = (
            f"Тип договора - {basic_contract.contract_type!r}\n"
            f"{crm_contract.error.human_readable}\n\n"
            "Не удалось выгрузить данные из CRM.\n"
            "Список раннее проделанной успешной работы:\n"
            "1) Найдена страница прикрепленного договора\n"
            "2) Скачаны все прикрепленные файлы, включая подписанный договор\n"
            "3) Обработан ДС, выгружены данные, проверены графики погашения в двух языках, сверены IBAN кода"
        )
        return reply

    macro = process_macro(contract_id=contract_id, db=db)
    macro.error.save(db)
    macro.save(db)

    if macro.error.traceback:
        reply = (
            f"Тип договора - {basic_contract.contract_type!r}\n"
            f"{macro.error.human_readable}\n\n"
            "Не удалось сформировать макрос.\n"
            "Список ранее проделанной успешной работы:\n"
            "1) Найдена страница прикрепленного договора\n"
            "2) Скачаны все прикрепленные файлы, включая подписанный ДС\n"
            "3) Обработан ДС, выгружены данные, проверены графики погашения в двух языках, сверены IBAN кода\n"
            "4) Найден номер протокола и выгружены данные из CRM, скачан файл выписки в формате PDF\n"
        )
        return reply

    reply = (
        f"Тип договора - {basic_contract.contract_type!r}\n"
        "Не найдено замечаний.\n"
        "Список ранее проделанной успешной работы:\n"
        "1) Найдена страница прикрепленного договора\n"
        "2) Скачаны все прикрепленные файлы, включая подписанный ДС\n"
        "3) Обработан ДС, выгружены данные, проверены графики погашения в двух языках, сверены IBAN кода\n"
        "4) Найден номер протокола и выгружены данные из CRM, скачан файл выписки в формате PDF\n"
        "5) Сформирован макрос и проверерен на грубые расхождения (>1 тиына) и ложные значения"
    )
    return reply


def process_notifications(db: DatabaseManager, edo: EDO, crm: CRM, registry: Registry) -> int:
    with edo:
        notifications = edo.get_notifications()

        logger.info(f"Found {len(notifications)} notifications")
        if not notifications:
            logger.info(f"Nothing to work on - sleeping...")
            return 60

        for notification in notifications:
            logger.info(f"Working on notification {notification.notif_id}")
            try:
                reply = process_notification(
                    db=db, edo=edo, crm=crm, registry=registry, notification=notification
                )
                reply_to_notification(edo=edo, notification=notification, reply=reply)
            except Exception:
                continue

    return 0


def main():
    dotenv.load_dotenv(".env")

    registry = Registry(download_folder=Path(f"downloads/{today}"))

    edo = EDO(
        user=os.environ["EDO_USERNAME"],
        password=os.environ["EDO_PASSWORD"],
        base_url=os.environ["EDO_BASE_URL"],
        download_folder=registry.download_folder,
        user_agent=os.environ["USER_AGENT"],
    )
    crm = CRM(
        user=os.environ["CRM_USERNAME"],
        password=os.environ["CRM_PASSWORD"],
        base_url=os.environ["CRM_BASE_URL"],
        download_folder=registry.download_folder,
        user_agent=os.environ["USER_AGENT"],
        schema_json_path=registry.schema_json_path,
    )

    start_time = time.time()
    max_duration = 12 * 60 * 60

    while True:
        elapsed_time = time.time() - start_time
        logger.info(f"Seconds passed: {int(elapsed_time)}")

        if elapsed_time >= max_duration:
            logger.info("12 hours have passed. Stopping the loop.")
            break

        with DatabaseManager(registry.database) as db:
            duration = process_notifications(db=db, edo=edo, crm=crm, registry=registry)
            logger.info(f"Current batch is processed - sleeping for {duration} sec...")
            time.sleep(duration)


if __name__ == "__main__":
    main()
