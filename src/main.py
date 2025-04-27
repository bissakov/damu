import inspect
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from time import sleep

import dotenv
import pytz

from src.structures import Registry
from src.subsidy import date_to_str
from src.utils.utils import safe_extract

project_folder = Path(__file__).resolve().parent.parent
os.environ["project_folder"] = str(project_folder)
os.chdir(project_folder)
sys.path.append(str(project_folder))

from src.crm import CRM, fetch_crm_data_one
from src.edo import EDO, EdoNotification
from src.parser import parse_document

from src.macros import process_macro
from src.utils.db_manager import DatabaseManager
from src.utils import logger


def reply_to_notification(edo: EDO, notification: EdoNotification, reply: str) -> None:
    reply = inspect.cleandoc(reply)
    logging.info(f"Notification reply - {reply!r}")
    # if not edo.reply_to_notification(notification=notification, reply=reply):
    #     logging.error("Unable to reply to the notification...")
    #     raise Exception("Unable to reply to the notification...")
    # if not edo.mark_as_read(notif_id=notification.notif_id):
    #     logging.error("Unable to mark the notification as read...")
    #     raise Exception("Unable to mark the notification as read...")


def process_notifications(db: DatabaseManager, edo: EDO, crm: CRM, registry: Registry) -> None:
    with edo:
        notifications = edo.get_notifications()

        logging.info(f"Found {len(notifications)} notifications")
        if not notifications:
            logging.info(f"Nothing to work on - sleeping...")
            return

        for notification in notifications:
            logging.info(f"Working on notification {notification.notif_id}")

            document_url = edo.get_attached_document_url(notification)
            old_contract_id = document_url.split("/")[-1]

            logging.info(f"Trying to find a row for {old_contract_id=!r}")

            # contract_id = "86caedab-c36a-4c65-a2bc-6773c1ce013e"

            contract_id = ""
            if old_contract_id == "53c1fa2a-b003-4cb1-94b8-6809f89d0187":
                contract_id = "95330ac3-a2e5-495c-9918-67ebd48e022a"
            elif old_contract_id == "e09554d6-4ece-4583-919a-6809f91c0123":
                contract_id = "0999ff00-163d-431f-b723-67d3e9820229"
            elif old_contract_id == "ad053f04-9f2a-42a5-86d2-6809f8c6004d":
                contract_id = "90f83597-0cc8-43d3-ba6f-67e3df220297"
            elif old_contract_id == "7af9f297-f750-413d-972d-6809f8fe00ca":
                contract_id = "60a3c643-ce9d-4742-9dfc-67ac78f1023d"
            elif old_contract_id == "0e10dbeb-a2b7-42e2-b7ef-6809f9470363":
                contract_id = "6d5cfa3d-f60b-4cf9-a960-67d2b5c6019e"
            elif old_contract_id == "e184866e-c0c0-43a1-9ab1-6809f9510093":
                contract_id = "eea98540-b8fb-4035-8205-67dab64100f8"
            elif old_contract_id == "849f695b-1192-410e-8d95-6809f94d0178":
                contract_id = "93f06f0d-84d4-4b13-b0e8-67cadf4a0244"
            elif old_contract_id == "23134be0-2429-4bd1-88c9-6809f84300bb":
                contract_id = "1f10179b-16de-4207-8679-680792d503b8"
            elif old_contract_id == "0c8bfd56-375e-46a9-8ff6-6809f80a014e":
                contract_id = "06861e37-c66a-4c82-be3d-68079d22017b"
            elif old_contract_id == "7941d27a-6438-4704-b363-6809f7c101f1":
                contract_id = "510a97d6-fcb7-486d-9f45-6808793a00b8"

            logging.info(f"{contract_id=!r}")

            if contract_id == "":
                raise Exception("Empty contract_id")

            save_folder = edo.download_folder / contract_id
            documents_folder = save_folder / "documents"
            documents_folder.mkdir(parents=True, exist_ok=True)

            soup, basic_contract_data = edo.get_basic_contract_data(contract_id=contract_id)

            if basic_contract_data.contract_type in [
                "Первый график к договору присоединения",
                "Транш к договору присоединения",
            ]:
                reply = f"Тип {basic_contract_data.contract_type!r} еще не поддерживается."
                reply_to_notification(edo=edo, notification=notification, reply=reply)
                continue

            edo.find_contract(basic_contract_data=basic_contract_data, db=db)
            edo.download_file(contract_id=contract_id)
            download_info = edo.get_signed_contract_url(contract_id=contract_id, soup=soup)
            for url_path, file_path in download_info:
                status = edo.download_signed_contract(url_path, file_path)
                if not status:
                    logging.error("Unable to download signed document...")

            safe_extract(save_folder / "contract.zip", documents_folder=documents_folder)

            parse_contract = parse_document(
                contract_id=contract_id,
                download_folder=registry.download_folder,
                patterns=registry.patterns,
                db=db,
            )

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
                    f"{crm_contract.error.human_readable}\n\n"
                    "Не удалось выгрузить данные из CRM.\n"
                    "Список раннее проделанной успешной работы:\n"
                    "1) Найдена страница прикрепленного договора\n"
                    "2) Скачаны все прикрепленные файлы, включая подписанный ДС\n"
                    "3) Обработан ДС, выгружены данные, проверены графики погашения в двух языках, сверены IBAN кода"
                )
                reply_to_notification(edo=edo, notification=notification, reply=reply)
                continue

            macro = process_macro(contract_id=contract_id, db=db)
            macro.error.save(db)
            macro.save(db)

            if macro.error.traceback:
                reply = (
                    f"{macro.error.human_readable}\n\n"
                    "Не удалось сформировать макрос.\n"
                    "Список ранее проделанной успешной работы:\n"
                    "1) Найдена страница прикрепленного договора\n"
                    "2) Скачаны все прикрепленные файлы, включая подписанный ДС\n"
                    "3) Обработан ДС, выгружены данные, проверены графики погашения в двух языках, сверены IBAN кода\n"
                    "4) Найден номер протокола и выгружены данные из CRM, скачан файл выписки в формате PDF\n"
                )
                reply_to_notification(edo=edo, notification=notification, reply=reply)
                continue

            reply = (
                "Не найдено замечаний.\n"
                "Список ранее проделанной успешной работы:\n"
                "1) Найдена страница прикрепленного договора\n"
                "2) Скачаны все прикрепленные файлы, включая подписанный ДС\n"
                "3) Обработан ДС, выгружены данные, проверены графики погашения в двух языках, сверены IBAN кода\n"
                "4) Найден номер протокола и выгружены данные из CRM, скачан файл выписки в формате PDF\n"
                "5) Сформирован макрос и проверерен на грубые расхождения (>1 тиына) и ложные значения"
            )
            reply_to_notification(edo=edo, notification=notification, reply=reply)


def main():
    today = datetime.now(pytz.timezone("Asia/Almaty")).date()
    # today = datetime(2025, 2, 26).date()
    # today = datetime(2025, 3, 14).date()
    os.environ["today"] = today.isoformat()

    logger.setup_logger(today)
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

    with DatabaseManager(registry.database) as db:
        while True:
            process_notifications(db=db, edo=edo, crm=crm, registry=registry)
            logging.info("Current batch is processed - sleeping...")
            break
            # sleep(1)
            # for notification in notifications:
            #     status = await edo.reply_to_notification(
            #         notification=notification, reply="Не согласовано. Замечания:\n 1. Тест\n2. Тест"
            #     )
            #     print(notification.notif_url)
            #     await edo.mark_as_read(notif_id=notification.notif_id)
            # for notification in notifications:
            #     status = await edo.reply_to_notification(
            #         notification=notification, reply="TEST test test"
            #     )
            #     if not status:
            #         raise ReplyError("Не удалось исполнить заявку")
            #     break
    #     await edo.process_contracts(
    #         max_page=1, batch_size=10, contracts_excel_path=contracts_excel_path
    #     )
    #
    # parse_documents(
    #     db=db,
    #     months_json_path=(resources_folder / "months.json"),
    #     download_folder=download_folder,
    # )
    #
    # with CRM(
    #     user=os.environ["CRM_USERNAME"],
    #     password=os.environ["CRM_PASSWORD"],
    #     base_url=os.environ["CRM_BASE_URL"],
    #     download_folder=download_folder,
    #     user_agent=os.environ["USER_AGENT"],
    #     schema_json_path=(resources_folder / "schemas.json"),
    # ) as crm:
    #     fetch_crm_data(crm=crm, db=db, resources_folder=resources_folder)
    # process_macros(db=db)


if __name__ == "__main__":
    # import timeit
    #
    # launch_count = 1
    # print(
    #     timeit.timeit(
    #         "asyncio.run(main())",
    #         "import asyncio; from __main__ import main",
    #         number=launch_count,
    #     )
    #     / launch_count
    # )

    main()
