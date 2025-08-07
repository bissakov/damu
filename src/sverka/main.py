from __future__ import annotations

import inspect
import logging
import os
import sys
import time
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import cast, TYPE_CHECKING

import dotenv
import pytz

project_folder = Path(__file__).resolve().parent.parent.parent
os.environ["project_folder"] = str(project_folder)
os.chdir(project_folder)
sys.path.append(str(project_folder))
sys.path.append(str(project_folder / "sverka"))
sys.path.append(str(project_folder / "utils"))
sys.path.append(str(project_folder / "zanesenie"))

from sverka.crm import CRM, fetch_crm_data_one, is_first_protocol_id_valid
from sverka.edo import EDO
from sverka.error import CRMNotFoundError, ProtocolDateNotInRangeError
from sverka.macros import process_macro
from sverka.parser import parse_document
from sverka.structures import Registry
from sverka.subsidy import date_to_str
from utils.db_manager import DatabaseManager
from utils.utils import (
    delete_leftovers,
    humanize_timedelta,
    is_tomorrow,
    safe_extract,
    TelegramAPI,
)

if TYPE_CHECKING:
    from sverka.edo import Task


def setup_logger(_today: date | None = None) -> None:
    log_format = "[%(asctime)s] %(levelname)-8s %(filename)s:%(funcName)s:%(lineno)s %(message)s"
    formatter = logging.Formatter(log_format, datefmt="%H:%M:%S")

    damu = logging.getLogger("DAMU")
    damu.setLevel(logging.DEBUG)

    formatter.converter = lambda *args: datetime.now(
        pytz.timezone("Asia/Almaty")
    ).timetuple()

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    log_folder = Path("logs/sverka")
    log_folder.mkdir(exist_ok=True, parents=True)

    if _today is None:
        _today = datetime.now(pytz.timezone("Asia/Almaty")).date()

    today_str = _today.strftime("%d.%m.%y")
    year_month_folder = log_folder / _today.strftime("%Y/%B")
    year_month_folder.mkdir(parents=True, exist_ok=True)
    logger_file = year_month_folder / f"{today_str}.log"

    file_handler = logging.FileHandler(logger_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    damu.addHandler(stream_handler)
    damu.addHandler(file_handler)


today = datetime.now(pytz.timezone("Asia/Almaty")).date()
os.environ["today"] = today.isoformat()
setup_logger(today)

logger = logging.getLogger("DAMU")


def reply_to_notification(
    edo: EDO, task: Task, bot: TelegramAPI, reply: str
) -> None:
    reply = inspect.cleandoc(reply)
    logger.info(f"Notification reply - {reply!r}")

    if "Неизвестная ошибка" in reply:
        return

    bot.send_message(f"{task.doc_id}:\n{reply}")

    # if reply != "Согласовано. Не найдено замечаний.":
    #     return

    edo.reply_to_notification(task=task, reply=reply)


def process_notification(
    db: DatabaseManager,
    edo: EDO,
    crm: CRM,
    registry: Registry,
    doctype_id: str,
    doc_id: str,
) -> str:
    document_url = edo.get_attached_document_url(doctype_id, doc_id)
    if not document_url:
        reply = "Не найден приложенный документ на странице поручения."
        return reply

    contract_id = document_url.split("/")[-1]

    logger.info(f"Trying to find a row for {contract_id=!r}")

    save_folder = edo.download_folder / contract_id
    documents_folder = save_folder / "documents"
    documents_folder.mkdir(parents=True, exist_ok=True)

    for file_path in documents_folder.iterdir():
        if file_path.name == "recovering.docx":
            file_path.unlink()

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
            dbz_id=edo_contract.dbz_id,
            dbz_date=edo_contract.dbz_date,
        )

        if crm_contract.error and crm_contract.error.traceback:
            reply = crm_contract.error.human_readable
            return reply

        if crm_contract.bank not in list(registry.banks.keys()):
            reply = f"Банк/лизинг {crm_contract.bank!r} не поддерживается."
            return reply

        macro = process_macro(
            contract_id=contract_id,
            db=db,
            macros_folder=macros_folder,
            documents_folder=documents_folder,
        )
        macro.error.save(db)
        macro.save(db)

        if (
            macro.error
            and macro.error.traceback
            and "не поддерживается для сверки" not in macro.error.human_readable
        ):
            reply = f"Не согласовано. {macro.error.human_readable}"
            logging.error(reply)
            logging.error("Temporarily disabled")
            # return reply

        check_date = "Транш" not in basic_contract.contract_type
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

    if "не поддерживается для сверки" in (macro.error.human_readable or ""):
        reply = macro.error.human_readable
    else:
        reply = "Согласовано. Не найдено замечаний."

    return reply


# FIXME
failed_tasks: set[str] = set()


def process_notifications(
    db: DatabaseManager,
    edo: EDO,
    crm: CRM,
    registry: Registry,
    bot: TelegramAPI,
) -> int:
    with edo:
        tasks = edo.get_tasks()

        logger.info(f"Found {len(tasks)} notifications")
        if not tasks:
            logger.info("Nothing to work on - sleeping...")
            return 150
        else:
            bot.send_message(f"Found {len(tasks)} notifications")

        for task in tasks:
            # if task.doc_id in failed_tasks:
            #     continue

            logger.info(f"Working on task {task}")
            try:
                reply = process_notification(
                    db=db,
                    edo=edo,
                    crm=crm,
                    registry=registry,
                    doctype_id=task.doctype_id,
                    doc_id=task.doc_id,
                )

                if "Неизвестная ошибка" in reply:
                    failed_tasks.add(task.doc_id)
                    logger.error(reply)
                    tg_dev_name = os.environ["TG_DEV_NAME"]
                    bot.send_message(
                        f"@{tg_dev_name} Поймана ошибка - кол-во неизвестных {len(failed_tasks)}."
                    )
                    logger.error(
                        f"Поймана ошибка - кол-во неизвестных {len(failed_tasks)}."
                    )
                    continue

                if "CRM на данный момент не доступен" in reply:
                    continue

                reply_to_notification(edo=edo, task=task, bot=bot, reply=reply)
            except Exception as err:
                logging.exception(err)
                logging.error(f"{err!r}")
                bot.send_message(f"{err!r}")
                failed_tasks.add(task.doc_id)
                logger.error(
                    f"Поймана ошибка - кол-во неизвестных {len(failed_tasks)}."
                )
                continue

    if failed_tasks:
        return 150
    else:
        return 0


def main() -> None:
    dotenv.load_dotenv(".env")

    registry = Registry(
        download_folder=Path(f"downloads/sverka/{today}"), db_name="sverka"
    )

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
    bot = TelegramAPI(process_name="sve")

    bot.send_message('START of the process "Сверка договоров"')
    logger.info('START of the process "Сверка договоров"')

    start_time = time.time()
    max_duration = 12 * 60 * 60
    tomorrow = today + timedelta(days=1)

    delete_leftovers(registry.download_folder, today)

    try:
        while True:
            elapsed_time = time.time() - start_time
            logger.info(f"Elapsed time: {humanize_timedelta(elapsed_time)!r}")

            if elapsed_time >= max_duration or is_tomorrow(tomorrow):
                logger.info("Time passed. Stopping the loop.")
                break

            with DatabaseManager(registry.database) as db:
                duration = process_notifications(
                    db=db, edo=edo, crm=crm, registry=registry, bot=bot
                )
                logger.info(
                    f"Current batch is processed - sleeping for {humanize_timedelta(duration)}..."
                )
                time.sleep(duration)

    finally:
        logger.info("FINISH")


if __name__ == "__main__":
    main()
