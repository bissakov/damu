import inspect
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin

import dotenv
import httpx
import pytz

project_folder = Path(__file__).resolve().parent.parent.parent
os.environ["project_folder"] = str(project_folder)
os.chdir(project_folder)
sys.path.append(str(project_folder))
sys.path.append(str(project_folder / "sverka"))
sys.path.append(str(project_folder / "utils"))
sys.path.append(str(project_folder / "zanesenie"))

from sverka.crm import CRM, fetch_crm_data_one
from sverka.edo import EDO, EdoNotification
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
)


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
    log_folder.mkdir(exist_ok=True)

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


class TelegramAPI:
    def __init__(self) -> None:
        self.client = httpx.Client()
        self.token, self.chat_id = os.environ["TOKEN"], os.environ["CHAT_ID"]
        self.api_url = f"https://api.telegram.org/bot{self.token}/"

        self.pending_messages: list[str] = []

    def reload_session(self) -> None:
        self.client.close()
        self.client = httpx.Client()

    def send_message(
        self,
        message: str | None = None,
        use_session: bool = True,
        use_md: bool = False,
    ) -> bool:
        send_data: dict[str, str | None] = {"chat_id": self.chat_id}

        if use_md:
            send_data["parse_mode"] = "MarkdownV2"

        pending_message = "\n".join(self.pending_messages)
        if pending_message:
            message = f"{pending_message}\n{message}"

        url = urljoin(self.api_url, "sendMessage")
        send_data["text"] = message

        status_code = 0

        try:
            if use_session:
                response = self.client.post(url, data=send_data, timeout=10)
            else:
                response = httpx.post(url, data=send_data, timeout=10)

            data = "" if not hasattr(response, "json") else response.json()
            status_code = response.status_code
            logger.debug(f"{status_code=}, {data=}")
            response.raise_for_status()

            if status_code == 200:
                self.pending_messages = []
                return True

            return False
        except httpx.HTTPError as err:
            if status_code == 429 and message:
                self.pending_messages.append(message)

            logger.exception(err)
            return False

    def send_with_retry(self, message: str) -> bool:
        retry = 0
        while retry < 5:
            try:
                use_session = retry < 5
                success = self.send_message(message, use_session)
                return success
            except httpx.HTTPError as e:
                self.reload_session()
                logger.exception(e)
                logger.warning(f"{e} intercepted. Retry {retry + 1}/10")
                retry += 1

        return False


def reply_to_notification(
    edo: EDO, notification: EdoNotification, bot: TelegramAPI, reply: str
) -> None:
    reply = inspect.cleandoc(reply)
    logger.info(f"Notification reply - {reply!r}")

    if "Неизвестная ошибка" in reply:
        return

    bot.send_message(f"{notification.notif_id}:\n{reply}")

    # if reply != "Согласовано. Не найдено замечаний.":
    #     return

    edo.reply_to_notification(notification=notification, reply=reply)
    edo.mark_as_read(notif_id=notification.notif_id)


def process_notification(
    db: DatabaseManager,
    edo: EDO,
    crm: CRM,
    registry: Registry,
    notification: EdoNotification,
) -> str:
    document_url = edo.get_attached_document_url(notification)
    if not document_url:
        reply = "Не найден приложенный документ на странице поручения."
        return reply

    contract_id = document_url.split("/")[-1]

    logger.info(f"Trying to find a row for {contract_id=!r}")

    save_folder = edo.download_folder / contract_id
    documents_folder = save_folder / "documents"
    documents_folder.mkdir(parents=True, exist_ok=True)
    macros_folder = save_folder / "macros"
    macros_folder.mkdir(parents=True, exist_ok=True)

    soup, basic_contract, _ = edo.get_basic_contract_data(
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
        "Дополнительное соглашение к договору субсидирования"
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
            dbz_id=parse_contract.dbz_id,
            dbz_date=parse_contract.dbz_date,
        )

    if crm_contract.error and crm_contract.error.traceback:
        reply = f"{crm_contract.error.human_readable}\nНе удалось выгрузить данные из CRM."
        return reply

    macro = process_macro(
        contract_id=contract_id, db=db, macros_folder=macros_folder
    )
    macro.error.save(db)
    macro.save(db)

    if macro.error and macro.error.traceback:
        reply = f"Не согласовано. {macro.error.human_readable}"
        logging.error(reply)
        logging.error("Temporarily disabled")
        # return reply

    reply = "Согласовано. Не найдено замечаний."
    return reply


# FIXME
failed_notifications: set[str] = set()


def process_notifications(
    db: DatabaseManager,
    edo: EDO,
    crm: CRM,
    registry: Registry,
    bot: TelegramAPI,
) -> int:
    with edo:
        notifications = edo.get_notifications()

        logger.info(f"Found {len(notifications)} notifications")
        if not notifications:
            logger.info("Nothing to work on - sleeping...")
            return 150
        else:
            bot.send_message(f"Found {len(notifications)} notifications")

        for notification in notifications:
            if notification.notif_id in failed_notifications:
                continue

            logger.info(f"Working on notification {notification.notif_id}")
            try:
                reply = process_notification(
                    db=db,
                    edo=edo,
                    crm=crm,
                    registry=registry,
                    notification=notification,
                )
                if "Неизвестная ошибка" in reply:
                    failed_notifications.add(notification.notif_id)
                    logger.error(reply)
                    tg_dev_name = os.environ["TG_DEV_NAME"]
                    bot.send_message(
                        f"@{tg_dev_name} Поймана ошибка - кол-во неизвестных {len(failed_notifications)}."
                    )
                    logger.error(
                        f"Поймана ошибка - кол-во неизвестных {len(failed_notifications)}."
                    )
                    continue
                reply_to_notification(
                    edo=edo, notification=notification, bot=bot, reply=reply
                )
            except Exception as err:
                logging.exception(err)
                logging.error(f"{err!r}")
                bot.send_message(f"{err!r}")
                failed_notifications.add(notification.notif_id)
                logger.error(
                    f"Поймана ошибка - кол-во неизвестных {len(failed_notifications)}."
                )
                continue

    if failed_notifications:
        return 150
    else:
        return 0


def main() -> None:
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
    bot = TelegramAPI()

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
