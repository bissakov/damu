import logging
import os
import shutil
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import dotenv
import pytz

project_folder = Path(__file__).resolve().parent.parent
os.environ["project_folder"] = str(project_folder)
os.chdir(project_folder)
sys.path.append(str(project_folder))

from sverka.crm import CRM, fetch_crm_data_one
from sverka.edo import EDO
from sverka.macros import process_macro
from sverka.parser import parse_document
from sverka.structures import Registry
from sverka.subsidy import date_to_str
from utils.db_manager import DatabaseManager
from utils.utils import safe_extract


def setup_logger(_today: date | None = None) -> Path:
    log_format = "[%(asctime)s] %(levelname)-8s %(filename)s:%(funcName)s:%(lineno)s %(message)s"
    formatter = logging.Formatter(log_format, datefmt="%H:%M:%S")

    root = logging.getLogger("DAMU")
    root.setLevel(logging.DEBUG)

    formatter.converter = lambda *args: datetime.now(pytz.timezone("Asia/Almaty")).timetuple()

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    log_folder = Path("logs")
    log_folder.mkdir(exist_ok=True)

    if _today is None:
        _today = datetime.now(pytz.timezone("Asia/Almaty")).date()

    today_str = _today.strftime("%d.%m.%y")
    year_month_folder = log_folder / today.strftime("%Y/%B")
    year_month_folder.mkdir(parents=True, exist_ok=True)
    logger_file = year_month_folder / f"{today_str}_test.log"

    file_handler = logging.FileHandler(logger_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    root.addHandler(stream_handler)
    root.addHandler(file_handler)

    return logger_file


today = datetime.now(pytz.timezone("Asia/Almaty")).date()
os.environ["today"] = today.isoformat()
setup_logger(today)

logger = logging.getLogger("DAMU")


def process_notification(db: DatabaseManager, edo: EDO, crm: CRM, registry: Registry, contract_id: str) -> str:
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

    start_date_str = date_to_str(parse_contract.start_date)
    end_date_str = date_to_str(parse_contract.end_date)

    assert start_date_str
    assert end_date_str

    with crm:
        crm_contract = fetch_crm_data_one(
            crm=crm,
            db=db,
            contract_id=contract_id,
            protocol_id=parse_contract.protocol_id,
            start_date=start_date_str,
            end_date=end_date_str,
            registry=registry,
        )

    if crm_contract.error.traceback:
        reply = f"{crm_contract.error.human_readable}\nНе удалось выгрузить данные из CRM."
        return reply

    macro = process_macro(contract_id=contract_id, db=db, macros_folder=macros_folder)
    macro.error.save(db)
    macro.save(db)

    if macro.error.traceback:
        reply = f"Не согласовано. {macro.error.human_readable}"
        return reply

    reply = "Согласовано. Не найдено замечаний."
    return reply


def humanize_timedelta(seconds: int | float) -> str:
    td = timedelta(seconds=int(seconds))
    return str(td)


def is_tomorrow(tomorrow: date) -> bool:
    return datetime.now(pytz.timezone("Asia/Almaty")).date() >= tomorrow


def delete_leftovers(download_folder: Path, max_days: int = 14) -> None:
    for folder in download_folder.parent.iterdir():
        if not folder.is_dir():
            continue

        if not any(folder.iterdir()):
            logger.info(f"Deleting empty {folder.name!r} folder")
            folder.rmdir()
            continue

        try:
            run_date = date.fromisoformat(folder.name)
        except ValueError:
            continue
        delta = (today - run_date).days
        if delta <= max_days:
            continue

        logger.info(f"Deleting {folder.name!r} folder. {delta} > {max_days}")
        shutil.rmtree(folder)


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

    logger.info('START of the process "Сверка договоров"')

    delete_leftovers(registry.download_folder)

    # with DatabaseManager(registry.database) as db:
    #     # reply = process_notification(
    #     #     db=db,
    #     #     edo=edo,
    #     #     crm=crm,
    #     #     registry=registry,
    #     #     contract_id="c4ad41f0-4c50-4c5c-8554-6814d36601cd",
    #     # )
    #     # logger.info(f"Reply: {reply}")
    #
    #     contract_ids = [row[0] for row in db.request("SELECT id FROM contracts", req_type=db.RequestType.FETCH_ALL)]
    #     err_count = 0
    #     for idx, contract_id in enumerate(contract_ids, start=1):
    #         break
    #         logger.info(f"Progress: {idx}/{len(contract_ids)}")
    #
    #         reply = process_notification(db=db, edo=edo, crm=crm, registry=registry, contract_id=contract_id)
    #         logger.info(f"Reply: {reply}")
    #
    #         if reply != "Согласовано. Не найдено замечаний.":
    #             err_count += 1
    #
    #     logger.info(f"err_count: {err_count}")


if __name__ == "__main__":
    try:
        main()
    finally:
        logger.info("FINISH")
