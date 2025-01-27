import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path

import dotenv
import pytz

from src.utils.db_manager import DatabaseManager

project_folder = Path(__file__).resolve().parent.parent
sys.path.append(str(project_folder))

from src.crm import CRM, fetch_crm_data
from src.edo import EDO
from src.utils import logger
from src.utils.utils import get_from_env
from src.parser import parse_documents


def main():
    today = datetime.now(pytz.timezone("Asia/Almaty")).date()
    today = today.replace(day=20)

    logger.setup_logger(project_folder, today)
    dotenv.load_dotenv(project_folder / ".env")

    user_agent = get_from_env("USER_AGENT")

    download_folder = project_folder / "downloads" / str(today)
    download_folder.mkdir(parents=True, exist_ok=True)

    resources_folder = project_folder / "resources"

    schema_json_path = resources_folder / "schemas.json"
    months_json_path = resources_folder / "months.json"
    banks_json_path = resources_folder / "banks.json"
    database = resources_folder / "database.sqlite"

    db = DatabaseManager(database)
    db.prepare_tables()

    edo = EDO(
        db=db,
        user=get_from_env("EDO_USERNAME"),
        password=get_from_env("EDO_PASSWORD"),
        base_url=get_from_env("EDO_BASE_URL"),
        download_folder=download_folder,
        user_agent=user_agent,
    )

    crm = CRM(
        user=get_from_env("CRM_USERNAME"),
        password=get_from_env("CRM_PASSWORD"),
        base_url=get_from_env("CRM_BASE_URL"),
        download_folder=download_folder,
        user_agent=user_agent,
        schema_json_path=schema_json_path,
    )

    max_page = 20
    with edo, crm:
        for page in range(max_page):
            logging.info(f"Page {page + 1}/{max_page}")
            status = edo.get_contracts(page=page, ascending=False)
            if not status:
                logging.warning("Robot is not logged in to the EDO...")
                raise Exception("Robot is not logged in to the EDO...")

        asyncio.run(edo.mass_download_async(batch_size=50))
        parse_documents(db=db, months_json_path=months_json_path)
        fetch_crm_data(crm=crm, db=db, banks_json_path=banks_json_path)


if __name__ == "__main__":
    main()
