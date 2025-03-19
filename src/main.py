import os
import sys
from datetime import datetime
from pathlib import Path

import dotenv

project_folder = Path(__file__).resolve().parent.parent
os.environ["project_folder"] = str(project_folder)
os.chdir(project_folder)
sys.path.append(str(project_folder))

from src.crm import CRM, fetch_crm_data
from src.edo import EDO
from src.parser import parse_documents

from src.macros import process_macros
from src.utils.db_manager import DatabaseManager
from src.utils import logger


async def main():
    # today = datetime.now(pytz.timezone("Asia/Almaty")).date()
    # today = datetime(2025, 2, 26).date()
    today = datetime(2025, 3, 14).date()
    os.environ["today"] = today.isoformat()

    logger.setup_logger(today)
    dotenv.load_dotenv(".env")

    download_folder = Path(f"downloads/{today}")
    download_folder.mkdir(parents=True, exist_ok=True)

    resources_folder = Path("resources")

    contracts_excel_path = Path("contracts.xlsx")

    database = resources_folder / "database.sqlite"
    with DatabaseManager(database) as db:
        async with EDO(
            db=db,
            user=os.environ["EDO_USERNAME"],
            password=os.environ["EDO_PASSWORD"],
            base_url=os.environ["EDO_BASE_URL"],
            download_folder=download_folder,
            user_agent=os.environ["USER_AGENT"],
        ) as edo:
            # notifications = await edo.get_notifications()
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
            await edo.process_contracts(
                max_page=3, batch_size=10, contracts_excel_path=contracts_excel_path
            )

        parse_documents(
            db=db,
            months_json_path=(resources_folder / "months.json"),
            download_folder=download_folder,
        )

        with CRM(
            user=os.environ["CRM_USERNAME"],
            password=os.environ["CRM_PASSWORD"],
            base_url=os.environ["CRM_BASE_URL"],
            download_folder=download_folder,
            user_agent=os.environ["USER_AGENT"],
            schema_json_path=(resources_folder / "schemas.json"),
        ) as crm:
            fetch_crm_data(crm=crm, db=db, resources_folder=resources_folder)
        process_macros(db=db)


if __name__ == "__main__":
    import timeit

    launch_count = 1
    print(
        timeit.timeit(
            "asyncio.run(main())",
            "import asyncio; from __main__ import main",
            number=launch_count,
        )
        / launch_count
    )

    # asyncio.run(main())
