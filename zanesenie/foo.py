import json
import logging
import os
import sys
import warnings
from datetime import datetime
from pathlib import Path
from typing import Dict

import dotenv
import pywinauto
from urllib3.exceptions import InsecureRequestWarning

from zanesenie.main import Contract, InterestRate
from zanesenie.utils.automation import child, menu_select_1c, window

project_folder = Path(__file__).resolve().parent.parent
os.environ["project_folder"] = str(project_folder)
os.chdir(project_folder)
sys.path.append(str(project_folder))

from zanesenie.utils import logger
from zanesenie.utils.app import App
from zanesenie.utils.db_manager import DatabaseManager


def process_contract(contract: Contract, rate: InterestRate) -> None:
    app_path = r"C:\Users\robot3\Desktop\damu_1c\test_base.v8i"
    app = pywinauto.Application(backend="uia").connect(process=16316)
    one_c = App(app_path=app_path, app=app)

    win = window(one_c.app, title="Конфигурация.+", regex=True)
    win.wait(wait_for="exists", timeout=20)

    table_form = child(win, ctrl="Pane", idx=63)
    # click(win, child(table_form, ctrl="Button", title="Проверка введенного графика"))
    # click(win, child(table_form, ctrl="Button", title="Прочее"))
    #
    # menu = child(win, ctrl="Menu")
    # print_element_tree(menu)
    # # menu_select(win, menu, ["БВУ", "Банк ЦентрКредит"])
    # menu_select(win, menu, ["Прикрепленные документы"])
    # all_menu_items(menu)

    # menu_select_1c(win, table_form, trigger_btn_name="Прочее", menu_names=["Прикрепленные документы"])

    menu_select_1c(
        win,
        table_form,
        trigger_btn_name="Проверка введенного графика",
        menu_names=[contract.category, contract.bank],
    )

    pass


def main() -> None:
    logger.setup_logger(project_folder)

    if sys.version_info.major != 3 or sys.version_info.minor != 12:
        error_msg = f"Python {sys.version_info} is not supported"
        logging.error(error_msg)
        raise RuntimeError(error_msg)

    warnings.simplefilter(action="ignore", category=UserWarning)
    warnings.simplefilter(action="ignore", category=InsecureRequestWarning)
    warnings.simplefilter(action="ignore", category=SyntaxWarning)

    today = datetime(2025, 3, 14).date()
    os.environ["today"] = today.isoformat()

    env_path = project_folder / ".env"
    dotenv.load_dotenv(env_path)

    resources_folder = Path("resources")

    database = resources_folder / "database.sqlite"
    with DatabaseManager(database) as db:
        for _ in Contract.iter_contracts(db, resources_folder):
            pass

        for contract in Contract.iter_contracts(db, resources_folder):
            # if contract.contract_id == "d1e64465-57de-40ca-b009-67554074016f":
            #     continue
            #
            # if contract.contract_id == "249dadb8-9651-43f7-b826-675295b90006":
            #     continue
            #
            # if contract.contract_id == "8223d792-bfd0-4b78-abfa-67597f0b0342":
            #     continue

            # if contract.contract_id in {"d1e64465-57de-40ca-b009-67554074016f"}:
            #     continue
            #
            # if contract.contract_id == "249dadb8-9651-43f7-b826-675295b90006":
            #     # TEMP
            #     continue

            rate = InterestRate.load(db, contract.contract_id)

            process_contract(contract, rate)

            break


if __name__ == "__main__":
    main()
