import dataclasses
import json
import logging
import os
import re
import shutil
import sys
import warnings
from contextlib import suppress
from dataclasses import asdict
from datetime import date, datetime
from difflib import SequenceMatcher
from pathlib import Path
from time import sleep
from typing import Generator, Optional, Union

import dotenv
import pandas as pd
import pyperclip
import pytz
from pywinauto import ElementNotFoundError
from urllib3.exceptions import InsecureRequestWarning

from sverka.crm import CRM, fetch_crm_data_one
from sverka.edo import EDO
from sverka.macros import process_macro
from sverka.parser import parse_document
from sverka.structures import Registry
from sverka.subsidy import date_to_str
from utils.db_manager import DatabaseManager
from utils.utils import safe_extract

project_folder = Path(__file__).resolve().parent.parent
os.environ["project_folder"] = str(project_folder)
os.chdir(project_folder)
sys.path.append(str(project_folder))

if sys.version_info.major != 3 or sys.version_info.minor != 12:
    error_msg = f"Python {sys.version_info} is not supported"
    logging.error(error_msg)
    raise RuntimeError(error_msg)


from sverka.process_contract import process_contract
from utils.db_manager import DatabaseManager
from zanesenie.utils.app import App
from zanesenie.utils.automation import (
    UiaButton,
    UiaList,
    UiaListItem,
    UiaPane,
    UiaWindow,
    check,
    child,
    children,
    click,
    click_type,
    contains_text,
    count_control_types,
    menu_select_1c,
    send_keys,
    text,
    text_to_float,
    wait,
    window,
)
from zanesenie.utils.office import docx_to_pdf


def setup_logger(_today: date | None = None) -> Path:
    log_format = "[%(asctime)s] %(levelname)-8s %(filename)s:%(funcName)s:%(lineno)s %(message)s"
    formatter = logging.Formatter(log_format, datefmt="%H:%M:%S")

    root = logging.getLogger("DAMU")
    root.setLevel(logging.DEBUG)

    formatter.converter = lambda *args: datetime.now(pytz.timezone("Asia/Almaty")).timetuple()

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    log_folder = Path("logs/zanesinie")
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

    root.addHandler(stream_handler)
    root.addHandler(file_handler)

    return logger_file


today = datetime.now(pytz.timezone("Asia/Almaty")).date()
os.environ["today"] = today.isoformat()
setup_logger(today)

logger = logging.getLogger("DAMU")


def prepare_query(contragent: str) -> str:
    query = f"""
        ВЫБРАТЬ Проекты.Ссылка
        ИЗ Справочник.Контрагенты КАК Агенты
        ВНУТРЕННЕЕ СОЕДИНЕНИЕ Справочник.Проектыконтрагентов КАК Проекты
        ПО Агенты.Ссылка = Проекты.Владелец
        ГДЕ Агенты.БИНИИН = "{contragent}"
    """

    query = re.sub(r" {2,}", "", query).strip()

    return query


def iso_to_standard(dt: str) -> str:
    if dt[2] == "." and dt[5] == ".":
        return dt
    return datetime.fromisoformat(dt).strftime("%d.%m.%Y")


@dataclasses.dataclass(slots=True)
class Contract:
    contract_id: str
    contragent: str
    project: str
    bank: str
    credit_purpose: str
    repayment_procedure: str
    loan_amount: float
    subsid_amount: float
    investment_amount: float
    pos_amount: float
    protocol_date: str
    vypiska_date: str
    decision_date: str
    settlement_date: int
    iban: str
    ds_id: str
    ds_date: str
    dbz_id: str
    dbz_date: str
    start_date: str
    end_date: str
    protocol_id: str
    sed_number: str
    document_path: Path
    macro_path: Union[bytes, Path]
    document_pdf_path: Optional[Path] = None
    protocol_pdf_path: Optional[Path] = None
    category: Optional[str] = None

    def __post_init__(self) -> None:
        self.protocol_date = iso_to_standard(self.protocol_date).replace(".", "")
        self.vypiska_date = iso_to_standard(self.vypiska_date).replace(".", "")
        self.ds_date = iso_to_standard(self.ds_date).replace(".", "")
        self.dbz_date = iso_to_standard(self.dbz_date).replace(".", "")
        self.start_date = iso_to_standard(self.start_date).replace(".", "")
        self.end_date = iso_to_standard(self.end_date).replace(".", "")
        self.decision_date = iso_to_standard(self.decision_date).replace(".", "")

        today = str(os.environ["today"])
        contract_folder = ("downloads" / Path(today) / self.contract_id).absolute()
        with suppress(FileNotFoundError):
            self.protocol_pdf_path = next((contract_folder / "vypiska").iterdir(), None)

        document_folder = contract_folder / "documents"

        self.document_path = document_folder / Path(self.document_path)

        self.document_pdf_path = self.document_path.with_suffix(".pdf")
        if not self.document_pdf_path.exists():
            docx_to_pdf(self.document_path, self.document_pdf_path)

        macro_path = document_folder / "macro.xlsx"
        with macro_path.open("wb") as f:
            f.write(self.macro_path)

        self.macro_path = macro_path

    @classmethod
    def iter_contracts(cls, db: DatabaseManager, resources_folder: Path) -> Generator["Contract", None, None]:
        raw_contracts = db.execute("""
            SELECT
                c.id,
                c.contragent,
                c.project,
                c.bank,
                c.credit_purpose,
                c.repayment_procedure,
                c.loan_amount,
                c.subsid_amount,
                c.investment_amount,
                c.pos_amount,
                c.protocol_date,
                c.vypiska_date,
                c.decision_date,
                c.settlement_date,
                c.iban,
                c.ds_id,
                c.ds_date,
                c.dbz_id,
                c.dbz_date,
                c.start_date,
                c.end_date,
                c.protocol_id,
                c.sed_number,
                c.file_name,
                mc.shifted_macro
            FROM contracts AS c
            INNER JOIN macros AS mc ON mc.id = c.id
            LEFT JOIN errors AS e ON e.id = c.id
            WHERE
                e.traceback IS NULL
                AND c.dbz_id IS NOT NULL
                AND c.project IS NOT NULL
                AND c.id NOT IN (
                    SELECT id FROM errors WHERE traceback IS NOT NULL
                )
        """)

        with (resources_folder / "banks.json").open(encoding="utf-8") as f:
            bank_mapping = json.load(f)
        bank_mapping = bank_mapping.get("mapping", {})

        for raw_contract in raw_contracts:
            contract = Contract(*raw_contract)

            if bvu := bank_mapping.get("БВУ", {}):
                category = "БВУ"
                bank = bvu.get(contract.bank)
            else:
                category = "Лизинг"
                bank = bank_mapping.get(contract.category, {}).get(contract.bank)

            if bank is None:
                # raise ValueError(f"Unknown bank - {contract.bank}")
                continue

            contract.category = category
            contract.bank = bank

            yield contract


@dataclasses.dataclass(slots=True)
class InterestRate:
    contract_id: str
    subsid_term: int
    nominal_rate: float
    rate_one_two_three_year: float
    rate_four_year: float
    rate_five_year: float
    rate_six_seven_year: float
    rate_fee_one_two_three_year: float
    rate_fee_four_year: float
    rate_fee_five_year: float
    rate_fee_six_seven_year: float
    start_date_one_two_three_year: str
    end_date_one_two_three_year: str
    start_date_four_year: str
    end_date_four_year: str
    start_date_five_year: str
    end_date_five_year: str
    start_date_six_seven_year: str
    end_date_six_seven_year: str

    def __post_init__(self) -> None:
        self.nominal_rate *= 100
        self.rate_one_two_three_year *= 100
        self.rate_four_year *= 100
        self.rate_five_year *= 100
        self.rate_six_seven_year *= 100
        self.rate_fee_one_two_three_year *= 100
        self.rate_fee_four_year *= 100
        self.rate_fee_five_year *= 100
        self.rate_fee_six_seven_year *= 100

    @classmethod
    def load(cls, db: DatabaseManager, contract_id: str) -> "InterestRate":
        raw_rate = db.execute(
            """
                SELECT
                    id,
                    subsid_term,
                    nominal_rate,
                    rate_one_two_three_year,
                    rate_four_year,
                    rate_five_year,
                    rate_six_seven_year,
                    rate_fee_one_two_three_year,
                    rate_fee_four_year,
                    rate_fee_five_year,
                    rate_fee_six_seven_year,
                    start_date_one_two_three_year,
                    end_date_one_two_three_year,
                    start_date_four_year,
                    end_date_four_year,
                    start_date_five_year,
                    end_date_five_year,
                    start_date_six_seven_year,
                    end_date_six_seven_year
                FROM interest_rates
                WHERE id = ?
            """,
            (contract_id,),
        )

        return InterestRate(*raw_rate[0])


def has_error(win: UiaWindow) -> bool:
    sleep(0.5)

    potential_alert = child(win, ctrl="Pane", idx=2)
    if (error_message := text(child(potential_alert, ctrl="Pane"))) and "Fail" in error_message:
        print(error_message)
        click(win, child(potential_alert, ctrl="Button", title="OK"))

    messages_pane = child(win, ctrl="Pane", idx=18)
    close_button = child(messages_pane, title="Close", ctrl="Button")

    if close_button.exists():
        click_type(win, child(messages_pane, ctrl="Document"), "^a^c")

        message = pyperclip.paste()
        print(message)

        click(win, close_button)

        return True

    return False


def open_file(one_c: App, file_path: Path) -> None:
    one_c.switch_backend("win32")
    save_dialog = one_c.app.window(title_re="Выберите ф.+")
    save_dialog["&Имя файла:Edit"].set_text(str(file_path))
    if not save_dialog.is_active():
        save_dialog.set_focus()
        save_dialog.wait(wait_for="visible")
    save_dialog.child_window(title="&Открыть", class_name="Button").click_input()
    one_c.switch_backend("uia")


def find_row(parent: UiaList, project: str) -> Optional[UiaListItem]:
    rows = children(parent)
    for row in rows:
        txt = text(row).strip()
        if not txt:
            continue
        score = SequenceMatcher(None, project, txt).ratio()
        if score >= 0.8:
            print(f"{len(rows)=}, {txt=}, {score=}")
            return row
    return None


def find_project(win: UiaWindow, contract: Contract) -> None:
    click(win, child(win, title="Консоль запросов и обработчик", ctrl="Button"))

    query_document_box = child(win, ctrl="Document")

    query = prepare_query(contract.contragent)
    pyperclip.copy(query)

    click(win, query_document_box)
    send_keys(win, "^a^v", pause=0.5)
    click(win, child(win, title="Выполнить", ctrl="Button"))

    if not wait(child(win, title="Delete", ctrl="Button", idx=1), wait_for="is_enabled"):
        print(f"{contract.contract_id=} not found")
        return

    row = find_row(child(win, ctrl="List", idx=1), project=contract.project)
    if not row:
        print(f"{contract.contract_id=} not found")
        return

    click(win, row, double=True)

    click(win, query_document_box)
    send_keys(win, "{ESC}")

    if (close_button := child(child(win, ctrl="Pane", idx=18), title="Close", ctrl="Button")).exists():
        click(win, close_button)


def fill_main_project_data(win: UiaWindow, form: UiaPane, contract: Contract) -> None:
    """
    :param win: Главное окно 1С
    :param form: Форма "Карточка проекта (форма элемента)"
    :param contract: Данные договора
    :return: None

    Заполнение данных в форме проекта во вкладке "Основные"
    (Цель кредитования, Номер протокола, Дата протокола, Дата получения протокола РКС филиалом)
    """
    click(win, child(parent=form, ctrl="Edit", idx=7))
    send_keys(win, "{F4}^f" + contract.credit_purpose + "{ENTER 2}", pause=0.1, spaces=True)
    click_type(win, child(form, ctrl="Edit", idx=1), contract.protocol_id)
    click_type(win, child(form, ctrl="Edit", idx=2), contract.protocol_date)
    click_type(win, child(form, ctrl="Edit", idx=3), contract.protocol_date)


def change_date(win: UiaWindow, form: UiaPane, goto_button: UiaButton, protocol_date: str) -> None:
    """
    :param win: Главное окно 1С
    :param form: Форма "Карточка проекта (форма элемента)"
    :param goto_button: "Go to" кнопка
    :param protocol_date: Дата протокола
    :return: None

    Возможное изменение даты протокола в форме проекта во вкладке "Пролонгация"
    """
    click(win, child(form, title="Пролонгация", ctrl="TabItem"))

    date_to_check = text(child(form, ctrl="Custom", idx=1)).split(" ")[0].replace(".", "")

    if date_to_check != protocol_date:
        click(win, goto_button)
        send_keys(win, "{DOWN}{ENTER 2}", pause=0.5)
        send_keys(win, protocol_date)
        send_keys(win, "{ENTER 4}{ESC}", pause=0.5)


def change_sums(win: UiaWindow, form: UiaPane, goto_button: UiaButton, contract: Contract) -> None:
    """
    :param win: Главное окно 1С
    :param form: Форма "Карточка проекта (форма элемента)"
    :param goto_button: "Go to" кнопка
    :param contract: Данные договора
    :return: None

    Заполнение данных в форме проекта во вкладке "БВУ/Рефинансирование" в зависимости от цели кредитования
    (Сумма субсидирования, На инвестиции, На ПОС)
    """
    if contract.credit_purpose not in {"Пополнение оборотных средств", "Инвестиционный", "Инвестиционный + ПОС"}:
        raise ValueError(f"Don't know what to do with {contract.credit_purpose!r}...")

    click(win, child(form, title="БВУ/Рефинансирование", ctrl="TabItem"))
    click(win, goto_button)
    send_keys(win, "{DOWN 8}{ENTER}", pause=0.2)

    list_win = child(win, ctrl="Pane", idx=51)

    existing_pos_amount = text_to_float(
        text(child(list_win, ctrl="Custom", idx=5)).replace(" Возобновляемая часть", ""), default=0.0
    )
    existing_investment_amount = text_to_float(
        text(child(list_win, ctrl="Custom", idx=6)).replace(" Не возобновляемая часть", ""), default=0.0
    )

    send_keys(win, "{ENTER}", pause=0.2)

    # record_win = child_win(win, ctrl="Pane", idx=56)

    if contract.credit_purpose == "Пополнение оборотных средств" and existing_pos_amount != contract.subsid_amount:
        send_keys(win, "{TAB 4}" + str(contract.subsid_amount), pause=0.1)
    elif contract.credit_purpose == "Инвестиционный" and existing_investment_amount != contract.subsid_amount:
        send_keys(win, "{TAB 5}" + str(contract.subsid_amount), pause=0.1)
    elif contract.credit_purpose == "Инвестиционный + ПОС":
        if existing_pos_amount != contract.pos_amount and existing_investment_amount != contract.investment_amount:
            send_keys(win, "{TAB 4}" + str(contract.pos_amount) + "{TAB}" + str(contract.investment_amount), pause=0.1)
        elif existing_pos_amount != contract.pos_amount:
            send_keys(win, "{TAB 4}" + str(contract.subsid_amount), pause=0.1)
        elif existing_investment_amount != contract.investment_amount:
            send_keys(win, "{TAB 5}" + str(contract.subsid_amount), pause=0.1)

    send_keys(win, "{ESC}", pause=0.5)
    with suppress(ElementNotFoundError):
        click(win, child(win, title="Yes", ctrl="Button"))
    send_keys(win, "{ESC}", pause=0.5)


def add_vypiska(one_c: App, win: UiaWindow, form: UiaPane, contract: Contract) -> None:
    """
    :param one_c: Главный объект
    :param win: Главное окно 1С
    :param form: Форма "Карточка проекта (форма элемента)"
    :param contract: Данные договора
    :return: None

    Прикрепление файла выписки из CRM во вкладке "Прикрепленные документы"
    """

    fname = contract.protocol_pdf_path.name

    click(win, child(form, title="Прикрепленные документы", ctrl="TabItem"))

    click(win, child(form, ctrl="Button", title="Set list filter and sort options..."))

    sort_win = one_c.app.window(title="Filter and Sort")

    check(child(sort_win, title="Наименование файла", ctrl="CheckBox"))

    click_type(win, child(sort_win, ctrl="Edit", idx=7), fname, spaces=True, escape_chars=True)

    click(win, child(sort_win, title="OK", ctrl="Button"))

    sleep(1)

    if contains_text(child(form, ctrl="Table")):
        return

    click(win, child(form, title="Add", ctrl="Button"))
    click(win, child(win, ctrl="Edit", idx=5))
    send_keys(win, "{F4}")

    open_file(one_c, contract.protocol_pdf_path)

    if (child(win, title="Value is not of object type (Сессия)", ctrl="Pane")).exists():
        click(win, child(win, title="OK", ctrl="Button"))
        sleep(1)

    click(win, child(win, title="OK", ctrl="Button", idx=1))


def check_project_type(win: UiaWindow, form: UiaPane, contract: Contract) -> None:
    if contract.credit_purpose == "Пополнение оборотных средств":
        click(win, child(form, title="Признаки проекта", ctrl="TabItem"))
        check(child(form, title="Возобновляемый проект", ctrl="CheckBox"))


def fill_contract_details(win: UiaWindow, ds_form: UiaPane, contract: Contract, rate: InterestRate) -> None:
    # FIXME POS CHANGES INDICES

    edit_count = count_control_types(ds_form, ctrl="Edit")

    # if contract.credit_purpose == "Пополнение оборотных средств":
    if edit_count == 22:
        # child_window(ds_form, ctrl="Edit", idx=2).set_text(contract.ds_id)
        click_type(win, child(ds_form, ctrl="Edit", idx=21), contract.iban, ent=True, cls=True)
        click_type(win, child(ds_form, ctrl="Edit", idx=12), contract.ds_id, ent=True, cls=True)
        click_type(win, child(ds_form, ctrl="Edit", idx=13), contract.ds_date, ent=True, cls=True)
        click_type(win, child(ds_form, ctrl="Edit", idx=7), contract.dbz_id, ent=True, cls=True)
        click_type(win, child(ds_form, ctrl="Edit", idx=8), contract.dbz_date, ent=True, cls=True)
        click_type(win, child(ds_form, ctrl="Edit", idx=9), contract.dbz_date, ent=True, cls=True)
        click_type(win, child(ds_form, ctrl="Edit", idx=10), contract.end_date, ent=True, cls=True)
        click_type(win, child(ds_form, ctrl="Edit", idx=11), rate.nominal_rate, ent=True, cls=True)
        click_type(win, child(ds_form, ctrl="Edit", idx=17), rate.rate_one_two_three_year, ent=True, cls=True)
        click_type(win, child(ds_form, ctrl="Edit", idx=14), contract.loan_amount, ent=True, cls=True)
        click_type(win, child(ds_form, ctrl="Edit", idx=20), contract.investment_amount, ent=True, cls=True)
        click_type(win, child(ds_form, ctrl="Edit", idx=19), contract.pos_amount, ent=True, cls=True)
        click_type(win, child(ds_form, ctrl="Edit", idx=3), contract.decision_date, ent=True, cls=True)

        # Вид погашения платежа - Аннуитетный/Равными долями/Индивидуальный
        click(win, child(ds_form, ctrl="Edit", idx=18))
        if contract.repayment_procedure == "Аннуитетный":
            send_keys(win, "{F4}{ENTER}", pause=0.5)
        elif contract.repayment_procedure == "Равными долями":
            send_keys(win, "{F4}{DOWN}{ENTER}", pause=0.5)
        elif contract.repayment_procedure == "Индивидуальный":
            send_keys(win, "{F4}{DOWN 2}{ENTER}", pause=0.5)
        else:
            raise ValueError(f"Don't know what to do with {contract.repayment_procedure!r}...")
    else:
        # child_window(ds_form, ctrl="Edit", idx=2).set_text(contract.ds_id)
        click_type(win, child(ds_form, ctrl="Edit", idx=20), contract.iban, ent=True, cls=True)
        click_type(win, child(ds_form, ctrl="Edit", idx=12), contract.ds_id, ent=True, cls=True)
        click_type(win, child(ds_form, ctrl="Edit", idx=13), contract.ds_date, ent=True, cls=True)
        click_type(win, child(ds_form, ctrl="Edit", idx=7), contract.dbz_id, ent=True, cls=True)
        click_type(win, child(ds_form, ctrl="Edit", idx=8), contract.dbz_date, ent=True, cls=True)
        click_type(win, child(ds_form, ctrl="Edit", idx=9), contract.dbz_date, ent=True, cls=True)
        click_type(win, child(ds_form, ctrl="Edit", idx=10), contract.end_date, ent=True, cls=True)
        click_type(win, child(ds_form, ctrl="Edit", idx=11), rate.nominal_rate, ent=True, cls=True)
        click_type(win, child(ds_form, ctrl="Edit", idx=14), contract.loan_amount, ent=True, cls=True)

        if contract.credit_purpose == "Пополнение оборотных средств":
            click_type(win, child(ds_form, ctrl="Edit", idx=4), rate.rate_fee_one_two_three_year, ent=True)
            click_type(win, child(ds_form, ctrl="Edit", idx=18), contract.pos_amount, ent=True)
        elif contract.credit_purpose == "Инвестиционный":
            click_type(win, child(ds_form, ctrl="Edit", idx=19), contract.investment_amount, ent=True)
        elif contract.credit_purpose == "Инвестиционный + ПОС":
            click_type(win, child(ds_form, ctrl="Edit", idx=18), contract.pos_amount, ent=True)
            click_type(win, child(ds_form, ctrl="Edit", idx=19), contract.investment_amount, ent=True)
        else:
            raise ValueError(f"Don't know what to do with {contract.credit_purpose!r}...")

        click_type(win, child(ds_form, ctrl="Edit", idx=3), contract.decision_date, ent=True)

        # Вид погашения платежа - Аннуитетный/Равными долями/Индивидуальный
        click(win, child(ds_form, ctrl="Edit", idx=17))
        if contract.repayment_procedure == "Аннуитетный":
            send_keys(win, "{F4}{ENTER}", pause=0.5)
        elif contract.repayment_procedure == "Равными долями":
            send_keys(win, "{F4}{DOWN}{ENTER}", pause=0.5)
        elif contract.repayment_procedure == "Индивидуальный":
            send_keys(win, "{F4}{DOWN 2}{ENTER}", pause=0.5)
        else:
            raise ValueError(f"Don't know what to do with {contract.repayment_procedure!r}...")


def fill_contract(one_c: App, win: UiaWindow, form: UiaPane, contract: Contract, rate: InterestRate) -> UiaPane:
    click(win, child(form, title="БВУ/Рефинансирование", ctrl="TabItem"))

    # click(win, child_window(form, ctrl="Custom"))
    # sleep(0.5)
    # _send_keys(win, "{DOWN 10}")
    # click(win, child(form, title="Clone", ctrl="Button", idx=1))

    click(win, child(form, title="Add", ctrl="Button"))
    ds_form = win.child_window(control_type="Pane", found_index=51)

    fill_contract_details(win, ds_form, contract, rate)

    click(win, child(ds_form, title="Записать", ctrl="Button"))

    click(win, child(ds_form, title="Основные реквизиты", ctrl="TabItem"))
    click(win, child(ds_form, title="Распоряжения на изменения статуса договора", ctrl="Button"))

    change_ds_status_form = child(win, ctrl="Pane", idx=74)

    table = child(change_ds_status_form, ctrl="Table")

    contract_field = child(table, ctrl="Custom", idx=1)
    date_field = child(table, ctrl="Custom", idx=2)
    type_field = child(table, ctrl="Custom", idx=3)

    existing_contract = text(contract_field).replace(" Договор субсидирования", "")
    if not existing_contract:
        click_type(win, contract_field, "{F4}", double=True, pause=0.1)
        dict_win = child(win, ctrl="Pane", idx=88)
        click(win, child(dict_win, ctrl="Button", title="Set list filter and sort options..."))
        sort_win = one_c.app.window(title="Filter and Sort")

        check(child(sort_win, title="Deletion mark", ctrl="CheckBox"))
        check(child(sort_win, title="Номер договора субсидирования", ctrl="CheckBox"))

        click_type(sort_win, child(sort_win, ctrl="Edit", idx=17), contract.ds_id, spaces=True)
        click(sort_win, child(sort_win, ctrl="Button", title="OK"))

        if contains_text(child(dict_win, ctrl="Table")):
            click(win, child(dict_win, ctrl="Custom"), double=True)
        else:
            print(f"Contract {contract.ds_id} not found")
            send_keys(win, "{ESC}")

    click(win, table)
    click_type(win, date_field, contract.start_date, cls=True, pause=0.2, double=True, ent=True)

    click_type(win, type_field, "{F4}", double=True, pause=0.1)
    dict_win = child(win, ctrl="Pane", idx=88)
    click(win, child(dict_win, ctrl="Button", title="Set list filter and sort options..."))
    sort_win = one_c.app.window(title="Filter and Sort")
    check(child(sort_win, ctrl="CheckBox", idx=2))
    click_type(win, child(sort_win, ctrl="Edit", idx=5), "Подписан ДС", spaces=True)

    click(win, child(sort_win, ctrl="Button", title="OK"))

    click(win, child(dict_win, ctrl="Custom"), double=True)

    click(win, child(change_ds_status_form, ctrl="Button", title="OK"))

    if has_error(win):
        click(win, child(change_ds_status_form, ctrl="Button", title="Закрыть"))
        click(win, child(win, ctrl="Button", title="No"))

    if (yes_button := child(win, title="Yes", ctrl="Button")).exists():
        click(win, yes_button)
        sleep(1)

    click_type(win, child(ds_form, ctrl="Edit", idx=4), "{F4}")
    sleep(1)
    send_keys(win, "{ENTER}")

    return ds_form


def fill_1c(contract: Contract, rate: InterestRate) -> None:
    with App(app_path=r"C:\Users\robot3\Desktop\damu_1c\test_base.v8i") as one_c:
        win = window(one_c.app, title="Конфигурация.+", regex=True)
        win.wait(wait_for="exists", timeout=60)

        find_project(win=win, contract=contract)

        form = child(win, ctrl="Pane", idx=27)

        goto_button = child(form, title="Go to", ctrl="Button")
        fill_main_project_data(win, form, contract)
        change_date(win, form, goto_button, contract.protocol_date)
        change_sums(win, form, goto_button, contract)
        add_vypiska(one_c, win, form, contract)
        check_project_type(win, form, contract)

        ds_form = fill_contract(one_c, win, form, contract, rate)

        # click(win, child(ds_form, title="Find in list", ctrl="Button"))
        # list_form = child(win, ctrl="Pane", idx=74)

        click(win, child(ds_form, title="ПрикрепленныеДокументы", ctrl="TabItem"))

        click(win, child(ds_form, title="Add", ctrl="Button"))
        sleep(1)
        send_keys(win, "{F4}")

        open_file(one_c, contract.document_pdf_path)

        if (child(win, title="Value is not of object type (Сессия)", ctrl="Pane")).exists():
            click(win, child(win, title="OK", ctrl="Button"))
            sleep(1)

        click(win, child(child(win, ctrl="Pane", idx=63), ctrl="Button", title="OK"))

        click(win, child(ds_form, title="Записать", ctrl="Button"))

        click(win, child(ds_form, title="Открыть текущий График погашения", ctrl="Button"))
        click(win, child(win, title="Yes", ctrl="Button"))

        sleep(5)

        table_form = child(win, ctrl="Pane", idx=63)

        click_type(win, child(table_form, ctrl="Edit", idx=9), "13", ent=True)
        click_type(win, child(table_form, ctrl="Edit", idx=5), contract.start_date, ent=True)
        click_type(win, child(table_form, ctrl="Edit", idx=6), contract.end_date, ent=True)
        click_type(win, child(table_form, ctrl="Edit", idx=8), contract.settlement_date, ent=True)

        click(win, child(table_form, title="Загрузить из внешней таблицы (обн)", ctrl="Button"))

        open_file(one_c, contract.macro_path)

        menu_select_1c(
            win,
            table_form,
            trigger_btn_name="Проверка введенного графика",
            menu_names=[contract.category, contract.bank],
        )

        if (
            close_button := child(win, ctrl="Pane", idx=18).child_window(title="Close", control_type="Button")
        ).exists():
            click(win, close_button)

        click(win, child(table_form, title="Записать", ctrl="Button"))

        if (yes_button := child(win, title="Yes", ctrl="Button")).exists():
            click(win, yes_button)
            sleep(1)

        # click(win, child(table_form, title="OK", ctrl="Button"))
        #
        # if (yes_button := child(win, title="Yes", ctrl="Button")).exists():
        #     click(win, yes_button)
        #     sleep(1)
        #
        # click(win, child(ds_form, title="Записать", ctrl="Button"))
        #
        # potential_error_pane = child(win, ctrl="Pane", idx=2)
        # if "Operation cannot be performed" in (err_msg := text(child(potential_error_pane, ctrl="Pane"))):
        #     print(err_msg)
        #     click(win, child(potential_error_pane, ctrl="Button", title="OK"))
        #
        # # click(win, child(ds_form, title="Закрыть", ctrl="Button"))
        # click(win, child(ds_form, title="Передать на проверку", ctrl="Button"))
        #
        # sleep(2)
        #
        # check_form = child(win, ctrl="Pane", idx=63)
        # click(
        #     win,
        #     child(
        #         check_form,
        #         ctrl="Button",
        #         title="Внести информацию по текущему событию",
        #     ),
        # )
        # click(win, child(check_form, ctrl="Button", title="Записать и закрыть"))
        #
        # if (yes_button := child(win, title="Yes", ctrl="Button")).exists():
        #     click(win, yes_button)
        #     sleep(1)
        #
        # click(win, child(ds_form, title="Закрыть", ctrl="Button"))
        #
        # if (yes_button := child(win, title="Yes", ctrl="Button")).exists():
        #     click(win, yes_button)
        #     sleep(1)
        #
        # click(win, child(win, ctrl="Button", title="No"))
        #
        # if (yes_button := child(win, title="Yes", ctrl="Button")).exists():
        #     click(win, yes_button)
        #     sleep(1)
        #
        # click(win, child(form, ctrl="Button", title="OK"))


def dump_data(db: DatabaseManager, resources_folder: Path) -> None:
    temp_folder = Path("temp2")
    temp_folder.mkdir(exist_ok=True)
    contracts = list(Contract.iter_contracts(db, resources_folder))
    for c in contracts:
        c_folder = temp_folder / c.contract_id
        c_folder.mkdir(exist_ok=True)

        shutil.copyfile(c.document_path, c_folder / c.document_path.name)
        shutil.copyfile(c.macro_path, c_folder / c.macro_path.name)
        shutil.copyfile(c.document_pdf_path, c_folder / c.document_pdf_path.name)
        shutil.copyfile(c.protocol_pdf_path, c_folder / c.protocol_pdf_path.name)

    contracts = list(asdict(c) for c in Contract.iter_contracts(db, resources_folder))
    df = pd.DataFrame(contracts)

    df.drop(
        ["contract_id", "macro_path", "document_path", "document_pdf_path", "protocol_pdf_path", "category"],
        axis=1,
        inplace=True,
    )

    df["protocol_date"] = pd.to_datetime(df["protocol_date"], format="%d%m%Y")
    df["vypiska_date"] = pd.to_datetime(df["vypiska_date"], format="%d%m%Y")
    df["decision_date"] = pd.to_datetime(df["decision_date"], format="%d%m%Y")
    df["ds_date"] = pd.to_datetime(df["ds_date"], format="%d%m%Y")
    df["dbz_date"] = pd.to_datetime(df["dbz_date"], format="%d%m%Y")
    df["start_date"] = pd.to_datetime(df["start_date"], format="%d%m%Y")
    df["end_date"] = pd.to_datetime(df["end_date"], format="%d%m%Y")

    df.rename(
        columns={
            "contragent": "Контрагент",
            "project": "Название проекта",
            "bank": "Банк/Лизинг",
            "credit_purpose": "Цель кредитования",
            "repayment_procedure": "Вид погашения",
            "loan_amount": "Сумма кредита",
            "subsid_amount": "Сумма субсидирования",
            "investment_amount": "Сумма инвестирования",
            "pos_amount": "Сумма на ПОС",
            "protocol_date": "Дата протокола",
            "vypiska_date": "Дата выписки",
            "decision_date": "Дата решения",
            "settlement_date": "Дата расчета",
            "iban": "IBAN",
            "ds_id": "№ДС",
            "ds_date": "Дата ДС",
            "dbz_id": "№ДБЗ",
            "dbz_date": "Дата ДБЗ",
            "start_date": "Дата начала",
            "end_date": "Дата окончания",
            "protocol_id": "Номер протокола",
            "sed_number": "Номер СЭД",
        }
    )

    df.to_excel("Отчет.xlsx", index=False)


def main() -> None:
    warnings.simplefilter(action="ignore", category=UserWarning)
    warnings.simplefilter(action="ignore", category=InsecureRequestWarning)
    warnings.simplefilter(action="ignore", category=SyntaxWarning)

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
        contract_ids = [
            "d1e64465-57de-40ca-b009-67554074016f",
            "249dadb8-9651-43f7-b826-675295b90006",
            "8223d792-bfd0-4b78-abfa-67597f0b0342",
            "d75c5c24-cba7-4dec-8aac-674433e601a4",
            "2a65b501-9886-4e81-b636-6756d92e0027",
            "3db431d8-0ccb-46fb-bb8a-6760fbbc02ba",
        ]

        for i, contract_id in enumerate(contract_ids):
            reply = process_contract(logger=logger, db=db, contract_id=contract_id, edo=edo, crm=crm, registry=registry)
            logger.info(f"Reply - {reply!r}")
            # rate = InterestRate.load(db, contract.contract_id)
            #
            # fill_1c(contract, rate)


if __name__ == "__main__":
    main()
