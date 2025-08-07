from __future__ import annotations

import dataclasses
import inspect
import logging
import os
import re
import subprocess
import sys
import time
import traceback
import warnings
from contextlib import suppress
from datetime import date, datetime, timedelta
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING, cast

import dotenv
import pyperclip
import pytz
import yaml
from pywinauto import ElementNotFoundError, Application
from urllib3.exceptions import InsecureRequestWarning
from PIL import ImageGrab
import win32com.client as win32


project_folder = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_folder))
sys.path.append(str(project_folder / "src"))
os.chdir(str(project_folder))


from sverka.crm import CRM, get_contact_data
from sverka.edo import EDO
from sverka.process_contract import process_contract
from sverka.structures import Registry
from utils.automation import (
    check,
    child,
    click,
    click_type,
    contains_text,
    count_control_types,
    exists,
    menu_select_1c,
    send_keys,
    text,
    text_to_float,
    wait,
    window,
    get_full_text,
    switch_backend,
)
from utils.db_manager import DatabaseManager
from utils.utils import (
    TelegramAPI,
    humanize_timedelta,
    is_tomorrow,
    kill_all_processes,
)

if TYPE_CHECKING:
    from pywinauto import WindowSpecification

    from sverka.edo import Task
    from utils.automation import ButtonWrapper, ListItemWrapper, UIAPaneWrapper
    from sverka.process_contract import Contract
    from sverka.crm import PrimaryContact
    from utils.office import WordProto


class PotentialError(Exception): ...


def setup_logger(_today: date | None = None) -> Path:
    log_format = "[%(asctime)s] %(levelname)-8s %(filename)s:%(funcName)s:%(lineno)s %(message)s"
    formatter = logging.Formatter(log_format, datefmt="%H:%M:%S")

    root = logging.getLogger("DAMU")
    root.setLevel(logging.DEBUG)

    formatter.converter = lambda *args: datetime.now(
        pytz.timezone("Asia/Almaty")
    ).timetuple()

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(formatter)

    log_folder = Path("logs/zanesinie")
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

    root.addHandler(stream_handler)
    root.addHandler(file_handler)

    return logger_file


today = datetime.now(pytz.timezone("Asia/Almaty")).date()
os.environ["today"] = today.isoformat()
setup_logger(today)

logger = logging.getLogger("DAMU")


def reply_to_notification(
    edo: EDO, task: Task, bot: TelegramAPI, reply: str
) -> None:
    reply = inspect.cleandoc(reply).strip()
    logger.info(f"Notification reply - {reply!r}")

    bot.send_message(f"{task.doc_id}:\n{reply}")

    edo.reply_to_notification(task=task, reply=reply)


def prepare_project_query(contragent: str, protocol_id: str) -> str:
    query = f"""
        ВЫБРАТЬ Проекты.Ссылка
        ИЗ Справочник.Контрагенты КАК Агенты
        ВНУТРЕННЕЕ СОЕДИНЕНИЕ Справочник.Проектыконтрагентов КАК Проекты
        ПО Агенты.Ссылка = Проекты.Владелец
        ГДЕ Агенты.БИНИИН = "{contragent}" И Проекты.НомерПротокола = "{protocol_id}"
        УПОРЯДОЧИТЬ ПО Проекты.ДатаПротокола УБЫВ
    """

    query = re.sub(r" {2,}", "", query).strip().replace("\n", "~")
    return query


def prepare_contragent_query(contragent: str) -> str:
    query = f"""
        ВЫБРАТЬ
            Ссылка,
            РазмерСубъекта,
            Улица,
            ДомНомер,
            Телефон1,
            ФИОПервогоРуководителя,
            АдресЭлектроннойПочты,
            ПолноеНаименование,
            ПолРуководителя,
            ДатаРождения
        ИЗ Справочник.Контрагенты КАК Агенты
        ГДЕ Агенты.БИНИИН = "{contragent}"
    """

    query = re.sub(r" {2,}", "", query).strip().replace("\n", "~")
    return query


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
        self.nominal_rate /= 100
        self.rate_one_two_three_year /= 100
        self.rate_four_year /= 100
        self.rate_five_year /= 100
        self.rate_six_seven_year /= 100
        self.rate_fee_one_two_three_year /= 100
        self.rate_fee_four_year /= 100
        self.rate_fee_five_year /= 100
        self.rate_fee_six_seven_year /= 100

    @classmethod
    def load_db(cls, db: DatabaseManager, contract_id: str) -> InterestRate:
        raw_rate = db.request(
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
            req_type="fetch_one",
        )

        return InterestRate(*raw_rate)


def get_error(win: WindowSpecification) -> str | None:
    sleep(0.5)

    potential_alert = child(win, ctrl="Pane", idx=2)
    if (
        error_message := text(child(potential_alert, ctrl="Pane"))
    ) and "Fail" in error_message:
        logger.info(f"{error_message=!r}")
        click(win, child(potential_alert, ctrl="Button", title="OK"))

    messages_pane = child(win, ctrl="Pane", idx=18)
    close_button = child(messages_pane, title="Close", ctrl="Button")

    if exists(close_button):
        click_type(win, child(messages_pane, ctrl="Document"), "^a^c")

        error_message = pyperclip.paste()
        logger.info(f"{error_message=!r}")

        click(win, close_button)

        return error_message

    if error_message:
        return error_message

    return None


def open_file(file_path: Path) -> None:
    app = switch_backend("win32")
    save_dialog = app.window(title_re="Выберите ф.+")
    save_dialog["Edit0"].set_text(str(file_path))
    if not save_dialog.is_active():
        save_dialog.set_focus()
        save_dialog.wait(wait_for="visible")
    save_dialog.child_window(class_name="Button", found_index=0).click_input()


def fill_contragent_data(
    primary_contact: PrimaryContact, app: Application, win: WindowSpecification
) -> None:
    pane = child(win, ctrl="Pane", idx=26)

    click(win, child(pane, title="Дополнительно", ctrl="TabItem"))

    if primary_contact.subject_type:
        send_keys(win, "{F4}")
        click(
            win,
            child(
                win, ctrl="Button", title="Set list filter and sort options..."
            ),
        )
        sort_win = window(app, title="Filter and Sort")

        check(child(sort_win, title="Description", ctrl="CheckBox"))
        click_type(win, child(sort_win, ctrl="Edit", idx=2), "{DOWN}")
        click(win, child(sort_win, title="Contains", ctrl="ListItem"))
        click_type(
            win,
            child(sort_win, ctrl="Edit", idx=3),
            primary_contact.subject_type,
            spaces=True,
        )
        click(sort_win, child(sort_win, ctrl="Button", title="OK"))

        table = child(win, ctrl="Table", idx=1)
        table_text = get_full_text(table)
        if primary_contact.subject_type.lower() in table_text.lower():
            click(win, child(table, ctrl="Custom"), double=True)
        else:
            print(f"Subject type {primary_contact.subject_type!r} not found!")
            send_keys(win, "{ESC}")

    if primary_contact.address:
        click_type(
            win,
            child(pane, ctrl="Edit", idx=3),
            primary_contact.address,
            spaces=True,
        )

    if primary_contact.phone:
        click_type(win, child(pane, ctrl="Edit", idx=5), primary_contact.phone)

    if primary_contact.contact_name:
        click_type(
            win,
            child(pane, ctrl="Edit", idx=7),
            primary_contact.contact_name,
            spaces=True,
        )

    if primary_contact.email:
        click_type(win, child(pane, ctrl="Edit", idx=8), primary_contact.email)

    if primary_contact.full_contragent_name:
        click_type(
            win,
            child(pane, ctrl="Edit", idx=10),
            primary_contact.full_contragent_name,
            spaces=True,
        )

    if primary_contact.gender:
        click_type(
            win, child(pane, ctrl="Edit", idx=12), primary_contact.gender
        )

    if primary_contact.birth_date:
        click_type(
            win, child(pane, ctrl="Edit", idx=14), primary_contact.birth_date
        )

    click(win, child(pane, title="Записать", ctrl="Button"))
    click(win, child(pane, ctrl="Button", title="Закрыть"))


def find_row_by_query(
    win: WindowSpecification, query: str
) -> ListItemWrapper | WindowSpecification | None:
    click(win, child(win, title="Консоль запросов и обработчик", ctrl="Button"))

    with suppress(ElementNotFoundError):
        click(
            child(win, ctrl="Pane", idx=27),
            child(win, ctrl="Button", title="Maximize"),
        )

    query_document_box = child(win, ctrl="Document")

    click(win, query_document_box, button="right")
    send_keys(win, "{DOWN 3}~")
    send_keys(win, query, spaces=True, pause=0)
    click(win, child(win, title="Выполнить", ctrl="Button"))

    if not wait(
        child(win, title="Delete", ctrl="Button", idx=1), wait_for="is_enabled"
    ):
        return None

    result_parent = child(win, ctrl="Tab", title="Результат", idx=1)
    return result_parent


def fill_contragent(
    app: Application,
    win: WindowSpecification,
    contragent: str,
    primary_contact: PrimaryContact | None,
) -> None:
    if not primary_contact:
        return
    if not primary_contact.to_be_filled():
        return

    contact_copy = dataclasses.replace(primary_contact)

    query = prepare_contragent_query(contragent)
    result_parent = find_row_by_query(win=win, query=query)

    table = child(result_parent, ctrl="Table")
    if not table.exists():
        logger.warning(f"{contragent=} not found")
        return

    cells = table.descendants(control_type="Custom")
    for cell in cells:
        txt = cell.window_text().strip()
        splits: list[str] = txt.rsplit(" ", maxsplit=1)
        if len(splits) == 1:
            splits.insert(0, "")
        value, column = splits

        value = value.strip()
        column = column.strip()

        match column:
            case "РазмерСубъекта":
                contact_column = "subject_type"
            case "Улица":
                contact_column = "address"
            case "Телефон1":
                contact_column = "phone"
            case "ФИОПервогоРуководителя":
                contact_column = "contact_name"
            case "АдресЭлектроннойПочты":
                contact_column = "email"
            case "ПолноеНаименование":
                contact_column = "full_contragent_name"
            case "ПолРуководителя":
                contact_column = "gender"
            case "ДатаРождения":
                contact_column = "birth_date"
            case _:
                contact_column = ""

        if not contact_column:
            continue

        if value:
            setattr(contact_copy, contact_column, None)

    logger.info(f"{contact_copy=!r}")

    if contact_copy.to_be_filled():
        click(win, cells[0], double=True)

        click(win, child(win, ctrl="Document"))
        send_keys(win, "{ESC}")

        if exists(
            close_button := child(
                child(win, ctrl="Pane", idx=18), title="Close", ctrl="Button"
            )
        ):
            click(win, close_button)

        fill_contragent_data(primary_contact=contact_copy, app=app, win=win)
    else:
        click(win, child(win, ctrl="Document"))
        send_keys(win, "{ESC}")

        if exists(
            close_button := child(
                child(win, ctrl="Pane", idx=18), title="Close", ctrl="Button"
            )
        ):
            click(win, close_button)


def find_project(win: WindowSpecification, contract: Contract) -> bool:
    query = prepare_project_query(contract.contragent, contract.protocol_id)

    ImageGrab.grab().save(r"C:\Users\robot2\Desktop\robots\damu\screens\2.png")
    result_parent = find_row_by_query(win=win, query=query)

    row = child(result_parent, ctrl="ListItem")
    if not row or not row.exists():
        logger.warning(f"{contract.contract_id=} not found")
        return False

    click(win, row, double=True)

    click(win, child(win, ctrl="Document"))
    send_keys(win, "{ESC}")

    if exists(
        close_button := child(
            child(win, ctrl="Pane", idx=18), title="Close", ctrl="Button"
        )
    ):
        click(win, close_button)
    return True


def fill_main_project_data(
    app: Application,
    win: WindowSpecification,
    project_form: WindowSpecification | UIAPaneWrapper,
    contract: Contract,
) -> None:
    """
    :param app: pywinauto.Application
    :param win: Главное окно 1С
    :param project_form: Форма "Карточка проекта (форма элемента)"
    :param contract: Данные договора
    :return: None

    Заполнение данных в форме проекта во вкладке "Основные"
    (Цель кредитования, Номер протокола, Дата протокола, Дата получения протокола РКС филиалом)
    """

    if contract.region:
        click_type(
            win, child(project_form, ctrl="Edit", idx=6), "{F4}", cls=False
        )
        dict_win = child(win, ctrl="Pane", idx=56)
        click(
            dict_win,
            child(
                win, ctrl="Button", title="Set list filter and sort options..."
            ),
        )
        sort_win = window(app, title="Filter and Sort")
        check(child(sort_win, title="Owner", ctrl="CheckBox"))
        check(child(sort_win, title="Description", ctrl="CheckBox"))
        click_type(win, child(sort_win, ctrl="Edit", idx=4), "{DOWN}")
        click(win, child(sort_win, title="Contains", ctrl="ListItem"))
        click_type(
            win,
            child(sort_win, ctrl="Edit", idx=5),
            contract.region,
            spaces=True,
        )
        click(sort_win, child(sort_win, ctrl="Button", title="OK"))

        table = child(win, ctrl="Table")
        if contract.region.lower() in get_full_text(table).lower():
            click(win, child(table, ctrl="Custom"), double=True)
        else:
            print(f"Region {contract.region!r} not found!")
            send_keys(win, "{ESC}")

    click(win, child(parent=project_form, ctrl="Edit", idx=7))
    send_keys(
        win,
        "{F4}^f" + contract.credit_purpose + "{ENTER 2}",
        pause=0.1,
        spaces=True,
    )
    click_type(
        win,
        child(project_form, ctrl="Edit", idx=3),
        contract.protocol_date,
        cls=True,
    )


def change_date(
    win: WindowSpecification,
    form: WindowSpecification | UIAPaneWrapper,
    goto_button: WindowSpecification | ButtonWrapper,
    protocol_date: str,
) -> None:
    """
    :param win: Главное окно 1С
    :param form: Форма "Карточка проекта (форма элемента)"
    :param goto_button: "Go to" кнопка
    :param protocol_date: Дата протокола
    :return: None

    Возможное изменение даты протокола в форме проекта во вкладке "Пролонгация"
    """
    click(win, child(form, title="Пролонгация", ctrl="TabItem"))

    date_to_check = (
        text(child(form, ctrl="Custom", idx=1)).split(" ")[0].replace(".", "")
    )

    if date_to_check != protocol_date:
        click(win, goto_button)
        send_keys(win, "{DOWN}{ENTER 2}", pause=0.1)
        send_keys(win, protocol_date)
        send_keys(win, "{ENTER 4}{ESC}", pause=0.1)


def change_sums(
    win: WindowSpecification,
    form: WindowSpecification | UIAPaneWrapper,
    goto_button: WindowSpecification | ButtonWrapper,
    contract: Contract,
) -> None:
    """
    :param win: Главное окно 1С
    :param form: Форма "Карточка проекта (форма элемента)"
    :param goto_button: "Go to" кнопка
    :param contract: Данные договора
    :return: None

    Заполнение данных в форме проекта во вкладке "БВУ/Рефинансирование" в зависимости от цели кредитования
    (Сумма субсидирования, На инвестиции, На ПОС)
    """
    if contract.credit_purpose not in {
        "Пополнение оборотных средств",
        "Инвестиционный",
        "Инвестиционный + ПОС",
        "Рефинансирование",
    }:
        raise ValueError(
            f"Don't know what to do with {contract.credit_purpose!r}..."
        )

    click(win, child(form, title="БВУ/Рефинансирование", ctrl="TabItem"))
    click(win, goto_button)
    send_keys(win, "{DOWN 8}{ENTER}", pause=0.2)

    list_win = child(win, ctrl="Pane", idx=51)

    existing_pos_amount = text_to_float(
        text(child(list_win, ctrl="Custom", idx=5)).replace(
            " Возобновляемая часть", ""
        ),
        default=0.0,
    )
    existing_investment_amount = text_to_float(
        text(child(list_win, ctrl="Custom", idx=6)).replace(
            " Не возобновляемая часть", ""
        ),
        default=0.0,
    )

    send_keys(win, "{ENTER}", pause=0.2)

    if (
        contract.credit_purpose == "Пополнение оборотных средств"
        and existing_pos_amount != contract.subsid_amount
    ):
        send_keys(win, "{TAB 4}" + str(contract.subsid_amount), pause=0.1)
    elif (
        contract.credit_purpose == "Инвестиционный"
        and existing_investment_amount != contract.subsid_amount
    ):
        send_keys(win, "{TAB 5}" + str(contract.subsid_amount), pause=0.1)
    elif contract.credit_purpose == "Инвестиционный + ПОС":
        if (
            existing_pos_amount != contract.subsid_amount
            and existing_investment_amount != contract.subsid_amount
        ):
            send_keys(
                win,
                "{TAB 4}"
                + str(contract.subsid_amount)
                + "{TAB}"
                + str(contract.subsid_amount),
                pause=0.1,
            )
        elif existing_pos_amount != contract.subsid_amount:
            send_keys(win, "{TAB 4}" + str(contract.subsid_amount), pause=0.1)
        elif existing_investment_amount != contract.subsid_amount:
            send_keys(win, "{TAB 5}" + str(contract.subsid_amount), pause=0.1)

    send_keys(win, "{ESC}", pause=0.5)
    with suppress(ElementNotFoundError):
        click(win, child(win, title="Yes", ctrl="Button"))
    send_keys(win, "{ESC}", pause=0.5)


def attach_vypiska(
    win: WindowSpecification,
    form: WindowSpecification | UIAPaneWrapper,
    contract: Contract,
) -> None:
    """
    :param win: Главное окно 1С
    :param form: Форма "Карточка проекта (форма элемента)"
    :param contract: Данные договора
    :return: None

    Прикрепление файла выписки из CRM во вкладке "Прикрепленные документы"
    """

    click(win, child(form, title="Прикрепленные документы", ctrl="TabItem"))

    click(win, child(form, title="Add", ctrl="Button"))
    click(win, child(win, ctrl="Edit", idx=5))
    send_keys(win, "{F4}")

    protocol_pdf_path = cast(Path, contract.protocol_pdf_path)
    open_file(protocol_pdf_path)

    if exists(
        child(win, title="Value is not of object type (Сессия)", ctrl="Pane")
    ):
        click(win, child(win, title="OK", ctrl="Button"))
        sleep(1)

    click(win, child(win, title="OK", ctrl="Button", idx=1))


def check_project_type(
    win: WindowSpecification,
    form: WindowSpecification | UIAPaneWrapper,
    contract: Contract,
) -> None:
    if contract.credit_purpose == "Пополнение оборотных средств":
        click(win, child(form, title="Признаки проекта", ctrl="TabItem"))
        check(child(form, title="Возобновляемый проект", ctrl="CheckBox"))


def fill_contract_details(
    win: WindowSpecification,
    ds_form: WindowSpecification | UIAPaneWrapper,
    contract: Contract,
    rate: InterestRate,
) -> None:
    edit_count = count_control_types(ds_form, ctrl="Edit")

    if edit_count == 22:
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=21),
            contract.iban,
            ent=True,
            cls=True,
        )
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=12),
            contract.ds_id,
            ent=True,
            cls=True,
        )
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=13),
            contract.ds_date,
            ent=True,
            cls=True,
        )
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=7),
            contract.dbz_id,
            ent=True,
            cls=True,
        )
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=8),
            contract.contract_start_date,
            ent=True,
            cls=True,
        )
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=9),
            contract.contract_start_date,
            ent=True,
            cls=True,
        )
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=10),
            contract.contract_end_date,
            ent=True,
            cls=True,
        )
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=11),
            str(rate.nominal_rate),
            ent=True,
            cls=True,
        )
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=17),
            str(rate.rate_one_two_three_year),
            ent=True,
            cls=True,
        )
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=14),
            str(contract.loan_amount),
            ent=True,
            cls=True,
        )

        if contract.credit_purpose == "Инвестиционный":
            click_type(
                win,
                child(ds_form, ctrl="Edit", idx=20),
                str(contract.loan_amount),
                ent=True,
                cls=True,
            )
        elif contract.credit_purpose == "Пополнение оборотных средств":
            click_type(
                win,
                child(ds_form, ctrl="Edit", idx=19),
                str(contract.loan_amount),
                ent=True,
                cls=True,
            )
        elif contract.credit_purpose == "Инвестиционный + ПОС":
            click_type(
                win,
                child(ds_form, ctrl="Edit", idx=19),
                str(contract.loan_amount),
                ent=True,
            )
            click_type(
                win,
                child(ds_form, ctrl="Edit", idx=20),
                str(contract.loan_amount),
                ent=True,
            )

        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=3),
            contract.decision_date,
            ent=True,
            cls=True,
        )

        # Вид погашения платежа - Аннуитетный/Равными долями/Индивидуальный
        click(win, child(ds_form, ctrl="Edit", idx=18))
        if contract.repayment_procedure == "Аннуитетный":
            send_keys(win, "{F4}{ENTER}", pause=0.2)
        elif contract.repayment_procedure == "Равными долями":
            send_keys(win, "{F4}{DOWN}{ENTER}", pause=0.2)
        elif contract.repayment_procedure == "Индивидуальный":
            send_keys(win, "{F4}{DOWN 2}{ENTER}", pause=0.2)
        else:
            raise ValueError(
                f"Don't know what to do with {contract.repayment_procedure!r}..."
            )
    else:
        # child_window(ds_form, ctrl="Edit", idx=2).set_text(contract.ds_id)
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=20),
            contract.iban,
            ent=True,
            cls=True,
        )
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=12),
            contract.ds_id,
            ent=True,
            cls=True,
        )
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=13),
            contract.ds_date,
            ent=True,
            cls=True,
        )
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=7),
            contract.dbz_id,
            ent=True,
            cls=True,
        )
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=8),
            contract.contract_start_date,
            ent=True,
            cls=True,
        )
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=9),
            contract.contract_start_date,
            ent=True,
            cls=True,
        )
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=10),
            contract.contract_end_date,
            ent=True,
            cls=True,
        )
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=11),
            str(rate.nominal_rate),
            ent=True,
            cls=True,
        )
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=14),
            str(contract.loan_amount),
            ent=True,
            cls=True,
        )

        if contract.credit_purpose == "Пополнение оборотных средств":
            click_type(
                win,
                child(ds_form, ctrl="Edit", idx=4),
                str(rate.rate_fee_one_two_three_year),
                ent=True,
            )
            click_type(
                win,
                child(ds_form, ctrl="Edit", idx=18),
                str(contract.loan_amount),
                ent=True,
            )
        elif contract.credit_purpose == "Инвестиционный":
            click_type(
                win,
                child(ds_form, ctrl="Edit", idx=19),
                str(contract.loan_amount),
                ent=True,
            )
        elif contract.credit_purpose == "Инвестиционный + ПОС":
            click_type(
                win,
                child(ds_form, ctrl="Edit", idx=18),
                str(contract.loan_amount),
                ent=True,
            )
            click_type(
                win,
                child(ds_form, ctrl="Edit", idx=19),
                str(contract.loan_amount),
                ent=True,
            )
        else:
            raise ValueError(
                f"Don't know what to do with {contract.credit_purpose!r}..."
            )

        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=3),
            contract.decision_date,
            ent=True,
        )

        # Вид погашения платежа - Аннуитетный/Равными долями/Индивидуальный
        click(win, child(ds_form, ctrl="Edit", idx=17))
        if contract.repayment_procedure == "Аннуитетный":
            send_keys(win, "{F4}{ENTER}", pause=0.2)
        elif contract.repayment_procedure == "Равными долями":
            send_keys(win, "{F4}{DOWN}{ENTER}", pause=0.2)
        elif contract.repayment_procedure == "Индивидуальный":
            send_keys(win, "{F4}{DOWN 2}{ENTER}", pause=0.2)
        else:
            raise ValueError(
                f"Don't know what to do with {contract.repayment_procedure!r}..."
            )


def correct_rate_date(
    table: WindowSpecification, idx: int, start_date_rate: str
) -> None:
    elem = child(table, ctrl="Custom", idx=idx)
    if not elem.exists():
        logger.error(f"Custom{idx} does not exist in the table")
        return

    txt = elem.window_text().strip()
    if "Период" not in txt:
        logger.error(f"Found wrong cell with txt - {txt!r}")
        return

    txt = txt.replace("Период", "").strip()
    rate_date = datetime.strptime(txt, "%d.%m.%Y").date()
    start_date_rate_obj = datetime.fromisoformat(start_date_rate)
    if rate_date == start_date_rate_obj:
        logger.info(f"No need to correct dates for this period")

    start_date_four_year_str = datetime.strftime(start_date_rate_obj, "%d%m%Y")
    click_type(table, elem, start_date_four_year_str, double=True)


def correct_rates(ds_form: WindowSpecification, rate: InterestRate) -> None:
    rate_table = child(ds_form, ctrl="Table")

    correct_rate_date(
        table=rate_table,
        idx=1,
        start_date_rate=rate.start_date_one_two_three_year,
    )
    if rate.rate_four_year:
        correct_rate_date(
            table=rate_table, idx=6, start_date_rate=rate.start_date_four_year
        )
    if rate.rate_five_year:
        correct_rate_date(
            table=rate_table, idx=11, start_date_rate=rate.start_date_five_year
        )


def fill_contract(
    app: Application,
    win: WindowSpecification,
    form: WindowSpecification | UIAPaneWrapper,
    contract: Contract,
    rate: InterestRate,
) -> WindowSpecification | UIAPaneWrapper:
    click(win, child(form, title="БВУ/Рефинансирование", ctrl="TabItem"))

    # click(win, child_window(form, ctrl="Custom"))
    # sleep(0.5)
    # _send_keys(win, "{DOWN 10}")
    # click(win, child(form, title="Clone", ctrl="Button", idx=1))

    click(win, child(form, title="Add", ctrl="Button"))
    ds_form = child(win, ctrl="Pane", idx=51)

    click(win, child(ds_form, title="Обновить", ctrl="Button"))
    if (yes_button := child(win, title="Yes", ctrl="Button")).exists():
        click(win, yes_button)
        sleep(1)

    fill_contract_details(win, ds_form, contract, rate)

    click(win, child(ds_form, title="Записать", ctrl="Button"))

    if error_msg := get_error(win):
        raise PotentialError(error_msg)

    if rate.rate_four_year:
        correct_rates(ds_form, rate)
        click(win, child(ds_form, title="Записать", ctrl="Button"))

    click(win, child(ds_form, title="Основные реквизиты", ctrl="TabItem"))
    click(
        win,
        child(
            ds_form,
            title="Распоряжения на изменения статуса договора",
            ctrl="Button",
        ),
    )

    change_ds_status_form = child(win, ctrl="Pane", idx=74)

    table = child(change_ds_status_form, ctrl="Table")

    contract_field = child(table, ctrl="Custom", idx=1)
    date_field = child(table, ctrl="Custom", idx=2)
    type_field = child(table, ctrl="Custom", idx=3)

    existing_contract = text(contract_field).replace(
        " Договор субсидирования", ""
    )
    if not existing_contract:
        click_type(win, contract_field, "{F4}", double=True, pause=0.1)
        dict_win = child(win, ctrl="Pane", idx=88)
        click(
            win,
            child(
                dict_win,
                ctrl="Button",
                title="Set list filter and sort options...",
            ),
        )
        sort_win = window(app, title="Filter and Sort")

        check(child(sort_win, title="Deletion mark", ctrl="CheckBox"))
        check(
            child(
                sort_win, title="Номер договора субсидирования", ctrl="CheckBox"
            )
        )

        click_type(
            sort_win,
            child(sort_win, ctrl="Edit", idx=17),
            contract.ds_id,
            spaces=True,
        )
        click(sort_win, child(sort_win, ctrl="Button", title="OK"))

        if contains_text(child(dict_win, ctrl="Table")):
            click(win, child(dict_win, ctrl="Custom"), double=True)
        else:
            print(f"Contract {contract.ds_id} not found")
            send_keys(win, "{ESC}")

    click(win, table)
    click_type(
        win,
        date_field,
        contract.start_date,
        cls=True,
        pause=0.1,
        double=True,
        ent=True,
    )

    click_type(win, type_field, "{F4}", double=True, pause=0.1)
    dict_win = child(win, ctrl="Pane", idx=88)
    click(
        win,
        child(
            dict_win, ctrl="Button", title="Set list filter and sort options..."
        ),
    )
    sort_win = window(app, title="Filter and Sort")
    check(child(sort_win, ctrl="CheckBox", idx=2))
    send_keys(sort_win, "{TAB 2}")
    send_keys(sort_win, "Подписан ДС", spaces=True)

    click(win, child(sort_win, ctrl="Button", title="OK"))

    click(win, child(dict_win, ctrl="Custom"), double=True)

    click(win, child(change_ds_status_form, ctrl="Button", title="Post"))

    click(win, child(change_ds_status_form, ctrl="Button", title="Записать"))
    click(win, child(change_ds_status_form, ctrl="Button", title="Закрыть"))
    if exists(yes_button := child(win, title="Yes", ctrl="Button")):
        click(win, yes_button)
        sleep(1)

    click_type(win, child(ds_form, ctrl="Edit", idx=4), "{F4}", cls=False)
    sleep(1)
    send_keys(win, "{ENTER}")
    click_type(win, child(ds_form, ctrl="Edit", idx=4), "^+{F4}", cls=False)

    return ds_form


def attach_document(
    win: WindowSpecification,
    ds_form: WindowSpecification | UIAPaneWrapper,
    document_pdf_path: Path,
) -> None:
    click(win, child(ds_form, title="ПрикрепленныеДокументы", ctrl="TabItem"))

    click(win, child(ds_form, title="Add", ctrl="Button"))
    sleep(1)
    send_keys(win, "{F4}")

    open_file(document_pdf_path)

    if (
        child(win, title="Value is not of object type (Сессия)", ctrl="Pane")
    ).exists():
        click(win, child(win, title="OK", ctrl="Button"))
        sleep(1)

    click(
        win, child(child(win, ctrl="Pane", idx=63), ctrl="Button", title="OK")
    )


def attach_graph(
    win: WindowSpecification,
    ds_form: WindowSpecification | UIAPaneWrapper,
    contract: Contract,
    macro_path: Path,
) -> bool:
    click(
        win,
        child(ds_form, title="Открыть текущий График погашения", ctrl="Button"),
    )
    click(win, child(win, title="Yes", ctrl="Button"))

    sleep(5)

    table_form = child(win, ctrl="Pane", idx=63)

    click_type(win, child(table_form, ctrl="Edit", idx=9), "13", ent=True)
    click_type(
        win,
        child(table_form, ctrl="Edit", idx=5),
        contract.start_date,
        ent=True,
    )
    click_type(
        win, child(table_form, ctrl="Edit", idx=6), contract.end_date, ent=True
    )
    click_type(
        win,
        child(table_form, ctrl="Edit", idx=8),
        str(contract.settlement_date),
        ent=True,
    )

    click(
        win,
        child(
            table_form,
            title="Загрузить из внешней таблицы (обн)",
            ctrl="Button",
        ),
    )

    open_file(macro_path)

    table_checked = True
    try:
        menu_select_1c(
            win, table_form, trigger_btn_name="Проверка введенного графика"
        )
    except ElementNotFoundError:
        table_checked = False

    if (
        close_button := child(win, ctrl="Pane", idx=18).child_window(
            title="Close", control_type="Button"
        )
    ).exists():
        click(win, close_button)

    click(win, child(table_form, title="Записать", ctrl="Button"))

    if (yes_button := child(win, title="Yes", ctrl="Button")).exists():
        click(win, yes_button)
        sleep(1)

    click(win, child(table_form, title="Закрыть", ctrl="Button"))

    return table_checked


def open_1c(
    app_path: Path | str, bin_path: Path | str
) -> tuple[Application, WindowSpecification]:
    kill_all_processes("1cv8.exe")

    os.startfile(app_path)

    app = None
    while app is None:
        app = Application(backend="uia").connect(path=bin_path)
        sleep(1)

    # assert app is not None and app.is_process_running(), Exception(
    #     "App has not been opened"
    # )

    win = window(app, title="Конфигурация.+", regex=True)
    windows = app.windows()
    logger.info(f"{windows=!r}")
    logger.info(f"{[t for w in windows if (t := w.window_text().strip())]=!r}")
    logger.info(f"{app.is_process_running()=!r}")

    win.wait(wait_for="exists", timeout=60)
    win.maximize()

    return app, win


def fill_1c(
    contract: Contract,
    rate: InterestRate,
    _: PrimaryContact | None,
    registry: Registry,
    base_name: str,
) -> str:
    if not contract.ds_id:
        return "Не удалось получить номер договора из .docx файла"
    if not contract.ds_date:
        return "Не удалось получить дату договора из .docx файла"

    macro_path = cast(Path, contract.macro_path)
    document_pdf_path = cast(Path, contract.document_pdf_path)

    app_path = registry.resources_folder / base_name
    bin_path = os.environ["ONE_C_PATH"]

    app, win = open_1c(app_path, bin_path)

    ImageGrab.grab().save(r"C:\Users\robot2\Desktop\robots\damu\screens\1.png")

    # try:
    #     fill_contragent(
    #         app=app,
    #         win=win,
    #         contragent=contract.contragent,
    #         primary_contact=primary_contact,
    #     )
    # except Exception as err:
    #     logger.warning(f"Filling contragent form failed - {str(err)!r}")
    #
    # try:
    #     click(
    #         win,
    #         child(win, title="Консоль запросов и обработчик", ctrl="Button"),
    #     )
    #     send_keys(win, "{ESC}")
    # except Exception as err:
    #     logger.warning(f"{str(err)!r}")
    #     app.kill()
    #     app, win = open_1c(app_path, bin_path)

    if not find_project(win=win, contract=contract):
        logger.error(f"Project not found")
        return (
            f"Не удалось найти проект '{contract.project.strip()}' контрагента c БИН "
            f"'{contract.contragent}' и номером протокола '{contract.protocol_id}'"
        )

    logger.info("Project found")

    project_form = child(win, ctrl="Pane", idx=27)

    click(win, child(project_form, title="Read", ctrl="Button"))
    if (yes_button := child(win, title="Yes", ctrl="Button")).exists():
        click(win, yes_button)
        sleep(1)

    if "Транш" not in contract.contract_type:
        goto_button = child(project_form, title="Go to", ctrl="Button")
        fill_main_project_data(app, win, project_form, contract)
        logger.info("Main data filled")
        change_date(win, project_form, goto_button, contract.protocol_date)
        logger.info("Dates changed")
        change_sums(win, project_form, goto_button, contract)
        logger.info("Sums changed")

    attach_vypiska(win, project_form, contract)
    logger.info("Vypiska attached")

    if "Транш" not in contract.contract_type:
        check_project_type(win, project_form, contract)
        logger.info("Project checked")

    click(win, child(project_form, title="Записать", ctrl="Button"))

    try:
        ds_form = fill_contract(app, win, project_form, contract, rate)
    except PotentialError as err:
        return f"Неизвестная ошибка 1С. {str(err)!r}"

    attach_document(win, ds_form, document_pdf_path)

    click(win, child(ds_form, title="Записать", ctrl="Button"))

    table_checked = attach_graph(win, ds_form, contract, macro_path)

    click(win, child(ds_form, title="Записать", ctrl="Button"))
    click(win, child(ds_form, title="OK", ctrl="Button"))

    click(win, child(project_form, title="OK", ctrl="Button"))

    reply = "Договор успешно занесен в 1С"

    if contract.credit_purpose == "Рефинансирование":
        reply += " в проект с целевым назначением 'Рефинансирование'"

    if re.match(r"ПР-\d+", contract.ds_id) is not None:
        reply += f" с номером '{contract.ds_id}' от {contract.bank}"

    if not table_checked:
        reply += ". Не удалось проверить график через меню 'Проверка введенного графика'"

    logger.info(f"{reply=!r}")

    if app:
        app.kill()

    return reply


def update_result(db: DatabaseManager, contract_id: str, result: bool) -> None:
    db.request(
        """
        INSERT OR REPLACE INTO results
            (id, result)
        VALUES
            (?, ?)
        """,
        (contract_id, int(result)),
        req_type="execute",
    )


def process_notification(
    db: DatabaseManager,
    edo: EDO,
    crm: CRM,
    registry: Registry,
    task: Task,
    word: WordProto,
) -> str:
    document_url = edo.get_attached_document_url(task.doctype_id, task.doc_id)
    if not document_url:
        reply = "Не найден приложенный документ на странице поручения."
        return reply

    contract_id = document_url.split("/")[-1]
    logger.info(f"{contract_id=!r}")

    contract_filled = db.request(
        "SELECT id FROM results WHERE id = ? AND result = 1",
        (contract_id,),
        req_type="fetch_one",
    )

    if contract_filled:
        return "Данный договор уже занесен в 1С"

    # if contract_id in ["8f3e0e23-eb5d-4449-a8da-687f3c1c030c"]:
    #     return "Неизвестная ошибка"

    with crm:
        contract, reply = process_contract(
            logger=logger,
            db=db,
            contract_id=contract_id,
            edo=edo,
            crm=crm,
            registry=registry,
            word=word,
        )
        reply = None if ("Расхождения" in (reply or "")) else reply

        if contract:
            rate = InterestRate.load_db(db, contract.contract_id)
            logger.info(f"{contract=!r}")
            logger.info(f"{rate=!r}")

            try:
                primary_contact = get_contact_data(
                    crm=crm, contragent_id=contract.contragent_id
                )
                logger.info(f"{primary_contact=!r}")
            except Exception as err:
                logger.exception(err)
                primary_contact = None

            try:
                reply = fill_1c(
                    contract, rate, primary_contact, registry, "base.v8i"
                )
                if "Договор успешно занесен в 1С" in reply:
                    if edo.mark_as_filled(contract_id):
                        logger.info(f"Contract is marked as filled")

                    update_result(db=db, contract_id=contract_id, result=True)

                return reply
            except (Exception, BaseException) as e:
                logger.exception(e)
                update_result(db=db, contract_id=contract_id, result=False)
                err = traceback.format_exc()
                return f"Неизвестная ошибка\n{err}"

    if "Договор успешно занесен в 1С" not in reply:
        update_result(db=db, contract_id=contract_id, result=False)

    return reply


def get_session() -> str:
    res = subprocess.run(
        ["query", "user", "%USERNAME%"], shell=True, capture_output=True
    )
    stdout = res.stdout.decode("utf-8")
    lines = [
        re.split(r"\s+", line.strip())
        for l in stdout.split("\r\n")
        if (line := l.strip())
    ]
    if len(lines) > 1:
        session = dict()
        for line in lines[1::]:
            for col, val in zip(lines[0], line):
                session[col] = val
            break

        return yaml.dump(session, indent=2)
    else:
        return "No active session"


# FIXME
failed_tasks: set[str] = set()


def process_notifications(
    db: DatabaseManager,
    edo: EDO,
    crm: CRM,
    registry: Registry,
    bot: TelegramAPI,
    word: WordProto,
) -> int:
    default_wait_time = 180

    with edo:
        tasks = edo.get_tasks()

        logger.info(f"{tasks=!r}")

        logger.info(f"Found {len(tasks)} tasks")
        if not tasks:
            logger.info("Nothing to work on - sleeping...")
            return default_wait_time
        else:
            bot.send_message(f"\n{get_session()}")
            bot.send_message(f"Found {len(tasks)} tasks")

        for task in tasks:
            if task.doc_id in failed_tasks:
                continue

            logger.info(f"Working on task {task}")
            try:
                reply = process_notification(
                    db=db,
                    edo=edo,
                    crm=crm,
                    registry=registry,
                    task=task,
                    word=word,
                )

                if "Неизвестная ошибка" in reply:
                    failed_tasks.add(task.doc_id)
                    logger.error(reply)
                    tg_dev_name = os.environ["TG_DEV_NAME"]
                    bot.send_message(
                        f"@{tg_dev_name} Поймана ошибка\n{reply}\nкол-во неизвестных {len(failed_tasks)}."
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

                bot.send_message(traceback.format_exc())
                failed_tasks.add(task.doc_id)

                logger.error(
                    f"Поймана ошибка - кол-во неизвестных {len(failed_tasks)}."
                )
                continue

    if failed_tasks:
        return default_wait_time
    else:
        return 0


def main() -> None:
    warnings.simplefilter(action="ignore", category=UserWarning)
    warnings.simplefilter(action="ignore", category=InsecureRequestWarning)
    warnings.simplefilter(action="ignore", category=SyntaxWarning)

    dotenv.load_dotenv(".env")

    registry = Registry(
        download_folder=Path(f"downloads/zanesenie/{today}"),
        db_name="zanesenie",
    )

    edo = EDO(
        user=os.environ["EDO_1C_USERNAME"],
        password=os.environ["EDO_1C_PASSWORD"],
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

    word: WordProto = win32.DispatchEx("Word.Application")
    word.Visible = 0
    word.DisplayAlerts = 0
    word.AutomationSecurity = 3

    bot = TelegramAPI(process_name="zan")

    bot.send_message('START of the process "Занесение договоров"')
    logger.info('START of the process "Занесения договоров"')

    start_time = time.time()
    max_duration = 12 * 60 * 60
    tomorrow = today + timedelta(days=1)

    bot.send_message(f"{__debug__=!r}")
    bot.send_message(f"{sys.argv=!r}")
    bot.send_message(f"{sys.executable=!r}")

    try:
        while True:
            elapsed_time = time.time() - start_time
            logger.info(f"Elapsed time: {humanize_timedelta(elapsed_time)!r}")

            if elapsed_time >= max_duration or is_tomorrow(tomorrow):
                logger.info("Time passed. Stopping the loop.")
                break

            with DatabaseManager(registry.database) as db:
                duration = process_notifications(
                    db=db,
                    edo=edo,
                    crm=crm,
                    registry=registry,
                    bot=bot,
                    word=word,
                )

            logger.info(
                f"Current batch is processed - sleeping for {humanize_timedelta(duration)}..."
            )
            time.sleep(duration)

    finally:
        word.Quit()
        logger.info("FINISH")


if __name__ == "__main__":
    main()
