from __future__ import annotations

import dataclasses
import inspect
import logging
import os
import re
import sys
import time
import warnings
from collections.abc import Generator
from contextlib import suppress
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from time import sleep
from typing import cast
from urllib.parse import urljoin

import dotenv
import httpx
import pyperclip
import pytz
from pywinauto import ElementNotFoundError, WindowSpecification
from pywinauto.controls.uiawrapper import UIAWrapper
from urllib3.exceptions import InsecureRequestWarning

project_folder = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_folder))
sys.path.append(str(project_folder / "src"))
os.chdir(str(project_folder))


from sverka.crm import CRM
from sverka.edo import EDO, Task
from sverka.process_contract import process_contract
from sverka.structures import Registry
from utils._automation import (
    _ButtonWrapper,
    _ListItemWrapper,
    _ListViewWrapper,
    _UIAPaneWrapper,
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
)
from utils.app import App
from utils.db_manager import DatabaseManager
from utils.office import docx_to_pdf
from utils.utils import humanize_timedelta, is_tomorrow


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
    edo: EDO, task: Task, bot: TelegramAPI, reply: str
) -> None:
    reply = inspect.cleandoc(reply)
    logger.info(f"Notification reply - {reply!r}")

    bot.send_message(f"{task.doc_id}:\n{reply}")

    # if reply != "Согласовано. Не найдено замечаний.":
    #     return

    edo.reply_to_notification(task=task, reply=reply)


def prepare_query(contragent: str, protocol_id: str) -> str:
    query = f"""
        ВЫБРАТЬ Проекты.Ссылка
        ИЗ Справочник.Контрагенты КАК Агенты
        ВНУТРЕННЕЕ СОЕДИНЕНИЕ Справочник.Проектыконтрагентов КАК Проекты
        ПО Агенты.Ссылка = Проекты.Владелец
        ГДЕ Агенты.БИНИИН = "{contragent}" И Проекты.НомерПротокола = "{protocol_id}"
        УПОРЯДОЧИТЬ ПО Проекты.ДатаПротокола УБЫВ
    """

    query = re.sub(r" {2,}", "", query).strip()

    return query


def iso_to_standard(dt: str) -> str:
    if isinstance(dt, date):
        return dt.strftime("%d.%m.%Y")
    if dt[2] == "." and dt[5] == ".":
        return dt
    return datetime.fromisoformat(dt).strftime("%d.%m.%Y")


@dataclasses.dataclass(slots=True)
class Contract:
    contract_id: str
    contract_type: str
    contragent: str
    project: str
    bank: str
    credit_purpose: str
    repayment_procedure: str
    loan_amount: float
    subsid_amount: float
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
    macro_path: Path
    document_pdf_path: Path | None = None
    protocol_pdf_path: Path | None = None
    category: str | None = None

    def __post_init__(self) -> None:
        self.protocol_date = iso_to_standard(self.protocol_date).replace(
            ".", ""
        )
        self.vypiska_date = iso_to_standard(self.vypiska_date).replace(".", "")
        self.ds_date = iso_to_standard(self.ds_date).replace(".", "")
        self.dbz_date = iso_to_standard(self.dbz_date).replace(".", "")
        self.start_date = iso_to_standard(self.start_date).replace(".", "")
        self.end_date = iso_to_standard(self.end_date).replace(".", "")
        self.decision_date = iso_to_standard(self.decision_date).replace(
            ".", ""
        )

        contract_folder = (
            "downloads/zanesenie"
            / Path(str(os.environ["today"]))
            / self.contract_id
        ).absolute()
        with suppress(FileNotFoundError):
            self.protocol_pdf_path = next(
                (contract_folder / "vypiska").iterdir(), None
            )

        document_folder = contract_folder / "documents"

        self.document_path = document_folder / Path(self.document_path)

        self.document_pdf_path = self.document_path.with_suffix(".pdf")
        if not self.document_pdf_path.exists():
            docx_to_pdf(self.document_path, self.document_pdf_path)

        assert isinstance(self.macro_path, bytes)
        macro_path = document_folder / "macro.xlsx"
        with macro_path.open("wb") as f:
            f.write(self.macro_path)

        self.macro_path = macro_path

        protocol_ids = self.protocol_id.split(";")
        if "Транш" in self.contract_type:
            self.protocol_id = protocol_ids[0]
        else:
            self.protocol_id = protocol_ids[-1]

    @classmethod
    def iter_contracts(
        cls, db: DatabaseManager, bank_mapping: dict[str, dict[str, str]]
    ) -> Generator[Contract]:
        raw_contracts = db.request(
            """
        SELECT
            c.id,
            c.contract_type,
            c.contragent,
            c.project,
            c.bank,
            c.credit_purpose,
            c.repayment_procedure,
            c.loan_amount,
            c.subsid_amount,
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
        """,
            req_type="fetch_all",
        )

        for raw_contract in raw_contracts:
            contract = Contract(*raw_contract)

            if bvu := bank_mapping.get("БВУ", {}):
                category = "БВУ"
                bank = bvu.get(contract.bank)
            else:
                category = "Лизинг"
                bank = bank_mapping.get(contract.category or "", {}).get(
                    contract.bank
                )

            if bank is None:
                # raise ValueError(f"Unknown bank - {contract.bank}")
                continue

            contract.category = category
            contract.bank = bank

            yield contract


def get_contract(
    contract_id: str,
    db: DatabaseManager,
    bank_mapping: dict[str, dict[str, str]],
) -> Contract:
    raw_contract = db.request(
        """
        SELECT
            c.id,
            c.contract_type,
            c.contragent,
            c.project,
            c.bank,
            c.credit_purpose,
            c.repayment_procedure,
            c.loan_amount,
            c.subsid_amount,
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
            c.id = ?
        """,
        params=(contract_id,),
        req_type="fetch_one",
    )

    logger.info(f"{raw_contract=!r}")
    contract = Contract(*raw_contract)

    if bvu := bank_mapping.get("БВУ", {}):
        category = "БВУ"
        bank = bvu.get(contract.bank, "")
    else:
        category = "Лизинг"
        bank = bank_mapping.get(contract.category or "", {}).get(
            contract.bank, ""
        )

    contract.category = category
    contract.bank = bank

    return contract


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
    def load(cls, db: DatabaseManager, contract_id: str) -> InterestRate:
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


def has_error(win: WindowSpecification) -> bool:
    sleep(0.5)

    potential_alert = child(win, ctrl="Pane", idx=2)
    if (
        error_message := text(child(potential_alert, ctrl="Pane"))
    ) and "Fail" in error_message:
        print(error_message)
        click(win, child(potential_alert, ctrl="Button", title="OK"))

    messages_pane = child(win, ctrl="Pane", idx=18)
    close_button = child(messages_pane, title="Close", ctrl="Button")

    if exists(close_button):
        click_type(win, child(messages_pane, ctrl="Document"), "^a^c")

        message = pyperclip.paste()
        print(message)

        click(win, close_button)

        return True

    return False


def open_file(one_c: App, file_path: Path) -> None:
    one_c.switch_backend("win32")
    save_dialog = one_c.app.window(title_re="Выберите ф.+")
    save_dialog["Edit0"].set_text(str(file_path))
    if not save_dialog.is_active():
        save_dialog.set_focus()
        save_dialog.wait(wait_for="visible")
    save_dialog.child_window(class_name="Button", found_index=0).click_input()
    one_c.switch_backend("uia")


def find_row(
    parent: _ListViewWrapper | WindowSpecification, project: str
) -> _ListItemWrapper | WindowSpecification | None:
    rows = parent.descendants(control_type="ListItem")
    for row in rows:
        txt = text(row).strip()
        if not txt:
            continue
        score = SequenceMatcher(None, project, txt).ratio()
        if score >= 0.8:
            print(f"{len(rows)=}, {txt=}, {score=}")
            return row
    return None


def find_project(win: WindowSpecification, contract: Contract) -> bool:
    click(win, child(win, title="Консоль запросов и обработчик", ctrl="Button"))

    with suppress(ElementNotFoundError):
        click(
            child(win, ctrl="Pane", idx=27),
            child(win, ctrl="Button", title="Maximize"),
        )

    query_document_box = cast(
        UIAWrapper | WindowSpecification, child(win, ctrl="Document")
    )

    query = prepare_query(contract.contragent, contract.protocol_id)
    query = query.replace("\n", "~")

    click(win, query_document_box, button="right")
    send_keys(win, "{DOWN 3}~")
    send_keys(win, query, spaces=True, pause=0)
    click(win, child(win, title="Выполнить", ctrl="Button"))

    if not wait(
        child(win, title="Delete", ctrl="Button", idx=1), wait_for="is_enabled"
    ):
        print(f"{contract.contract_id=} not found")
        return False

    row = find_row(win, project=contract.project)
    if not row:
        print(f"{contract.contract_id=} not found")
        return False

    click(win, row, double=True)

    click(win, query_document_box)
    send_keys(win, "{ESC}")

    if exists(
        close_button := child(
            child(win, ctrl="Pane", idx=18), title="Close", ctrl="Button"
        )
    ):
        click(win, close_button)
    return True


def fill_main_project_data(
    win: WindowSpecification,
    form: WindowSpecification | _UIAPaneWrapper,
    contract: Contract,
) -> None:
    """
    :param win: Главное окно 1С
    :param form: Форма "Карточка проекта (форма элемента)"
    :param contract: Данные договора
    :return: None

    Заполнение данных в форме проекта во вкладке "Основные"
    (Цель кредитования, Номер протокола, Дата протокола, Дата получения протокола РКС филиалом)
    """

    click(win, child(parent=form, ctrl="Edit", idx=7))
    send_keys(
        win,
        "{F4}^f" + contract.credit_purpose + "{ENTER 2}",
        pause=0.1,
        spaces=True,
    )
    click_type(
        win, child(form, ctrl="Edit", idx=3), contract.protocol_date, cls=True
    )


def change_date(
    win: WindowSpecification,
    form: WindowSpecification | _UIAPaneWrapper,
    goto_button: WindowSpecification | _ButtonWrapper,
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
        send_keys(win, "{DOWN}{ENTER 2}", pause=0.5)
        send_keys(win, protocol_date)
        send_keys(win, "{ENTER 4}{ESC}", pause=0.5)


def change_sums(
    win: WindowSpecification,
    form: WindowSpecification | _UIAPaneWrapper,
    goto_button: WindowSpecification | _ButtonWrapper,
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

    # record_win = child_win(win, ctrl="Pane", idx=56)

    if (
        contract.credit_purpose == "Пополнение оборотных средств"
        and existing_pos_amount != contract.loan_amount
    ):
        send_keys(win, "{TAB 4}" + str(contract.loan_amount), pause=0.1)
    elif (
        contract.credit_purpose == "Инвестиционный"
        and existing_investment_amount != contract.loan_amount
    ):
        send_keys(win, "{TAB 5}" + str(contract.loan_amount), pause=0.1)
    elif contract.credit_purpose == "Инвестиционный + ПОС":
        if (
            existing_pos_amount != contract.loan_amount
            and existing_investment_amount != contract.loan_amount
        ):
            send_keys(
                win,
                "{TAB 4}"
                + str(contract.loan_amount)
                + "{TAB}"
                + str(contract.loan_amount),
                pause=0.1,
            )
        elif existing_pos_amount != contract.loan_amount:
            send_keys(win, "{TAB 4}" + str(contract.loan_amount), pause=0.1)
        elif existing_investment_amount != contract.loan_amount:
            send_keys(win, "{TAB 5}" + str(contract.loan_amount), pause=0.1)

    send_keys(win, "{ESC}", pause=0.5)
    with suppress(ElementNotFoundError):
        click(win, child(win, title="Yes", ctrl="Button"))
    send_keys(win, "{ESC}", pause=0.5)


def add_vypiska(
    one_c: App,
    win: WindowSpecification,
    form: WindowSpecification | _UIAPaneWrapper,
    contract: Contract,
) -> None:
    """
    :param one_c: Главный объект
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
    open_file(one_c, protocol_pdf_path)

    if exists(
        child(win, title="Value is not of object type (Сессия)", ctrl="Pane")
    ):
        click(win, child(win, title="OK", ctrl="Button"))
        sleep(1)

    click(win, child(win, title="OK", ctrl="Button", idx=1))


def check_project_type(
    win: WindowSpecification,
    form: WindowSpecification | _UIAPaneWrapper,
    contract: Contract,
) -> None:
    if contract.credit_purpose == "Пополнение оборотных средств":
        click(win, child(form, title="Признаки проекта", ctrl="TabItem"))
        check(child(form, title="Возобновляемый проект", ctrl="CheckBox"))


def fill_contract_details(
    win: WindowSpecification,
    ds_form: WindowSpecification | _UIAPaneWrapper,
    contract: Contract,
    rate: InterestRate,
) -> None:
    # FIXME POS CHANGES INDICES

    edit_count = count_control_types(ds_form, ctrl="Edit")

    # if contract.credit_purpose == "Пополнение оборотных средств":
    if edit_count == 22:
        # child_window(ds_form, ctrl="Edit", idx=2).set_text(contract.ds_id)
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
            contract.dbz_date,
            ent=True,
            cls=True,
        )
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=9),
            contract.dbz_date,
            ent=True,
            cls=True,
        )
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=10),
            contract.end_date,
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
            send_keys(win, "{F4}{ENTER}", pause=0.5)
        elif contract.repayment_procedure == "Равными долями":
            send_keys(win, "{F4}{DOWN}{ENTER}", pause=0.5)
        elif contract.repayment_procedure == "Индивидуальный":
            send_keys(win, "{F4}{DOWN 2}{ENTER}", pause=0.5)
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
            contract.dbz_date,
            ent=True,
            cls=True,
        )
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=9),
            contract.dbz_date,
            ent=True,
            cls=True,
        )
        click_type(
            win,
            child(ds_form, ctrl="Edit", idx=10),
            contract.end_date,
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
            send_keys(win, "{F4}{ENTER}", pause=0.5)
        elif contract.repayment_procedure == "Равными долями":
            send_keys(win, "{F4}{DOWN}{ENTER}", pause=0.5)
        elif contract.repayment_procedure == "Индивидуальный":
            send_keys(win, "{F4}{DOWN 2}{ENTER}", pause=0.5)
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

    txt = elem.window_txt().strip()
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
    one_c: App,
    win: WindowSpecification,
    form: WindowSpecification | _UIAPaneWrapper,
    contract: Contract,
    rate: InterestRate,
) -> WindowSpecification | _UIAPaneWrapper:
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
        sort_win = window(one_c.app, title="Filter and Sort")

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
        pause=0.2,
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
    sort_win = window(one_c.app, title="Filter and Sort")
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


def fill_1c(
    contract: Contract, rate: InterestRate, registry: Registry, base_name: str
) -> str:
    if not contract.ds_id:
        return "Не удалось получить номер договора из .docx файла"
    if not contract.ds_date:
        return "Не удалось получить дату договора из .docx файла"

    macro_path = cast(Path, contract.macro_path)
    document_pdf_path = cast(Path, contract.document_pdf_path)

    app_path = registry.resources_folder / base_name
    with App(app_path=str(app_path)) as one_c:
        win = window(one_c.app, title="Конфигурация.+", regex=True)
        win.wait(wait_for="exists", timeout=60)
        win.maximize()

        if not find_project(win=win, contract=contract):
            return (
                f"Не удалось найти проект '{contract.project.strip()}' контрагента c БИН "
                f"'{contract.contragent}' и номером протокола '{contract.protocol_id}'"
            )

        form = child(win, ctrl="Pane", idx=27)

        click(win, child(form, title="Read", ctrl="Button"))
        if (yes_button := child(win, title="Yes", ctrl="Button")).exists():
            click(win, yes_button)
            sleep(1)

        if "Транш" not in contract.contract_type:
            goto_button = child(form, title="Go to", ctrl="Button")
            fill_main_project_data(win, form, contract)
            change_date(win, form, goto_button, contract.protocol_date)
            change_sums(win, form, goto_button, contract)

        add_vypiska(one_c, win, form, contract)

        if "Транш" not in contract.contract_type:
            check_project_type(win, form, contract)

        ds_form = fill_contract(one_c, win, form, contract, rate)

        # click(win, child(ds_form, title="Find in list", ctrl="Button"))
        # list_form = child(win, ctrl="Pane", idx=74)

        click(
            win, child(ds_form, title="ПрикрепленныеДокументы", ctrl="TabItem")
        )

        click(win, child(ds_form, title="Add", ctrl="Button"))
        sleep(1)
        send_keys(win, "{F4}")

        open_file(one_c, document_pdf_path)

        if (
            child(
                win, title="Value is not of object type (Сессия)", ctrl="Pane"
            )
        ).exists():
            click(win, child(win, title="OK", ctrl="Button"))
            sleep(1)

        click(
            win,
            child(child(win, ctrl="Pane", idx=63), ctrl="Button", title="OK"),
        )

        click(win, child(ds_form, title="Записать", ctrl="Button"))

        click(
            win,
            child(
                ds_form, title="Открыть текущий График погашения", ctrl="Button"
            ),
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
            win,
            child(table_form, ctrl="Edit", idx=6),
            contract.end_date,
            ent=True,
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

        open_file(one_c, macro_path)

        table_checked = True
        try:
            menu_select_1c(
                win,
                table_form,
                trigger_btn_name="Проверка введенного графика",
                menu_names=[contract.category, contract.bank],
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

        click(win, child(ds_form, title="Записать", ctrl="Button"))
        click(win, child(ds_form, title="OK", ctrl="Button"))

        click(win, child(form, title="Записать", ctrl="Button"))
        click(win, child(form, title="OK", ctrl="Button"))

    reply = "Договор успешно занесен в 1С"

    if contract.credit_purpose == "Рефинансирование":
        reply += " в проект с целевым назначением 'Рефинансирование'"

    if re.match(r"ПР-\d+", contract.ds_id) is not None:
        reply += f" с номером '{contract.ds_id}' от {contract.bank}"

    if not table_checked:
        reply += ". Не удалось проверить график через меню 'Проверка введенного графика'"

    return reply


def process_notification(
    db: DatabaseManager, edo: EDO, crm: CRM, registry: Registry, task: Task
) -> str:
    document_url = edo.get_attached_document_url(task.doctype_id, task.doc_id)
    if not document_url:
        reply = "Не найден приложенный документ на странице поручения."
        return reply

    contract_id = document_url.split("/")[-1]

    # if contract_id in ["587e98a2-cb98-4c1b-9304-685b846e025b"]:
    #     return "Неизвестная ошибка"

    reply = process_contract(
        logger=logger,
        db=db,
        contract_id=contract_id,
        edo=edo,
        crm=crm,
        registry=registry,
    )

    if "Согласовано. Не найдено замечаний." in reply or "Расхождения" in reply:
        contract = get_contract(
            contract_id, db, registry.banks.get("mapping", {})
        )
        rate = InterestRate.load(db, contract.contract_id)
        logger.info(f"{contract=!r}")
        logger.info(f"{rate=!r}")

        try:
            reply = fill_1c(contract, rate, registry, "base.v8i")
            return reply
        except Exception as e:
            logger.exception(e)
            return "Неизвестная ошибка"

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

        logger.info(f"{tasks=!r}")

        logger.info(f"Found {len(tasks)} tasks")
        if not tasks:
            logger.info("Nothing to work on - sleeping...")
            return 150
        else:
            bot.send_message(f"Found {len(tasks)} tasks")

        for task in tasks:
            if task.doc_id in failed_tasks:
                continue

            logger.info(f"Working on task {task}")
            try:
                reply = process_notification(
                    db=db, edo=edo, crm=crm, registry=registry, task=task
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
    warnings.simplefilter(action="ignore", category=UserWarning)
    warnings.simplefilter(action="ignore", category=InsecureRequestWarning)
    warnings.simplefilter(action="ignore", category=SyntaxWarning)

    dotenv.load_dotenv(".env")

    registry = Registry(download_folder=Path(f"downloads/zanesenie/{today}"))

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

    bot = TelegramAPI()

    bot.send_message('START of the process "Занесение договоров"')
    logger.info('START of the process "Занесения договоров"')

    start_time = time.time()
    max_duration = 12 * 60 * 60
    tomorrow = today + timedelta(days=1)

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
