import dataclasses
import logging
import random
import re
import time
from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import Type, cast, override
from urllib.parse import urlparse

from bs4 import BeautifulSoup, SoupStrainer

from sverka.error import LoginError
from sverka.subsidy import EdoContract
from utils.db_manager import DatabaseManager
from utils.request_handler import RequestHandler

logger = logging.getLogger("DAMU")


@dataclasses.dataclass
class EdoNotification:
    notif_id: str
    notif_date: datetime
    subject: str
    person: str
    notif_url: str
    doctype_id: str
    doc_id: str


@dataclasses.dataclass
class EdoBasicContract:
    contract_id: str
    contract_type: str
    contract_subject: str


class EDO(RequestHandler):
    def __init__(
        self,
        user: str,
        password: str,
        base_url: str,
        download_folder: Path,
        user_agent: str,
    ) -> None:
        super().__init__(user, password, base_url, download_folder)
        self.update_headers(
            {
                "accept-language": "en-US,en;q=0.9",
                "cache-control": "no-cache",
                "pragma": "no-cache",
                "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-site": "same-origin",
                "referer": "https://edo.fund.kz/user/login",
                "referrer-policy": "strict-origin-when-cross-origin",
                "user-agent": user_agent,
            }
        )

        self.is_logged_in = False

    def login(self) -> None:
        self.is_logged_in = False
        self.clear_cookies()

        headers = self.headers
        headers.update(
            {
                "accept": "application/json, text/javascript, */*; q=0.01",
                "content-type": "application/x-www-form-urlencoded",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "x-requested-with": "XMLHttpRequest",
            }
        )

        browser_key = str(int(time.time() * 1000))
        browser_token = str(random.randint(9, 99999))
        self.set_cookie("browser_token", browser_token)

        user_agent = self.client.headers.get("user-agent")
        if not user_agent:
            raise ValueError("'user-agent' is not in the Headers")

        data = {
            "login": self.user,
            "password": self.password,
            "language": "ru",
            "user_agent_js": user_agent,
            "win_h_w": "1080|1920",
            "browser_name": "Chrome",
            "browser_version": "131",
            "browser_key": browser_key,
            "browser_token": browser_token,
        }

        response = self.request(
            method="post",
            path="user/login?back=/",
            data=data,
            update_cookies=True,
        )
        if not response:
            logger.error("Request failed")
            raise LoginError("Robot was unable to login into the EDO...")

        data = response.json()
        data_status = data.get("status")
        logger.debug(f"Response internal {data_status}")

        if data_status not in {3, 9}:
            raise LoginError("Robot was unable to login into the EDO...")

        logger.debug("Login process completed successfully")

        self.is_logged_in = True

    def get_notifications(self) -> list[EdoNotification]:
        if not self.is_logged_in:
            self.login()

        response = self.request(method="get", path="lms/get-notify-list")
        if not response:
            logger.error("Request failed")
            raise LoginError("Robot was unable to login into the EDO...")

        if not hasattr(response, "json"):
            logger.error("Request failed")
            raise LoginError("Robot was unable to login into the EDO...")

        data = response.json()
        raw_notifications = data.get("data" or {}).get("lms", [])
        logger.debug(f"raw_notifications={data!r}")
        notifications = [
            EdoNotification(
                notif_id=notif.get("id"),
                notif_date=datetime.fromisoformat(notif.get("date1")),
                subject=notif.get("full_subject"),
                person=notif.get("person"),
                notif_url=notif.get("url"),
                doctype_id=notif.get("doctype_id"),
                doc_id=notif.get("document_id"),
            )
            for notif in raw_notifications
        ]
        return notifications

    def get_attached_document_url(
        self, notification: EdoNotification
    ) -> str | None:
        if not self.is_logged_in:
            self.login()

        response = self.request(
            method="get",
            path=f"workflow/document/related-document/{notification.doctype_id}/{notification.doc_id}/v_516fba03",
        )
        if not response:
            logger.error("Request failed")
            raise LoginError("Robot was unable to login into the EDO...")

        html = response.text
        soup = BeautifulSoup(html, "lxml")

        anchors = soup.select("div > span > a")
        if not anchors:
            logger.error("Unable to find an attached document")
            raise Exception("Robot was unable to find an attached document")

        document_url = next(
            (
                cast(str, d_url)
                for anchor in anchors
                if (d_url := anchor.get("href"))
                if "beff8bc1-14fd-4657-86f1-55797181018f" in d_url
            ),
            None,
        )
        logger.info(f"Document URL - {document_url!r}")
        return document_url

    def reply_to_notification(
        self, notification: EdoNotification, reply: str
    ) -> bool:
        if not self.is_logged_in:
            self.login()

        reply = reply.strip()

        data = {
            "msf_id": f"workflow:{notification.doctype_id}:{notification.doc_id}",
            "tstamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "comment": reply,
            "files[0]": "",
            "use_eds": "0",
        }

        response = self.request(
            method="post",
            path=f"workflow/document/decision/{notification.doctype_id}/{notification.doc_id}/t_51945d1?mydocuments&",
            data=data,
        )
        if not response:
            logger.error("Request failed")
            raise LoginError("Robot was unable to login into the EDO...")

        if not hasattr(response, "json"):
            logger.error("Request failed")
            raise LoginError("Robot was unable to login into the EDO...")

        response_data = response.json()
        logger.debug(f"raw_response={response_data!r}")

        response_msg = cast(str, response_data.get("message", "").strip())
        return response_msg == "Выполнена задача: Исполнить"

    def mark_as_read(self, notif_id: str) -> bool:
        data = {
            "items": notif_id,
            "is_read": "1",
        }

        response = self.request(
            method="post",
            path="lms/mark-as",
            data=data,
        )
        if not response:
            logger.error("Request failed")
            raise LoginError("Robot was unable to login into the EDO...")

        return True

    def get_filtered_html(self, basic_contract_data: EdoBasicContract) -> str:
        if not self.is_logged_in:
            self.login()

        headers = self.headers
        headers.update(
            {
                "accept": "*/*",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "X-Requested-With": "XMLHttpRequest",
            }
        )

        # contract_subject = basic_contract_data.contract_subject.replace("«", "").replace("»", "")
        contract_subject = basic_contract_data.contract_subject.strip()

        params = {
            "p": "0",  # Страница (индекс с 0)
            "s[f_01ef77b]": "desc",  # Рег. дата в порядке возрастания
            "qs": "",
            "flt[2][name]": "f_4135638",  # Тип договора == basic_contract_data.contract_type"
            "flt[2][type]": "string",
            "flt[2][condition]": "LIKE",
            "flt[2][value]": basic_contract_data.contract_type,
            "flt[3][name]": "f_f111a29",  # Предмет договора == basic_contract_data.contract_subject
            "flt[3][type]": "string",
            "flt[3][condition]": "LIKE",
            "flt[3][value]": contract_subject,
            "pp": "node",
            "_": "1736098323272",
        }

        response = self.request(
            method="get",
            path="/workflow/folder/index/167955b9-2e25-4044-a253-5a0ae83e01e0/f48469f3-1a44-4ddb-ba0f-5a0aea09009c",
            headers=headers,
            params=params,
        )

        if not response:
            logger.warning("Robot is not logged in to the EDO...")
            raise Exception("Robot is not logged in to the EDO...")

        contract_list_html = response.text
        return contract_list_html

    def find_contract(
        self, basic_contract: EdoBasicContract, db: DatabaseManager
    ) -> None:
        if not self.is_logged_in:
            self.login()

        html = self.get_filtered_html(basic_contract)
        soup = BeautifulSoup(html, features="lxml")

        headers = [
            (idx, text)
            for idx, el in enumerate(soup.select("tr.title > td"))
            if (text := el.text.strip())
        ]

        contract = None

        row_idx = 0
        while (
            current_row := soup.select_one(selector=f"tr#grid_row_{row_idx}")
        ) is not None:
            row = {}
            for html_col_idx, header in headers:
                tag = current_row.select_one(
                    selector=f":nth-child({html_col_idx + 1})"
                )
                tag_text = tag.text.strip() if tag else ""
                row[header] = tag_text

            anchor = current_row.select_one(
                selector="td.document_extra_info > div > a"
            )
            if not anchor:
                raise ValueError(
                    "Selector 'td.document_extra_info > div > a' not found"
                )

            href = cast(str, anchor.get("href", ""))
            if not href.endswith(basic_contract.contract_id):
                row_idx += 1
                continue

            contragent = row["Заемщик"]
            contragent_match = re.search(r"\d{12}", contragent)
            if not contragent_match:
                contragent = ""
            else:
                contragent = contragent_match.group(0)

            ds_id = row["Рег.№"]
            ds_id = ds_id.strip().split(" ")[-1].replace("№", "")

            try:
                ds_date = datetime.strptime(row["Рег. дата"], "%d.%m.%Y").date()
            except ValueError:
                ds_date = None

            contract = EdoContract(
                contract_id=basic_contract.contract_id,
                ds_id=ds_id,
                contragent=contragent,
                ds_date=ds_date,
                sed_number=row["Порядковый номер"],
            )
            contract.save(db)
            break

        if not contract:
            raise Exception(f"Contract not found {basic_contract.contract_id}!")

    def download_file(self, contract_id: str) -> tuple[bool, str]:
        download_path = f"/media/download-multiple/workflow/beff8bc1-14fd-4657-86f1-55797181018f/{contract_id}"

        save_folder = self.download_folder / contract_id
        save_folder.mkdir(exist_ok=True, parents=True)
        save_location = save_folder / "contract.zip"
        if save_location.exists() and not save_location.stat().st_size == 0:
            logger.info("Valid archive potentially exists...")
            return True, contract_id

        if not self.is_logged_in:
            self.login()

        headers = self.headers
        headers.update(
            {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
                "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "ru;ru-RU;q=0.9",
                "connection": "keep-alive",
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
            }
        )

        response = self.request(
            method="get", path=download_path, headers=headers
        )
        if not response:
            return False, contract_id

        with save_location.open("wb") as file:
            file.write(response.content)

        return True, contract_id

    def get_basic_contract_data(
        self, contract_id: str, db: DatabaseManager
    ) -> tuple[
        BeautifulSoup | None, EdoBasicContract | None, EdoContract | None
    ]:
        if not self.is_logged_in:
            self.login()

        response = self.request(
            method="get",
            path=f"/workflow/document/view/beff8bc1-14fd-4657-86f1-55797181018f/{contract_id}",
        )

        if not response:
            logger.error(f"Unable to fetch {contract_id} page data...")
            raise Exception(f"Unable to fetch {contract_id} page data...")

        soup = BeautifulSoup(response.text, features="lxml")

        not_found = (
            soup.select_one("div.block_alert_contrast.warning") is not None
        )
        if not_found:
            logger.info("Message - 'Документ не найден!'")
            return None, None, None

        contract_type = (
            tag.text.strip()
            if (tag := soup.select_one("span.referenceView_f_7127e44"))
            else None
        )
        if not contract_type:
            raise ValueError(
                "Selector 'span.referenceView_f_7127e44' not found"
            )

        contract_subject = (
            cast(str, tag.get("value"))
            if (tag := soup.select_one("input#js_f_9190289"))
            else None
        )
        if not contract_subject:
            raise ValueError("Selector 'input#js_f_9190289' not found")

        basic_contract = EdoBasicContract(
            contract_id=contract_id,
            contract_type=contract_type,
            contract_subject=contract_subject,
        )

        ds_id_tag = soup.select_one("input[value='Договор №']")
        if not ds_id_tag:
            raise ValueError("Selector 'input[value='Договор №']' not found")
        else:
            sibling = ds_id_tag.nextSibling
            if not sibling:
                raise ValueError(
                    "Sibling of 'input[value='Договор №']' not found"
                )
            ds_id = sibling.text.strip()
            ds_id = re.sub(r"\s+", " ", ds_id)
            ds_id = ds_id.strip().split(" ")[-1].replace("№", "")

        page_data: dict[str, str] = {}
        rows = soup.select(".panel-row")
        for row in rows:
            key_tag = row.select_one(".panel-row_key")
            value_tag = row.select_one(".panel-row_value")

            if not key_tag or not value_tag:
                continue

            key = key_tag.text.strip().replace(":*", "")
            if key[-1] == ":":
                key = key[0:-1]
            value = re.sub(r"[\r\n]+", "\n", value_tag.text.strip())
            value = re.sub(r" +", " ", value)
            page_data[key] = value

        contragent = page_data.get("Заёмщики")
        if not contragent:
            contragent = ""
        else:
            contragent_match = re.search(r"\d{12}", contragent)
            if not contragent_match:
                contragent = ""
            else:
                contragent = contragent_match.group(0)

        try:
            ds_date_str = page_data.get("Дата подписания")
            if not ds_date_str:
                raise ValueError("'Дата подписания' not found")
            ds_date = datetime.strptime(ds_date_str, "%d.%m.%Y").date()
        except ValueError:
            ds_date = None

        sed_number = page_data["Порядковый номер"]

        contract = EdoContract(
            contract_id=contract_id,
            ds_id=ds_id,
            contragent=contragent,
            ds_date=ds_date,
            sed_number=sed_number,
        )
        contract.save(db)

        return soup, basic_contract, contract

    def get_signed_contract_url(
        self, contract_id: str, soup: BeautifulSoup | None = None
    ) -> list[tuple[str, Path]]:
        if not soup:
            if not self.is_logged_in:
                self.login()

            response = self.request(
                method="get",
                path=f"/workflow/document/view/beff8bc1-14fd-4657-86f1-55797181018f/{contract_id}",
            )

            if not response:
                logger.error(f"Unable to fetch {contract_id} page data...")
                raise Exception(f"Unable to fetch {contract_id} page data...")

            soup = BeautifulSoup(
                response.text,
                features="lxml",
                parse_only=SoupStrainer("div", {"id": "tabcontent1"}),
            )

        download_folder = self.download_folder / contract_id / "documents"
        download_folder.mkdir(exist_ok=True, parents=True)

        download_urls: list[tuple[str, Path]] = []
        for el in soup.select("span.attached_file"):
            file_name_el = el.select_one('a.filename[id*="fileview"]')
            url_el = el.select_one("a.decisions_btn")
            if not url_el:
                continue

            file_name = file_name_el.text.strip() if file_name_el else None
            href = cast(str, url_el.get("href"))
            url_path = urlparse(href).path if url_el else None

            if not file_name or not url_path:
                continue

            if not file_name.lower().endswith("docx"):
                continue

            file_path = download_folder / file_name
            if file_path.exists():
                logger.info("Valid file potentially exists...")
                continue

            download_urls.append((url_path, file_path))

        return download_urls

    def download_signed_contract(self, path: str, file_path: Path) -> bool:
        headers = self.client.headers.copy()

        headers["accept"] = (
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
            "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
        )
        headers["Sec-Fetch-Dest"] = "document"

        params = {
            "d": "t_21a1f9a,t_d1866f4,t_11d0618,t_41830bc,t_11c1fdf,t_810358e,t_1180410",
            "f": "Создан {Дата/Время создания}. DOC24 ID - {DOC24 ID}, "
            "Проверка ЭЦП контрагента - {ПРОВЕРКА_ЭЦП_Подпись контрагента}",
            "b": "",
            "btt": "{Регистрационный номер} от {Регистрационная дата}",
            "ep": "1",
            "field": "f_d180879",
            "wmk_tpl": "",
        }

        response = self.request(
            method="get", path=path, params=params, headers=headers
        )
        if not response:
            return False

        with file_path.open("wb") as file:
            file.write(response.content)

        return True

    @override
    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.is_logged_in = False
        super().__exit__(exc_type, exc_val, exc_tb)
