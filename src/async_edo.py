import asyncio
import dataclasses
import logging
import math
import random
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple, Union
from urllib.parse import urlparse

import aiofiles
import pandas as pd
from bs4 import BeautifulSoup, SoupStrainer

from src.error import LoginError, async_retry
from src.subsidy import EdoContract
from src.utils.collections import batched
from src.utils.db_manager import DatabaseManager
from src.utils.request_handler import AsyncRequestHandler
from src.utils.utils import safe_extract

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
    contract_type2: str
    contract_amount: str
    contract_subject: str


class EDO(AsyncRequestHandler):
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

    @async_retry(exceptions=(LoginError,), tries=5, delay=5, backoff=5)
    async def login(self) -> None:
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

        response = await self.request(
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

    async def get_notifications(self) -> List[EdoNotification]:
        if not self.is_logged_in:
            await self.login()

        response = await self.request(method="get", path="lms/get-notify-list")
        if not response:
            logger.error("Request failed")
            raise LoginError("Robot was unable to login into the EDO...")

        if not hasattr(response, "json"):
            logger.error("Request failed")
            raise LoginError("Robot was unable to login into the EDO...")

        data = response.json()
        logger.debug(f"raw_response={data!r}")
        raw_notifications = data.get("data" or {}).get("lms", [])
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

    async def get_attached_document_url(self, notification: EdoNotification) -> str:
        if not self.is_logged_in:
            await self.login()

        response = await self.request(
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

        anchor = anchors[0]
        document_url = anchor.get("href")
        logger.info(f"Document URL - {document_url!r}")
        return document_url

    async def reply_to_notification(self, notification: EdoNotification, reply: str) -> bool:
        if not self.is_logged_in:
            await self.login()

        reply = reply.strip()

        data = {
            "msf_id": f"workflow:{notification.doctype_id}:{notification.doc_id}",
            "tstamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "comment": reply,
            "files[0]": "",
            "use_eds": "0",
        }

        response = await self.request(
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

        response_msg = response_data.get("message", "").strip()
        return response_msg == "Выполнена задача: Исполнить"

    async def mark_as_read(self, notif_id: str) -> bool:
        data = {
            "items": notif_id,
            "is_read": "1",
        }

        response = await self.request(
            method="post",
            path=f"lms/mark-as",
            data=data,
        )
        if not response:
            logger.error("Request failed")
            raise LoginError("Robot was unable to login into the EDO...")

        return True

    async def get_contract_list_row_count(self) -> Tuple[bool, int]:
        headers = self.headers
        headers.update(
            {
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Connection": "keep-alive",
                "Origin": "https://edo.fund.kz",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "X-Requested-With": "XMLHttpRequest",
            }
        )

        self.set_cookie("FontSize", "9")

        params = {
            "qs": "",
            "flt[1][name]": "f_4135638",
            "flt[1][type]": "string",
            "flt[1][condition]": "LIKE",
            "flt[1][value]": "Договор субсидирования",
            "g[f_e1a2e56]": "Подписан",
            "pp": "getTotalRows",
        }

        response = await self.request(
            method="post",
            path="workflow/folder/index/167955b9-2e25-4044-a253-5a0ae83e01e0/f48469f3-1a44-4ddb-ba0f-5a0aea09009c",
            headers=headers,
            params=params,
        )

        if not response:
            return False, -1

        if not hasattr(response, "json"):
            logger.error(f"Response does not have JSON. Text instead - {response.text=}")
            return False, -1
        data = response.json()

        row_count = data.get("total")
        if not row_count:
            logger.error(f"No key 'total' in {response.json()=}")
            return False, -1

        return True, int(row_count)

    @staticmethod
    async def parse_table_rows(soup: BeautifulSoup, db: DatabaseManager) -> List[str]:
        contract_ids = []

        headers = [
            (idx, text)
            for idx, el in enumerate(soup.select("tr.title > td"))
            if (text := el.text.strip())
        ]

        row_idx = 0
        while (current_row := soup.select_one(selector=f"tr#grid_row_{row_idx}")) is not None:
            row = {
                header: current_row.select_one(
                    selector=f":nth-child({html_col_idx + 1})"
                ).text.strip()
                for html_col_idx, header in headers
            }

            anchor = current_row.select_one(selector="td.document_extra_info > div > a")
            href = anchor.get("href")
            contract_id = href.split("/")[-1]

            contragent = row["Заемщик"]
            contragent_match = re.search(r"\d{12}", contragent)
            if not contragent_match:
                contragent = ""
            else:
                contragent = contragent_match.group(0)

            ds_id = row["Рег.№"]
            ds_id = ds_id.strip().split(" ")[-1].replace("№", "")

            contract = EdoContract(
                contract_id=contract_id,
                ds_id=ds_id,
                contragent=contragent,
                ds_date=datetime.strptime(row["Рег. дата"], "%d.%m.%Y").date(),
                sed_number=row["Порядковый номер"],
            )
            contract.save(db)

            contract_ids.append(contract_id)

            row_idx += 1

        return contract_ids

    async def get_filtered_html(self, basic_contract_data: EdoBasicContract) -> str:
        if not self.is_logged_in:
            await self.login()

        headers = self.headers
        headers.update(
            {
                "accept": "*/*",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "X-Requested-With": "XMLHttpRequest",
            }
        )

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
            "flt[3][value]": basic_contract_data.contract_subject,
            "pp": "node",
            "_": "1736098323272",
        }

        response = await self.request(
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

    async def find_contract(
        self, basic_contract_data: EdoBasicContract, db: DatabaseManager
    ) -> None:
        if not self.is_logged_in:
            await self.login()

        html = await self.get_filtered_html(basic_contract_data)
        soup = BeautifulSoup(html, features="lxml")

        headers = [
            (idx, text)
            for idx, el in enumerate(soup.select("tr.title > td"))
            if (text := el.text.strip())
        ]

        row_idx = 0
        while (current_row := soup.select_one(selector=f"tr#grid_row_{row_idx}")) is not None:
            row = {
                header: current_row.select_one(
                    selector=f":nth-child({html_col_idx + 1})"
                ).text.strip()
                for html_col_idx, header in headers
            }

            anchor = current_row.select_one(selector="td.document_extra_info > div > a")
            href = anchor.get("href", "")
            if not href.endswith(basic_contract_data.contract_id):
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

            contract = EdoContract(
                contract_id=basic_contract_data.contract_id,
                ds_id=ds_id,
                contragent=contragent,
                ds_date=datetime.strptime(row["Рег. дата"], "%d.%m.%Y").date(),
                sed_number=row["Порядковый номер"],
            )
            contract.save(db)
            break

    async def contracts_page_html(self, page: Union[int, str], ascending: bool) -> Optional[str]:
        if not self.is_logged_in:
            await self.login()

        logger.info(f"Page {page + 1}")

        headers = self.headers
        headers.update(
            {
                "accept": "*/*",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "X-Requested-With": "XMLHttpRequest",
            }
        )

        ascending_type = "asc" if ascending else "desc"
        year_ago = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d %H:%M")
        params = {
            "g[f_e1a2e56]": "Подписан",  # По статусу "Подписан"
            "p": str(page),  # Страница (индекс с 0)
            "s[f_01ef77b]": ascending_type,  # Рег. дата в порядке возрастания
            "qs": "",
            "flt[1][name]": "f_4135638",  # Тип договора == Договор субсидирования"
            "flt[1][type]": "string",
            "flt[1][condition]": "LIKE",
            "flt[1][value]": "Договор субсидирования",
            "flt[10][name]": "f_01ef77b",
            "flt[10][type]": "date",  # Рег. дата за последний год
            "flt[10][condition]": "MOREEQUAL",
            "flt[10][value]": year_ago,
            "pp": "node",
            "_": "1736098323272",
        }

        response = await self.request(
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

    async def download_file_async(self, contract_id: str) -> Tuple[bool, str]:
        download_path = (
            f"/media/download-multiple/workflow/beff8bc1-14fd-4657-86f1-55797181018f/{contract_id}"
        )

        save_folder = self.download_folder / contract_id
        save_folder.mkdir(exist_ok=True, parents=True)
        save_location = save_folder / "contract.zip"
        if save_location.exists() and not save_location.stat().st_size == 0:
            logger.info(f"Valid archive potentially exists...")
            return True, contract_id

        if not self.is_logged_in:
            await self.login()

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

        response = await self.request(method="get", path=download_path, headers=headers)
        if not response:
            return False, contract_id

        async with aiofiles.open(save_location, "wb") as file:
            await file.write(response.content)

        return True, contract_id

    async def mass_download_async(self, contract_ids: List[str], batch_size: int = 10) -> None:
        logger.info(f"Preparing to download {len(contract_ids)} archives...")

        batches = batched(contract_ids, batch_size)
        batch_count = math.ceil(len(contract_ids) / batch_size)

        failed_signed_paths = []

        for idx, batch in enumerate(batches, start=1):
            logger.info(f"Processing batch {idx}/{batch_count}")

            tasks = []
            download_url_tasks = []
            for contract_id in batch:
                save_folder = self.download_folder / contract_id
                documents_folder = save_folder / "documents"
                documents_folder.mkdir(parents=True, exist_ok=True)

                if not (save_folder / "contract.zip").exists():
                    tasks.append(self.download_file_async(contract_id=contract_id))

                if not any(f.name.lower().endswith("docx") for f in documents_folder.iterdir()):
                    download_url_tasks.append(self.get_signed_contract_url(contract_id))

            await asyncio.gather(*tasks)
            results = await asyncio.gather(*download_url_tasks)

            download_signed_tasks = [
                (self.download_signed_contract(url_path, file_path), url_path, file_path)
                for res in results
                for url_path, file_path in res
            ]
            results = await asyncio.gather(*download_signed_tasks)
            for status, url_path, file_path in results:
                if not status:
                    failed_signed_paths.append((url_path, file_path))

        for url_path, file_path in failed_signed_paths:
            await self.download_signed_contract(url_path, file_path)

        for contract_id in contract_ids:
            save_folder = self.download_folder / contract_id
            documents_folder = save_folder / "documents"
            safe_extract(save_folder / "contract.zip", documents_folder=documents_folder)

    async def process_contracts(
        self,
        db: DatabaseManager,
        max_page: int,
        batch_size: int,
        contracts_excel_path: Optional[Path] = None,
    ) -> None:
        if contracts_excel_path is not None and contracts_excel_path.exists():
            df = pd.read_excel(contracts_excel_path)
            df["Рег.№"] = df["Рег.№"].str.strip().str.split(" ").str[-1].replace("№", "")
            df["Заемщик"] = df["Заемщик"].str.extract(r"(\d{12})")
            df["contract_id"] = df["ссылка на ЭДС"].str.split("/").str[-1].str.strip()
            df["Рег.дата"] = df["Рег.дата"].dt.date

            contract_ids = []
            contracts = []
            for _, row in df.iterrows():
                contract = EdoContract(
                    contract_id=row["contract_id"],
                    ds_id=row["Рег.№"],
                    contragent=row["Заемщик"],
                    ds_date=row["Рег.дата"],
                    sed_number=row["Порядковый номер"],
                )
                contract.save(db)
                contracts.append(contract)
                contract_ids.append(contract.contract_id)

            await self.mass_download_async(contract_ids=contract_ids, batch_size=batch_size)
            return

        if not self.is_logged_in:
            await self.login()

        for page in range(max_page):
            html = await self.contracts_page_html(page=page, ascending=False)
            if not isinstance(html, str):
                raise Exception(f"Unable to fetch page data...")

            soup = BeautifulSoup(html, "lxml")
            first_line = soup.text.strip().split("\n")[0]

            if "Вход" in first_line:
                raise Exception(f"Unable to fetch page data...")

            contract_ids = await self.parse_table_rows(soup, db)
            await self.mass_download_async(contract_ids=contract_ids, batch_size=batch_size)

    async def get_basic_contract_data(
        self, contract_id: str
    ) -> Tuple[BeautifulSoup, EdoBasicContract]:
        if not self.is_logged_in:
            await self.login()

        response = await self.request(
            method="get",
            path=f"/workflow/document/view/beff8bc1-14fd-4657-86f1-55797181018f/{contract_id}",
        )

        if not response:
            logger.error(f"Unable to fetch {contract_id} page data...")
            raise Exception(f"Unable to fetch {contract_id} page data...")

        soup = BeautifulSoup(response.text, features="lxml")

        contract_type = (
            tag.text.strip() if (tag := soup.select_one("span.referenceView_f_7127e44")) else None
        )
        contract_type2 = (
            tag.text.strip()
            if (tag := soup.select_one("div#pervichkaTransh > div.panel-row_value"))
            else None
        )
        contract_amount = (
            tag.text.strip() if (tag := soup.select_one("div#summ > div.panel-row_value")) else None
        )
        contract_subject = (
            tag.get("value") if (tag := soup.select_one("input#js_f_9190289")) else None
        )

        basic_contract = EdoBasicContract(
            contract_id=contract_id,
            contract_type=contract_type,
            contract_type2=contract_type2,
            contract_amount=contract_amount,
            contract_subject=contract_subject,
        )

        return soup, basic_contract

    async def get_signed_contract_url(
        self, contract_id: str, soup: Optional[BeautifulSoup] = None
    ) -> List[Tuple[str, Path]]:
        if not soup:
            if not self.is_logged_in:
                await self.login()

            response = await self.request(
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

        download_urls = []
        for el in soup.select("span.attached_file"):
            file_name_el = el.select_one('a.filename[id*="fileview"]')
            url_el = el.select_one("a.decisions_btn")

            file_name = file_name_el.text.strip() if file_name_el else None
            url_path = urlparse(url_el.get("href")).path if url_el else None

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

    async def download_signed_contract(self, path: str, file_path: Path) -> bool:
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

        response = await self.request(method="get", path=path, params=params, headers=headers)
        if not response:
            return False

        async with aiofiles.open(file_path, "wb") as file:
            await file.write(response.content)

        return True
