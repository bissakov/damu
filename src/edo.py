import json
import logging
import random
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

from bs4 import BeautifulSoup

from src.error import HTMLElementNotFound, LoginError, retry
from src.utils.custom_list import CustomList
from src.utils.request_handler import RequestHandler
from src.utils.utils import (
    safe_extract,
    select_one,
)


class ProtocolIDs(CustomList[str]):
    pass


@dataclass
class SubsidyContract:
    contract_id: str
    reg_number: str
    contract_type: str
    contract_subject: str
    signatory: str
    status: str
    creation_date: str
    contract_amount_numeric: str
    incorrect_subsidy_contract: str
    recorded: str
    version_number: str
    reg_date: str
    counterparty: str
    all_counterparties: str
    borrower: str
    serial_number: str
    download_path: str
    save_location: str
    protocol_ids: ProtocolIDs = ProtocolIDs()
    data: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, str]:
        reverse = {v: k for k, v in HEADER_MAPPING.items()}
        return {reverse[key]: getattr(self, key) for key in asdict(self)}

    def save(self) -> None:
        file_path = Path(self.save_location).with_suffix(".json")
        file_path.parent.mkdir(parents=True, exist_ok=True)
        contract = asdict(self)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(contract, f, ensure_ascii=False, indent=2)


HEADER_MAPPING = {
    "contract_id": "contract_id",
    "Рег.№": "reg_number",
    "Тип договора": "contract_type",
    "Предмет договора": "contract_subject",
    "Подписывающий": "signatory",
    "Состояние": "status",
    "Дата создания": "creation_date",
    "Сумма договорa числовое": "contract_amount_numeric",
    "Не верный ДС": "incorrect_subsidy_contract",
    "Заведен": "recorded",
    "Номер версии": "version_number",
    "Рег. дата": "reg_date",
    "Контрагент": "counterparty",
    "Контрагенты все участники": "all_counterparties",
    "Заемщик": "borrower",
    "Порядковый номер": "serial_number",
    "download_path": "download_path",
    "save_location": "save_location",
}


def map_row_to_subsidy_contract(
    contract_id: str, download_folder: Path, row: Dict[str, str]
) -> SubsidyContract:
    kwargs = {HEADER_MAPPING[header]: value for header, value in row.items()}
    kwargs["save_location"] = (
        download_folder / contract_id / f"{contract_id}.zip"
    ).as_posix()
    return SubsidyContract(contract_id=contract_id, **kwargs)


class SubsidyContracts(CustomList[SubsidyContract]):
    pass


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
        self.session.headers = {
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

        self.is_logged_in = False

    @retry(exceptions=(LoginError,), tries=5, delay=5, backoff=5)
    def login(self) -> None:
        headers = self.session.headers
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
        self.session.cookies.set("browser_token", browser_token)

        user_agent = self.session.headers.get("user-agent")
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
            logging.error("Request failed")
            raise LoginError("Robot was unable to login into the EDO...")

        data = response.json()
        data_status = data.get("status")
        logging.debug(f"Response internal {data_status}")

        if data_status not in {3, 9}:
            raise LoginError("Robot was unable to login into the EDO...")

        logging.debug("Login process completed successfully")

        self.is_logged_in = True

    def get_contract_list_row_count(self) -> Tuple[bool, int]:
        headers = self.session.headers
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

        self.session.cookies.set("FontSize", "9")

        params = {
            "qs": "",
            "flt[1][name]": "f_4135638",
            "flt[1][type]": "string",
            "flt[1][condition]": "LIKE",
            "flt[1][value]": "Договор субсидирования",
            "g[f_e1a2e56]": "Подписан",
            "pp": "getTotalRows",
        }

        response = self.request(
            method="post",
            path="workflow/folder/index/167955b9-2e25-4044-a253-5a0ae83e01e0/f48469f3-1a44-4ddb-ba0f-5a0aea09009c",
            headers=headers,
            params=params,
        )

        if not response:
            return False, -1

        if not hasattr(response, "json"):
            logging.error(
                f"Response does not have JSON. Text instead - {response.text=}"
            )
            return False, -1
        data = response.json()

        row_count = data.get("total")
        if not row_count:
            logging.error(f"No key 'total' in {response.json()=}")
            return False, -1

        return True, int(row_count)

    def get_contracts(
        self, page: Union[int, str], ascending: bool, current_retry: int = 0
    ) -> Tuple[bool, SubsidyContracts]:
        contracts = SubsidyContracts()

        headers = self.session.headers
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

        response = self.request(
            method="get",
            path="/workflow/folder/index/167955b9-2e25-4044-a253-5a0ae83e01e0/f48469f3-1a44-4ddb-ba0f-5a0aea09009c",
            headers=headers,
            params=params,
        )

        if not response:
            logging.warning("Robot is not logged in to the EDO...")
            current_retry += 1
            if current_retry >= 5:
                return False, contracts
            self.login()
            res = self.get_contracts(page, ascending, current_retry)
            return res

        contract_list_html = response.text
        soup = BeautifulSoup(contract_list_html, features="lxml")

        if "Вход" in soup.text.strip().split("\n")[0]:
            logging.warning("Robot is not logged in to the EDO...")
            current_retry += 1
            if current_retry >= 5:
                return False, contracts
            self.login()
            res = self.get_contracts(page, ascending, current_retry)
            return res

        contracts.extend(self.parse_table_rows(soup))
        return True, contracts

    def download_file(self, contact_save_location: Path, path: str) -> bool:
        if contact_save_location.exists():
            return True

        headers = self.session.headers
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

        response = self.request(method="get", path=path, headers=headers)
        if not response:
            logging.error("Download request failed")
            return False

        contact_save_location.parent.mkdir(parents=True, exist_ok=True)
        with open(contact_save_location, "wb") as file:
            file.write(response.content)

        extract_folder = contact_save_location.parent
        safe_extract(contact_save_location, extract_folder=extract_folder)

        for file_path in extract_folder.iterdir():
            stem, name = file_path.stem, file_path.name
            if (stem == contact_save_location.stem) or (name.endswith(".docx")):
                continue
            file_path.unlink()

        return True

    def batch_download(self, contracts: SubsidyContracts) -> None:
        contract_count = len(contracts)
        logging.info(f"Preparing to download {contract_count} archives...")
        downloaded_count = 0
        for idx, contract in enumerate(contracts, start=1):
            if not self.download_file(
                Path(contract.save_location), contract.download_path
            ):
                logging.warning(f"ERROR - {idx}/{contract_count}")
            logging.info(f"SUCCESS - {idx}/{contract_count}")
            downloaded_count += 1
        logging.info(f"Downloaded - {downloaded_count}/{contract_count}")

    def parse_table_rows(self, soup: BeautifulSoup) -> SubsidyContracts:
        headers = [
            (idx, text)
            for idx, el in enumerate(soup.select("tr.title > td"))
            if (text := el.text.strip())
        ]

        contracts = SubsidyContracts()
        row_idx = 0

        while True:
            try:
                current_row = select_one(root=soup, selector=f"tr#grid_row_{row_idx}")
            except HTMLElementNotFound:
                break

            row = {
                header: select_one(
                    root=current_row, selector=f":nth-child({html_col_idx + 1})"
                ).text.strip()
                for html_col_idx, header in headers
            }

            anchor = select_one(
                root=current_row, selector="td.document_extra_info > div > a"
            )
            row["download_path"] = anchor.get("href")

            contract_id = row["download_path"].split("/")[-1]

            contracts.append(
                map_row_to_subsidy_contract(contract_id, self.download_root_folder, row)
            )
            row_idx += 1

        return contracts
