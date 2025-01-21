import asyncio
import logging
import math
import random
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple, Union

import aiofiles
from bs4 import BeautifulSoup

from src.error import LoginError, retry
from src.subsidy import contract_count, iter_contracts, map_row_to_subsidy_contract
from src.utils.collections import batched
from src.utils.request_handler import RequestHandler


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

    @retry(exceptions=(LoginError,), tries=5, delay=5, backoff=5)
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

    def parse_table_rows(self, soup: BeautifulSoup) -> None:
        headers = [
            (idx, text)
            for idx, el in enumerate(soup.select("tr.title > td"))
            if (text := el.text.strip())
        ]

        row_idx = 0
        while (
            current_row := soup.select_one(selector=f"tr#grid_row_{row_idx}")
        ) is not None:
            row = {
                header: current_row.select_one(
                    selector=f":nth-child({html_col_idx + 1})"
                ).text.strip()
                for html_col_idx, header in headers
            }

            anchor = current_row.select_one(selector="td.document_extra_info > div > a")
            row["download_path"] = anchor.get("href")

            contract_id = row["download_path"].split("/")[-1]

            contract = map_row_to_subsidy_contract(
                contract_id, self.download_folder, row
            )
            contract.save()
            row_idx += 1

    def get_contracts(
        self, page: Union[int, str], ascending: bool, current_retry: int = 0
    ) -> bool:
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
                return False
            self.login()
            res = self.get_contracts(
                page=page, ascending=ascending, current_retry=current_retry
            )
            return res

        contract_list_html = response.text
        soup = BeautifulSoup(contract_list_html, features="lxml")

        if "Вход" in soup.text.strip().split("\n")[0]:
            logging.warning("Robot is not logged in to the EDO...")
            current_retry += 1
            if current_retry >= 5:
                return False
            self.login()
            res = self.get_contracts(
                page=page, ascending=ascending, current_retry=current_retry
            )
            return res

        self.parse_table_rows(soup)
        return True

    def download_file(self, contact_save_folder: Path, path: str) -> bool:
        save_location = contact_save_folder / "contract.zip"
        if save_location.exists():
            return True

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

        response = self.request(method="get", path=path, headers=headers)
        if not response:
            logging.error("Download request failed")
            return False

        with open(save_location, "wb") as file:
            file.write(response.content)

        return True

    async def download_file_async(
        self, save_folder: Path, download_path: str
    ) -> Tuple[bool, Path, str]:
        save_location = save_folder / "contract.zip"
        if save_location.exists() and not save_location.stat().st_size == 0:
            logging.info(f"Valid archive potentially exists...")
            return True, save_folder, download_path

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

        response = await self.async_request(
            method="get", path=download_path, headers=headers
        )
        if not response:
            return False, save_folder, download_path

        async with aiofiles.open(save_location, "wb") as file:
            await file.write(response.content)

        return True, save_folder, download_path

    async def process_batch(
        self, batch: Tuple[Tuple[str, str], ...]
    ) -> List[Tuple[bool, str, Path, Optional[Path]]]:
        tasks = []
        for save_folder, download_path in batch:
            save_folder = Path(save_folder)
            save_folder.mkdir(exist_ok=True)

            tasks.append(
                self.download_file_async(
                    save_folder=save_folder, download_path=download_path
                )
            )

        return await asyncio.gather(*tasks)

    async def mass_download_async(self, batch_size: int = 10) -> None:
        filtered_data = set()
        for contract in iter_contracts(self.download_folder):
            if not contract:
                continue

            save_folder = Path(contract.save_folder)
            save_folder.mkdir(exist_ok=True)
            documents_folder = save_folder / "documents"
            documents_folder.mkdir(exist_ok=True)
            archive_path = save_folder / "contract.zip"

            if (
                archive_path.exists() and not archive_path.stat().st_size == 0
            ) or documents_folder.stat().st_size != 0:
                continue
            filtered_data.add((contract.save_folder, contract.download_path))

        batches = batched(filtered_data, batch_size)
        batch_count = math.ceil(len(filtered_data) / batch_size)

        undone_tasks = []

        async with self.async_client:
            for idx, batch in enumerate(batches, start=1):
                logging.info(f"Processing batch {idx}/{batch_count}")

                results = await self.process_batch(batch)
                for jdx, (status, save_folder, path) in enumerate(results, start=1):
                    if not status:
                        logging.warning(f"EDO - ERROR - {jdx:02}/{len(batch)}")
                        undone_tasks.append(
                            self.download_file_async(
                                save_folder=save_folder, download_path=path
                            )
                        )
                        continue
                    logging.info(f"EDO - {jdx:02}/{len(batch)}")

            results = await asyncio.gather(*undone_tasks)
            for status, save_folder, path in results:
                if not status:
                    logging.error(f"Was unable to download {path!r}")
                    continue

    def mass_download(self, max_page: int) -> None:
        # for page in range(max_page):
        #     logging.info(f"Page {page + 1}/{max_page}")
        #     if not self.get_contracts(page=page, ascending=False):
        #         logging.warning("Robot is not logged in to the EDO...")
        #         raise Exception("Robot is not logged in to the EDO...")

        logging.info(
            f"Preparing to download {contract_count(self.download_folder)} archives..."
        )

        asyncio.run(self.mass_download_async(batch_size=50))

        # for idx, contract in enumerate(contracts, start=1):
        #     save_folder = Path(contract.save_folder)
        #     save_folder.mkdir(exist_ok=True)
        #     documents_folder = save_folder / "documents"
        #     documents_folder.mkdir(exist_ok=True)
        #
        #     if not self.download_file(
        #         save_folder, documents_folder, contract.download_path
        #     ):
        #         logging.warning(
        #             f"EDO - ERROR - {idx:03}/{contract_count} - {contract.contract_id}"
        #         )
        #         continue
        #     logging.info(
        #         f"EDO - SUCCESS - {idx:03}/{contract_count} - {contract.contract_id}"
        #     )
