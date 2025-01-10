import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from src.error import LoginError, retry
from src.utils.request_handler import RequestHandler


class Schemas:
    def __init__(self, schema_json_path: Path) -> None:
        self.schema_json_path = schema_json_path

        with open(schema_json_path, "r", encoding="utf-8") as f:
            self.schemas = json.load(f)

    def project_info(self, protocol_id: str) -> Dict[str, Any]:
        schema = self.schemas["project_info"]
        schema["filters"]["items"]["5e7b1496-66c3-44b7-9098-0f071a07751c"]["items"][
            "CustomFilters"
        ]["items"]["customFilterProtocolDS_Subsidies"]["rightExpression"]["parameter"][
            "value"
        ] = protocol_id
        return schema

    def project(self, project_id: str) -> Dict[str, Any]:
        schema = self.schemas["project"]
        col_filter = schema["filters"]["items"]["primaryColumnFilter"]
        col_filter["rightExpression"]["parameter"]["value"] = project_id
        return schema


class CRM(RequestHandler):
    def __init__(
        self,
        user: str,
        password: str,
        base_url: str,
        download_folder: Path,
        user_agent: str,
        schema_json_path: Path,
    ) -> None:
        super().__init__(user, password, base_url, download_folder)
        self.session.headers = {
            "accept": "application/json",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/json",
            "origin": "https://crm.fund.kz",
            "priority": "u=1, i",
            "referer": "https://crm.fund.kz/Login/NuiLogin.aspx?ReturnUrl=%2f%3fsimpleLogin&simpleLogin",
            "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": user_agent,
            "x-request-source": "ajax-provider",
            "x-requested-with": "XMLHttpRequest",
        }

        self.schemas = Schemas(schema_json_path)

    @retry(exceptions=(LoginError,), tries=5, delay=5, backoff=5)
    def login(self) -> bool:
        credentials = {
            "UserName": self.user,
            "UserPassword": self.password,
            "TimeZoneOffset": -300,
        }

        logging.info("Fetching '.ASPXAUTH', 'BPMCSRF', and 'UserName' cookies")
        if not self.request(
            method="post",
            path="servicemodel/authservice.svc/login",
            json=credentials,
            update_cookies=True,
        ):
            logging.error(
                "Request failed while fetching '.ASPXAUTH', 'BPMCSRF', and 'UserName' cookies"
            )
            return False
        logging.info(
            "Fetched '.ASPXAUTH', 'BPMCSRF', and 'UserName' cookies successfully"
        )

        logging.debug("Extracting 'BPMCSRF' token from cookies")
        self.session.headers["BPMCSRF"] = self.session.cookies.get("BPMCSRF") or ""
        logging.info("'BPMCSRF' token added to headers")

        logging.info("Login process completed successfully")
        return True

    def find_project(self, protocol_id: str) -> Tuple[bool, Optional[Dict[Any, Any]]]:
        json_data = self.schemas.project_info(protocol_id)

        response = self.request(
            method="post",
            path="0/DataService/json/SyncReply/SelectQuery",
            json=json_data,
        )
        if not response:
            return False, None

        if hasattr(response, "json"):
            return True, response.json()
        else:
            return False, None

    def get_project_data(
        self, project_id: str
    ) -> Tuple[bool, Optional[Dict[Any, Any]]]:
        json_data = self.schemas.project(project_id)

        response = self.request(
            method="post",
            path="0/DataService/json/SyncReply/SelectQuery",
            json=json_data,
        )
        if not response:
            return False, None

        if hasattr(response, "json"):
            return True, response.json()
        else:
            return False, None
