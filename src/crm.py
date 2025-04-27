import json
import logging
import re
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep
from types import TracebackType
from typing import Any, Dict, Optional, Tuple, Type, override

import pandas as pd
from dateutil.relativedelta import relativedelta

from src.error import CRMNotFoundError, LoginError, VypiskaDownloadError, retry
from src.structures import Registry
from src.subsidy import Bank, CrmContract, Error, InterestRate
from src.utils.db_manager import DatabaseManager
from src.utils.request_handler import RequestHandler


@dataclass(slots=True)
class Record:
    value: str
    display_value: str


@dataclass(slots=True)
class ProjectInfo:
    project_id: str
    bank: str
    bank_id: str
    project: str
    customer: str
    customer_id: str


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

    def vypiska_project(self, project_id: str) -> Dict[str, Any]:
        schema = self.schemas["vypiska_project"]
        col_filter = schema["filters"]["items"]["c72e0a89-19a9-441c-bc2c-cb0148ffce91"]
        col_filter["items"]["masterRecordFilter"]["rightExpression"]["parameter"]["value"] = (
            project_id
        )
        return schema

    def vypiska(self, vypiska_id: str) -> Dict[str, Any]:
        schema = self.schemas["vypiska"]
        col_filter = schema["filters"]["items"]["entityFilterGroup"]["items"]
        col_filter["masterRecordFilter"]["rightExpression"]["parameter"]["value"] = vypiska_id
        col_filter["b19c9ce1-07f7-41ae-9f85-17a3d6cbc788"]["rightExpression"]["parameter"][
            "value"
        ] = vypiska_id
        return schema

    def agreements(self, project_id: str) -> Dict[str, Any]:
        schema = self.schemas["agreements"]
        col_filter = schema["filters"]["items"]["d6ff8291-010e-4c2e-b230-6727f954b94f"]
        col_filter["items"]["masterRecordFilter"]["rightExpression"]["parameter"]["value"] = (
            project_id
        )
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
        self.client.headers = {
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
        self.is_logged_in = False

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
            self.is_logged_in = False
            return False
        logging.info("Fetched '.ASPXAUTH', 'BPMCSRF', and 'UserName' cookies successfully")

        logging.debug("Extracting 'BPMCSRF' token from cookies")
        self.client.headers["BPMCSRF"] = self.client.cookies.get("BPMCSRF") or ""
        logging.info("'BPMCSRF' token added to headers")

        logging.info("Login process completed successfully")
        self.is_logged_in = True
        return True

    def find_project(self, protocol_id: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
        if not self.is_logged_in:
            self.login()

        json_data = self.schemas.project_info(protocol_id)

        response = self.request(
            method="post",
            path="0/DataService/json/SyncReply/SelectQuery",
            json=json_data,
        )
        if not response:
            self.is_logged_in = False
            return False, None

        if hasattr(response, "json"):
            data = response.json()
            rows = data.get("rows")

            if not isinstance(rows, list) or not rows:
                return False, None

            row = rows[0]

            return True, row
        else:
            return False, None

    def get_project_data(self, project_id: str) -> Tuple[bool, Optional[Dict[Any, Any]]]:
        if not self.is_logged_in:
            self.login()

        json_data = self.schemas.project(project_id)

        response = self.request(
            method="post",
            path="0/DataService/json/SyncReply/SelectQuery",
            json=json_data,
        )
        if not response:
            self.is_logged_in = False
            return False, None

        if hasattr(response, "json"):
            data = response.json()
            rows = data.get("rows")
            assert isinstance(rows, list)
            return True, rows[0]
        else:
            return False, None

    def fetch_agreement_data(self, crm_contract: CrmContract) -> Optional[Dict[str, Any]]:
        if not self.is_logged_in:
            self.login()

        json_data = self.schemas.agreements(crm_contract.project_id)

        response = self.request(
            method="post",
            path="0/DataService/json/SyncReply/SelectQuery",
            json=json_data,
        )
        if not response:
            self.is_logged_in = False
            return None

        if hasattr(response, "json"):
            data = response.json()
            rows = data.get("rows")
            assert isinstance(rows, list)
            if rows:
                return rows[0]

        return None

    def fetch_vypiska_id(self, crm_contract: CrmContract) -> Optional[Dict[str, Any]]:
        if not self.is_logged_in:
            self.login()

        json_data = self.schemas.vypiska_project(crm_contract.project_id)
        response = self.request(
            method="post",
            path="0/DataService/json/SyncReply/SelectQuery",
            json=json_data,
        )
        if not response:
            self.is_logged_in = False
            return None

        if not hasattr(response, "json"):
            return None

        data = response.json()
        rows = data.get("rows")
        assert isinstance(rows, list)

        vypiska_row = next(
            (row for row in rows if row.get("Type", {}).get("displayValue") == "Выписка ДС"),
            None,
        )

        return vypiska_row

    def download_vypiska(self, contract_id: str, file_id: str, file_name: str) -> bool:
        folder_path = self.download_folder / contract_id / "vypiska"
        folder_path.mkdir(exist_ok=True)

        file_path = folder_path / file_name

        response = self.request(
            method="get",
            path=f"0/rest/FileService/GetFile/7b332db9-3993-4136-ac32-09353333cc7a/{file_id}",
        )
        if not response:
            self.is_logged_in = False
            return False

        with file_path.open("wb") as f:
            f.write(response.content)

        return True

    def download_vypiskas(self, crm_contract: CrmContract) -> Optional[Dict[str, Any]]:
        if not self.is_logged_in:
            self.login()

        vypiska_row = self.fetch_vypiska_id(crm_contract=crm_contract)
        if not isinstance(vypiska_row, dict):
            return None

        vypiska_id = vypiska_row.get("Id")
        if not vypiska_id:
            return None

        json_data = self.schemas.vypiska(vypiska_id=vypiska_id)
        response = self.request(
            method="post",
            path="0/DataService/json/SyncReply/SelectQuery",
            json=json_data,
        )
        if not response:
            self.is_logged_in = False
            return None

        if not hasattr(response, "json"):
            return None

        data = response.json()
        rows = data.get("rows")
        assert isinstance(rows, list)

        for row in rows:
            file_id, file_name = row.get("Id"), row.get("Name")
            if not file_id or not file_name:
                continue
            self.download_vypiska(
                contract_id=crm_contract.contract_id,
                file_id=file_id,
                file_name=file_name,
            )

        return vypiska_row

    @override
    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.is_logged_in = False
        super().__exit__(exc_type, exc_val, exc_tb)


def fetch_crm_data_one(
    crm: CRM,
    contract_id: str,
    protocol_id: str,
    start_date: str,
    end_date: str,
    db: DatabaseManager,
    registry: Registry,
) -> CrmContract:
    contract = CrmContract(contract_id=contract_id, error=Error(contract_id=contract_id))

    status, row = crm.find_project(protocol_id=protocol_id)
    if not status:
        try:
            raise CRMNotFoundError(f"Protocol {protocol_id} not found...")
        except CRMNotFoundError as err:
            logging.error(f"CRM - ERROR - {contract.project_id=} - {err!r}")
            contract.error.traceback = f"{err!r}\n{traceback.format_exc()}"
            contract.error.human_readable = contract.error.get_human_readable()
        contract.error.save(db)
        contract.save(db)
        return contract
    logging.info(f"CRM - SUCCESS - {protocol_id=}")

    contract.project_id = row.get("Id")
    contract.project = row.get("Project", {}).get("displayValue")
    contract.customer = row.get("Customer", {}).get("displayValue")
    contract.customer_id = row.get("Customer", {}).get("value")
    contract.bank_id = row.get("BvuLk", {}).get("value")

    bank = Bank(
        contract_id=contract_id,
        bank_id=contract.bank_id,
        bank=row.get("BvuLk", {}).get("displayValue"),
        year_count=registry.banks.get(contract.bank_id),
    )
    bank.save(db)

    status, project = crm.get_project_data(contract.project_id)
    if not status:
        try:
            raise CRMNotFoundError(
                f"Project {contract.project_id} of protocol {protocol_id} not found..."
            )
        except CRMNotFoundError as err:
            logging.error(f"CRM - ERROR - {contract.project_id=} - {err!r}")
            contract.error.traceback = f"{err!r}\n{traceback.format_exc()}"
            contract.error.human_readable = contract.error.get_human_readable()
        contract.error.save(db)
        contract.save(db)
        return contract
    logging.info(f"CRM - SUCCESS - {contract.project_id=}")

    contract.subsid_amount = project.get("ProjectSubsidAmount") or 0.0
    contract.investment_amount = project.get("ForInvestment") or 0.0
    contract.pos_amount = project.get("ForPOS") or 0.0
    contract.credit_purpose = registry.mappings.get("credit_purpose", {}).get(
        project.get("CreditingPurpose", {}).get("displayValue")
    )
    contract.request_number = project.get("RequestNumber")
    contract.protocol_date = datetime.strptime(
        project.get("DateScoring"), "%Y-%m-%dT%H:%M:%S.%f"
    ).date()
    contract.repayment_procedure = registry.mappings.get("repayment_procedure", {}).get(
        project.get("RepaymentOrderMainLoan", {}).get("displayValue")
    )
    contract.decision_date = datetime.strptime(
        project.get("BvuLkDate"), "%Y-%m-%dT%H:%M:%S.%f"
    ).date()

    agreement_data = crm.fetch_agreement_data(contract)
    if agreement_data:
        contract.dbz_id = (agreement_data.get("NumberDBZ") or "").strip() or None
        contract.dbz_date = pd.to_datetime(agreement_data.get("DateDBZ"))

    interest_rate = InterestRate(
        contract_id=contract_id,
        subsid_term=project.get("SubsidTerm"),
        nominal_rate=project.get("NominalInterestRate"),
        rate_one_two_three_year=project.get("SubsidInterestRate"),
        rate_four_year=project.get("INFSubsidInterestRateFourYear"),
        rate_five_year=project.get("INFSubsidInterestRateFiveYear"),
        rate_six_seven_year=project.get("INFSubsidInterestRateSixSevenYear"),
        rate_fee_one_two_three_year=project.get("INFSubsidInterestRateFee"),
        rate_fee_four_year=project.get("INFSubsidInterestRateFeeFourYear"),
        rate_fee_five_year=project.get("INFSubsidInterestRateFeeFiveYear"),
        rate_fee_six_seven_year=project.get("INFSubsidInterestRateFeeSixSevenYear"),
    )

    start_date1 = pd.to_datetime(start_date)

    if interest_rate.rate_four_year != 0:
        start_date2 = start_date1 + relativedelta(years=3)
        end_date1 = start_date2 - timedelta(days=1)

        if interest_rate.rate_five_year != 0:
            start_date3 = start_date2 + relativedelta(years=1)
            end_date2 = start_date3 - timedelta(days=1)

            if interest_rate.rate_six_seven_year != 0:
                start_date4 = start_date3 + relativedelta(years=1)
                end_date3 = start_date4 - timedelta(days=1)
                end_date4 = pd.to_datetime(end_date)
            else:
                start_date4 = None
                end_date3 = pd.to_datetime(end_date)
                end_date4 = None
        else:
            start_date3 = None
            end_date2 = pd.to_datetime(end_date)
            end_date3 = None
            start_date4 = None
            end_date4 = None
    else:
        start_date2 = None
        end_date1 = pd.to_datetime(end_date)
        start_date3 = None
        end_date2 = None
        start_date4 = None
        end_date3 = None
        end_date4 = None

    interest_rate.start_date_one_two_three_year = start_date1
    interest_rate.end_date_one_two_three_year = end_date1
    interest_rate.start_date_four_year = start_date2
    interest_rate.end_date_four_year = end_date2
    interest_rate.start_date_five_year = start_date3
    interest_rate.end_date_five_year = end_date3
    interest_rate.start_date_six_seven_year = start_date4
    interest_rate.end_date_six_seven_year = end_date4
    interest_rate.save(db)

    vypiska_row = crm.download_vypiskas(crm_contract=contract)
    if not vypiska_row:
        try:
            raise VypiskaDownloadError(f"Vypiska of protocol {protocol_id} was not downloaded...")
        except VypiskaDownloadError as err:
            logging.error(f"CRM - ERROR - {protocol_id=} - {err!r}")
            contract.error.traceback = f"{err!r}\n{traceback.format_exc()}"
            contract.error.human_readable = contract.error.get_human_readable()
        contract.error.save(db)
        contract.save(db)
        return contract

    try:
        contract.vypiska_date = datetime.fromisoformat(vypiska_row.get("Date")).date()
    except TypeError as err:
        logging.error(f"CRM - ERROR - {protocol_id=} - {err!r}")
        contract.error.traceback = f"{err!r}\n{traceback.format_exc()}"
        contract.error.human_readable = contract.error.get_human_readable()
        contract.error.save(db)
        contract.save(db)
        return contract

    if not contract.repayment_procedure:
        repayment_procedure = vypiska_row.get("Note")
        if not isinstance(repayment_procedure, str):
            try:
                raise ValueError(f"{repayment_procedure=} is not str. {vypiska_row=}")
            except ValueError as err:
                logging.error(f"CRM - ERROR - {protocol_id=} - {err!r}")
                contract.error.traceback = f"{err!r}\n{traceback.format_exc()}"
                contract.error.human_readable = contract.error.get_human_readable()
            contract.error.save(db)
            contract.save(db)
            return contract

        repayment_procedure = re.sub(r"[^\w\s]", "", repayment_procedure.lower())
        repayment_procedure = re.sub(r"\s{2,}", " ", repayment_procedure)

        contract.repayment_procedure = next(
            (
                value
                for key, value in registry.mappings.get("repayment_procedure" or {}).items()
                if key in repayment_procedure
            ),
            None,
        )
        if not contract.repayment_procedure:
            try:
                raise ValueError(f"{contract.repayment_procedure=} is still None. {vypiska_row=}")
            except ValueError as err:
                logging.error(f"CRM - ERROR - {protocol_id=} - {err!r}")
                contract.error.traceback = f"{err!r}\n{traceback.format_exc()}"
                contract.error.human_readable = contract.error.get_human_readable()
            contract.error.save(db)
            contract.save(db)
            return contract

    contract.save(db)

    sleep(0.05)

    return contract


def fetch_crm_data(crm: CRM, db: DatabaseManager, registry: Registry) -> None:
    contracts = db.execute(
        """
            SELECT c.id, c.protocol_id, c.start_date, c.end_date
            FROM contracts AS c
            LEFT JOIN errors AS e ON c.id = e.id
            WHERE e.traceback IS NULL
        """,
    )

    count = len(contracts)
    for idx, (contract_id, protocol_id, start_date, end_date) in enumerate(contracts, start=1):
        logging.info(f"CRM - {idx:02}/{count} - {contract_id}")
        fetch_crm_data_one(crm, contract_id, protocol_id, start_date, end_date, db, registry)
