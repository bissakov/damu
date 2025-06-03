import json
import logging
import re
import traceback
from datetime import datetime, timedelta, date
from pathlib import Path
from types import TracebackType
from typing import Any, Type, cast, override

import pandas as pd
from dateutil.relativedelta import relativedelta

from sverka.error import CRMNotFoundError, VypiskaDownloadError
from sverka.structures import Registry
from sverka.subsidy import Bank, CrmContract, Error, InterestRate
from utils.db_manager import DatabaseManager
from utils.request_handler import RequestHandler

logger = logging.getLogger("DAMU")


class Schemas:
    def __init__(self, schema_json_path: Path) -> None:
        self.schema_json_path = schema_json_path

        with open(schema_json_path, "r", encoding="utf-8") as f:
            self.schemas: dict[str, Any] = json.load(f)

    def project_info(self, protocol_id: str) -> dict[str, Any]:
        schema = self.schemas["project_info"]
        schema["filters"]["items"]["5e7b1496-66c3-44b7-9098-0f071a07751c"][
            "items"
        ]["CustomFilters"]["items"]["customFilterProtocolDS_Subsidies"][
            "rightExpression"
        ]["parameter"]["value"] = protocol_id
        return schema  # type: ignore

    def project(self, project_id: str) -> dict[str, Any]:
        schema = self.schemas["project"]
        col_filter = schema["filters"]["items"]["primaryColumnFilter"]
        col_filter["rightExpression"]["parameter"]["value"] = project_id
        return schema  # type: ignore

    def vypiska_project(self, project_id: str) -> dict[str, Any]:
        schema = self.schemas["vypiska_project"]
        col_filter = schema["filters"]["items"][
            "c72e0a89-19a9-441c-bc2c-cb0148ffce91"
        ]
        col_filter["items"]["masterRecordFilter"]["rightExpression"][
            "parameter"
        ]["value"] = project_id
        return schema  # type: ignore

    def vypiska(self, vypiska_id: str) -> dict[str, Any]:
        schema = self.schemas["vypiska"]
        col_filter = schema["filters"]["items"]["entityFilterGroup"]["items"]
        col_filter["masterRecordFilter"]["rightExpression"]["parameter"][
            "value"
        ] = vypiska_id
        col_filter["b19c9ce1-07f7-41ae-9f85-17a3d6cbc788"]["rightExpression"][
            "parameter"
        ]["value"] = vypiska_id
        return schema  # type: ignore

    def agreements(self, project_id: str) -> dict[str, Any]:
        schema = self.schemas["agreements"]
        col_filter = schema["filters"]["items"][
            "d6ff8291-010e-4c2e-b230-6727f954b94f"
        ]
        col_filter["items"]["masterRecordFilter"]["rightExpression"][
            "parameter"
        ]["value"] = project_id
        return schema  # type: ignore


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

    def login(self) -> bool:
        credentials = {
            "UserName": self.user,
            "UserPassword": self.password,
            "TimeZoneOffset": -300,
        }

        logger.info("Fetching '.ASPXAUTH', 'BPMCSRF', and 'UserName' cookies")
        if not self.request(
            method="post",
            path="servicemodel/authservice.svc/login",
            json=credentials,  # type: ignore
            update_cookies=True,
        ):
            logger.error(
                "Request failed while fetching '.ASPXAUTH', 'BPMCSRF', and 'UserName' cookies"
            )
            self.is_logged_in = False
            return False
        logger.info(
            "Fetched '.ASPXAUTH', 'BPMCSRF', and 'UserName' cookies successfully"
        )

        logger.debug("Extracting 'BPMCSRF' token from cookies")
        self.client.headers["BPMCSRF"] = (
            self.client.cookies.get("BPMCSRF") or ""
        )
        logger.info("'BPMCSRF' token added to headers")

        logger.info("Login process completed successfully")
        self.is_logged_in = True
        return True

    def find_project(self, protocol_id: str) -> dict[str, Any] | None:
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
            return None

        if not hasattr(response, "json"):
            return None

        data = response.json()
        rows: list[dict[str, Any]] = data.get("rows")

        if not rows:
            return None

        row = rows[0]

        return row

    def get_project_data(self, project_id: str) -> dict[str, Any] | None:
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
            return None

        if hasattr(response, "json"):
            data = response.json()
            rows = data.get("rows")
            assert isinstance(rows, list)
            return rows[0]  # type: ignore
        else:
            return None

    def fetch_agreement_data(self, project_id: str) -> dict[str, Any] | None:
        if not self.is_logged_in:
            self.login()

        json_data = self.schemas.agreements(project_id)

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
                return rows[0]  # type: ignore

        return None

    def fetch_vypiska_id(self, project_id: str) -> dict[str, Any] | None:
        if not self.is_logged_in:
            self.login()

        json_data = self.schemas.vypiska_project(project_id)
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
            (
                row
                for row in rows
                if row.get("Type", {}).get("displayValue") == "Выписка ДС"
            ),
            None,
        )

        return vypiska_row

    def download_vypiska(
        self, contract_id: str, file_id: str, file_name: str
    ) -> bool:
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

    def download_vypiskas(
        self, contract_id: str, project_id: str
    ) -> dict[str, Any] | None:
        if not self.is_logged_in:
            self.login()

        vypiska_row = self.fetch_vypiska_id(project_id=project_id)
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
            file_name = file_name.replace("/", " ").replace("\\", " ")
            if not file_id or not file_name:
                continue
            self.download_vypiska(
                contract_id=contract_id, file_id=file_id, file_name=file_name
            )

        return vypiska_row

    @override
    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.is_logged_in = False
        super().__exit__(exc_type, exc_val, exc_tb)


def normalize_float(value: float) -> int:
    return int(value * 100)


def build_interest_rate(
    contract_id: str, project: dict[str, Any], start_date: str, end_date: str
) -> InterestRate:
    subsid_term = cast(int, project["SubsidTerm"])
    nominal_rate = normalize_float(project["NominalInterestRate"])
    rate_one_two_three_year = normalize_float(project["SubsidInterestRate"])
    rate_four_year = normalize_float(project["INFSubsidInterestRateFourYear"])
    rate_five_year = normalize_float(project["INFSubsidInterestRateFiveYear"])
    rate_six_seven_year = normalize_float(
        project["INFSubsidInterestRateSixSevenYear"]
    )
    rate_fee_one_two_three_year = normalize_float(
        project["INFSubsidInterestRateFee"]
    )
    rate_fee_four_year = normalize_float(
        project["INFSubsidInterestRateFeeFourYear"]
    )
    rate_fee_five_year = normalize_float(
        project["INFSubsidInterestRateFeeFiveYear"]
    )
    rate_fee_six_seven_year = normalize_float(
        project["INFSubsidInterestRateFeeSixSevenYear"]
    )

    start_date1 = pd.to_datetime(start_date)

    if rate_four_year != 0:
        start_date2 = start_date1 + relativedelta(years=3)
        end_date1 = start_date2 - timedelta(days=1)

        if rate_five_year != 0:
            start_date3 = start_date2 + relativedelta(years=1)
            end_date2 = start_date3 - timedelta(days=1)

            if rate_six_seven_year != 0:
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

    start_date_one_two_three_year = start_date1
    end_date_one_two_three_year = end_date1
    start_date_four_year = start_date2
    end_date_four_year = end_date2
    start_date_five_year = start_date3
    end_date_five_year = end_date3
    start_date_six_seven_year = start_date4
    end_date_six_seven_year = end_date4

    ir = InterestRate(
        contract_id=contract_id,
        subsid_term=subsid_term,
        nominal_rate=nominal_rate,
        rate_one_two_three_year=rate_one_two_three_year,
        rate_four_year=rate_four_year,
        rate_five_year=rate_five_year,
        rate_six_seven_year=rate_six_seven_year,
        rate_fee_one_two_three_year=rate_fee_one_two_three_year,
        rate_fee_four_year=rate_fee_four_year,
        rate_fee_five_year=rate_fee_five_year,
        rate_fee_six_seven_year=rate_fee_six_seven_year,
        start_date_one_two_three_year=start_date_one_two_three_year,
        end_date_one_two_three_year=end_date_one_two_three_year,
        start_date_four_year=start_date_four_year,
        end_date_four_year=end_date_four_year,
        start_date_five_year=start_date_five_year,
        end_date_five_year=end_date_five_year,
        start_date_six_seven_year=start_date_six_seven_year,
        end_date_six_seven_year=end_date_six_seven_year,
    )
    return ir


def fetch_crm_data_one(
    crm: CRM,
    contract_id: str,
    protocol_id: str,
    start_date: str,
    end_date: str,
    db: DatabaseManager,
    registry: Registry,
    dbz_id: str | None,
    dbz_date: date | None,
) -> CrmContract:
    contract = CrmContract(
        contract_id=contract_id, error=Error(contract_id=contract_id)
    )

    row = crm.find_project(protocol_id=protocol_id)
    if not row:
        try:
            raise CRMNotFoundError(f"Protocol {protocol_id} not found...")
        except CRMNotFoundError as err:
            logger.exception(err)
            logger.error(f"CRM - ERROR - {contract.project_id=} - {err!r}")
            contract.error.traceback = f"{err!r}\n{traceback.format_exc()}"
            contract.error.error = err
            contract.error.human_readable = contract.error.get_human_readable()
        contract.error.save(db)
        contract.save(db)
        return contract
    logger.info(f"CRM - SUCCESS - {protocol_id=}")

    contract.project_id = row.get("Id")
    contract.project = row.get("Project", {}).get("displayValue")
    contract.customer = row.get("Customer", {}).get("displayValue")
    contract.customer_id = row.get("Customer", {}).get("value")
    contract.bank_id = row.get("BvuLk", {}).get("value")

    assert contract.bank_id

    bank = Bank(
        contract_id=contract_id,
        bank_id=contract.bank_id,
        bank=row.get("BvuLk", {}).get("displayValue"),
        year_count=registry.banks.get(contract.bank_id),
    )
    bank.save(db)

    assert contract.project_id

    project = crm.get_project_data(contract.project_id)
    if not project:
        try:
            raise CRMNotFoundError(
                f"Project {contract.project_id} of protocol {protocol_id} not found..."
            )
        except CRMNotFoundError as err:
            logger.exception(err)
            logger.error(f"CRM - ERROR - {contract.project_id=} - {err!r}")
            contract.error.traceback = f"{err!r}\n{traceback.format_exc()}"
            contract.error.human_readable = contract.error.get_human_readable()
        contract.error.save(db)
        contract.save(db)
        return contract
    logger.info(f"CRM - SUCCESS - {contract.project_id=}")

    contract.subsid_amount = project.get("ProjectSubsidAmount") or 0.0
    contract.investment_amount = project.get("ForInvestment") or 0.0
    contract.pos_amount = project.get("ForPOS") or 0.0
    contract.credit_purpose = registry.mappings.get("credit_purpose", {}).get(
        project.get("CreditingPurpose", {}).get("displayValue")
    )
    contract.request_number = project.get("RequestNumber")

    date_scoring = project.get("DateScoring") or ""
    contract.protocol_date = datetime.strptime(
        date_scoring, "%Y-%m-%dT%H:%M:%S.%f"
    ).date()
    contract.repayment_procedure = registry.mappings.get(
        "repayment_procedure", {}
    ).get(project.get("RepaymentOrderMainLoan", {}).get("displayValue"))

    bvulk_date = project.get("BvuLkDate") or ""
    contract.decision_date = datetime.strptime(
        bvulk_date, "%Y-%m-%dT%H:%M:%S.%f"
    ).date()

    if dbz_id:
        contract.dbz_id = dbz_id
    if dbz_date:
        contract.dbz_date = dbz_date
    agreement_data = crm.fetch_agreement_data(contract.project_id)
    if agreement_data:
        if not contract.dbz_id:
            contract.dbz_id = (agreement_data.get("NumberDBZ")).strip()

        if not contract.dbz_date:
            dbz_date = agreement_data.get("DateDBZ") or ""
            contract.dbz_date = pd.to_datetime(dbz_date)

    ir = build_interest_rate(contract_id, project, start_date, end_date)
    ir.save(db)

    vypiska_row = crm.download_vypiskas(
        contract_id=contract_id, project_id=contract.project_id
    )
    if not vypiska_row:
        try:
            raise VypiskaDownloadError(
                f"Vypiska of protocol {protocol_id} was not downloaded..."
            )
        except VypiskaDownloadError as err:
            logger.exception(err)
            logger.error(f"CRM - ERROR - {protocol_id=} - {err!r}")
            contract.error.traceback = f"{err!r}\n{traceback.format_exc()}"
            contract.error.human_readable = contract.error.get_human_readable()
        contract.error.save(db)
        contract.save(db)
        return contract

    try:
        vypiska_date = vypiska_row.get("Date") or ""
        contract.vypiska_date = datetime.fromisoformat(vypiska_date).date()
    except TypeError as err:
        logger.exception(err)
        logger.error(f"CRM - ERROR - {protocol_id=} - {err!r}")
        contract.error.traceback = f"{err!r}\n{traceback.format_exc()}"
        contract.error.human_readable = contract.error.get_human_readable()
        contract.error.save(db)
        contract.save(db)
        return contract

    if not contract.repayment_procedure:
        repayment_procedure = vypiska_row.get("Note")
        if not isinstance(repayment_procedure, str):
            try:
                raise ValueError(
                    f"{repayment_procedure=} is not str. {vypiska_row=}"
                )
            except ValueError as err:
                logger.exception(err)
                logger.error(f"CRM - ERROR - {protocol_id=} - {err!r}")
                contract.error.traceback = f"{err!r}\n{traceback.format_exc()}"
                contract.error.human_readable = (
                    contract.error.get_human_readable()
                )
            contract.error.save(db)
            contract.save(db)
            return contract

        repayment_procedure = re.sub(
            r"[^\w\s]", "", repayment_procedure.lower()
        )
        repayment_procedure = re.sub(r"\s{2,}", " ", repayment_procedure)

        contract.repayment_procedure = next(
            (
                value
                for key, value in registry.mappings.get(
                    "repayment_procedure" or {}
                ).items()
                if key in repayment_procedure
            ),
            None,
        )
        # if not contract.repayment_procedure:
        #     try:
        #         raise ValueError(f"{contract.repayment_procedure=} is still None. {vypiska_row=}")
        #     except ValueError as err:
        #         logger.error(f"CRM - ERROR - {protocol_id=} - {err!r}")
        #         contract.error.traceback = f"{err!r}\n{traceback.format_exc()}"
        #         contract.error.human_readable = contract.error.get_human_readable()
        #     contract.error.save(db)
        #     contract.save(db)
        #     return contract

    contract.save(db)

    return contract
