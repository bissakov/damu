import asyncio
import json
import logging
import sys
from dataclasses import fields
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Dict, List

import dotenv
import pytz

from src.subsidy import (
    SubsidyContract,
    contract_count,
    iter_contracts,
)
from src.utils.db_manager import DatabaseManager

project_folder = Path(__file__).resolve().parent.parent
sys.path.append(str(project_folder))

from src.crm import CRM
from src.edo import EDO
from src.utils import logger
from src.utils.utils import get_from_env
from src.parser import RegexPatterns, parse_documents


def main():
    today = datetime.now(pytz.timezone("Asia/Almaty")).date()
    today = today.replace(day=20)

    logger.setup_logger(project_folder, today)
    dotenv.load_dotenv(project_folder / ".env")

    user_agent = get_from_env("USER_AGENT")

    download_folder = project_folder / "downloads" / str(today)
    download_folder.mkdir(parents=True, exist_ok=True)

    resources_folder = project_folder / "resources"

    schema_json_path = resources_folder / "schemas.json"
    months_json_path = resources_folder / "months.json"
    banks_json_path = resources_folder / "banks.json"
    database = resources_folder / "database.db"

    db = DatabaseManager(database)

    edo = EDO(
        db=db,
        user=get_from_env("EDO_USERNAME"),
        password=get_from_env("EDO_PASSWORD"),
        base_url=get_from_env("EDO_BASE_URL"),
        download_folder=download_folder,
        user_agent=user_agent,
    )

    crm = CRM(
        user=get_from_env("CRM_USERNAME"),
        password=get_from_env("CRM_PASSWORD"),
        base_url=get_from_env("CRM_BASE_URL"),
        download_folder=download_folder,
        user_agent=user_agent,
        schema_json_path=schema_json_path,
    )

    db.execute("""
        CREATE TABLE IF NOT EXISTS contracts (
            id TEXT PRIMARY KEY,
            reg_date TEXT,
            date_modified TEXT,
            json BLOB,
            contract BLOB,
            error TEXT
        );
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS edo_contracts (
            id TEXT PRIMARY KEY,
            reg_number TEXT,
            contract_type TEXT,
            reg_date TEXT,
            download_path TEXT,
            save_folder TEXT,
            date_modified TEXT
        );
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS parse_contracts (
            id TEXT PRIMARY KEY,
            start_date TEXT,
            end_date TEXT,
            loan_amount REAL,
            iban TEXT,
            protocol_ids BLOB,
            interest_rates BLOB,
            error TEXT,
            date_modified TEXT
        );
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS crm_contracts (
            id TEXT PRIMARY KEY,
            project_id TEXT,
            bank BLOB,
            project BLOB,
            customer BLOB,
            date_modified TEXT
        );
    """)

    with (
        months_json_path.open("r", encoding="utf-8") as f1,
        banks_json_path.open("r", encoding="utf-8") as f2,
    ):
        months = json.load(f1)
        patterns = RegexPatterns(months=months)
        banks = json.load(f2)

    max_page = 1
    with edo, crm:
        # for page in range(max_page):
        #     logging.info(f"Page {page + 1}/{max_page}")
        #     status = edo.get_contracts(page=page, ascending=False)
        #     if not status:
        #         logging.warning("Robot is not logged in to the EDO...")
        #         raise Exception("Robot is not logged in to the EDO...")
        #
        # asyncio.run(edo.mass_download_async(batch_size=50))
        parse_documents(patterns=patterns, banks=banks, db=db)

        # count = contract_count(db)
        # for idx, contract in enumerate(iter_contracts(db), start=1):
        #     if not contract:
        #         logging.warning(f"CRM - {idx:02}/{count} - not found...")
        #         continue
        #
        #     if (
        #         contract.bank
        #         and contract.project
        #         and contract.customer
        #         and contract.project_id
        #     ):
        #         continue
        #
        #     if not contract.protocol_ids:
        #         logging.error(f"CRM - {idx:02}/{count} - protocol_ids not found...")
        #         continue
        #
        #     logging.info(f"CRM - {idx:02}/{count} - {contract.contract_id}")
        #
        #     protocol_id = contract.protocol_ids[-1]
        #     status, project_info = crm.find_project(protocol_id=protocol_id)
        #     if not status:
        #         logging.error(f"CRM - ERROR - {protocol_id=}")
        #         continue
        #     logging.info(f"CRM - SUCCESS - {protocol_id=}")
        #
        #     contract.bank = project_info.bank
        #     contract.project = project_info.project
        #     contract.customer = project_info.customer
        #     contract.project_id = project_info.project_id
        #
        #     # row = dict()
        #     # for protocol_id in contract.protocol_ids:
        #     #     status, project_info = crm.find_project(protocol_id=protocol_id)
        #     #     if not status:
        #     #         logging.error(f"CRM - ERROR - {protocol_id=}")
        #     #         continue
        #     #     logging.info(f"CRM - SUCCESS - {protocol_id=}")
        #     #
        #     #     row["project_info"] = project_info
        #     #
        #     #     # status, project = crm.get_project_data(project_info.project_id)
        #     #     # if not status:
        #     #     #     logging.error(f"CRM - ERROR - {project_info.project_id=}")
        #     #     #     continue
        #     #     # logging.info(f"CRM - SUCCESS - {project_info.project_id=}")
        #     #     # row["project"] = project
        #     #     contract.data[protocol_id] = row
        #
        #     contract.save(db)


if __name__ == "__main__":
    main()
