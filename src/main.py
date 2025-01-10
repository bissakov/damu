import json
import logging
import pickle
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import dotenv
import pytz

from src.crm import CRM
from src.edo import EDO, ProtocolIDs, SubsidyContracts
from src.error import LoginError
from src.subsidy import RegexPatterns, SubsidyParser
from src.utils import logger
from src.utils.utils import get_from_env


def main():
    project_folder = Path(__file__).parent.parent
    today = datetime.now(pytz.timezone("Asia/Almaty")).date()

    logger.setup_logger(project_folder, today)
    dotenv.load_dotenv(project_folder / ".env")

    user_agent = get_from_env("USER_AGENT")

    edo_user = get_from_env("EDO_USERNAME")
    edo_password = get_from_env("EDO_PASSWORD")
    edo_base_url = get_from_env("EDO_BASE_URL")

    download_root_folder = project_folder / "downloads"
    download_root_folder.mkdir(exist_ok=True)
    download_folder = download_root_folder / str(today)
    download_folder.mkdir(exist_ok=True)

    resources_folder = project_folder / "resources"

    edo = EDO(
        user=edo_user,
        password=edo_password,
        base_url=edo_base_url,
        download_folder=download_folder,
        user_agent=user_agent,
    )
    regex_patterns = RegexPatterns()

    crm_user = get_from_env("CRM_USERNAME")
    crm_password = get_from_env("CRM_PASSWORD")
    crm_base_url = get_from_env("CRM_BASE_URL")
    schema_json_path = resources_folder / "schemas.json"

    crm = CRM(
        user=crm_user,
        password=crm_password,
        base_url=crm_base_url,
        download_folder=download_folder,
        user_agent=user_agent,
        schema_json_path=schema_json_path,
    )

    with edo, crm:
        edo.login()
        crm.login()

        max_page = 3
        for page in range(max_page):
            logging.info(f"Page {page + 1}/{max_page}")

            contracts_pkl_path = download_folder / f"contracts_page_{page}.pkl"
            contracts_json_path = download_folder / f"contracts_page_{page}.json"
            if contracts_pkl_path.exists():
                with open(contracts_pkl_path, "rb") as f:
                    contracts: SubsidyContracts = pickle.load(f)
            else:
                status, contracts = edo.get_contracts(page, ascending=False)
                if not status:
                    logging.warning("Robot is not logged in to the EDO...")
                    edo.login()

                with (
                    open(contracts_pkl_path, "wb") as f1,
                    open(contracts_json_path, "w", encoding="utf-8") as f2,
                ):
                    pickle.dump(contracts, f1)
                    json.dump(
                        [asdict(c) for c in contracts], f2, ensure_ascii=False, indent=2
                    )

                if not contracts:
                    raise LoginError("Robot is not logged in to the EDO...")

            # edo.batch_download(contracts)

            contract_count = len(contracts)
            logging.info(f"Preparing to download {contract_count} archives...")
            for idx, contract in enumerate(contracts, start=1):
                if not edo.download_file(
                    Path(contract.save_location), contract.download_path
                ):
                    logging.warning(
                        f"EDO - ERROR - {idx:02}/{contract_count} - {contract.contract_id}"
                    )
                    continue
                logging.info(
                    f"EDO - SUCCESS - {idx:02}/{contract_count} - {contract.contract_id}"
                )

                if not (folder := Path(contract.save_location)).parent.exists():
                    logging.warning(f"{folder.name} folder does not exist...")
                    continue

                parser = SubsidyParser(contract, regex_patterns)

                file_path = parser.find_subsidy_contact_file()
                if not file_path:
                    logging.warning(
                        f"EDO - {contract.contract_id} does not have subsidy contracts..."
                    )

                contract.protocol_ids.extend(parser.find_protocol_ids())

                for protocol_id in contract.protocol_ids:
                    status, data = crm.find_project(protocol_id=protocol_id)
                    if not status:
                        logging.error(f"CRM - ERROR - {protocol_id=}")
                        continue
                    logging.info(f"CRM - SUCCESS - {protocol_id=}")
                    rows = data.get("rows")
                    for row in rows:
                        project_id = row.get("Id")
                        status, row_data = crm.get_project_data(project_id)
                        if not status:
                            logging.error(f"CRM - ERROR - {project_id=}")
                            continue
                        logging.info(f"CRM - SUCCESS - {project_id=}")
                        row["data"] = row_data.get("rows")
                    contract.data.extend(rows)

                contract.save()


if __name__ == "__main__":
    main()
