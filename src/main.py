import json
import sys
from datetime import datetime
from pathlib import Path

import dotenv
import pytz

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

    edo = EDO(
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

    with months_json_path.open("r", encoding="utf-8") as f:
        months = json.load(f)
        patterns = RegexPatterns(months=months)

    with edo, crm:
        # edo.mass_download(max_page=20)
        errors = parse_documents(download_folder, patterns)

        # for contract in contracts:
        #     contract.data = dict()
        #     row = dict()
        #     for protocol_id in contract.protocol_ids:
        #         status, project_info = crm.find_project(protocol_id=protocol_id)
        #         if not status:
        #             logging.error(f"CRM - ERROR - {protocol_id=}")
        #             continue
        #         logging.info(f"CRM - SUCCESS - {protocol_id=}")
        #
        #         row["project_info"] = project_info
        #
        #         status, project = crm.get_project_data(project_info.project_id)
        #         if not status:
        #             logging.error(f"CRM - ERROR - {project_info.project_id=}")
        #             continue
        #         logging.info(f"CRM - SUCCESS - {project_info.project_id=}")
        #         row["project"] = project
        #         contract.data[protocol_id] = row
        #
        # pkl_path = download_folder / f"contracts.pkl"
        # json_path = download_folder / f"contracts.json"
        # with open(pkl_path, "wb") as f1, open(json_path, "w", encoding="utf-8") as f2:
        #     pickle.dump(contracts, f1)
        #     json.dump(
        #         list(contracts),
        #         f2,
        #         ensure_ascii=False,
        #         indent=2,
        #         default=lambda obj: obj.to_dict(),
        #     )


if __name__ == "__main__":
    main()
