from __future__ import annotations

import logging
import os
from datetime import datetime

import dotenv
import pytz

today = datetime.now(pytz.timezone("Asia/Almaty")).date()
os.environ["today"] = today.isoformat()

logger = logging.getLogger("DAMU")


def main() -> None:
    dotenv.load_dotenv(".env")

    import sys

    print(sys.path)
    # registry = Registry(download_folder=Path(f"downloads/{today}"))
    #
    # edo = EDO(
    #     user=os.environ["EDO_USERNAME"],
    #     password=os.environ["EDO_PASSWORD"],
    #     base_url=os.environ["EDO_BASE_URL"],
    #     download_folder=registry.download_folder,
    #     user_agent=os.environ["USER_AGENT"],
    # )
    # crm = CRM(
    #     user=os.environ["CRM_USERNAME"],
    #     password=os.environ["CRM_PASSWORD"],
    #     base_url=os.environ["CRM_BASE_URL"],
    #     download_folder=registry.download_folder,
    #     user_agent=os.environ["USER_AGENT"],
    #     schema_json_path=registry.schema_json_path,
    # )
    #
    # with DatabaseManager(registry.database) as db:
    #     # contract_ids = [
    #     #     "d1e64465-57de-40ca-b009-67554074016f",
    #     #     "249dadb8-9651-43f7-b826-675295b90006",
    #     #     "8223d792-bfd0-4b78-abfa-67597f0b0342",
    #     #     "d75c5c24-cba7-4dec-8aac-674433e601a4",
    #     #     "2a65b501-9886-4e81-b636-6756d92e0027",
    #     #     "3db431d8-0ccb-46fb-bb8a-6760fbbc02ba",
    #     # ]
    #
    #     contract_ids = [
    #         "0764baaa-69d7-4c4a-ae56-68382cdc017b",
    #         "20c8a867-0180-4dad-9486-683836b2020a",
    #         "915c9b98-631f-4447-bc30-6838075a021c",
    #         "27f2001a-61ca-4cd2-9e61-6837f9fe018a",
    #     ]
    #
    #     for _, contract_id in enumerate(contract_ids):
    #         reply = process_contract(
    #             logger=logger,
    #             db=db,
    #             contract_id=contract_id,
    #             edo=edo,
    #             crm=crm,
    #             registry=registry,
    #         )
    #         logger.info(f"Reply - {reply!r}")
    #
    #         # contract = get_contract(
    #         #     contract_id, db, registry.banks.get("mapping", {})
    #         # )
    #         # rate = InterestRate.load(db, contract.contract_id)
    #         # logger.info(f"{contract=!r}")
    #         # logger.info(f"{rate=!r}")
    #         #
    #         # fill_1c(contract, rate, registry, "test_base.v8i")


if __name__ == "__main__":
    main()
