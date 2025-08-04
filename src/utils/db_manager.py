from __future__ import annotations

import logging
import re
import sqlite3
from contextlib import contextmanager
from typing import TYPE_CHECKING, overload, cast

if TYPE_CHECKING:
    from typing import Any, ContextManager, Literal, Type
    from pathlib import Path
    from collections.abc import Generator
    from types import TracebackType

    SqlParams = tuple[Any, ...] | dict[str, Any] | None


logger = logging.getLogger("DAMU")


class DatabaseManager:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    def connect(self) -> ContextManager[sqlite3.Cursor]:
        @contextmanager
        def wrapped() -> Generator[sqlite3.Cursor]:
            conn = sqlite3.connect(self.db_path)
            conn.execute("PRAGMA foreign_keys = ON;")
            cursor = conn.cursor()
            try:
                yield cursor
                conn.commit()
            finally:
                conn.close()

        return wrapped()

    def execute(self, query: str, params: SqlParams = None) -> None:
        with self.connect() as cursor:
            cursor.execute(query, params or ())

    def fetch_one(
        self, query: str, params: SqlParams = None
    ) -> tuple[Any, ...]:
        with self.connect() as cursor:
            cursor.execute(query, params or ())
            return cast(tuple["Any", ...], cursor.fetchone())

    def fetch_all(
        self, query: str, params: SqlParams = None
    ) -> list[tuple[Any, ...]]:
        with self.connect() as cursor:
            cursor.execute(query, params or ())
            return cursor.fetchall()

    @overload
    def request(self, query: str, params: SqlParams | None = None) -> None: ...

    @overload
    def request(
        self,
        query: str,
        params: SqlParams = None,
        *,
        req_type: Literal["execute"],
    ) -> None: ...

    @overload
    def request(
        self,
        query: str,
        params: SqlParams = None,
        *,
        req_type: Literal["fetch_one"],
    ) -> tuple[Any, ...]: ...

    @overload
    def request(
        self,
        query: str,
        params: SqlParams = None,
        *,
        req_type: Literal["fetch_all"],
    ) -> list[tuple[Any, ...]]: ...

    def request(
        self,
        query: str,
        params: SqlParams = None,
        *,
        req_type: Literal["execute", "fetch_one", "fetch_all"] = "execute",
    ) -> tuple[Any, ...] | list[tuple[Any, ...]] | None:
        try:
            return getattr(self, req_type)(query, params)
        except sqlite3.IntegrityError as err:
            query = re.sub(r"\s+", " ", query).strip()
            logger.error(f"{query!r} with {params=}")
            raise err
        except sqlite3.Error as err:
            logger.error(f"Database error: {err} - {query!r}")
            raise err

    def prepare_tables(self) -> None:
        self.request("PRAGMA journal_mode=WAL")

        self.request("""
            CREATE TABLE IF NOT EXISTS contracts (
                id TEXT NOT NULL UNIQUE PRIMARY KEY,
                modified TEXT DEFAULT (datetime('now','localtime')),
                ds_id TEXT NOT NULL,
                ds_date TEXT,
                file_name TEXT,
                contragent TEXT,
                sed_number TEXT,
                contract_type TEXT,
                protocol_id TEXT,
                protocol_date TEXT,
                decision_date TEXT,
                settlement_date INTEGER,
                start_date TEXT,
                end_date TEXT,
                contract_start_date TEXT,
                contract_end_date TEXT,
                loan_amount REAL,
                subsid_amount REAL,
                investment_amount REAL,
                pos_amount REAL,
                vypiska_date TEXT,
                iban TEXT,
                df BLOB,
                credit_purpose TEXT,
                repayment_procedure TEXT,
                dbz_id TEXT,
                dbz_date TEXT,
                request_number INTEGER,
                project_id TEXT,
                project TEXT,
                customer TEXT,
                customer_id TEXT,
                bank_id TEXT,
                bank TEXT,
                year_count INTEGER,
                region TEXT
            )
        """)

        self.request("""
            CREATE TABLE IF NOT EXISTS interest_rates (
                id TEXT PRIMARY KEY,
                modified TEXT DEFAULT (datetime('now','localtime')),
                subsid_term INTEGER,
                nominal_rate INTEGER,
                rate_one_two_three_year INTEGER,
                rate_four_year INTEGER,
                rate_five_year INTEGER,
                rate_six_seven_year INTEGER,
                rate_fee_one_two_three_year INTEGER,
                rate_fee_four_year INTEGER,
                rate_fee_five_year INTEGER,
                rate_fee_six_seven_year INTEGER,
                start_date_one_two_three_year TEXT,
                end_date_one_two_three_year TEXT,
                start_date_four_year TEXT,
                end_date_four_year TEXT,
                start_date_five_year TEXT,
                end_date_five_year TEXT,
                start_date_six_seven_year TEXT,
                end_date_six_seven_year TEXT,
                FOREIGN KEY (id) REFERENCES contracts (id)
            )
        """)

        self.request("""
            CREATE TABLE IF NOT EXISTS macros (
                id TEXT NOT NULL PRIMARY KEY,
                modified TEXT DEFAULT (datetime('now','localtime')),
                macro BLOB,
                shifted_macro BLOB,
                df BLOB,
                FOREIGN KEY (id) REFERENCES contracts (id)
            )
        """)

        self.request("""
            CREATE TABLE IF NOT EXISTS results (
                id TEXT NOT NULL PRIMARY KEY,
                modified TEXT DEFAULT (datetime('now','localtime')),
                result INTEGER,
                FOREIGN KEY (id) REFERENCES contracts (id)
            )
        """)

    def clean_up(self) -> None:
        self.request("DELETE FROM errors WHERE traceback IS NULL")
        self.request("VACUUM")
        self.request("PRAGMA optimize")

    def __enter__(self) -> DatabaseManager:
        self.prepare_tables()
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if exc_type is None:
            self.clean_up()
