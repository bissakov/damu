import logging
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, ContextManager, Dict, List, Mapping, Optional, Sequence, Union


SQLParam = Union[None, int, float, str, bytes, bool]
SQLParams = Union[Sequence[SQLParam], Mapping[str, SQLParam]]


class DatabaseManager:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    def connect(self) -> ContextManager[sqlite3.Cursor]:
        @contextmanager
        def wrapped():
            conn = sqlite3.connect(self.db_path)
            conn.execute("PRAGMA foreign_keys = ON;")
            cursor = conn.cursor()
            try:
                yield cursor
                conn.commit()
            finally:
                conn.close()

        return wrapped()

    def execute(self, query: str, params: Optional[SQLParams] = None) -> Sequence[SQLParam]:
        try:
            with self.connect() as cursor:
                cursor.execute(query, params or ())
                return cursor.fetchall()
        except sqlite3.IntegrityError as err:
            query = re.sub(r"\s+", " ", query).strip()
            logging.error(f"{query!r} with {params=}")
            raise err
        except sqlite3.Error as err:
            logging.error(f"Database error: {err} - {query!r}")
            raise err

    def execute_many(
        self,
        query: str,
        params: Optional[List[Dict[Any, ...]]] = None,
    ) -> None:
        with self.connect() as cursor:
            cursor.executemany(query, params or ())

    def execute_script(self, query: str) -> None:
        with self.connect() as cursor:
            cursor.executescript(query)

    def prepare_tables(self) -> None:
        self.execute("PRAGMA journal_mode=WAL")

        self.execute("""
            CREATE TABLE IF NOT EXISTS banks (
                bank_id TEXT NOT NULL PRIMARY KEY,
                bank TEXT,
                year_count INTEGER
            )
        """)

        self.execute("""
            CREATE TABLE IF NOT EXISTS contracts (
                id TEXT NOT NULL UNIQUE PRIMARY KEY,
                modified TEXT DEFAULT (datetime('now','localtime')),
                ds_id TEXT NOT NULL,
                ds_date TEXT NOT NULL,
                file_name TEXT,
                contragent TEXT NOT NULL,
                sed_number TEXT,
                protocol_id TEXT,
                protocol_date TEXT,
                decision_date TEXT,
                start_date TEXT,
                end_date TEXT,
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
                FOREIGN KEY (bank_id) REFERENCES banks (bank_id)
            )
        """)

        self.execute("""
            CREATE TABLE IF NOT EXISTS interest_rates (
                id TEXT PRIMARY KEY,
                modified TEXT DEFAULT (datetime('now','localtime')),
                subsid_term INTEGER,
                nominal_rate REAL,
                rate_one_two_three_year REAL,
                rate_four_year REAL,
                rate_five_year REAL,
                rate_six_seven_year REAL,
                rate_fee_one_two_three_year REAL,
                rate_fee_four_year REAL,
                rate_fee_five_year REAL,
                rate_fee_six_seven_year REAL,
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

        self.execute("""
            CREATE TABLE IF NOT EXISTS macros (
                id TEXT NOT NULL PRIMARY KEY,
                modified TEXT DEFAULT (datetime('now','localtime')),
                macro BLOB,
                shifted_macro BLOB,
                df BLOB,
                FOREIGN KEY (id) REFERENCES contracts (id)
            )
        """)

        self.execute("""
            CREATE TABLE IF NOT EXISTS errors (
                id TEXT NOT NULL PRIMARY KEY,
                modified TEXT DEFAULT (datetime('now','localtime')),
                traceback TEXT,
                human_readable TEXT,
                FOREIGN KEY (id) REFERENCES contracts (id)
            )
        """)

    def clean_up(self) -> None:
        self.execute("""
            DELETE FROM errors
            WHERE traceback IS NULL
        """)

        self.execute_script("VACUUM;PRAGMA optimize;")

    def __enter__(self) -> "DatabaseManager":
        self.prepare_tables()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is None:
            self.clean_up()
