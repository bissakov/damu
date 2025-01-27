import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, ContextManager, Dict, List, Optional, Tuple, Union


class DatabaseManager:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    def connect(self) -> ContextManager[sqlite3.Cursor]:
        @contextmanager
        def wrapped():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            try:
                yield cursor
                conn.commit()
            finally:
                conn.close()

        return wrapped()

    def execute(
        self,
        query: str,
        params: Optional[Union[Tuple[Any, ...], Dict[str, Any]]] = None,
        fetch_one: bool = False,
    ) -> List[Tuple[Any, ...]]:
        with self.connect() as cursor:
            cursor.execute(query, params or ())
            return cursor.fetchone() if fetch_one else cursor.fetchall()

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
            CREATE TABLE IF NOT EXISTS edo_contracts (
                id TEXT NOT NULL UNIQUE PRIMARY KEY,
                reg_number TEXT NOT NULL,
                contract_type TEXT NOT NULL,
                reg_date TEXT NOT NULL,
                download_path TEXT NOT NULL,
                save_folder TEXT NOT NULL,
                date_modified TEXT NOT NULL
            )
        """)

        self.execute("""
            CREATE TABLE IF NOT EXISTS parse_contracts (
                id TEXT UNIQUE PRIMARY KEY,
                start_date TEXT,
                end_date TEXT,
                loan_amount REAL,
                iban TEXT,
                error TEXT,
                date_modified TEXT NOT NULL,
                FOREIGN KEY (id) REFERENCES edo_contracts (id)
            )
        """)

        self.execute("""
            CREATE TABLE IF NOT EXISTS interest_rates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rate REAL NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                contract_id TEXT NOT NULL,
                date_modified TEXT NOT NULL,
                FOREIGN KEY (contract_id) REFERENCES parse_contracts (id)
            )
        """)

        self.execute("""
            CREATE TABLE IF NOT EXISTS protocol_ids (
                protocol_id TEXT NOT NULL PRIMARY KEY,
                contract_id TEXT NOT NULL,
                newest INTEGER NOT NULL,
                date_modified TEXT NOT NULL,
                FOREIGN KEY (contract_id) REFERENCES parse_contracts (id)
            )
        """)

        self.execute("""
            CREATE TABLE IF NOT EXISTS banks (
                bank_id TEXT NOT NULL PRIMARY KEY,
                bank TEXT,
                year_count INTEGER
            )
        """)

        self.execute("""
            CREATE TABLE IF NOT EXISTS crm_contracts (
                id TEXT NOT NULL PRIMARY KEY,
                project_id TEXT NOT NULL,
                project TEXT NOT NULL,
                customer TEXT NOT NULL,
                customer_id TEXT NOT NULL,
                date_modified TEXT NOT NULL,
                bank_id TEXT NOT NULL,
                FOREIGN KEY (id) REFERENCES parse_contracts (id),
                FOREIGN KEY (bank_id) REFERENCES banks (bank_id)
            )
        """)
