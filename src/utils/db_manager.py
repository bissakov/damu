import hashlib
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
