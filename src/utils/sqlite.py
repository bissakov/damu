import sqlite3
from pathlib import Path
from sqlite3 import Connection, Cursor


class DBCursor(Cursor):
    def __init__(self, connection: Connection) -> None:
        super().__init__(connection)

    def __enter__(self) -> "DBCursor":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


class SQLiteConnection:
    def __init__(self, database: Path) -> None:
        self.conn = sqlite3.connect(database)
        self.cursor = DBCursor(self.conn)

    def __enter__(self) -> "SQLiteConnection":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.conn.close()
