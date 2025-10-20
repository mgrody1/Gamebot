from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

try:
    import duckdb
except ImportError:  # pragma: no cover
    duckdb = None


class GamebotClient:
    """Simple wrapper around the exported SQLite database."""

    def __init__(self, sqlite_path: Path):
        self.sqlite_path = Path(sqlite_path)
        if not self.sqlite_path.exists():
            raise FileNotFoundError(
                f"SQLite file {self.sqlite_path} not found. "
                "Run `scripts/export_sqlite.py --layer silver --package` first or "
                "download the packaged file."
            )

    def connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.sqlite_path)

    def list_tables(self) -> Iterable[str]:
        with self.connect() as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
            return [row[0] for row in cursor.fetchall()]

    def load_table(self, table_name: str, **read_sql_kwargs) -> pd.DataFrame:
        query = f"SELECT * FROM {table_name}"
        with self.connect() as conn:
            return pd.read_sql_query(query, conn, **read_sql_kwargs)

    def duckdb_query(self, sql: str) -> pd.DataFrame:
        if duckdb is None:
            raise ImportError("duckdb is not installed. Run `pip install duckdb`." )
        sqlite_path = str(self.sqlite_path)
        con = duckdb.connect()
        try:
            con.execute(f"ATTACH '{sqlite_path}' AS gamebot")
            return con.execute(sql).fetch_df()
        finally:
            con.close()


def load_table(table_name: str, path: Optional[Path] = None, **read_sql_kwargs) -> pd.DataFrame:
    from . import DEFAULT_SQLITE_PATH

    client = GamebotClient(path or DEFAULT_SQLITE_PATH)
    return client.load_table(table_name, **read_sql_kwargs)


def duckdb_query(sql: str, path: Optional[Path] = None) -> pd.DataFrame:
    from . import DEFAULT_SQLITE_PATH

    client = GamebotClient(path or DEFAULT_SQLITE_PATH)
    return client.duckdb_query(sql)
