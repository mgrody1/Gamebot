from __future__ import annotations

import sqlite3
import threading
from contextlib import closing
from functools import lru_cache
from pathlib import Path
from typing import Optional, Sequence, Tuple

import pandas as pd

try:
    import duckdb
except ImportError:  # pragma: no cover
    duckdb = None

from .catalog import (
    METADATA_TABLES,
    TABLE_LAYER_MAP,
    VALID_LAYERS,
    WAREHOUSE_TABLE_MAP,
    friendly_tables_for_layer,
)

# SQLite declared type -> DuckDB type, since pandas cannot infer dates/timestamps
# stored as TEXT, booleans stored as 0/1, or all-NULL columns. FLOAT is mapped
# explicitly because DuckDB's FLOAT is 32-bit.
DUCKDB_TYPES = {
    "TEXT": "VARCHAR",
    "BIGINT": "BIGINT",
    "FLOAT": "DOUBLE",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
}


class GamebotClient:
    """Simple wrapper around the exported SQLite database."""

    def list_tables(self, layer: Optional[str] = None) -> list[str]:
        """Return the table names in the SQLite database.

        Pass ``layer`` (``bronze``, ``silver``, ``gold``, or ``metadata``) to
        return only that layer's tables.
        """
        tables = list(self._fetch_table_names())
        if layer is None:
            return tables
        if layer not in (*VALID_LAYERS, "metadata"):
            raise ValueError(
                f"Unknown layer '{layer}'. Expected one of {VALID_LAYERS} or 'metadata'."
            )
        return [table for table in tables if TABLE_LAYER_MAP.get(table) == layer]

    def show_table_schema(self, table_name: str) -> None:
        """Print the schema (columns and types) for a given table."""
        with closing(self.connect()) as conn:
            cursor = conn.execute(f'PRAGMA table_info("{table_name}")')
            columns = cursor.fetchall()
            if not columns:
                print(f"Table '{table_name}' does not exist.")
                return
            print(f"Schema for table '{table_name}':")
            for col in columns:
                # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
                print(f"  {col[1]} ({col[2]})")

    def __init__(self, sqlite_path: Path):
        self.sqlite_path = Path(sqlite_path)
        self._check_exists()
        self._duckdb_con = None
        self._duckdb_signature = None
        self._duckdb_columns: dict[str, list[str]] = {}
        self._duckdb_lock = threading.Lock()

    def _check_exists(self) -> None:
        if not self.sqlite_path.exists():
            raise FileNotFoundError(
                f"SQLite file {self.sqlite_path} not found. "
                "Run `scripts/export_sqlite.py --layer gold --package` first or "
                "download the packaged file."
            )

    def connect(self) -> sqlite3.Connection:
        # Clients are cached, so the file can vanish after __init__; without this
        # check sqlite3.connect would silently create an empty database.
        self._check_exists()
        return sqlite3.connect(self.sqlite_path)

    def _fetch_table_names(self) -> Sequence[str]:
        with closing(self.connect()) as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
            return [row[0] for row in cursor.fetchall()]

    def load_table(
        self,
        table_name: str,
        *,
        layer: Optional[str] = None,
        **read_sql_kwargs,
    ) -> pd.DataFrame:
        """Load a friendly Gamebot Lite table into a dataframe.

        Parameters
        ----------
        table_name:
            Gamebot Lite table name (e.g. ``castaway_profile``). You can also
            pass a fully-qualified identifier like ``silver.castaway_profile``.
        layer:
            Optional hint that asserts which layer the table comes from. If
            omitted, the layer is inferred from the catalog metadata.
        """

        sqlite_table, resolved_layer = self._normalize_identifier(table_name, layer)
        if resolved_layer == "metadata":
            source = sqlite_table
        else:
            source = WAREHOUSE_TABLE_MAP[sqlite_table]

        query = f'SELECT * FROM "{sqlite_table}"'
        with closing(self.connect()) as conn:
            df = pd.read_sql_query(query, conn, **read_sql_kwargs)
        df.attrs["gamebot_layer"] = resolved_layer
        df.attrs["warehouse_table"] = source
        return df

    def duckdb_query(self, sql: str) -> pd.DataFrame:
        # A cursor per query: a DuckDB connection is not safe to share across
        # threads (e.g. Streamlit sessions), but its cursors are.
        con = self._duckdb_connection().cursor()
        try:
            # The copy is shared by every query, so each one runs in a transaction
            # that is always rolled back: DDL/DML such as CREATE TABLE tmp never
            # leaks into later queries.
            con.execute("BEGIN TRANSACTION")
            return con.execute(sql).fetch_df()
        except Exception as e:
            # Enhanced error message for missing tables/columns
            msg = str(e)
            if (
                "not found" in msg
                or "does not have a column" in msg
                or "does not exist" in msg
            ):
                print(
                    "\n[GamebotLite Debug] Query failed. Available tables and columns:"
                )
                for t, cols in self._duckdb_columns.items():
                    print(f"  {t}: {', '.join(cols)}")
                print("\n[GamebotLite Debug] Error:", msg)
            raise
        finally:
            try:
                con.execute("ROLLBACK")
            except duckdb.Error:
                pass  # the query already ended the transaction
            con.close()

    def _duckdb_connection(self):
        """Build an in-memory DuckDB copy of the snapshot and reuse it.

        Tables are read through sqlite3/pandas rather than DuckDB's sqlite
        extension, so no extension download (and no network) is needed. The copy
        is rebuilt when the SQLite file changes on disk.
        """
        with self._duckdb_lock:
            self._check_exists()
            stat = self.sqlite_path.stat()
            signature = (stat.st_mtime_ns, stat.st_size, stat.st_ino)
            if self._duckdb_con is None or signature != self._duckdb_signature:
                # A replaced copy is left to garbage collection, since other
                # threads may still be reading from it.
                self._duckdb_con = self._build_duckdb_connection()
                self._duckdb_signature = signature
        return self._duckdb_con

    def _build_duckdb_connection(self):
        if duckdb is None:
            raise ImportError("duckdb is not installed. Run `pip install duckdb`.")

        con = duckdb.connect()
        table_columns = {}
        try:
            with closing(self.connect()) as conn:
                for table in self._fetch_table_names():
                    columns = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
                    df = pd.read_sql_query(f'SELECT * FROM "{table}"', conn)
                    # Cast back to the declared column types (see DUCKDB_TYPES).
                    select_list = ", ".join(
                        f'CAST("{col[1]}" AS {DUCKDB_TYPES[col[2].upper()]}) AS "{col[1]}"'
                        if col[2].upper() in DUCKDB_TYPES
                        else f'"{col[1]}"'
                        for col in columns
                    )
                    con.register("_gamebot_df", df)
                    con.execute(
                        f'CREATE TABLE "{table}" AS SELECT {select_list} FROM _gamebot_df'
                    )
                    con.unregister("_gamebot_df")
                    table_columns[table] = [col[1] for col in columns]

                    # Layer-qualified views, so bronze.*, silver.*, gold.*, and metadata.* resolve.
                    layer = TABLE_LAYER_MAP.get(table)
                    if layer is not None:
                        con.execute(f"CREATE SCHEMA IF NOT EXISTS {layer}")
                        con.execute(
                            f'CREATE VIEW {layer}."{table}" AS SELECT * FROM main."{table}"'
                        )
        except Exception:
            con.close()
            raise

        self._duckdb_columns = table_columns
        return con

    def _normalize_identifier(
        self, table_name: str, layer: Optional[str]
    ) -> Tuple[str, str]:
        candidate = table_name
        inferred_layer = layer
        if "." in table_name:
            prefix, remainder = table_name.split(".", 1)
            if prefix in (*VALID_LAYERS, "metadata"):
                inferred_layer = prefix
                candidate = remainder

        if inferred_layer is None:
            inferred_layer = TABLE_LAYER_MAP.get(candidate)
            if inferred_layer is None:
                raise ValueError(
                    f"Unknown table '{table_name}'. Pass a fully-qualified name like "
                    "'silver.castaway_profile' or specify the layer explicitly."
                )
        elif inferred_layer not in (*VALID_LAYERS, "metadata"):
            raise ValueError(
                f"Unknown layer '{inferred_layer}'. Expected one of {VALID_LAYERS} or 'metadata'."
            )

        if inferred_layer == "metadata":
            if candidate not in METADATA_TABLES:
                raise ValueError(
                    f"Table '{table_name}' is not part of the metadata export. "
                    f"Available metadata tables: {', '.join(METADATA_TABLES)}."
                )
            return candidate, "metadata"

        valid_tables = set(friendly_tables_for_layer(inferred_layer))
        if candidate not in valid_tables:
            raise ValueError(
                f"Table '{candidate}' does not belong to the {inferred_layer} layer. "
                f"Valid {inferred_layer} tables: {', '.join(sorted(valid_tables))}."
            )
        return candidate, inferred_layer


@lru_cache(maxsize=None)
def _cached_client(sqlite_path: Path) -> GamebotClient:
    # One client per snapshot path, so the DuckDB copy is built once per process.
    return GamebotClient(sqlite_path)


def _default_client(path: Optional[Path]) -> GamebotClient:
    from . import DEFAULT_SQLITE_PATH

    return _cached_client(Path(path or DEFAULT_SQLITE_PATH).resolve())


def load_table(
    table_name: str,
    path: Optional[Path] = None,
    *,
    layer: Optional[str] = None,
    **read_sql_kwargs,
) -> pd.DataFrame:
    client = _default_client(path)
    return client.load_table(table_name, layer=layer, **read_sql_kwargs)


def duckdb_query(sql: str, path: Optional[Path] = None) -> pd.DataFrame:
    return _default_client(path).duckdb_query(sql)
