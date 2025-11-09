from __future__ import annotations

import sqlite3
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


class GamebotClient:
    def list_tables(self) -> list[str]:
        """Return a list of all available table names in the SQLite database."""
        with self.connect() as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
            return [row[0] for row in cursor.fetchall()]

    def show_table_schema(self, table_name: str) -> None:
        """Print the schema (columns and types) for a given table."""
        with self.connect() as conn:
            cursor = conn.execute(f'PRAGMA table_info("{table_name}")')
            columns = cursor.fetchall()
            if not columns:
                print(f"Table '{table_name}' does not exist.")
                return
            print(f"Schema for table '{table_name}':")
            for col in columns:
                # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
                print(f"  {col[1]} ({col[2]})")

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

    def _fetch_table_names(self) -> Sequence[str]:
        with self.connect() as conn:
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
        with self.connect() as conn:
            df = pd.read_sql_query(query, conn, **read_sql_kwargs)
        df.attrs["gamebot_layer"] = resolved_layer
        df.attrs["warehouse_table"] = source
        return df

    def duckdb_query(self, sql: str) -> pd.DataFrame:
        if duckdb is None:
            raise ImportError("duckdb is not installed. Run `pip install duckdb`.")
        con = duckdb.connect()
        try:
            # Get all table names from SQLite
            with self.connect() as conn:
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
                table_names = [row[0] for row in cursor.fetchall()]
                table_columns = {}
                for table in table_names:
                    col_cursor = conn.execute(f'PRAGMA table_info("{table}")')
                    table_columns[table] = [row[1] for row in col_cursor.fetchall()]
            # Register each table as a DuckDB table using sqlite_scan
            for table in table_names:
                con.execute(
                    f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM sqlite_scan('{self.sqlite_path}', '{table}')"
                )
            try:
                return con.execute(sql).fetch_df()
            except Exception as e:
                # Enhanced error message for missing tables/columns
                msg = str(e)
                if "not found" in msg or "does not have a column" in msg:
                    print(
                        "\n[GamebotLite Debug] Query failed. Available tables and columns:"
                    )
                    for t, cols in table_columns.items():
                        print(f"  {t}: {', '.join(cols)}")
                    print("\n[GamebotLite Debug] Error:", msg)
                raise
        finally:
            con.close()

    # _register_layer_schemas is no longer needed with direct table registration

    def _normalize_identifier(
        self, table_name: str, layer: Optional[str]
    ) -> Tuple[str, str]:
        candidate = table_name
        inferred_layer = layer
        if "." in table_name:
            prefix, remainder = table_name.split(".", 1)
            if prefix in VALID_LAYERS:
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


def load_table(
    table_name: str,
    path: Optional[Path] = None,
    *,
    layer: Optional[str] = None,
    **read_sql_kwargs,
) -> pd.DataFrame:
    from . import DEFAULT_SQLITE_PATH

    client = GamebotClient(path or DEFAULT_SQLITE_PATH)
    return client.load_table(table_name, layer=layer, **read_sql_kwargs)


def duckdb_query(sql: str, path: Optional[Path] = None) -> pd.DataFrame:
    from . import DEFAULT_SQLITE_PATH

    client = GamebotClient(path or DEFAULT_SQLITE_PATH)
    return client.duckdb_query(sql)
