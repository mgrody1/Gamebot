from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable, Optional, Sequence, Tuple

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

    def list_tables(self, layer: Optional[str] = None) -> Iterable[str]:
        """Return available tables, optionally filtered by layer."""

        tables = self._fetch_table_names()
        if layer is None:
            return tables

        if layer not in VALID_LAYERS:
            raise ValueError(f"Unknown layer '{layer}'. Expected one of {VALID_LAYERS}.")

        allowed = set(friendly_tables_for_layer(layer))
        return [table for table in tables if table in allowed]

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
        sqlite_path = str(self.sqlite_path)
        con = duckdb.connect()
        try:
            con.execute(f"ATTACH '{sqlite_path}' AS gamebot")
            self._register_layer_schemas(con)
            return con.execute(sql).fetch_df()
        finally:
            con.close()

    def _register_layer_schemas(self, con) -> None:
        """Expose schema-qualified views in DuckDB so layers stay explicit."""

        for layer in VALID_LAYERS:
            tables = friendly_tables_for_layer(layer)
            if not tables:
                continue
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {layer}")
            for table in tables:
                con.execute(
                    f'CREATE OR REPLACE VIEW {layer}.{table} AS SELECT * FROM gamebot."{table}"'
                )

        if METADATA_TABLES:
            con.execute("CREATE SCHEMA IF NOT EXISTS metadata")
            for table in METADATA_TABLES:
                con.execute(
                    f'CREATE OR REPLACE VIEW metadata.{table} AS SELECT * FROM gamebot."{table}"'
                )

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
