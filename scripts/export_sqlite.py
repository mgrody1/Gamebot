#!/usr/bin/env python3
# ruff: noqa: E402

"""
Export warehouse tables to a local SQLite file for quick analysis.

Usage:
    pipenv run python scripts/export_sqlite.py --layer bronze --output gamebot.sqlite
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import List

import pandas as pd
from sqlalchemy import create_engine, text

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

import params
from gamebot_core.db_utils import create_sql_engine
from gamebot_core.env import require_prod_on_main
from gamebot_lite.catalog import friendly_name_overrides


def _list_tables(pg_engine, schema: str) -> List[str]:
    query = text(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
    )
    with pg_engine.connect() as conn:
        result = conn.execute(query, {"schema": schema})
        return [row[0] for row in result]


def _latest_ingestion(pg_engine):
    query = "SELECT * FROM bronze.ingestion_runs ORDER BY run_started_at DESC LIMIT 1"
    return pd.read_sql(query, con=pg_engine)


def _friendly_table_name(schema: str, table: str) -> str:
    overrides = friendly_name_overrides(schema)
    return overrides.get(table, table)


def export_sqlite(layer: str, output_path: Path) -> None:
    require_prod_on_main(params.environment)
    pg_engine = create_sql_engine()
    sqlite_engine = create_engine(f"sqlite:///{output_path}")

    schemas = {
        "bronze": ["bronze"],
        "silver": ["bronze", "silver"],
        "gold": ["bronze", "silver", "gold"],
    }
    selected_schemas = schemas[layer]

    for schema in selected_schemas:
        tables = _list_tables(pg_engine, schema)
        for table in tables:
            fq_table = f'"{schema}"."{table}"'
            df = pd.read_sql(f"SELECT * FROM {fq_table}", con=pg_engine)
            df.to_sql(
                _friendly_table_name(schema, table),
                sqlite_engine,
                if_exists="replace",
                index=False,
            )

    metadata_df = _latest_ingestion(pg_engine)
    metadata_df.to_sql(
        "gamebot_ingestion_metadata", sqlite_engine, if_exists="replace", index=False
    )


def main():
    parser = argparse.ArgumentParser(description="Export warehouse data to SQLite.")
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold"],
        default="bronze",
        help="Highest layer to include (includes all preceding layers).",
    )
    parser.add_argument(
        "--output",
        default="gamebot.sqlite",
        help="Path to the output SQLite database.",
    )
    parser.add_argument(
        "--package",
        action="store_true",
        help="Also copy the exported SQLite file into gamebot_lite/data/ for packaging.",
    )

    args = parser.parse_args()

    output_path = Path(args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    export_sqlite(args.layer, output_path)
    if args.package:
        package_path = Path("gamebot_lite") / "data" / output_path.name
        package_path.parent.mkdir(parents=True, exist_ok=True)
        package_path.write_bytes(output_path.read_bytes())
        logger.info("Copied export into package data: %s", package_path)
    logger.info("Export complete: %s", output_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
logger = logging.getLogger(__name__)
