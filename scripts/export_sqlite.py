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
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List
from uuid import UUID

import pandas as pd
from sqlalchemy import create_engine, text

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from gamebot_core.db_utils import create_sql_engine
from gamebot_lite.catalog import friendly_name_overrides

logger = logging.getLogger(__name__)


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
    pg_engine = create_sql_engine()
    sqlite_engine = create_engine(f"sqlite:///{output_path}")

    schemas = {
        "bronze": ["bronze"],
        "silver": ["bronze", "silver"],
        "gold": ["bronze", "silver", "gold"],
    }
    selected_schemas = schemas[layer]

    exported_tables = []
    for schema in selected_schemas:
        tables = _list_tables(pg_engine, schema)
        for table in tables:
            fq_table = f'"{schema}"."{table}"'
            df = pd.read_sql(f"SELECT * FROM {fq_table}", con=pg_engine)

            # Convert UUID columns to strings for SQLite compatibility
            for col in df.columns:
                if df[col].dtype == "object" and not df[col].empty:
                    # Check if any values in this column are UUIDs
                    sample_val = (
                        df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                    )
                    if isinstance(sample_val, UUID):
                        df[col] = df[col].astype(str)

            df.to_sql(
                _friendly_table_name(schema, table),
                sqlite_engine,
                if_exists="replace",
                index=False,
            )
        # collect exported table names for manifest
        exported_tables.extend([_friendly_table_name(schema, t) for t in tables])

    metadata_df = _latest_ingestion(pg_engine)

    # Convert UUID columns to strings in metadata as well
    for col in metadata_df.columns:
        if metadata_df[col].dtype == "object" and not metadata_df[col].empty:
            sample_val = (
                metadata_df[col].dropna().iloc[0]
                if not metadata_df[col].dropna().empty
                else None
            )
            if isinstance(sample_val, UUID):
                metadata_df[col] = metadata_df[col].astype(str)

    metadata_df.to_sql(
        "gamebot_ingestion_metadata", sqlite_engine, if_exists="replace", index=False
    )
    return metadata_df, exported_tables


def _compute_sha256(path: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


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

    metadata_df, exported_tables = export_sqlite(args.layer, output_path)
    if args.package:
        package_path = Path("gamebot_lite") / "data" / output_path.name
        package_path.parent.mkdir(parents=True, exist_ok=True)

        # Use shutil.copy2 instead of write_bytes for better permission handling
        import shutil

        package_copy_succeeded = False
        try:
            shutil.copy2(output_path, package_path)
            logger.info("Copied export into package data: %s", package_path)
            package_copy_succeeded = True
        except PermissionError:
            logger.warning(
                "Permission denied writing to %s, export available at %s",
                package_path,
                output_path,
            )
            # Continue without failing - the export is still available at output_path

        # Write a simple manifest describing this export so release tooling can make
        # deterministic decisions. Include ingestion metadata, exported tables and
        # a checksum for integrity.
        # Only create manifest if package copy succeeded
        if package_copy_succeeded:
            manifest: dict = {}
            manifest["exported_at"] = datetime.now(timezone.utc).isoformat()
            manifest["layer"] = args.layer
            manifest["sqlite_filename"] = package_path.name
            manifest["sqlite_sha256"] = _compute_sha256(package_path)

            # ingestion metadata (if available)
            try:
                if not metadata_df.empty:
                    # convert first row to serializable dict
                    row = metadata_df.iloc[0].to_dict()
                    # stringify any non-serializable values
                    serializable_row = {
                        k: (str(v) if not isinstance(v, (str, int, float, bool)) else v)
                        for k, v in row.items()
                    }
                    manifest["ingestion"] = serializable_row
            except Exception:
                manifest["ingestion"] = None

            try:
                manifest["exported_tables"] = exported_tables
            except Exception:
                manifest["exported_tables"] = []

            # repo metadata
            try:
                import subprocess

                git_sha = (
                    subprocess.check_output(["git", "rev-parse", "HEAD"])
                    .decode()
                    .strip()
                )
                manifest["exporter_git_sha"] = git_sha
            except Exception:
                manifest["exporter_git_sha"] = None

            manifest_path = package_path.parent / "manifest.json"
            manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
            logger.info("Wrote export manifest: %s", manifest_path)
        else:
            logger.info("Skipping manifest creation due to package copy failure")

    logger.info("Export complete: %s", output_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
logger = logging.getLogger(__name__)
