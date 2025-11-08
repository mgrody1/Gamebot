#!/usr/bin/env python3
"""
Lightweight smoke test for the packaged Gamebot Lite SQLite snapshot.

Checks that the bundled database exists, can be opened, and contains each
friendly table defined in the catalog metadata. Exits with a non-zero status
if any validation fails so it can be chained in CI or release scripts.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

from gamebot_lite import DEFAULT_SQLITE_PATH
from gamebot_lite.catalog import (
    METADATA_TABLES,
    friendly_tables_for_layer,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke test the packaged Gamebot Lite SQLite snapshot."
    )
    parser.add_argument(
        "--sqlite-path",
        type=Path,
        default=DEFAULT_SQLITE_PATH,
        help="Path to the SQLite file to validate (default: packaged database).",
    )
    return parser.parse_args()


def connect(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(
            f"SQLite file not found at {path}. Did you run the export step?"
        )
    return sqlite3.connect(path)


def fetch_tables(conn: sqlite3.Connection) -> set[str]:
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return {row[0] for row in cursor.fetchall()}


def expected_tables() -> set[str]:
    tables = set()
    for layer in ("bronze", "silver", "gold"):
        tables.update(friendly_tables_for_layer(layer))
    tables.update(METADATA_TABLES)
    return tables


def main() -> int:
    args = parse_args()
    try:
        with connect(args.sqlite_path) as conn:
            present_tables = fetch_tables(conn)
    except FileNotFoundError as exc:
        sys.stderr.write(f"[FAIL] {exc}\n")
        return 1

    missing = expected_tables() - present_tables
    if missing:
        sys.stderr.write(
            "[FAIL] Missing tables: "
            + ", ".join(sorted(missing))
            + f" (path: {args.sqlite_path})\n"
        )
        return 2

    print(f"[PASS] Gamebot Lite smoke test succeeded ({args.sqlite_path})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
