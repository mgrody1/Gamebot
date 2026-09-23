#!/usr/bin/env python3
"""
Lightweight smoke test for the packaged Gamebot Lite SQLite snapshot.

Checks that the bundled database exists, can be opened, contains each
friendly table defined in the catalog metadata (with rows), and matches the
checksum recorded in manifest.json. Exits with a non-zero status if any
validation fails so it can be chained in CI or release scripts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gamebot_lite import DEFAULT_SQLITE_PATH  # noqa: E402
from gamebot_lite.catalog import (  # noqa: E402
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


# Tables allowed to ship empty. The loader does not populate dataset_versions
# in the current snapshot.
ALLOWED_EMPTY_TABLES = {"dataset_versions"}


def empty_tables(conn: sqlite3.Connection, tables: set[str]) -> set[str]:
    return {
        table
        for table in tables
        if conn.execute(f'SELECT 1 FROM "{table}" LIMIT 1').fetchone() is None
    }


def manifest_mismatch(path: Path) -> str | None:
    """Compare the file against manifest.json's sqlite_sha256, if one sits beside it."""

    manifest_path = path.parent / "manifest.json"
    if not manifest_path.exists():
        print(f"[WARN] No manifest.json next to {path}; skipping checksum check.")
        return None
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("sqlite_filename", path.name) != path.name:
        print(f"[WARN] {manifest_path} describes a different file; skipping checksum.")
        return None
    expected = manifest.get("sqlite_sha256")
    if not expected:
        print(f"[WARN] {manifest_path} has no sqlite_sha256; skipping checksum check.")
        return None
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual != expected:
        return f"sha256 {actual} does not match {manifest_path} ({expected})"
    return None


def main() -> int:
    args = parse_args()
    try:
        with closing(connect(args.sqlite_path)) as conn:
            present_tables = fetch_tables(conn)
            empty = empty_tables(conn, expected_tables() & present_tables)
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

    empty -= ALLOWED_EMPTY_TABLES
    if empty:
        sys.stderr.write(
            "[FAIL] Empty tables: "
            + ", ".join(sorted(empty))
            + f" (path: {args.sqlite_path})\n"
        )
        return 3

    mismatch = manifest_mismatch(args.sqlite_path)
    if mismatch:
        sys.stderr.write(f"[FAIL] {mismatch}\n")
        return 4

    print(f"[PASS] Gamebot Lite smoke test succeeded ({args.sqlite_path})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
