#!/usr/bin/env python3
"""
Write one bronze.dataset_versions row per survivoR dataset for the latest bronze load.

The Airflow DAG records these rows in its persist_dataset_metadata task. The lite path
(scripts/run_lite.py) recreates the bronze schema without that task, so it runs this script
after the load. With GITHUB_TOKEN set, each row also carries the upstream commit.

Usage:
    python scripts/record_dataset_versions.py
    GITHUB_TOKEN=$(gh auth token) python scripts/record_dataset_versions.py
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

import params  # noqa: E402
from gamebot_core.data_freshness import (  # noqa: E402
    detect_dataset_changes,
    persist_metadata,
    upsert_dataset_metadata,
)
from gamebot_core.db_utils import connect_to_db  # noqa: E402


def latest_run_id() -> str:
    conn = connect_to_db()
    if not conn:
        raise SystemExit("cannot connect to the database")
    try:
        with conn.cursor() as cur:
            cur.execute(
                "select run_id from bronze.ingestion_runs where status = 'succeeded' "
                "order by run_started_at desc limit 1"
            )
            row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        raise SystemExit("no succeeded bronze load in bronze.ingestion_runs; run the bronze step first")
    return str(row[0])


def main() -> int:
    names = [d["dataset"] for d in params.dataset_order]
    current, _ = detect_dataset_changes(names, params.base_raw_url, params.json_raw_url)
    run_id = latest_run_id()
    upsert_dataset_metadata(current, run_id)
    persist_metadata(current)
    with_commit = sum(1 for m in current.values() if m.get("commit_sha"))
    print(f"dataset_versions: {len(current)} rows for run {run_id}; {with_commit} with an upstream commit")
    return 0 if len(current) == len(names) else 1


if __name__ == "__main__":
    sys.exit(main())
