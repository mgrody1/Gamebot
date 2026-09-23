#!/usr/bin/env python3
"""
Run the Gamebot pipeline without Airflow.

Needs one reachable Postgres and the DB_* values in .env. Steps run in order
and the script stops at the first failure.

Usage:
    python scripts/run_lite.py                 # bronze, dbt, export, check
    python scripts/run_lite.py --steps dbt export
    python scripts/run_lite.py --force-refresh # re-download survivoR files

Steps:
    bronze  Load survivoR into the bronze schema (drops and recreates all schemas), then
            record one bronze.dataset_versions row per dataset (scripts/record_dataset_versions.py).
    dbt     Build silver and gold and run the dbt tests.
    export  Write gamebot_lite/data/gamebot.sqlite and manifest.json.
    check   Smoke-test the packaged SQLite and run pytest.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
STEPS = ["bronze", "dbt", "export", "check"]


def _tool(name: str) -> str:
    """Prefer the executable that sits beside the running interpreter."""
    sibling = Path(sys.executable).parent / name
    return str(sibling) if sibling.exists() else name


def _run(cmd: list[str]) -> None:
    print(f"\n$ {' '.join(cmd)}", flush=True)
    env = {**os.environ, "PYTHONPATH": str(REPO_ROOT)}
    subprocess.run(cmd, cwd=REPO_ROOT, env=env, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the Gamebot pipeline without Airflow."
    )
    parser.add_argument("--steps", nargs="+", choices=STEPS, default=STEPS)
    parser.add_argument("--force-refresh", action="store_true")
    args = parser.parse_args()

    load_dotenv(REPO_ROOT / ".env")
    missing = [
        k
        for k in ("DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_PORT")
        if not os.getenv(k)
    ]
    if missing:
        print(f"Missing {missing}. Copy .env.example to .env first.")
        return 2

    py = sys.executable

    if "bronze" in args.steps:
        if args.force_refresh:
            shutil.rmtree(REPO_ROOT / "data_cache", ignore_errors=True)
        _run([py, "-m", "Database.load_survivor_data"])
        _run([py, "scripts/record_dataset_versions.py"])

    if "dbt" in args.steps:
        _run([_tool("dbt"), "build", "--project-dir", "dbt", "--profiles-dir", "dbt"])

    if "export" in args.steps:
        with tempfile.TemporaryDirectory() as tmp:
            out = str(Path(tmp) / "gamebot.sqlite")
            _run(
                [
                    py,
                    "scripts/export_sqlite.py",
                    "--layer",
                    "gold",
                    "--package",
                    "--output",
                    out,
                ]
            )

    if "check" in args.steps:
        _run([py, "scripts/smoke_gamebot_lite.py"])
        _run([py, "-m", "pytest", "-q", "tests"])

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except subprocess.CalledProcessError as exc:
        sys.exit(exc.returncode)
