#!/usr/bin/env python3
"""
Utility script to derive the Airflow Postgres connection string from the repo `.env`.

Usage:
    python scripts/build_airflow_conn.py [--env-file .env] [--write-airflow]

The script logs when a connection URL is derived. Passing `--write-airflow` also
updates/creates `airflow/.env` with the computed value.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Dict
from urllib.parse import quote_plus

from dotenv import dotenv_values

logger = logging.getLogger(__name__)


def load_env(env_file: Path) -> Dict[str, str]:
    """Load key/value pairs from a dotenv file."""
    if not env_file.exists():
        raise FileNotFoundError(f"Could not find environment file at {env_file}")
    return {k: v for k, v in dotenv_values(env_file).items() if v is not None}


def build_connection_url(values: Dict[str, str]) -> str:
    """Build a SQLAlchemy-compatible Postgres connection string."""
    required_keys = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD", "PORT"]
    missing = [key for key in required_keys if not values.get(key)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    user = quote_plus(values["DB_USER"])
    password = quote_plus(values["DB_PASSWORD"])
    host = values["DB_HOST"]
    port = values["PORT"]
    database = values["DB_NAME"]

    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


def update_airflow_env(
    connection_url: str,
    airflow_env_path: Path,
    source_env: Dict[str, str],
) -> None:
    """Persist the connection URL and DB settings into airflow/.env."""
    airflow_env_path.parent.mkdir(parents=True, exist_ok=True)

    current_values = {}
    if airflow_env_path.exists():
        current_values = {
            k: v for k, v in dotenv_values(airflow_env_path).items() if v is not None
        }

    current_values["AIRFLOW_CONN_SURVIVOR_POSTGRES"] = connection_url

    for key in ("DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD", "PORT", "SURVIVOR_ENV"):
        value = source_env.get(key)
        if value:
            current_values[key] = value

    with airflow_env_path.open("w", encoding="utf-8") as fh:
        for key, value in current_values.items():
            fh.write(f"{key}={value}\n")


def parse_args() -> argparse.Namespace:
    """Configure CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Generate the AIRFLOW_CONN_SURVIVOR_POSTGRES value from .env."
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        type=Path,
        help="Path to the source .env file (default: %(default)s)",
    )
    parser.add_argument(
        "--write-airflow",
        action="store_true",
        help="Persist the derived connection string into airflow/.env",
    )
    return parser.parse_args()


def main() -> None:
    """Entrypoint for the CLI utility."""
    args = parse_args()
    env_values = load_env(args.env_file)
    connection_url = build_connection_url(env_values)

    logger.info(
        "Derived AIRFLOW_CONN_SURVIVOR_POSTGRES for host '%s' and database '%s'",
        env_values["DB_HOST"],
        env_values["DB_NAME"],
    )

    if args.write_airflow:
        airflow_env_path = Path("airflow") / ".env"
        update_airflow_env(connection_url, airflow_env_path, env_values)
        logger.info("Updated %s with connection settings", airflow_env_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
