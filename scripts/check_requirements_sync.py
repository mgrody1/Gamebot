#!/usr/bin/env python3
"""
Keep airflow/requirements.txt in step with the pyproject `pipeline` group.

The Airflow image installs airflow/requirements.txt. Local runs install the
`pipeline` dependency group. Both must list the same requirements, except for
packages the Airflow base image already ships.

Usage:
    python scripts/check_requirements_sync.py           # Rewrite requirements.txt
    python scripts/check_requirements_sync.py --check   # Verify only
"""

from __future__ import annotations

import argparse
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
REQUIREMENTS = REPO_ROOT / "airflow" / "requirements.txt"

# Shipped by the apache/airflow base image.
IMAGE_PROVIDED = {"psycopg2-binary"}


def _name(requirement: str) -> str:
    for index, char in enumerate(requirement):
        if char in "<>=!~[; ":
            return requirement[:index].lower()
    return requirement.lower()


def expected_lines() -> list[str]:
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
    group = pyproject["dependency-groups"]["pipeline"]
    return [req for req in group if _name(req) not in IMAGE_PROVIDED]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--check", action="store_true", help="Verify without writing")
    args = parser.parse_args()

    expected = expected_lines()
    current = [
        line.strip()
        for line in REQUIREMENTS.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]
    if current == expected:
        return 0

    if args.check:
        print(
            "airflow/requirements.txt differs from the pyproject pipeline group. "
            "Run: python scripts/check_requirements_sync.py",
            file=sys.stderr,
        )
        return 1

    REQUIREMENTS.write_text("\n".join(expected) + "\n")
    print("Rewrote airflow/requirements.txt from the pyproject pipeline group.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
