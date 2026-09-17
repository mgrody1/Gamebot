#!/usr/bin/env python3
"""
Pre-commit helper that keeps version pins consistent.

Checks:
    * `.python-version` matches the Python base image in Dockerfile.
    * The Airflow pin in the pyproject `airflow` group matches airflow/Dockerfile.
"""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _search(path: Path, pattern: str) -> str:
    match = re.search(pattern, path.read_text(encoding="utf-8"), re.MULTILINE)
    if not match:
        raise RuntimeError(f"Could not find {pattern!r} in {path.name}")
    return match.group(1)


def main() -> int:
    errors = []

    pinned_python = (REPO_ROOT / ".python-version").read_text().strip()
    docker_python = _search(REPO_ROOT / "Dockerfile", r"^FROM\s+python:(\d+\.\d+)")
    if not pinned_python.startswith(docker_python):
        errors.append(
            f".python-version is {pinned_python} but Dockerfile uses {docker_python}"
        )

    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
    airflow_group = " ".join(pyproject["dependency-groups"]["airflow"])
    group_airflow = re.search(r"apache-airflow[^=]*==([\d.]+)", airflow_group)
    image_airflow = _search(
        REPO_ROOT / "airflow" / "Dockerfile", r"^FROM\s+apache/airflow:([\d.]+)"
    )
    if not group_airflow or group_airflow.group(1) != image_airflow:
        errors.append(
            f"pyproject airflow group does not pin apache-airflow=={image_airflow}"
        )

    for error in errors:
        print(f"ERROR: {error}", file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
