#!/usr/bin/env python3
"""
Pre-commit helper to ensure key version pins stay consistent between Pipenv and Docker.

Checks:
    * Python version in Pipfile `[requires]` matches the Python base image in Dockerfile.
    * Apache Airflow version pinned in Pipfile matches the loader image expectation.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Tuple

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    print("Python 3.11+ is required to parse Pipfile via tomllib.", file=sys.stderr)
    sys.exit(1)


REPO_ROOT = Path(__file__).resolve().parents[1]
PIPFILE = REPO_ROOT / "Pipfile"
DOCKERFILE = REPO_ROOT / "Dockerfile"


def read_python_version_from_pipfile() -> str:
    """Return the required Python version declared in Pipfile."""
    data = tomllib.loads(PIPFILE.read_text(encoding="utf-8"))
    try:
        return str(data["requires"]["python_version"])
    except KeyError as exc:
        raise RuntimeError("Expected `[requires].python_version` in Pipfile") from exc


def read_airflow_version_from_pipfile() -> str:
    """Return the pinned Airflow version from Pipfile packages."""
    data = tomllib.loads(PIPFILE.read_text(encoding="utf-8"))
    packages = data.get("packages", {})
    airflow_entry = packages.get("apache-airflow")
    if airflow_entry is None:
        raise RuntimeError("Expected `apache-airflow` dependency in Pipfile.")

    if isinstance(airflow_entry, str):
        return airflow_entry

    version = airflow_entry.get("version")
    if not version:
        raise RuntimeError("Expected a `version` key for apache-airflow in Pipfile.")
    return version


def read_python_version_from_dockerfile() -> str:
    """Parse the Python base image tag from the Dockerfile."""
    dockerfile_text = DOCKERFILE.read_text(encoding="utf-8")
    match = re.search(r"^FROM\s+python:(\d+\.\d+)(?:[\w.-]*)", dockerfile_text, re.MULTILINE)
    if not match:
        raise RuntimeError("Could not parse Python base image version from Dockerfile.")
    return match.group(1)


def main() -> None:
    pipfile_python = read_python_version_from_pipfile()
    docker_python = read_python_version_from_dockerfile()

    if pipfile_python != docker_python:
        print(
            f"Python version mismatch: Pipfile specifies {pipfile_python} "
            f"but Dockerfile uses {docker_python}",
            file=sys.stderr,
        )
        sys.exit(1)

    airflow_version = read_airflow_version_from_pipfile()
    expected_airflow_version = "==2.9.1"
    if airflow_version != expected_airflow_version:
        print(
            f"Apache Airflow version mismatch: expected {expected_airflow_version} "
            f"in Pipfile but found {airflow_version}",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
