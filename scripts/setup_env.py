#!/usr/bin/env python3
"""
Create or switch the active dotenv files (root `.env` and `airflow/.env`).

Usage:
    python scripts/setup_env.py dev
    python scripts/setup_env.py prod --from-template

By default the script looks for `env/.env.<env>` as the source of truth. Passing
`--from-template` copies from `env/.env.<env>.example` instead. If the root
`.env` does not exist yet, it is created automatically with sane defaults.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_DIR = REPO_ROOT / "env"
DEST = REPO_ROOT / ".env"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or switch the active .env files.")
    parser.add_argument(
        "environment",
        choices=["dev", "prod"],
        help="Environment to activate.",
    )
    parser.add_argument(
        "--from-template",
        action="store_true",
        help="Copy from the example template if the concrete file should be ignored.",
    )
    return parser.parse_args()


def _parse_env(path: Path) -> dict[str, str]:
    """Load simple KEY=VALUE pairs from a dotenv file."""
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def resolve_source(environment: str, from_template: bool) -> Path:
    candidate = ENV_DIR / f".env.{environment}"
    if candidate.exists() and not from_template:
        return candidate

    template = ENV_DIR / f".env.{environment}.example"
    if template.exists():
        return template

    raise FileNotFoundError(
        f"Neither {candidate} nor {template} exists. Create one before switching."
    )


def _collect_keys_from_file(path: Path) -> list[str]:
    if not path.exists():
        return []
    keys: list[str] = []
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key = stripped.split("=", 1)[0].strip()
        if key not in keys:
            keys.append(key)
    return keys


def _write_env_file(path: Path, ordered_keys: list[str], values: dict[str, str]) -> None:
    lines: list[str] = []
    written = set()
    for key in ordered_keys:
        if key in values:
            lines.append(f"{key}={values[key]}")
            written.add(key)
    for key, value in values.items():
        if key not in written:
            lines.append(f"{key}={value}")
    path.write_text("\n".join(lines) + "\n")


def main() -> None:
    args = parse_args()
    profile_path = ENV_DIR / f".env.{args.environment}"
    template_path = ENV_DIR / f".env.{args.environment}.example"

    if args.from_template:
        if not template_path.exists():
            raise FileNotFoundError(f"Template {template_path.relative_to(REPO_ROOT)} does not exist.")
        shutil.copyfile(template_path, profile_path)
        print(f"Refreshed {profile_path.relative_to(REPO_ROOT)} from template")
    elif not profile_path.exists():
        if template_path.exists():
            shutil.copyfile(template_path, profile_path)
            print(f"Created {profile_path.relative_to(REPO_ROOT)} from template")
        else:
            profile_path.write_text("")
            print(f"Created empty {profile_path.relative_to(REPO_ROOT)}")

    current_env_vars = _parse_env(DEST)
    profile_env_vars = _parse_env(profile_path)
    template_env_vars = _parse_env(template_path)

    merged_profile: dict[str, str] = dict(template_env_vars)
    merged_profile.update(profile_env_vars)
    merged_profile["SURVIVOR_ENV"] = args.environment

    ordered_keys: list[str] = []
    for path in (profile_path, DEST, template_path):
        for key in _collect_keys_from_file(path):
            if key not in ordered_keys:
                ordered_keys.append(key)

    final_env: dict[str, str] = dict(merged_profile)
    for key, value in current_env_vars.items():
        if key not in final_env:
            final_env[key] = value

    final_env.setdefault("AIRFLOW__API_RATELIMIT__STORAGE", "redis://redis:6379/1")
    final_env.setdefault("AIRFLOW__API_RATELIMIT__ENABLED", "True")

    if "AIRFLOW__API_RATELIMIT__STORAGE" not in ordered_keys:
        ordered_keys.append("AIRFLOW__API_RATELIMIT__STORAGE")
    if "AIRFLOW__API_RATELIMIT__ENABLED" not in ordered_keys:
        ordered_keys.append("AIRFLOW__API_RATELIMIT__ENABLED")

    _write_env_file(DEST, ordered_keys, final_env)
    print(f"Wrote {DEST.relative_to(REPO_ROOT)}")

    profile_keys = _collect_keys_from_file(profile_path)
    template_keys = _collect_keys_from_file(template_path)
    for key in template_keys:
        if key not in profile_keys:
            profile_keys.append(key)
    if "SURVIVOR_ENV" not in profile_keys:
        profile_keys.insert(0, "SURVIVOR_ENV")
    _write_env_file(profile_path, profile_keys, merged_profile)
    print(f"Updated {profile_path.relative_to(REPO_ROOT)} with environment defaults")

    airflow_env = REPO_ROOT / "airflow" / ".env"
    airflow_env.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(DEST, airflow_env)
    print(f"Synced {DEST.name} -> airflow/.env")

    try:
        subprocess.run(
            [sys.executable, str(REPO_ROOT / "scripts" / "build_airflow_conn.py"), "--write-airflow"],
            check=True,
        )
        print("Updated Airflow connection configuration from .env")
    except FileNotFoundError:
        print("Warning: scripts/build_airflow_conn.py not found; skipping Airflow connection update.")
    except subprocess.CalledProcessError as exc:
        print(f"Warning: Failed to update Airflow connection (exit code {exc.returncode}).")


if __name__ == "__main__":
    main()
