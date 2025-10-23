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


def _update_env_file(path: Path, updates: dict[str, str]) -> None:
    """Upsert key/value pairs into a dotenv file while preserving order."""
    lines = path.read_text().splitlines()
    updated_keys = set()
    new_lines: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            new_lines.append(line)
            continue
        key, _ = stripped.split("=", 1)
        key = key.strip()
        if key in updates:
            new_lines.append(f"{key}={updates[key]}")
            updated_keys.add(key)
        else:
            new_lines.append(line)

    for key, value in updates.items():
        if key not in updated_keys:
            new_lines.append(f"{key}={value}")

    path.write_text("\n".join(new_lines) + "\n")


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


def main() -> None:
    args = parse_args()
    current_env_vars = _parse_env(DEST)
    source = resolve_source(args.environment, args.from_template)
    template = ENV_DIR / f".env.{args.environment}.example"
    source_env_vars = _parse_env(source)
    template_env_vars = _parse_env(template)

    ordered_keys: list[str] = []
    for path in (DEST, source, template):
        for key in _collect_keys_from_file(path):
            if key not in ordered_keys:
                ordered_keys.append(key)

    env_specific_keys = {"SURVIVOR_ENV", "DB_NAME", "DB_HOST"}
    final_env: dict[str, str] = {}

    for key in ordered_keys:
        value = None
        if key == "SURVIVOR_ENV":
            value = args.environment
        elif key in env_specific_keys:
            value = source_env_vars.get(key) or template_env_vars.get(key) or current_env_vars.get(key)
        else:
            value = current_env_vars.get(key)
            if value is None:
                value = source_env_vars.get(key) or template_env_vars.get(key)

        if value is not None:
            final_env[key] = value

    final_env.setdefault("AIRFLOW__API_RATELIMIT__STORAGE", "redis://redis:6379/1")
    final_env.setdefault("AIRFLOW__API_RATELIMIT__ENABLED", "True")

    for required in ("AIRFLOW__API_RATELIMIT__STORAGE", "AIRFLOW__API_RATELIMIT__ENABLED"):
        if required not in ordered_keys:
            ordered_keys.append(required)

    if source.exists():
        shutil.copyfile(source, DEST)
        print(f"Copied {source.relative_to(REPO_ROOT)} -> {DEST.name}")
    else:
        DEST.write_text("")
        print(f"Created {DEST.name} from defaults")

    _update_env_file(
        DEST,
        {key: final_env[key] for key in ordered_keys if key in final_env},
    )

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
