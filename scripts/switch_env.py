#!/usr/bin/env python3
"""
Copy an environment-specific dotenv file into the project root.

Usage:
    python scripts/switch_env.py dev
    python scripts/switch_env.py prod --from-example

By default the script looks for `env/.env.<env>`; passing `--from-example`
copies from `env/.env.<env>.example` instead.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_DIR = REPO_ROOT / "env"
DEST = REPO_ROOT / ".env"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Switch the active .env file.")
    parser.add_argument(
        "environment",
        choices=["dev", "prod"],
        help="Environment to activate.",
    )
    parser.add_argument(
        "--from-example",
        action="store_true",
        help="Copy from the example file if a concrete file is not present.",
    )
    return parser.parse_args()


def resolve_source(environment: str, from_example: bool) -> Path:
    candidate = ENV_DIR / f".env.{environment}"
    if candidate.exists() and not from_example:
        return candidate

    example = ENV_DIR / f".env.{environment}.example"
    if example.exists():
        return example

    raise FileNotFoundError(
        f"Neither {candidate} nor {example} exists. Create one before switching."
    )


def main() -> None:
    args = parse_args()
    source = resolve_source(args.environment, args.from_example)
    shutil.copyfile(source, DEST)
    print(f"Copied {source.relative_to(REPO_ROOT)} -> {DEST.name}")


if __name__ == "__main__":
    main()
