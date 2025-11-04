#!/usr/bin/env python3
"""Utility to inspect the most recent Gamebot run logs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Optional

base_dir = Path(__file__).resolve().parents[1]
if str(base_dir) not in sys.path:
    sys.path.append(str(base_dir))

from gamebot_core.log_utils import get_run_log_dir  # noqa: E402


def iter_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*"):
        if path.is_file():
            yield path


def find_latest_file(root: Path, pattern: Optional[str]) -> Optional[Path]:
    files = []
    for path in iter_files(root):
        if pattern and pattern not in path.name:
            continue
        files.append(path)
    if not files:
        return None
    return max(files, key=lambda p: p.stat().st_mtime)


def tail_file(path: Path, lines: int) -> str:
    try:
        content = path.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError:
        return "<binary file - cannot display>"
    if lines <= 0:
        lines = len(content)
    return "\n".join(content[-lines:])


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Show the newest artefact in run_logs (validation, notifications, etc.)."
    )
    parser.add_argument(
        "category",
        nargs="?",
        choices=["all", "validation", "notifications", "legacy"],
        default=None,
        help="Limit search to a subdirectory under run_logs.",
    )
    parser.add_argument(
        "--category",
        dest="category_flag",
        choices=["all", "validation", "notifications", "legacy"],
        help="Optional flag form of the category filter.",
    )
    parser.add_argument(
        "--pattern",
        help="Filter by filename substring (e.g., dataset name).",
    )
    parser.add_argument(
        "--tail",
        action="store_true",
        help="Print the last N lines of the file instead of just the path.",
    )
    parser.add_argument(
        "--lines",
        type=int,
        default=40,
        help="Number of lines to show when using --tail (default: 40).",
    )
    args = parser.parse_args()

    category = args.category_flag or args.category or "all"

    base = get_run_log_dir()
    search_root = base if category == "all" else base / category
    if not search_root.exists():
        print(f"No logs found: {search_root}")
        sys.exit(1)

    latest = find_latest_file(search_root, args.pattern)
    if latest is None:
        hint = f" (pattern '{args.pattern}')" if args.pattern else ""
        print(f"No log files found under {search_root}{hint}.")
        sys.exit(1)

    print(f"Latest log: {latest}")
    if args.tail:
        print("\n" + tail_file(latest, args.lines))


if __name__ == "__main__":
    main()
