#!/usr/bin/env python3

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime


def run(cmd):
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return result.stdout.strip()


def main():
    parser = argparse.ArgumentParser(description="Tag Gamebot releases.")
    parser.add_argument("type", choices=["data", "code"], help="Release type")
    parser.add_argument("--version", help="Version string for code releases (vX.Y.Z)")
    parser.add_argument(
        "--date", help="Date for data releases (YYYYMMDD). Defaults to today (UTC)."
    )
    parser.add_argument(
        "--no-push", action="store_true", help="Create tag locally without pushing"
    )
    args = parser.parse_args()

    if args.type == "code":
        if not args.version:
            print("error: --version is required for code releases", file=sys.stderr)
            sys.exit(1)
        tag_name = f"code-{args.version}"
    else:
        tag_date = args.date or datetime.utcnow().strftime("%Y%m%d")
        tag_name = f"data-{tag_date}"

    print(f"Creating tag {tag_name}...")
    run(["git", "tag", "-a", tag_name, "-m", f"Gamebot {args.type} release {tag_name}"])

    if args.no_push:
        print("Tag created locally. Push it with: git push origin", tag_name)
    else:
        print(f"Pushing tag {tag_name} to origin...")
        run(["git", "push", "origin", tag_name])
    print("Done.")


if __name__ == "__main__":
    main()
