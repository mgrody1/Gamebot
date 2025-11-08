#!/usr/bin/env python3
"""Trigger data release from an environment with access to the warehouse.

This helper is intended to be called from your Airflow DAG at the end of a successful
ETL run when `SURVIVOR_ENV=="prod"` and `IS_DEPLOYED=="true"`.

Behavior:
- Runs the exporter to produce a sqlite snapshot and copies it into gamebot_lite/data/
- Runs the smoke test
- Creates a new git branch `data-release/<date>-<shortsha>`, commits the snapshot
  and pushes the branch
- Opens a pull request against `main` and triggers the `data-release` Actions workflow
  via the repository dispatch API with the PR number and branch in the payload.

Notes:
- This script requires a GitHub token with repo permissions available via the
  environment variable `AIRFLOW_GITHUB_TOKEN` (or passed via --token).
- It should be run on a host that has git configured and can reach the repo and
  the production Postgres database.

"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import requests

REPO = "mgrody1/Gamebot"


def run(cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    print(f"> {cmd}")
    return subprocess.run(cmd, shell=True, check=check, text=True)


def git(cmd: str) -> subprocess.CompletedProcess:
    return run(f"git {cmd}")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--token", help="GitHub token (env AIRFLOW_GITHUB_TOKEN used by default)"
    )
    parser.add_argument("--branch-prefix", default="data-release", help="Branch prefix")
    parser.add_argument("--target-branch", default="main", help="Target branch for PR")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    token = args.token or os.environ.get("AIRFLOW_GITHUB_TOKEN")
    if not token:
        print(
            "ERROR: GitHub token required via --token or AIRFLOW_GITHUB_TOKEN env var",
            file=sys.stderr,
        )
        return 2

    # Run exporter and package
    try:
        run("pipenv run python scripts/export_sqlite.py --layer silver --package")
    except subprocess.CalledProcessError:
        print("Export failed", file=sys.stderr)
        return 3

    # Smoke test
    try:
        run("python scripts/smoke_gamebot_lite.py")
    except subprocess.CalledProcessError:
        print("Smoke test failed", file=sys.stderr)
        return 4

    # Compare manifest with origin/main to decide whether to create a release
    manifest_path = Path("gamebot_lite") / "data" / "manifest.json"
    if not manifest_path.exists():
        print(
            f"Manifest not found at {manifest_path}; aborting release.", file=sys.stderr
        )
        return 5

    try:
        run("git fetch origin main:refs/remotes/origin/main")
    except subprocess.CalledProcessError:
        # fetching may fail in some CI environments; proceed conservatively
        pass

    remote_manifest = None
    try:
        # Attempt to read the manifest from origin/main
        res = subprocess.run(
            ["git", "show", "origin/main:gamebot_lite/data/manifest.json"],
            capture_output=True,
            text=True,
        )
        if res.returncode == 0 and res.stdout:
            remote_manifest = json.loads(res.stdout)
    except Exception:
        remote_manifest = None

    local_manifest = json.loads(manifest_path.read_text())
    if remote_manifest is not None and remote_manifest == local_manifest:
        print("No manifest change vs origin/main. Skipping release.")
        return 0

    # Ensure git repo is clean and on target
    run("git fetch origin")
    # Create branch
    shortsha = (
        subprocess.check_output(["git", "rev-parse", "--short", "HEAD"])
        .decode()
        .strip()
    )
    today = datetime.utcnow().strftime("%Y%m%d")
    branch = f"{args.branch_prefix}/{today}-{shortsha}"

    try:
        git(f"checkout -b {shlex.quote(branch)}")
    except subprocess.CalledProcessError:
        # Branch may already exist locally
        git(f"checkout {shlex.quote(branch)}")

    # Stage the packaged sqlite and any metadata
    git("add gamebot_lite/data || true")
    try:
        git('commit -m "chore(data): packaged sqlite snapshot"')
    except subprocess.CalledProcessError:
        print("No changes to commit")

    # Push branch
    try:
        git(f"push origin {shlex.quote(branch)}")
    except subprocess.CalledProcessError as exc:
        print(f"Failed to push branch: {exc}", file=sys.stderr)
        return 5

    # Create PR via GitHub API
    url = f"https://api.github.com/repos/{REPO}/pulls"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
    }
    body = {
        "title": f"data-release: packaged sqlite snapshot {today}",
        "head": branch,
        "base": args.target_branch,
        "body": "Automated data release from Airflow: packaged sqlite snapshot.",
    }
    resp = requests.post(url, headers=headers, json=body)
    if resp.status_code not in (200, 201):
        print("Failed to create PR:", resp.status_code, resp.text, file=sys.stderr)
        return 6
    pr = resp.json()
    pr_number = pr.get("number")
    print(f"Created PR #{pr_number}")

    # Trigger repository dispatch for Actions workflow
    dispatch_url = f"https://api.github.com/repos/{REPO}/dispatches"
    payload = {
        "event_type": "data-release",
        "client_payload": {"pr_number": pr_number, "branch": branch},
    }
    resp2 = requests.post(dispatch_url, headers=headers, json=payload)
    if resp2.status_code not in (204,):
        print(
            "Failed to dispatch event:", resp2.status_code, resp2.text, file=sys.stderr
        )
        return 7
    print("Dispatched data-release event to GitHub Actions")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
