#!/usr/bin/env python3
"""Check the upstream survivoR repository for new dataset commits.

This script compares the latest commit touching the survivoR `.rda` (under
`data/`) and JSON exports (under `dev/json/`) against the snapshot recorded in
`monitoring/survivor_upstream_snapshot.json`. Use it in two modes:

1. Monitoring (default) – exits with a non-zero status if upstream data changed.
2. Update snapshot (`--update`) – refreshes the recorded commits after you ingest
   new data so future checks pass cleanly.

It only relies on the Python standard library so it can run inside GitHub
Actions without extra dependencies.
"""

from __future__ import annotations

import argparse
import json
import sys
import textwrap
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

SNAPSHOT_PATH = Path("monitoring") / "survivor_upstream_snapshot.json"
DEFAULT_REPORT_PATH = Path("monitoring") / "upstream_report.md"

GITHUB_API = "https://api.github.com"
COMMITS_ENDPOINT = f"{GITHUB_API}/repos/doehm/survivoR/commits"

MONITORED_TARGETS: Mapping[str, Mapping[str, str]] = {
    "rda_data": {
        "path": "data",
        "description": ".rda dataset directory",
    },
    "json_data": {
        "path": "dev/json",
        "description": "JSON exports directory",
    },
}


@dataclass
class CommitInfo:
    target_id: str
    path: str
    sha: str
    committed_at: str
    url: str
    description: str

    @classmethod
    def from_api(
        cls,
        target_id: str,
        description: str,
        path: str,
        payload: Mapping[str, object],
    ) -> "CommitInfo":
        commit = payload["commit"]
        sha = payload["sha"]
        committed_at = commit["committer"]["date"]
        html_url = payload["html_url"]
        return cls(
            target_id=target_id,
            path=path,
            sha=sha,
            committed_at=committed_at,
            url=html_url,
            description=description,
        )

    def to_dict(self) -> Dict[str, str]:
        return {
            "target_id": self.target_id,
            "path": self.path,
            "sha": self.sha,
            "committed_at": self.committed_at,
            "url": self.url,
            "description": self.description,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--token",
        help="GitHub token for higher rate limits (defaults to env GITHUB_TOKEN).",
    )
    parser.add_argument(
        "--snapshot",
        type=Path,
        default=SNAPSHOT_PATH,
        help="Path to the snapshot JSON file to read/write.",
    )
    parser.add_argument(
        "--report-md",
        type=Path,
        default=DEFAULT_REPORT_PATH,
        help="Path to write a Markdown status report.",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Update the snapshot file with the latest upstream commits.",
    )
    return parser.parse_args()


def api_request(url: str, token: Optional[str]) -> Mapping[str, object]:
    request = Request(url)
    request.add_header("Accept", "application/vnd.github+json")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urlopen(request) as response:
            return json.load(response)
    except HTTPError as exc:  # pragma: no cover - network error handling
        raise RuntimeError(
            f"GitHub API error ({exc.code}): {exc.reason}. URL={url}"
        ) from exc
    except URLError as exc:  # pragma: no cover - network error handling
        raise RuntimeError(f"Network error contacting GitHub: {exc.reason}") from exc


def fetch_latest_commit(target_id: str, token: Optional[str]) -> CommitInfo:
    target = MONITORED_TARGETS[target_id]
    params = f"path={target['path']}&per_page=1"
    url = f"{COMMITS_ENDPOINT}?{params}"
    payloads = api_request(url, token)
    if not payloads:
        raise RuntimeError(f"No commits found for path {target['path']}")
    payload = payloads[0]
    return CommitInfo.from_api(
        target_id, target["description"], target["path"], payload
    )


def fetch_all_commits(token: Optional[str]) -> Dict[str, CommitInfo]:
    commits: Dict[str, CommitInfo] = {}
    for target_id in MONITORED_TARGETS:
        commits[target_id] = fetch_latest_commit(target_id, token)
    return commits


def load_snapshot(path: Path) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def save_snapshot(path: Path, commits: Mapping[str, CommitInfo]) -> None:
    payload: MutableMapping[str, Dict[str, str]] = {}
    for target_id, info in commits.items():
        payload[target_id] = info.to_dict()
        payload[target_id]["checked_at"] = datetime.now(timezone.utc).isoformat()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def render_markdown_report(
    snapshot: Mapping[str, Dict[str, str]],
    latest: Mapping[str, CommitInfo],
    mismatches: Iterable[str],
) -> str:
    lines: List[str] = []
    lines.append("# survivoR upstream status\n")
    if not snapshot:
        lines.append(
            "Snapshot file missing or empty. Run "
            "`python scripts/check_survivor_updates.py --update` after verifying "
            "the upstream data."
        )
        return "\n".join(lines)

    if not mismatches:
        checked = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M %Z")
        lines.append(f"*Last checked:* {checked}\n")
        lines.append("No new survivoR dataset commits detected.\n")
    else:
        lines.append("⚠️ **New survivoR dataset commits detected.**\n")
        lines.append(
            "Review the upstream repository, refresh bronze, and then rerun "
            "`python scripts/check_survivor_updates.py --update`."
        )
        lines.append("")
        for target_id in mismatches:
            latest_info = latest[target_id]
            previous = snapshot.get(target_id)
            previous_sha = previous.get("sha") if previous else None
            lines.append(f"## {latest_info.description}")
            lines.append(f"- Path: `{latest_info.path}`")
            lines.append(f"- Latest commit: [{latest_info.sha[:7]}]({latest_info.url})")
            lines.append(f"- Commit date: {latest_info.committed_at}")
            if previous_sha:
                lines.append(f"- Snapshot commit: `{previous_sha[:7]}`")
            else:
                lines.append("- Snapshot commit: _none recorded_")
            lines.append("")
    lines.append("\n---\n")
    lines.append(
        "This report is generated by `scripts/check_survivor_updates.py`. "
        "Update the snapshot after ingesting new data so nightly checks return to green."
    )
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    token = args.token or None

    try:
        latest_commits = fetch_all_commits(token)
    except RuntimeError as exc:
        sys.stderr.write(f"[FAIL] {exc}\n")
        return 1

    if args.update:
        save_snapshot(args.snapshot, latest_commits)
        snapshot = load_snapshot(args.snapshot)
        report = render_markdown_report(snapshot, latest_commits, mismatches=[])
        args.report_md.parent.mkdir(parents=True, exist_ok=True)
        args.report_md.write_text(report)
        print(f"[OK] Snapshot updated at {args.snapshot}")
        return 0

    snapshot = load_snapshot(args.snapshot)
    mismatches: List[str] = []
    for target_id, commit_info in latest_commits.items():
        recorded = snapshot.get(target_id)
        if not recorded or recorded.get("sha") != commit_info.sha:
            mismatches.append(target_id)

    report = render_markdown_report(snapshot, latest_commits, mismatches)
    args.report_md.parent.mkdir(parents=True, exist_ok=True)
    args.report_md.write_text(report)

    if mismatches:
        sys.stderr.write(
            "[FAIL] Detected updated survivoR datasets:\n"
            + textwrap.indent(
                "\n".join(
                    f"- {MONITORED_TARGETS[mid]['description']} (path: {MONITORED_TARGETS[mid]['path']})"
                    for mid in mismatches
                ),
                prefix="  ",
            )
            + "\n"
            "Refresh bronze data, then rerun with --update after ingestion.\n"
        )
        return 2

    print("[OK] survivoR dataset snapshot matches recorded commits.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
