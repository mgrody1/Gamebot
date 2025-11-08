#!/usr/bin/env python3
"""Generate human-friendly release notes from a data export manifest.

Usage:
  python scripts/generate_release_notes.py \
      --manifest gamebot_lite/data/manifest.json \
      [--upstream-report monitoring/upstream_report.md] \
      [--out release_note.md]

The script emits a short, templated release note describing the export: ingestion
run id, changed tables, top-level metadata and an optional summary from the
upstream report. It's intentionally conservative and avoids heavy computation.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional


def render(manifest: dict, upstream_md: Optional[str] = None) -> str:
    lines = []
    exported_at = manifest.get("exported_at") or manifest.get("timestamp")
    layer = manifest.get("layer")
    sqlite = manifest.get("sqlite_filename")
    sha = manifest.get("sqlite_sha256")
    exporter = manifest.get("exporter_git_sha")

    lines.append(f"Data release: {exported_at}")
    if layer:
        lines.append(f"Layer: {layer}")
    if sqlite:
        lines.append(f"Snapshot: `{sqlite}` (sha256: {sha[:8] if sha else 'unknown'})")

    ingestion = manifest.get("ingestion")
    if ingestion:
        run_id = ingestion.get("run_id") or ingestion.get("ingest_run_id")
        started = ingestion.get("run_started_at") or ingestion.get("started_at")
        if run_id:
            lines.append(f"Ingestion run: {run_id}")
        if started:
            lines.append(f"Ingestion started: {started}")

    tables = manifest.get("exported_tables") or []
    if tables:
        # show up to 10 tables; if many, summarize
        if len(tables) > 10:
            lines.append(
                f"Changed/Exported tables (sample): {', '.join(tables[:10])} (+{len(tables) - 10} more)"
            )
        else:
            lines.append(f"Changed/Exported tables: {', '.join(tables)}")

    if exporter:
        lines.append(f"Exporter git sha: {exporter}")

    # optional upstream summary
    if upstream_md:
        # take the first 10 non-empty lines as a short summary
        snippet = []
        for ln in upstream_md.splitlines():
            ln = ln.strip()
            if ln:
                snippet.append(ln)
            if len(snippet) >= 10:
                break
        if snippet:
            lines.append("")
            lines.append("Upstream summary:")
            lines.extend([f"> {s}" for s in snippet])

    lines.append("")
    lines.append("Notes:")
    lines.append(
        "- This note is generated from the export manifest. For details, inspect gamebot_lite/data/manifest.json and monitoring/upstream_report.md."
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", default="gamebot_lite/data/manifest.json")
    parser.add_argument("--upstream-report", default=None)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    manifest_path = Path(args.manifest)
    if not manifest_path.exists():
        print(f"Manifest not found: {manifest_path}")
        return 2

    manifest = json.loads(manifest_path.read_text())
    upstream_md = None
    if args.upstream_report:
        upath = Path(args.upstream_report)
        if upath.exists():
            upstream_md = upath.read_text()

    note = render(manifest, upstream_md)
    if args.out:
        Path(args.out).write_text(note)
        print(f"Wrote release note to {args.out}")
    else:
        print(note)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
