"""Utility helpers for surfacing schema drift or new dataset events."""

from __future__ import annotations

import hashlib
import logging
import os
from typing import Iterable, Optional

import requests

from .log_utils import get_run_log_dir

logger = logging.getLogger(__name__)

NOTIFICATION_LOG_DIR = get_run_log_dir() / "notifications"
NOTIFICATION_LOG_DIR.mkdir(parents=True, exist_ok=True)

SCHEMA_DRIFT_LOG = NOTIFICATION_LOG_DIR / "schema_drift.log"
ISSUE_CACHE_PATH = NOTIFICATION_LOG_DIR / ".schema_notifications.cache"


def _mark_event_seen(event_key: str) -> bool:
    """Return True if the event key has already been recorded."""

    cache: set[str] = set()
    if ISSUE_CACHE_PATH.exists():
        try:
            cache = {
                line.strip()
                for line in ISSUE_CACHE_PATH.read_text(encoding="utf-8").splitlines()
                if line.strip()
            }
        except OSError as exc:  # pragma: no cover - filesystem issues
            logger.warning("Could not read schema notification cache: %s", exc)

    if event_key in cache:
        return True

    try:
        with ISSUE_CACHE_PATH.open("a", encoding="utf-8") as handle:
            handle.write(f"{event_key}\n")
    except OSError as exc:  # pragma: no cover
        logger.warning("Unable to update schema notification cache: %s", exc)
    return False


def _append_drift_log(message: str) -> None:
    try:
        with SCHEMA_DRIFT_LOG.open("a", encoding="utf-8") as handle:
            handle.write(f"{message}\n")
    except OSError as exc:  # pragma: no cover
        logger.warning("Unable to write schema drift log: %s", exc)


def _create_github_issue(
    title: str,
    body: str,
    labels: Optional[Iterable[str]] = None,
) -> bool:
    """Open a GitHub issue via the REST API (no-op if env vars are absent)."""

    repo = os.getenv("GITHUB_REPO")
    token = os.getenv("GITHUB_TOKEN")
    if not repo or not token:
        logger.info(
            "Skipping GitHub issue creation (set GITHUB_REPO and GITHUB_TOKEN to enable automation)."
        )
        return False

    url = f"https://api.github.com/repos/{repo}/issues"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }
    payload = {"title": title, "body": body}
    if labels:
        payload["labels"] = list(labels)

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        if response.status_code >= 400:
            logger.warning(
                "GitHub issue creation failed (%s): %s",
                response.status_code,
                response.text,
            )
            return False
    except requests.RequestException as exc:  # pragma: no cover
        logger.warning("GitHub issue creation raised an exception: %s", exc)
        return False

    logger.info("Opened GitHub issue for schema drift: %s", title)
    return True


def notify_schema_event(
    *,
    event_type: str,
    dataset: str,
    table: str,
    summary: str,
    remediation: str,
    labels: Optional[Iterable[str]] = None,
) -> None:
    """Record schema drift details and optionally create a GitHub issue."""

    event_key = hashlib.sha1(
        f"{event_type}|{dataset}|{table}|{summary}".encode("utf-8")
    ).hexdigest()
    if _mark_event_seen(event_key):
        logger.debug(
            "Schema event already recorded (skipping duplicate issue): %s", summary
        )
        return

    body = (
        f"Dataset: `{dataset}`\n"
        f"Target table: `{table}`\n\n"
        f"Summary: {summary}\n\n"
        f"Recommended action: {remediation}\n"
    )

    _append_drift_log(body)

    issue_title = f"Schema drift detected in {dataset} -> {table}"
    _create_github_issue(issue_title, body, labels=labels)


def notify_new_source_dataset(dataset: str, location: str) -> None:
    """Highlight a newly detected survivoR dataset so configs can be updated."""

    event_key = hashlib.sha1(
        f"new-source-dataset|{dataset}".encode("utf-8")
    ).hexdigest()
    if _mark_event_seen(event_key):
        logger.debug("New dataset event already recorded for %s", dataset)
        return

    summary = (
        f"New survivoR dataset detected: `{dataset}` (location: {location}). "
        "Consider updating Database/db_run_config.json and create_tables.sql if you want to ingest it."
    )

    _append_drift_log(summary)
    _create_github_issue(
        title=f"Review new survivoR dataset '{dataset}'",
        body=summary,
        labels=["schema-drift", "upstream-change"],
    )
