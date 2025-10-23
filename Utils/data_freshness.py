"""Helpers for detecting upstream data changes and capturing Git metadata."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Tuple

import requests

from Utils.db_utils import connect_to_db

logger = logging.getLogger(__name__)

_CACHE_DIR = Path("data_cache")
_CACHE_DIR.mkdir(parents=True, exist_ok=True)
_METADATA_PATH = _CACHE_DIR / "fingerprints.json"
_SURVIVOR_REPO_COMMITS = "https://api.github.com/repos/doehm/survivoR/commits"
_GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")


def _github_headers() -> Dict[str, str]:
    headers = {"Accept": "application/vnd.github+json"}
    if _GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {_GITHUB_TOKEN}"
    return headers


def _load_metadata() -> Dict[str, Dict[str, str]]:
    if _METADATA_PATH.exists():
        try:
            return json.loads(_METADATA_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.warning("Could not parse %s; rebuilding cache", _METADATA_PATH)
    return {}


def _save_metadata(metadata: Dict[str, Dict[str, str]]) -> None:
    _METADATA_PATH.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")


def _signature_from_headers(headers: requests.structures.CaseInsensitiveDict) -> str:
    parts = [headers.get("ETag"), headers.get("Last-Modified"), headers.get("Content-Length")]
    return "|".join(part for part in parts if part)


def _fetch_dataset_signature(dataset_name: str, base_raw_url: str, timeout: int = 30) -> str:
    url = f"{base_raw_url.rstrip('/')}/{dataset_name}.rda"
    try:
        response = requests.head(url, timeout=timeout, allow_redirects=True)
        response.raise_for_status()
        signature = _signature_from_headers(response.headers)
        if signature:
            return signature
        logger.debug("HEAD response for %s lacked signature headers; falling back to GET", url)
    except Exception as exc:  # broad fallback to ensure we attempt GET
        logger.warning("HEAD request failed for %s (%s); falling back to GET", url, exc)

    response = requests.get(url, timeout=timeout, allow_redirects=True)
    response.raise_for_status()
    digest = hashlib.sha256(response.content).hexdigest()
    return f"sha256:{digest}"


def _fetch_latest_commit(dataset_name: str, timeout: int = 30) -> Dict[str, str]:
    params = {"path": f"data/{dataset_name}.rda", "per_page": 1}
    try:
        response = requests.get(_SURVIVOR_REPO_COMMITS, params=params, headers=_github_headers(), timeout=timeout)
        response.raise_for_status()
        payload = response.json()
        if payload:
            commit = payload[0]
            committed_at = commit.get("commit", {}).get("author", {}).get("date")
            return {
                "commit_sha": commit.get("sha"),
                "commit_url": commit.get("html_url"),
                "committed_at": committed_at,
            }
    except Exception as exc:  # pragma: no cover - GitHub outages or rate limits
        logger.warning("Could not fetch Git metadata for %s: %s", dataset_name, exc)
    return {"commit_sha": None, "commit_url": None, "committed_at": None}


def fetch_dataset_metadata(dataset_name: str, base_raw_url: str) -> Dict[str, str]:
    signature = _fetch_dataset_signature(dataset_name, base_raw_url)
    commit_info = _fetch_latest_commit(dataset_name)
    return {"signature": signature, **commit_info}


def detect_dataset_changes(
    dataset_names: Iterable[str], base_raw_url: str
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, str]]]:
    """Return current metadata and the subset that changed since last cache."""
    stored = _load_metadata()
    current: Dict[str, Dict[str, str]] = {}
    changed: Dict[str, Dict[str, str]] = {}

    for name in dataset_names:
        try:
            metadata = fetch_dataset_metadata(name, base_raw_url)
            current[name] = metadata
            previous = stored.get(name, {})
            if metadata["signature"] != previous.get("signature") or metadata.get("commit_sha") != previous.get("commit_sha"):
                changed[name] = metadata
        except Exception as exc:  # pragma: no cover
            logger.warning("Could not build metadata for %s: %s", name, exc)
            changed[name] = {"signature": "unknown", "commit_sha": None, "commit_url": None, "committed_at": None}

    return current, changed


def persist_metadata(metadata: Dict[str, Dict[str, str]]) -> None:
    current = _load_metadata()
    current.update(metadata)
    _save_metadata(current)


def upsert_dataset_metadata(metadata: Dict[str, Dict[str, str]], ingest_run_id: str | None) -> None:
    if not metadata:
        return

    conn = connect_to_db()
    if not conn:
        logger.error("Unable to connect to database to persist dataset metadata")
        return

    try:
        with conn.cursor() as cur:
            for dataset, meta in metadata.items():
                committed_at = meta.get("committed_at")
                committed_ts = None
                if committed_at:
                    try:
                        committed_ts = datetime.fromisoformat(committed_at.replace("Z", "+00:00"))
                    except ValueError:
                        committed_ts = None

                cur.execute(
                    """
                    INSERT INTO bronze.dataset_versions (dataset_name, signature, commit_sha, commit_url, committed_at, last_ingest_run_id, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (dataset_name)
                    DO UPDATE SET
                        signature = EXCLUDED.signature,
                        commit_sha = EXCLUDED.commit_sha,
                        commit_url = EXCLUDED.commit_url,
                        committed_at = EXCLUDED.committed_at,
                        last_ingest_run_id = EXCLUDED.last_ingest_run_id,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        dataset,
                        meta.get("signature"),
                        meta.get("commit_sha"),
                        meta.get("commit_url"),
                        committed_ts,
                        ingest_run_id,
                    ),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("Failed to upsert dataset metadata into bronze.dataset_versions")
        raise
    finally:
        conn.close()
