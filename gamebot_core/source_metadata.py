"""Shared helpers for selecting survivoR dataset sources (RDA vs JSON)."""

from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Optional

import requests

logger = logging.getLogger(__name__)

_GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
_ENABLE_GITHUB_METADATA = _GITHUB_TOKEN is not None
_COMMITS_ENDPOINT = "https://api.github.com/repos/doehm/survivoR/commits"
_COMMIT_CACHE: Dict[str, Dict[str, Optional[str]]] = {}


def _github_headers() -> Dict[str, str]:
    headers = {"Accept": "application/vnd.github+json"}
    if _GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {_GITHUB_TOKEN}"
    return headers


def _fetch_signature(
    base_url: str, dataset_name: str, extension: str, timeout: int = 30
) -> str:
    url = f"{base_url.rstrip('/')}/{dataset_name}{extension}"
    try:
        response = requests.head(url, timeout=timeout, allow_redirects=True)
        response.raise_for_status()
        etag = response.headers.get("ETag")
        last_modified = response.headers.get("Last-Modified")
        content_length = response.headers.get("Content-Length")
        parts = [part for part in (etag, last_modified, content_length) if part]
        if parts:
            return "|".join(parts)
    except Exception as exc:
        logger.debug("HEAD request failed for %s (%s); falling back to GET", url, exc)

    response = requests.get(url, timeout=timeout, allow_redirects=True)
    response.raise_for_status()
    digest = hashlib.sha256(response.content).hexdigest()
    return f"sha256:{digest}"


def _fetch_latest_commit(path: str, timeout: int = 30) -> Dict[str, Optional[str]]:
    if not _ENABLE_GITHUB_METADATA:
        return {"commit_sha": None, "commit_url": None, "committed_at": None}

    if path in _COMMIT_CACHE:
        return _COMMIT_CACHE[path]

    params = {"path": path, "per_page": 1}
    try:
        response = requests.get(
            _COMMITS_ENDPOINT,
            params=params,
            headers=_github_headers(),
            timeout=timeout,
        )
        if response.status_code == 403:
            logger.warning(
                "GitHub rate limit exceeded while fetching metadata for %s. "
                "Set GITHUB_TOKEN to increase limits.",
                path,
            )
            result = {"commit_sha": None, "commit_url": None, "committed_at": None}
            _COMMIT_CACHE[path] = result
            return result

        response.raise_for_status()
        payload = response.json()
        if payload:
            commit = payload[0]
            committed_at = commit.get("commit", {}).get("author", {}).get("date")
            result = {
                "commit_sha": commit.get("sha"),
                "commit_url": commit.get("html_url"),
                "committed_at": committed_at,
            }
            _COMMIT_CACHE[path] = result
            return result
    except Exception as exc:  # pragma: no cover - GitHub outages or throttling
        logger.warning("Could not fetch Git metadata for %s: %s", path, exc)
    result = {"commit_sha": None, "commit_url": None, "committed_at": None}
    _COMMIT_CACHE[path] = result
    return result


def _build_metadata(
    dataset_name: str,
    *,
    source_type: str,
    base_url: str,
    relative_path: str,
) -> Dict[str, Optional[str]]:
    extension = ".rda" if source_type == "rda" else ".json"
    signature = _fetch_signature(base_url, dataset_name, extension)
    commit_info = _fetch_latest_commit(relative_path)
    return {
        "dataset_name": dataset_name,
        "source_type": source_type,
        "base_url": base_url,
        "relative_path": relative_path,
        "signature": signature,
        **commit_info,
    }


def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def select_dataset_metadata(
    dataset_name: str,
    base_rda_url: Optional[str],
    json_url: Optional[str],
) -> Dict[str, Optional[str]]:
    """Return metadata for the freshest available dataset source."""

    candidates = []

    if base_rda_url:
        candidates.append(
            _build_metadata(
                dataset_name,
                source_type="rda",
                base_url=base_rda_url,
                relative_path=f"data/{dataset_name}.rda",
            )
        )

    if json_url:
        candidates.append(
            _build_metadata(
                dataset_name,
                source_type="json",
                base_url=json_url,
                relative_path=f"dev/json/{dataset_name}.json",
            )
        )

    if not candidates:
        raise ValueError("No base URLs configured for dataset ingestion.")

    # Choose the candidate with the most recent commit; fall back to the first entry.
    candidates.sort(
        key=lambda meta: _parse_timestamp(meta.get("committed_at"))
        or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    chosen = candidates[0]

    # When commit timestamps tie (or are missing), prefer RDA for historical compatibility.
    if len(candidates) > 1:
        top_time = _parse_timestamp(candidates[0].get("committed_at"))
        second_time = _parse_timestamp(candidates[1].get("committed_at"))
        if top_time == second_time and candidates[0]["source_type"] != "rda":
            for meta in candidates:
                if meta["source_type"] == "rda":
                    chosen = meta
                    break

    return chosen
