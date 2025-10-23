"""Helpers for detecting upstream data changes before running the pipeline."""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Dict, Iterable, Tuple

import requests

logger = logging.getLogger(__name__)

_CACHE_DIR = Path("data_cache")
_CACHE_DIR.mkdir(parents=True, exist_ok=True)
_FINGERPRINT_PATH = _CACHE_DIR / "fingerprints.json"


def _load_fingerprints() -> Dict[str, str]:
    if _FINGERPRINT_PATH.exists():
        try:
            return json.loads(_FINGERPRINT_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.warning("Could not parse %s; rebuilding cache", _FINGERPRINT_PATH)
    return {}


def _save_fingerprints(signatures: Dict[str, str]) -> None:
    _FINGERPRINT_PATH.write_text(json.dumps(signatures, indent=2, sort_keys=True), encoding="utf-8")


def _signature_from_headers(headers: requests.structures.CaseInsensitiveDict) -> str:
    parts = [headers.get("ETag"), headers.get("Last-Modified"), headers.get("Content-Length")]
    return "|".join(part for part in parts if part)


def fetch_dataset_signature(dataset_name: str, base_raw_url: str, timeout: int = 30) -> str:
    """Return a lightweight fingerprint for a remote survivoR dataset."""
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

    # Fallback: GET request and use content hash
    response = requests.get(url, timeout=timeout, allow_redirects=True)
    response.raise_for_status()
    digest = hashlib.sha256(response.content).hexdigest()
    return f"sha256:{digest}"


def detect_dataset_changes(dataset_names: Iterable[str], base_raw_url: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Return all current signatures plus the subset that changed since the last run."""
    stored = _load_fingerprints()
    current: Dict[str, str] = {}
    changed: Dict[str, str] = {}

    for name in dataset_names:
        try:
            signature = fetch_dataset_signature(name, base_raw_url)
            current[name] = signature
            if stored.get(name) != signature:
                changed[name] = signature
        except Exception as exc:  # pragma: no cover - network issues should trigger a rerun
            logger.warning("Could not fetch signature for %s: %s", name, exc)
            changed[name] = "unknown"

    return current, changed


def persist_signatures(signatures: Dict[str, str]) -> None:
    current = _load_fingerprints()
    current.update(signatures)
    _save_fingerprints(current)
