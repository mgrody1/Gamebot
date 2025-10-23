"""Helpers for detecting upstream data changes and capturing Git metadata."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Tuple

from Utils.db_utils import connect_to_db
from Utils.source_metadata import select_dataset_metadata

logger = logging.getLogger(__name__)

_CACHE_DIR = Path("data_cache")
_CACHE_DIR.mkdir(parents=True, exist_ok=True)
_METADATA_PATH = _CACHE_DIR / "fingerprints.json"


def _load_metadata() -> Dict[str, Dict[str, str]]:
    if _METADATA_PATH.exists():
        try:
            return json.loads(_METADATA_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.warning("Could not parse %s; rebuilding cache", _METADATA_PATH)
    return {}


def _save_metadata(metadata: Dict[str, Dict[str, str]]) -> None:
    _METADATA_PATH.write_text(
        json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8"
    )


def detect_dataset_changes(
    dataset_names: Iterable[str],
    base_raw_url: str,
    json_raw_url: str | None,
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, str]]]:
    """Return current metadata and the subset that changed since last cache."""
    stored = _load_metadata()
    current: Dict[str, Dict[str, str]] = {}
    changed: Dict[str, Dict[str, str]] = {}

    for name in dataset_names:
        try:
            metadata = select_dataset_metadata(name, base_raw_url, json_raw_url)
            current[name] = metadata
            previous = stored.get(name, {})
            signature_changed = metadata["signature"] != previous.get("signature")
            commit_changed = metadata.get("commit_sha") != previous.get("commit_sha")
            source_changed = metadata.get("source_type") != previous.get("source_type")
            if signature_changed or commit_changed or source_changed:
                changed[name] = metadata
        except Exception as exc:  # pragma: no cover
            logger.warning("Could not build metadata for %s: %s", name, exc)
            changed[name] = {
                "signature": "unknown",
                "commit_sha": None,
                "commit_url": None,
                "committed_at": None,
                "source_type": None,
            }

    return current, changed


def persist_metadata(metadata: Dict[str, Dict[str, str]]) -> None:
    current = _load_metadata()
    current.update(metadata)
    _save_metadata(current)


def upsert_dataset_metadata(
    metadata: Dict[str, Dict[str, str]], ingest_run_id: str | None
) -> None:
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
                        committed_ts = datetime.fromisoformat(
                            committed_at.replace("Z", "+00:00")
                        )
                    except ValueError:
                        committed_ts = None

                cur.execute(
                    """
                    INSERT INTO bronze.dataset_versions (dataset_name, signature, commit_sha, commit_url, committed_at, source_type, last_ingest_run_id, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (dataset_name)
                    DO UPDATE SET
                        signature = EXCLUDED.signature,
                        commit_sha = EXCLUDED.commit_sha,
                        commit_url = EXCLUDED.commit_url,
                        committed_at = EXCLUDED.committed_at,
                        source_type = EXCLUDED.source_type,
                        last_ingest_run_id = EXCLUDED.last_ingest_run_id,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        dataset,
                        meta.get("signature"),
                        meta.get("commit_sha"),
                        meta.get("commit_url"),
                        committed_ts,
                        meta.get("source_type"),
                        ingest_run_id,
                    ),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception(
            "Failed to upsert dataset metadata into bronze.dataset_versions"
        )
        raise
    finally:
        conn.close()
