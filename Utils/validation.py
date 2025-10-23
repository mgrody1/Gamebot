"""Lightweight bronze layer data quality checks."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

VALIDATION_DIR = Path("docs/run_logs")
CHECK_PATTERN = re.compile(
    r"^(?P<metric>missing_count|duplicate_count)\((?P<column>[^)]+)\)\s*=\s*(?P<expected>-?\d+)$"
)


def _write_result(dataset_name: str, result: Dict) -> Path:
    """Persist the validation results for observability."""
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_path = VALIDATION_DIR / f"validation_{dataset_name}_{timestamp}.json"
    output_path.write_text(json.dumps(result, indent=2, default=str))
    return output_path


def _evaluate_check(df: pd.DataFrame, check: str) -> Tuple[bool, Dict]:
    """Evaluate a single validation rule against the dataframe."""
    match = CHECK_PATTERN.match(check.strip())
    if not match:
        raise ValueError(f"Unsupported validation rule syntax: '{check}'")

    metric = match.group("metric")
    column = match.group("column")
    expected = int(match.group("expected"))

    if column not in df.columns:
        return False, {
            "check": check,
            "status": "failed",
            "reason": "column_missing",
            "detail": f"Column '{column}' not present in dataframe.",
        }

    if metric == "missing_count":
        observed = int(df[column].isna().sum())
    elif metric == "duplicate_count":
        observed = int(df[column].duplicated().sum())
    else:
        raise ValueError(f"Unsupported validation metric: '{metric}'")

    passed = observed == expected
    result = {
        "check": check,
        "status": "passed" if passed else "failed",
        "observed": observed,
        "expected": expected,
    }
    return passed, result


def _run_dataframe_checks(
    dataset_name: str, df: pd.DataFrame, checks: Iterable[str]
) -> None:
    """Execute validation rules and raise when they fail."""
    check_results: List[Dict] = []
    all_passed = True

    for check in checks:
        passed, result = _evaluate_check(df, check)
        check_results.append(result)
        if not passed:
            all_passed = False

    summary = {
        "dataset": dataset_name,
        "total_checks": len(check_results),
        "failed_checks": sum(1 for r in check_results if r["status"] == "failed"),
        "checks": check_results,
        "timestamp": datetime.utcnow().isoformat(),
    }

    result_path = _write_result(dataset_name, summary)

    if not all_passed:
        logger.error("Validation failed for %s. See %s", dataset_name, result_path)
        raise ValueError(
            f"Validation failed for dataset '{dataset_name}' (details in {result_path})"
        )

    logger.info(
        "Validation succeeded for %s (results written to %s)", dataset_name, result_path
    )


def _base_checks(key_column: str) -> List[str]:
    return [
        f"missing_count({key_column}) = 0",
        f"duplicate_count({key_column}) = 0",
    ]


DATASET_CHECKS: Dict[str, List[str]] = {
    "castaway_details": _base_checks("castaway_id"),
    "season_summary": _base_checks("version_season"),
    "episodes": [
        "missing_count(version_season) = 0",
        "missing_count(episode) = 0",
    ],
    "advantage_details": [
        "missing_count(advantage_id) = 0",
        "missing_count(version_season) = 0",
    ],
    "challenge_description": [
        "missing_count(challenge_id) = 0",
        "missing_count(version_season) = 0",
    ],
    "vote_history": [
        "missing_count(vote_history_id) = 0",
        "missing_count(castaway_id) = 0",
        "missing_count(version_season) = 0",
    ],
}


def validate_bronze_dataset(dataset_name: str, df: pd.DataFrame) -> None:
    """Run dataframe-based validations for a given bronze dataset."""
    checks = DATASET_CHECKS.get(dataset_name)
    if not checks:
        logger.debug(
            "No validation rules registered for dataset '%s'; skipping.", dataset_name
        )
        return

    _run_dataframe_checks(dataset_name, df.copy(), checks)
