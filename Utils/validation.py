"""Bronze layer data quality checks powered by Soda Core."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd
from soda.scan import Scan

logger = logging.getLogger(__name__)

VALIDATION_DIR = Path("docs/run_logs")
_PANDAS_CONFIG_YAML = """
data_source pandas:
  type: pandas
"""


def _write_result(dataset_name: str, result: Dict) -> Path:
    """Persist the Soda scan output for observability."""
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_path = VALIDATION_DIR / f"soda_{dataset_name}_{timestamp}.json"
    output_path.write_text(json.dumps(result, indent=2, default=str))
    return output_path


def _run_soda_scan(dataset_name: str, df: pd.DataFrame, checks: Iterable[str]) -> None:
    """Execute a Soda Core scan against an in-memory bronze dataframe."""
    scan = Scan()
    scan.set_scan_definition_name(f"bronze_{dataset_name}")
    scan.set_data_source_name("pandas")
    scan.add_configuration_yaml_str(_PANDAS_CONFIG_YAML)
    scan.add_pandas_dataframe(dataset_name, df, data_source_name="pandas")

    checks_yaml = f"checks for {dataset_name}:\n"
    for check in checks:
        checks_yaml += f"  - {check}\n"
    scan.add_check_yaml_str(checks_yaml)

    scan.execute()
    results = json.loads(scan.get_results_json_str())
    result_path = _write_result(dataset_name, results)

    if results.get("has_errors") or results.get("has_failures"):
        logger.error("Soda scan failed for %s. See %s", dataset_name, result_path)
        raise ValueError(f"Validation failed for dataset '{dataset_name}' (details in {result_path})")

    logger.info("Soda scan succeeded for %s (results written to %s)", dataset_name, result_path)


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
    """Run Soda Core validations for a given bronze dataset."""
    checks = DATASET_CHECKS.get(dataset_name)
    if not checks:
        logger.debug("No Soda checks registered for dataset '%s'; skipping.", dataset_name)
        return

    _run_soda_scan(dataset_name, df.copy(), checks)
