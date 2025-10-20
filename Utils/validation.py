import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict

import pandas as pd
import great_expectations as ge

logger = logging.getLogger(__name__)


VALIDATION_DIR = Path("docs/run_logs")


def _write_result(dataset_name: str, result: Dict) -> None:
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_path = VALIDATION_DIR / f"ge_{dataset_name}_{timestamp}.json"
    output_path.write_text(json.dumps(result, indent=2, default=str))


def _base_expectations(ge_df: ge.dataset.PandasDataset, key_column: str) -> None:
    ge_df.expect_column_values_to_not_be_null(key_column)
    ge_df.expect_column_values_to_be_unique(key_column)


def _castaway_details_expectations(ge_df: ge.dataset.PandasDataset) -> None:
    _base_expectations(ge_df, "castaway_id")


def _season_summary_expectations(ge_df: ge.dataset.PandasDataset) -> None:
    _base_expectations(ge_df, "version_season")


def _episodes_expectations(ge_df: ge.dataset.PandasDataset) -> None:
    ge_df.expect_column_values_to_not_be_null("version_season")
    ge_df.expect_column_values_to_not_be_null("episode")


def _advantage_details_expectations(ge_df: ge.dataset.PandasDataset) -> None:
    ge_df.expect_column_values_to_not_be_null("advantage_id")
    ge_df.expect_column_values_to_not_be_null("version_season")


def _challenge_description_expectations(ge_df: ge.dataset.PandasDataset) -> None:
    ge_df.expect_column_values_to_not_be_null("challenge_id")
    ge_df.expect_column_values_to_not_be_null("version_season")


def _vote_history_expectations(ge_df: ge.dataset.PandasDataset) -> None:
    ge_df.expect_column_values_to_not_be_null("vote_history_id")
    ge_df.expect_column_values_to_not_be_null("castaway_id")
    ge_df.expect_column_values_to_not_be_null("version_season")


DATASET_EXPECTATIONS: Dict[str, Callable[[ge.dataset.PandasDataset], None]] = {
    "castaway_details": _castaway_details_expectations,
    "season_summary": _season_summary_expectations,
    "episodes": _episodes_expectations,
    "advantage_details": _advantage_details_expectations,
    "challenge_description": _challenge_description_expectations,
    "vote_history": _vote_history_expectations,
}


def validate_bronze_dataset(dataset_name: str, df: pd.DataFrame) -> None:
    expectation_fn = DATASET_EXPECTATIONS.get(dataset_name)
    if expectation_fn is None:
        return

    ge_df = ge.dataset.PandasDataset(df.copy())
    expectation_fn(ge_df)
    result = ge_df.validate()
    _write_result(dataset_name, result)

    if not result.get("success", False):
        logger.error("Great Expectations validation failed for %s", dataset_name)
        raise ValueError(f"Validation failed for dataset '{dataset_name}'")
