# ruff: noqa: E402

import copy
import difflib
import logging
import re
import sys
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from itertools import zip_longest
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from uuid import UUID, uuid4

import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection
from psycopg2.extras import execute_values
from sqlalchemy import create_engine

# Add the base directory to sys.path so `params` can be imported reliably
base_dir = Path(__file__).resolve().parent.parent
if str(base_dir) not in sys.path:
    sys.path.append(str(base_dir))

import params  # noqa: E402
from .github_data_loader import load_dataset  # noqa: E402
from .validation import (  # noqa: E402
    register_data_issue,
    validate_bronze_dataset,
    VALIDATION_SUMMARIES,
)
from .log_utils import setup_logging  # noqa: E402
from .notifications import notify_schema_event  # noqa: E402

setup_logging(logging.INFO)
logger = logging.getLogger(__name__)

BOOL_LIKE_TYPES = tuple(
    t
    for t in (bool, getattr(np, "bool_", None), getattr(np, "bool8", None))
    if t is not None
)

# Fallback overrides for the rare cases where stage-based remediation cannot infer
# the intended challenge. These are legacy quirks we still paper over explicitly.
VOTE_HISTORY_CHALLENGE_FIXUPS: Dict[Tuple[str, int], int] = {
    ("US37", 26): 25,
    ("AU11", 13): 12,
}

REMEDIATION_DETAIL_LIMIT = 200

DEDUPLICATION_DATASETS: Dict[str, Optional[List[str]]] = {
    # Use the configured unique constraint columns for deduplication
    "auction_details": None,
}

COERCION_CONTEXT_COLUMNS: Dict[str, List[str]] = {
    "auction_details": ["version_season", "auction_num", "item", "castaway_id"],
    "advantage_movement": [
        "version_season",
        "advantage_id",
        "sequence_id",
        "castaway_id",
    ],
    "journeys": ["version_season", "episode", "sog_id", "castaway_id"],
    "tribe_mapping": ["version_season", "episode", "day", "castaway_id", "tribe"],
    "vote_history": ["version_season", "episode", "vote_event", "castaway_id"],
}


def _note_extra_columns(
    dataset_name: str, table_name: str, extra_columns: Iterable[str]
) -> None:
    extra_cols_sorted = sorted(extra_columns)
    summary = f"Unexpected columns detected: {extra_cols_sorted}"
    remediation = (
        "Review the new columns. If you want to keep them, update the bronze DDL, dbt models, and docs; "
        "otherwise drop them during preprocessing."
    )
    logger.info(
        "Dataset '%s' includes new columns not in %s: %s. They will be dropped during load.",
        dataset_name,
        table_name,
        extra_cols_sorted,
    )
    notify_schema_event(
        event_type="extra-columns",
        dataset=dataset_name,
        table=table_name,
        summary=summary,
        remediation=remediation,
        labels=["schema-drift", "upstream-change"],
    )


class SchemaMismatchError(RuntimeError):
    """Raised when the incoming dataset structure diverges from the warehouse schema."""


@dataclass(frozen=True)
class SchemaValidationResult:
    """Holds the outcome of a schema comparison between a dataset and a target table."""

    is_valid: bool
    missing_columns: Set[str]
    extra_columns: Set[str]
    type_mismatches: Dict[str, Tuple[str, str]]
    db_schema: Dict[str, str]


def _split_table_reference(table_name: str) -> Tuple[str, str]:
    """Split a table reference into schema and table components."""
    if "." in table_name:
        schema, table = table_name.split(".", 1)
    else:
        schema, table = "public", table_name
    return schema, table


def _safe_int(value: Any) -> Optional[int]:
    """Best-effort conversion of assorted numeric representations to int."""
    if value is None:
        return None
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        if np.isnan(value):
            return None
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _load_challenge_reference_data(
    conn: connection,
) -> Tuple[Set[Tuple[str, int]], Dict[Tuple[str, int], Set[int]]]:
    """
    Fetch challenge reference data to validate vote history entries.

    Returns
    -------
    (valid_keys, stage_map)
        valid_keys: set of (version_season, challenge_id) combinations that exist in challenge_description.
        stage_map: mapping of (version_season, sog_id) -> {challenge_id, ...} based on challenge_results.
    """
    valid_keys: Set[Tuple[str, int]] = set()
    stage_map: Dict[Tuple[str, int], Set[int]] = defaultdict(set)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT version_season, challenge_id
              FROM bronze.challenge_description
             WHERE challenge_id IS NOT NULL
            """
        )
        for version_season, challenge_id in cur.fetchall():
            if version_season and challenge_id is not None:
                valid_keys.add((str(version_season).strip(), int(challenge_id)))

        cur.execute(
            """
            SELECT version_season, sog_id, challenge_id
              FROM bronze.challenge_results
             WHERE sog_id IS NOT NULL
               AND challenge_id IS NOT NULL
            """
        )
        for version_season, sog_id, challenge_id in cur.fetchall():
            vs = str(version_season).strip() if version_season else None
            sog = _safe_int(sog_id)
            cid = _safe_int(challenge_id)
            if vs and sog is not None and cid is not None:
                stage_map[(vs, sog)].add(cid)

    return valid_keys, stage_map


def _apply_unique_key_deduplication(
    dataset_name: str, table_name: str, df: pd.DataFrame
) -> pd.DataFrame:
    """Drop duplicate rows for datasets where we trust the configured unique key."""
    if dataset_name not in DEDUPLICATION_DATASETS:
        return df

    configured_subset = DEDUPLICATION_DATASETS[dataset_name]
    if configured_subset:
        unique_cols = configured_subset
    else:
        try:
            unique_cols = get_unique_constraint_cols_from_table_name(table_name)
        except (AssertionError, KeyError):
            return df

    subset_cols = [col for col in unique_cols if col in df.columns]
    if not subset_cols:
        return df

    duplicate_mask = df.duplicated(subset=subset_cols, keep=False)
    if not duplicate_mask.any():
        return df

    original_rows_df = df.loc[duplicate_mask]
    rows_to_remove_mask = df.duplicated(subset=subset_cols, keep="first")
    removed_rows_df = df.loc[rows_to_remove_mask]
    kept_rows_df = df.loc[duplicate_mask & ~rows_to_remove_mask]

    before = len(df)
    deduped = df.drop_duplicates(subset=subset_cols).reset_index(drop=True)
    dropped = before - len(deduped)
    if dropped:
        sample_records = (
            original_rows_df[subset_cols].drop_duplicates().head(5).to_dict("records")
        )
        logger.warning(
            "Deduplicated %s rows in dataset '%s' using columns %s. Sample duplicates: %s",
            dropped,
            dataset_name,
            subset_cols,
            sample_records,
        )
        register_data_issue(
            dataset_name,
            "deduplicated_rows",
            {
                "rows_removed": int(dropped),
                "subset_columns": subset_cols,
                "before": int(before),
                "after": int(len(deduped)),
                "original_rows": original_rows_df.head(
                    REMEDIATION_DETAIL_LIMIT
                ).to_dict("records"),
                "removed_rows": removed_rows_df.head(REMEDIATION_DETAIL_LIMIT).to_dict(
                    "records"
                ),
                "result_rows": kept_rows_df.head(REMEDIATION_DETAIL_LIMIT).to_dict(
                    "records"
                ),
            },
        )
    return deduped


def connect_to_db() -> Optional[connection]:
    """Establish a psycopg2 connection using credentials from params."""
    conn_args = {
        "host": params.db_host,
        "dbname": params.db_name,
        "user": params.db_user,
        "password": params.db_pass,
    }
    if params.port:
        conn_args["port"] = params.port

    try:
        conn = psycopg2.connect(**conn_args)
        logger.info("Database connection successful")
        return conn
    except Exception as exc:
        logger.error("Error connecting to database: %s", exc)
        return None


def create_sql_engine():
    """Build a SQLAlchemy engine for the configured PostgreSQL database."""
    port_part = f":{params.port}" if params.port else ""
    url = f"postgresql://{params.db_user}:{params.db_pass}@{params.db_host}{port_part}/{params.db_name}"
    return create_engine(url)


def truncate_table(table_name: str, conn: connection) -> None:
    """Truncate a table (optionally schema-qualified) and reset identity columns."""
    schema, table = _split_table_reference(table_name)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("TRUNCATE TABLE {}.{} RESTART IDENTITY CASCADE;").format(
                sql.Identifier(schema),
                sql.Identifier(table),
            )
        )
    conn.commit()


def get_db_column_types(table_name: str, conn: connection) -> Dict[str, str]:
    """Return a mapping of column name to data type for a target table."""
    schema, table = _split_table_reference(table_name)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
              AND table_schema = %s
            ORDER BY ordinal_position;
            """,
            (table, schema),
        )
        return {row[0]: row[1] for row in cur.fetchall()}


def _coerce_boolean_value(value) -> Optional[bool]:
    """Best-effort coercion of miscellaneous truthy/falsey representations."""
    if pd.isna(value):
        return None

    if isinstance(value, (bool, np.bool_)):
        return bool(value)

    if isinstance(value, (int, np.integer)):
        if value in (0, 1):
            return bool(value)
        return None

    if isinstance(value, (float, np.floating)):
        if np.isnan(value):
            return None
        if value in (0.0, 1.0):
            return bool(int(value))
        return None

    text = str(value).strip().lower()
    if not text or text in {"nan", "na", "n/a", "none", "null"}:
        return None
    if text in {"true", "t", "yes", "y", "1", "1.0", "on"}:
        return True
    if text in {"false", "f", "no", "n", "0", "0.0", "off"}:
        return False

    try:
        numeric = float(text)
    except ValueError:
        return None
    if numeric in (0.0, 1.0):
        return bool(int(numeric))
    return None


def preprocess_dataframe(
    df: pd.DataFrame, db_schema: Dict[str, str], dataset_name: Optional[str] = None
) -> pd.DataFrame:
    """Coerce dataframe columns to match database types as closely as possible."""
    df = df.copy()
    coerced_log: Dict[str, List[int]] = {}

    for col in df.columns:
        db_col_type = db_schema.get(col)
        if not db_col_type:
            continue

        try:
            original_non_null = df[col].notna()

            if pd.api.types.is_categorical_dtype(df[col]):
                df[col] = df[col].astype(str)

            if db_col_type == "boolean":
                # Save original values for error reporting
                original_values = df[col].copy()
                if pd.api.types.is_bool_dtype(df[col]):
                    df[col] = df[col].astype("boolean")
                else:
                    df[col] = df[col].apply(_coerce_boolean_value).astype("boolean")
                # Log any values that could not be converted
                failed_mask = df[col].isna() & original_non_null
                if failed_mask.any():
                    failed_vals = original_values[failed_mask].unique()
                    logger.warning(
                        "Column '%s' (boolean) had unconvertible values: %s (showing up to 10)",
                        col,
                        failed_vals[:10],
                    )
            elif db_col_type in {"integer", "bigint"}:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif db_col_type in {"double precision", "numeric", "real"}:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif db_col_type == "date":
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
            elif db_col_type in {
                "timestamp without time zone",
                "timestamp with time zone",
            }:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif db_col_type == "uuid":
                df[col] = df[col].apply(
                    lambda value: None
                    if pd.isna(value)
                    else (
                        cleaned
                        if (cleaned := str(value).strip())
                        and cleaned.lower() not in {"none", "null", "nan"}
                        else None
                    )
                )
            else:
                df[col] = df[col].apply(
                    lambda value: None
                    if pd.isna(value)
                    else (
                        cleaned
                        if (cleaned := str(value).strip())
                        and cleaned.lower() not in {"none", "null", "nan"}
                        else None
                    )
                )

            coerced_rows = original_non_null & df[col].isna()
            if coerced_rows.any():
                coerced_log[col] = df[coerced_rows].index.tolist()
        except Exception as exc:
            logger.warning(
                "Could not convert column %s to %s: %s", col, db_col_type, exc
            )

    datetime_cols = df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns
    for col in datetime_cols:
        df[col] = df[col].astype(object).where(pd.notnull(df[col]), None)

    for col, indices in coerced_log.items():
        sample_indices = indices[:10]
        suffix = "..." if len(indices) > 10 else ""
        context_cols = []
        if dataset_name:
            context_cols = COERCION_CONTEXT_COLUMNS.get(dataset_name, [])
        context_cols = list(dict.fromkeys(context_cols + ["index_reference"]))
        preview_records: List[Dict[str, Any]] = []
        for idx in sample_indices:
            record: Dict[str, Any] = {"index_reference": idx}
            for context_col in context_cols:
                if context_col == "index_reference":
                    continue
                if context_col in df.columns:
                    record[context_col] = df.at[idx, context_col]
            preview_records.append(record)
        logger.warning(
            "Coercion occurred in dataset '%s' column '%s' impacting %s rows %s. Sample context: %s",
            dataset_name or "unknown",
            col,
            len(indices),
            suffix,
            preview_records,
        )
        register_data_issue(
            dataset_name or "unknown_dataset",
            "value_coercion",
            {
                "column": col,
                "rows_impacted": int(len(indices)),
                "sample_indices": sample_indices,
                "sample_context": preview_records,
            },
        )

    df.replace(["NaT", "nan", "NaN"], np.nan, inplace=True)
    df = df.where(pd.notnull(df), None)

    return df


def fetch_existing_keys(
    table_name: str, conn: connection, key_columns: Iterable[str]
) -> pd.DataFrame:
    """Fetch the existing key values for a table to support delta detection."""
    schema, table = _split_table_reference(table_name)
    with conn.cursor() as cur:
        query = sql.SQL("SELECT {} FROM {}.{}").format(
            sql.SQL(", ").join(sql.Identifier(col) for col in key_columns),
            sql.Identifier(schema),
            sql.Identifier(table),
        )
        cur.execute(query)
        return pd.DataFrame(cur.fetchall(), columns=key_columns)


def _first_non_null_value(series: Optional[pd.Series]) -> Optional[Any]:
    """Return the first non-null value from a Series."""
    if series is None:
        return None
    non_null = series.dropna()
    if non_null.empty:
        return None
    return non_null.iloc[0]


def _ensure_challenge_description_rows(
    df: pd.DataFrame, conn: connection, ingest_run_id: str
) -> None:
    """Backfill challenge metadata rows when challenge_results references missing IDs."""
    try:
        default_ingest_uuid = UUID(str(ingest_run_id))
    except ValueError as exc:
        raise ValueError(f"Invalid ingest_run_id supplied: {ingest_run_id}") from exc

    required_columns = {"version_season", "challenge_id"}
    if df.empty or not required_columns.issubset(df.columns):
        return

    normalized = df.assign(
        _version_season_norm=df["version_season"].astype(str).str.strip(),
        _challenge_id_norm=pd.to_numeric(df["challenge_id"], errors="coerce").astype(
            "Int64"
        ),
    )
    candidates = normalized[
        [
            "_version_season_norm",
            "_challenge_id_norm",
            "version",
            "season",
            "episode",
            "challenge_type",
        ]
    ].rename(
        columns={
            "_version_season_norm": "version_season",
            "_challenge_id_norm": "challenge_id",
        }
    )
    candidates = candidates.dropna(subset=["version_season", "challenge_id"])
    if candidates.empty:
        return

    candidate_pairs = {
        (row.version_season, int(row.challenge_id)) for row in candidates.itertuples()
    }

    existing = fetch_existing_keys(
        f"{params.bronze_schema}.challenge_description",
        conn,
        ["version_season", "challenge_id"],
    )
    existing_pairs = {
        (str(row.version_season), int(row.challenge_id))
        for row in existing.itertuples()
        if row.challenge_id is not None
    }

    missing_pairs = candidate_pairs - existing_pairs
    if not missing_pairs:
        return

    stub_rows: List[Dict[str, Any]] = []
    for version_season, challenge_id in sorted(missing_pairs):
        subset = normalized[
            (normalized["_version_season_norm"] == version_season)
            & (normalized["_challenge_id_norm"] == challenge_id)
        ]
        if subset.empty:
            continue

        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    SELECT ingest_run_id
                      FROM {}.challenge_description
                     WHERE version_season = %s
                  ORDER BY ingest_run_id DESC
                     LIMIT 1
                    """
                ).format(sql.Identifier(params.bronze_schema)),
                (version_season,),
            )
            existing_ingest_row = cur.fetchone()

        if existing_ingest_row and existing_ingest_row[0]:
            try:
                ingest_uuid = UUID(str(existing_ingest_row[0]))
            except ValueError:
                ingest_uuid = default_ingest_uuid
        else:
            ingest_uuid = default_ingest_uuid

        version_value = _first_non_null_value(subset.get("version"))
        season_value = _first_non_null_value(subset.get("season"))
        episode_value = _first_non_null_value(subset.get("episode"))
        challenge_type_value = _first_non_null_value(subset.get("challenge_type"))

        # Attempt to augment from challenge_summary when available
        summary_version = summary_season = summary_episode = summary_type = None
        try:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        SELECT version, season, episode, challenge_type
                          FROM {}.challenge_summary
                         WHERE version_season = %s
                           AND challenge_id = %s
                         LIMIT 1
                        """
                    ).format(sql.Identifier(params.bronze_schema)),
                    (version_season, challenge_id),
                )
                summary_row = cur.fetchone()
                if summary_row:
                    summary_version, summary_season, summary_episode, summary_type = (
                        summary_row
                    )
        except psycopg2.Error:
            summary_row = None  # Table may not exist yet; ignore

        stub_rows.append(
            {
                "version": _first_non_null_value(
                    pd.Series([summary_version, version_value])
                ),
                "version_season": version_season,
                "season": _first_non_null_value(
                    pd.Series([summary_season, season_value])
                ),
                "challenge_id": challenge_id,
                "episode": _first_non_null_value(
                    pd.Series([summary_episode, episode_value])
                ),
                "challenge_number": None,
                "challenge_type": _first_non_null_value(
                    pd.Series([summary_type, challenge_type_value])
                ),
                "ingest_run_id": str(ingest_uuid),
                "name": None,
                "recurring_name": None,
                "description": None,
                "reward": None,
                "additional_stipulation": None,
                "balance": None,
                "balance_ball": None,
                "balance_beam": None,
                "endurance": None,
                "fire": None,
                "food": None,
                "knowledge": None,
                "memory": None,
                "mud": None,
                "obstacle_blindfolded": None,
                "obstacle_cargo_net": None,
                "obstacle_chopping": None,
                "obstacle_combination_lock": None,
                "obstacle_digging": None,
                "obstacle_knots": None,
                "obstacle_padlocks": None,
                "precision": None,
                "precision_catch": None,
                "precision_roll_ball": None,
                "precision_slingshot": None,
                "precision_throw_balls": None,
                "precision_throw_coconuts": None,
                "precision_throw_rings": None,
                "precision_throw_sandbags": None,
                "puzzle": None,
                "puzzle_slide": None,
                "puzzle_word": None,
                "race": None,
                "strength": None,
                "turn_based": None,
                "water": None,
                "water_paddling": None,
                "water_swim": None,
                "source_dataset": "challenge_results_stub",
                "ingested_at": pd.Timestamp.utcnow(),
            }
        )

    if not stub_rows:
        return

    stub_df = pd.DataFrame(stub_rows)
    db_schema = get_db_column_types(
        f"{params.bronze_schema}.challenge_description", conn
    )

    # Ensure only columns we intend to upsert are present
    allowed_columns = [col for col in db_schema.keys() if col in stub_df.columns]
    stub_df = stub_df[allowed_columns]

    stub_df = _align_with_schema(stub_df, db_schema)
    stub_df = preprocess_dataframe(
        stub_df, db_schema, dataset_name="challenge_description_stub"
    )

    payload = {
        "rows_added": int(len(stub_df)),
        "missing_pairs": list(sorted(missing_pairs)),
        "target_table": f"{params.bronze_schema}.challenge_description",
        "result_rows": stub_df.to_dict("records"),
    }
    register_data_issue(
        "challenge_results",
        "challenge_description_backfill",
        payload,
    )
    payload_for_description = copy.deepcopy(payload)
    register_data_issue(
        "challenge_description",
        "challenge_description_backfill",
        copy.deepcopy(payload_for_description),
    )
    if "challenge_description" in VALIDATION_SUMMARIES:
        VALIDATION_SUMMARIES["challenge_description"].setdefault("issues", []).append(
            {
                "dataset": "challenge_description",
                "issue_type": "challenge_description_backfill",
                "timestamp": datetime.utcnow().isoformat(),
                "details": copy.deepcopy(payload_for_description),
            }
        )

    _upsert_dataframe(
        conn=conn,
        table_name=f"{params.bronze_schema}.challenge_description",
        df=stub_df,
        conflict_columns=["version_season", "challenge_id"],
    )
    sample_pairs = sorted(missing_pairs)
    summary_list = ", ".join(f"{vs}-{cid}" for vs, cid in sample_pairs[:10])
    if len(sample_pairs) > 10:
        summary_list += ", ..."
    logger.warning(
        "Backfilled %s challenge_description rows for missing challenges: %s",
        len(stub_df),
        summary_list,
    )


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise dataframe column names to snake_case for warehouse alignment."""
    df.columns = df.columns.astype(str).str.strip().str.lower().str.replace(" ", "_")
    return df


def _apply_dataset_specific_rules(
    dataset_name: str,
    df: pd.DataFrame,
    conn: connection,
    ingest_run_id: str,
) -> pd.DataFrame:
    """Apply per-dataset cleansing rules prior to schema validation."""
    df = _normalize_column_names(df)

    def _clean_castaway_id(value: Any) -> Optional[str]:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        text = str(value).strip()
        if not text or text.lower() in {"none", "null", "nan"}:
            return None
        return text

    if dataset_name == "castaways":
        before = len(df)
        df.rename(columns={"order": "castaways_order"}, inplace=True)
        existing_ids = fetch_existing_keys(
            f"{params.bronze_schema}.castaway_details", conn, ["castaway_id"]
        )
        if not existing_ids.empty:
            valid_ids = set(existing_ids["castaway_id"].dropna())
            df = df[df["castaway_id"].isin(valid_ids)]
            dropped = before - len(df)
            if dropped:
                logger.warning(
                    "Skipped %s castaways due to missing castaway_id in castaway_details",
                    dropped,
                )
                register_data_issue(
                    dataset_name,
                    "rows_dropped_missing_castaway_details",
                    {
                        "rows_removed": int(dropped),
                        "before": int(before),
                        "after": int(len(df)),
                    },
                )
    elif dataset_name == "boot_mapping":
        df.rename(columns={"order": "boot_mapping_order"}, inplace=True)
    elif dataset_name == "vote_history":
        df.rename(columns={"order": "vote_history_order"}, inplace=True)
        if "castaway_id" in df.columns:
            df["castaway_id"] = df["castaway_id"].map(_clean_castaway_id)
            missing_mask = df["castaway_id"].isna()
            if missing_mask.any():
                dropped = int(missing_mask.sum())
                sample_rows = (
                    df.loc[
                        missing_mask,
                        [
                            "version_season",
                            "episode",
                            "vote_event",
                            "tribe_status",
                            "vote_history_order"
                            if "vote_history_order" in df.columns
                            else "vote_order",
                        ],
                    ]
                    .head(5)
                    .to_dict("records")
                )
                sample_events = {
                    str(row.get("vote_event") or "unknown") for row in sample_rows
                }
                logger.warning(
                    "Vote history contains %s rows without castaway_id (sample vote events=%s) — keeping them as null. Sample context: %s",  # noqa: E501
                    dropped,
                    ", ".join(sorted(sample_events)[:3]),
                    sample_rows,
                )
                register_data_issue(
                    dataset_name,
                    "null_castaway_ids",
                    {
                        "rows": dropped,
                        "sample_vote_events": list(sorted(sample_events)[:5]),
                        "sample_rows": sample_rows,
                        "original_rows": df.loc[missing_mask].to_dict("records"),
                    },
                )
        if {"version_season", "challenge_id"}.issubset(df.columns):
            challenge_ids = pd.to_numeric(df["challenge_id"], errors="coerce")
            if not challenge_ids.empty:
                applied_fixes: Set[Tuple[str, int, int]] = set()
                known_fix_changes: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
                for idx, value in challenge_ids.items():
                    if pd.isna(value):
                        continue
                    version = str(df.at[idx, "version_season"]).strip()
                    key = (version, int(value))
                    if key in VOTE_HISTORY_CHALLENGE_FIXUPS:
                        new_value = VOTE_HISTORY_CHALLENGE_FIXUPS[key]
                        original_row = df.loc[idx].to_dict()
                        challenge_ids.at[idx] = new_value
                        applied_fixes.add((version, int(value), new_value))
                        updated_row = original_row.copy()
                        updated_row["challenge_id"] = new_value
                        known_fix_changes.append((original_row, updated_row))
                if applied_fixes:
                    for version, old_id, new_id in sorted(applied_fixes):
                        logger.info(
                            "Vote history remediation (known fix) version=%s challenge_id %s → %s",
                            version,
                            old_id,
                            new_id,
                        )
                    if known_fix_changes:
                        grouped_changes: Dict[
                            Tuple[Optional[str], Optional[int], Optional[int]],
                            List[Tuple[Dict[str, Any], Dict[str, Any]]],
                        ] = defaultdict(list)
                        for original_row, updated_row in known_fix_changes:
                            version_key = original_row.get("version_season")
                            old_value = original_row.get("challenge_id")
                            new_value = updated_row.get("challenge_id")
                            grouped_changes[(version_key, old_value, new_value)].append(
                                (original_row, updated_row)
                            )
                        for (_, _, _), pair_list in sorted(grouped_changes.items()):
                            register_data_issue(
                                dataset_name,
                                "challenge_id_known_fix",
                                {
                                    "rows_corrected": int(len(pair_list)),
                                    "original_rows": [
                                        original for original, _ in pair_list
                                    ],
                                    "result_rows": [
                                        updated for _, updated in pair_list
                                    ],
                                },
                            )
                df["challenge_id"] = challenge_ids.astype("Int64")

        if {"version_season", "challenge_id", "sog_id"}.issubset(df.columns):
            valid_keys, stage_map = _load_challenge_reference_data(conn)
            candidate_rows = df[
                df["challenge_id"].notna() & df["version_season"].notna()
            ].index.tolist()
            invalid_indices: List[int] = []
            # Any vote history row referencing a non-existent challenge_id gets queued for
            # remediation; we will attempt to infer the correct id by aligning on stage-of-game.
            for idx in candidate_rows:
                version = str(df.at[idx, "version_season"]).strip()
                challenge_id = df.at[idx, "challenge_id"]
                if pd.isna(challenge_id):
                    continue
                key = (version, int(challenge_id))
                if key not in valid_keys:
                    invalid_indices.append(idx)

            if invalid_indices:
                replacements: List[Tuple[str, int, int, int]] = []
                stage_fix_changes: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
                for idx in invalid_indices:
                    version = str(df.at[idx, "version_season"]).strip()
                    old_id = _safe_int(df.at[idx, "challenge_id"])
                    sog_value = df.at[idx, "sog_id"]
                    stage_key = (version, _safe_int(sog_value))
                    if stage_key[1] is None:
                        continue
                    candidates = stage_map.get(stage_key)
                    if not candidates or len(candidates) != 1:
                        continue
                    new_id = next(iter(candidates))
                    if old_id == new_id:
                        continue
                    original_row = df.loc[idx].to_dict()
                    df.at[idx, "challenge_id"] = new_id
                    sog_int = stage_key[1] if isinstance(stage_key[1], int) else -1
                    replacements.append((version, old_id or -1, new_id, sog_int))
                    updated_row = original_row.copy()
                    updated_row["challenge_id"] = new_id
                    stage_fix_changes.append((original_row, updated_row))

                if replacements:
                    logged: Set[Tuple[str, int, int, int]] = set()
                    for version, old_id, new_id, sog in sorted(set(replacements)):
                        if (version, old_id, new_id, sog) in logged:
                            continue
                        logger.info(
                            "Vote history remediation (stage-of-game) version=%s sog_id=%s challenge_id %s → %s",
                            version,
                            sog if sog != -1 else "unknown",
                            old_id if old_id != -1 else "None",
                            new_id,
                        )
                        logged.add((version, old_id, new_id, sog))

                    detailed_log_limit = 20
                    unique_replacements = sorted(set(replacements))
                    detailed_records = []
                    for version, old_id, new_id, sog in unique_replacements[
                        :detailed_log_limit
                    ]:
                        detailed_records.append(
                            {
                                "version_season": version,
                                "sog_id": sog if sog != -1 else None,
                                "original_challenge_id": None
                                if old_id == -1
                                else old_id,
                                "corrected_challenge_id": new_id,
                            }
                        )
                    if detailed_records:
                        logger.info(
                            "Vote history challenge remediation details (sample up to %s rows): %s",
                            detailed_log_limit,
                            detailed_records,
                        )
                    if len(unique_replacements) > detailed_log_limit:
                        logger.info(
                            "Additional vote history challenge corrections suppressed from log: %s",
                            len(unique_replacements) - detailed_log_limit,
                        )
                    if stage_fix_changes:
                        grouped_stage: Dict[
                            Tuple[Optional[str], Optional[int]],
                            List[Tuple[Dict[str, Any], Dict[str, Any]]],
                        ] = defaultdict(list)
                        for original_row, updated_row in stage_fix_changes:
                            version_key = original_row.get("version_season")
                            sog_value = original_row.get("sog_id")
                            grouped_stage[(version_key, sog_value)].append(
                                (original_row, updated_row)
                            )
                        for (_, _), pair_list in sorted(grouped_stage.items()):
                            register_data_issue(
                                dataset_name,
                                "challenge_id_remediation",
                                {
                                    "rows_corrected": int(len(pair_list)),
                                    "original_rows": [
                                        original for original, _ in pair_list
                                    ],
                                    "result_rows": [
                                        updated for _, updated in pair_list
                                    ],
                                },
                            )

                # Final validation check
                remaining_invalid = []
                for idx in candidate_rows:
                    version = str(df.at[idx, "version_season"]).strip()
                    challenge_id = df.at[idx, "challenge_id"]
                    if pd.isna(challenge_id):
                        continue
                    key = (version, int(challenge_id))
                    if key not in valid_keys:
                        remaining_invalid.append((version, int(challenge_id)))
                if remaining_invalid:
                    unique_remaining = sorted(set(remaining_invalid))
                    logger.warning(
                        "Vote history still references %s challenge IDs missing from challenge_description: %s",
                        len(unique_remaining),
                        unique_remaining[:10],
                    )
        df["challenge_id"] = pd.to_numeric(
            df.get("challenge_id"), errors="coerce"
        ).astype("Int64")
    elif dataset_name == "boot_order":
        df.rename(columns={"order": "boot_order_position"}, inplace=True)
        if "castaway_id" in df.columns:
            df["castaway_id"] = df["castaway_id"].map(_clean_castaway_id)
        if "castaway" in df.columns:
            df["castaway"] = df["castaway"].apply(
                lambda value: None
                if value is None or (isinstance(value, float) and pd.isna(value))
                else str(value).strip() or None
            )
    elif dataset_name == "auction_details":
        if "castaway_id" in df.columns:
            df["castaway_id"] = df["castaway_id"].map(_clean_castaway_id)
        if "castaway" in df.columns:
            df["castaway"] = df["castaway"].apply(
                lambda value: None
                if value is None or (isinstance(value, float) and pd.isna(value))
                else str(value).strip() or None
            )
    elif dataset_name == "castaway_scores":
        before_rows = len(df)
        if "castaway_id" in df.columns:
            df["castaway_id"] = df["castaway_id"].map(_clean_castaway_id)
        if "castaway" in df.columns:
            df["castaway"] = df["castaway"].apply(
                lambda value: None
                if value is None or (isinstance(value, float) and pd.isna(value))
                else str(value).strip() or None
            )
        unique_cols = [
            col for col in ["version_season", "castaway_id"] if col in df.columns
        ]
        if unique_cols:
            df = df.drop_duplicates(subset=unique_cols).reset_index(drop=True)
            dropped = before_rows - len(df)
            if dropped:
                logger.info(
                    "Dropped %s duplicate castaway_scores rows based on %s",
                    dropped,
                    unique_cols,
                )
                register_data_issue(
                    dataset_name,
                    "deduplicated_rows",
                    {
                        "rows_removed": int(dropped),
                        "subset_columns": unique_cols,
                        "before": int(before_rows),
                        "after": int(len(df)),
                    },
                )
    elif dataset_name == "journeys":
        if "castaway_id" in df.columns:
            df["castaway_id"] = df["castaway_id"].map(_clean_castaway_id)
            missing_mask = df["castaway_id"].isna()
            if missing_mask.any() and {"version_season", "castaway"}.issubset(
                df.columns
            ):
                reference = fetch_existing_keys(
                    f"{params.bronze_schema}.castaways",
                    conn,
                    ["castaway_id", "castaway", "version_season"],
                )
                reference_records: Dict[str, Dict[str, Any]] = {}

                def _normalise_name(text: str) -> str:
                    normalised = unicodedata.normalize("NFKD", text)
                    normalised = "".join(
                        ch for ch in normalised if not unicodedata.combining(ch)
                    )
                    return re.sub(r"[^a-z0-9]", "", normalised.lower())

                castaway_lookup: Dict[Tuple[str, str], str] = {}
                normalised_lookup: Dict[Tuple[str, str], str] = {}
                first_name_lookup: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
                normalised_first_lookup: Dict[Tuple[str, str], Set[str]] = defaultdict(
                    set
                )
                normalised_names_by_version: Dict[str, Dict[str, str]] = defaultdict(
                    dict
                )

                for row in reference.itertuples():
                    castaway_id = row.castaway_id
                    if not castaway_id:
                        continue
                    version_key = str(row.version_season).strip()
                    name_key = str(row.castaway).strip()
                    if not version_key or not name_key:
                        continue
                    lower_name = name_key.lower()
                    normalised_name = _normalise_name(name_key)
                    castaway_lookup[(version_key, lower_name)] = castaway_id
                    normalised_lookup[(version_key, normalised_name)] = castaway_id
                    normalised_names_by_version[version_key][normalised_name] = (
                        castaway_id
                    )
                    record_dict = row._asdict()
                    record_dict.pop("Index", None)
                    reference_records[str(castaway_id)] = record_dict

                    first_token = lower_name.split()[0]
                    first_token_norm = _normalise_name(first_token)
                    first_name_lookup[(version_key, first_token)].add(castaway_id)
                    normalised_first_lookup[(version_key, first_token_norm)].add(
                        castaway_id
                    )

                original_subset = df.loc[missing_mask].copy()
                fuzzy_events: List[Dict[str, Any]] = []

                def _backfill_castaway_id(row: pd.Series) -> Optional[str]:
                    version_season = str(row.get("version_season") or "").strip()
                    name_raw = str(row.get("castaway") or "").strip()
                    if not version_season or not name_raw:
                        return None
                    lower_name = name_raw.lower()
                    normalised_name = _normalise_name(name_raw)

                    direct = castaway_lookup.get((version_season, lower_name))
                    if direct:
                        return direct

                    normalised_direct = normalised_lookup.get(
                        (version_season, normalised_name)
                    )
                    if normalised_direct:
                        return normalised_direct

                    first_token = lower_name.split()[0]
                    first_token_norm = _normalise_name(first_token)

                    candidates = first_name_lookup.get((version_season, first_token))
                    if candidates and len(candidates) == 1:
                        return next(iter(candidates))

                    candidates_norm = normalised_first_lookup.get(
                        (version_season, first_token_norm)
                    )
                    if candidates_norm and len(candidates_norm) == 1:
                        return next(iter(candidates_norm))

                    version_candidates = normalised_names_by_version.get(version_season)
                    if version_candidates:
                        match = difflib.get_close_matches(
                            normalised_name,
                            list(version_candidates.keys()),
                            n=1,
                            cutoff=0.7,
                        )
                        if match:
                            matched_key = match[0]
                            matched_value = str(version_candidates[matched_key])
                            logger.warning(
                                "Journeys fuzzy match castaway_id for %s: '%s' → '%s'",
                                version_season,
                                name_raw,
                                matched_key,
                            )
                            fuzzy_events.append(
                                {
                                    "index": row.name,
                                    "version_season": version_season,
                                    "source_name": name_raw,
                                    "matched_name": matched_key,
                                    "castaway_id": matched_value,
                                    "reference_row": reference_records.get(
                                        str(matched_value)
                                    ),
                                }
                            )
                            return matched_value

                    return None

                filled_values = df.loc[missing_mask].apply(
                    _backfill_castaway_id, axis=1
                )
                df.loc[missing_mask, "castaway_id"] = filled_values
                filled_indices = filled_values[filled_values.notna()].index
                fuzzy_index_set = {
                    event["index"]
                    for event in fuzzy_events
                    if event["index"] in filled_indices
                }
                direct_indices = [
                    idx for idx in filled_indices if idx not in fuzzy_index_set
                ]

                if direct_indices:
                    reference_rows: List[Dict[str, Any]] = []
                    for idx in direct_indices:
                        assigned_id = df.at[idx, "castaway_id"]
                        ref_record = reference_records.get(str(assigned_id))
                        if ref_record:
                            reference_rows.append(ref_record)
                    register_data_issue(
                        dataset_name,
                        "castaway_id_backfilled",
                        {
                            "rows_updated": int(len(direct_indices)),
                            "available_reference_rows": int(len(reference)),
                            "original_rows": original_subset.loc[
                                direct_indices
                            ].to_dict("records"),
                            "result_rows": df.loc[direct_indices].to_dict("records"),
                            "reference_rows": reference_rows,
                        },
                    )

                for event in fuzzy_events:
                    idx = event["index"]
                    if idx not in filled_indices:
                        continue
                    original_rows = (
                        original_subset.loc[[idx]].to_dict("records")
                        if idx in original_subset.index
                        else []
                    )
                    result_rows = (
                        df.loc[[idx]].to_dict("records") if idx in df.index else []
                    )
                    register_data_issue(
                        dataset_name,
                        "castaway_id_fuzzy_backfill",
                        {
                            "rows_updated": 1,
                            "version_season": event["version_season"],
                            "source_name": event["source_name"],
                            "matched_name": event["matched_name"],
                            "castaway_id": event["castaway_id"],
                            "original_rows": original_rows,
                            "result_rows": result_rows,
                            "reference_rows": [event.get("reference_row")]
                            if event.get("reference_row")
                            else [],
                        },
                    )

        if "castaway" in df.columns:
            df["castaway"] = df["castaway"].apply(
                lambda value: None
                if value is None or (isinstance(value, float) and pd.isna(value))
                else str(value).strip() or None
            )
        if "lost_vote" in df.columns:
            df["lost_vote"] = df["lost_vote"].apply(_coerce_boolean_value)
        # Abort if any rows still lack a castaway_id after attempted backfill.
        if "castaway_id" in df.columns:
            still_missing = df["castaway_id"].isna()
            if still_missing.any():
                dropped = int(still_missing.sum())
                removed_rows_df = df.loc[still_missing].copy()
                sample_missing = (
                    removed_rows_df[["version_season", "episode", "sog_id", "event"]]
                    .head(5)
                    .to_dict("records")
                )
                logger.error(
                    "Journeys contains %s rows without castaway_id after backfill. Sample context: %s",
                    dropped,
                    sample_missing,
                )
                raise ValueError(
                    "journeys data still missing castaway_id after remediation "
                    f"({dropped} rows). Resolve upstream data before re-running."
                )

        unique_cols = [
            col
            for col in ["version_season", "episode", "sog_id", "castaway_id"]
            if col in df.columns
        ]
        if unique_cols:
            before = len(df)
            df = df.drop_duplicates(subset=unique_cols).reset_index(drop=True)
            dropped = before - len(df)
            if dropped:
                logger.info(
                    "Dropped %s duplicate journeys rows based on %s",
                    dropped,
                    unique_cols,
                )
                register_data_issue(
                    dataset_name,
                    "deduplicated_rows",
                    {
                        "rows_removed": int(dropped),
                        "subset_columns": unique_cols,
                        "before": int(before),
                        "after": int(len(df)),
                    },
                )
    elif dataset_name == "survivor_auction":
        if "castaway_id" in df.columns:
            df["castaway_id"] = df["castaway_id"].map(_clean_castaway_id)
        if "castaway" in df.columns:
            df["castaway"] = df["castaway"].apply(
                lambda value: None
                if value is None or (isinstance(value, float) and pd.isna(value))
                else str(value).strip() or None
            )
    elif dataset_name == "advantage_movement":
        if "joint_play" not in df.columns:
            df["joint_play"] = False
        else:
            df["joint_play"] = df["joint_play"].fillna(False).astype(bool)

        if "multi_target_play" not in df.columns:
            df["multi_target_play"] = False
        else:
            df["multi_target_play"] = df["multi_target_play"].fillna(False).astype(bool)

        if "co_castaway_ids" not in df.columns:
            df["co_castaway_ids"] = None

        def _split_list(value: Any) -> List[Optional[str]]:
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return [None]
            text = str(value).strip()
            if not text or text.lower() in {"nan", "none"}:
                return [None]
            parts = [part.strip() for part in text.split(",")]
            cleaned = [part for part in parts if part]
            return cleaned or [None]

        def _align_list_lengths(
            base_list: List[Optional[str]], other_list: Optional[Any]
        ) -> List[Optional[str]]:
            base = base_list if isinstance(base_list, list) else [base_list]
            if other_list is None:
                return [None] * len(base)
            if not isinstance(other_list, list):
                other_list = [other_list]
            aligned = list(other_list)
            fill_value = aligned[-1] if aligned else None
            if len(aligned) < len(base):
                aligned.extend([fill_value] * (len(base) - len(aligned)))
            elif len(aligned) > len(base):
                aligned = aligned[: len(base)]
            if not aligned:
                aligned = [None] * len(base)
            return aligned

        def _stringify_ids(entries: Optional[Any]) -> Optional[str]:
            if entries is None:
                return None
            if not isinstance(entries, list):
                entries = [entries]
            ordered: List[str] = []
            for entry in entries:
                if entry is None:
                    continue
                if isinstance(entry, list):
                    tokens = entry
                else:
                    tokens = [entry]
                for token in tokens:
                    if token is None:
                        continue
                    for part in str(token).split(","):
                        cleaned = part.strip()
                        if cleaned and cleaned not in ordered:
                            ordered.append(cleaned)
            return ", ".join(ordered) if ordered else None

        def _merge_co_strings(
            existing: Optional[str], additional: Optional[str]
        ) -> Optional[str]:
            return _stringify_ids([existing, additional])

        def _compute_co_list(values: List[Optional[str]]) -> List[Optional[str]]:
            if not isinstance(values, list):
                values = [values]
            cleaned = [val for val in values if val]
            if not cleaned:
                return [None] * len(values)
            co_values: List[Optional[str]] = []
            for val in values:
                if val:
                    others = [other for other in cleaned if other != val]
                    co_values.append(_stringify_ids(others))
                else:
                    co_values.append(None)
            return co_values or [None]

        if "castaway_id" in df.columns:
            holder_lists = df["castaway_id"].apply(_split_list)
            holder_co_lists = holder_lists.apply(_compute_co_list)

            holder_name_lists_aligned: Optional[pd.Series] = None
            if "castaway" in df.columns:
                raw_names = df["castaway"].apply(_split_list)
                holder_name_lists_aligned = pd.Series(index=df.index, dtype=object)
                for idx in df.index:
                    holder_name_lists_aligned.at[idx] = _align_list_lengths(
                        holder_lists.loc[idx], raw_names.loc[idx]
                    )

            joint_mask = holder_lists.apply(
                lambda values: len([val for val in values if val]) > 1
            )

            if joint_mask.any():
                logger.info(
                    "Splitting %s advantage_movement rows with multiple holders.",
                    int(joint_mask.sum()),
                )
                original_rows = df.loc[joint_mask].copy()
                original_rows["joint_play"] = True
                original_rows["co_castaway_ids"] = original_rows.index.map(
                    lambda idx: _stringify_ids(holder_co_lists.loc[idx])
                )
                original_rows_records = original_rows.to_dict("records")
                result_rows: List[Dict[str, Any]] = []
                for idx in holder_lists[joint_mask].index:
                    base_row = df.loc[idx].to_dict()
                    base_row["joint_play"] = True
                    holders = holder_lists.loc[idx]
                    names = (
                        holder_name_lists_aligned.loc[idx]
                        if holder_name_lists_aligned is not None
                        else None
                    )
                    co_values = holder_co_lists.loc[idx]
                    if names:
                        paired = list(zip_longest(holders, names, fillvalue=names[-1]))
                    else:
                        paired = [(holder, None) for holder in holders]
                    for position, (holder, name_value) in enumerate(paired):
                        new_row = base_row.copy()
                        new_row["castaway_id"] = holder
                        if name_value is not None:
                            new_row["castaway"] = name_value
                        co_value: Optional[str] = None
                        if isinstance(co_values, list) and position < len(co_values):
                            co_value = co_values[position]
                        if co_value is None:
                            co_value = _stringify_ids(base_row.get("co_castaway_ids"))
                        new_row["co_castaway_ids"] = co_value
                        result_rows.append(new_row)
                register_data_issue(
                    dataset_name,
                    "multi_holder_advantage_split",
                    {
                        "rows_split": int(joint_mask.sum()),
                        "original_rows": original_rows_records[
                            :REMEDIATION_DETAIL_LIMIT
                        ],
                        "result_rows": result_rows[:REMEDIATION_DETAIL_LIMIT],
                    },
                )

            df["joint_play"] = df["joint_play"] | joint_mask
            df["castaway_id"] = holder_lists
            df["co_castaway_ids"] = holder_co_lists
            if holder_name_lists_aligned is not None:
                df["castaway"] = holder_name_lists_aligned
                df = df.explode(
                    ["castaway_id", "castaway", "co_castaway_ids"]
                ).reset_index(drop=True)
            else:
                df = df.explode(["castaway_id", "co_castaway_ids"]).reset_index(
                    drop=True
                )
            df["joint_play"] = df["joint_play"].fillna(False).astype(bool)
            df["castaway_id"] = df["castaway_id"].map(_clean_castaway_id)
            df["co_castaway_ids"] = df["co_castaway_ids"].apply(_stringify_ids)

        if "played_for_id" in df.columns:
            existing_co_series = df["co_castaway_ids"].apply(_stringify_ids)
            valid_castaways = fetch_existing_keys(
                f"{params.bronze_schema}.castaway_details", conn, ["castaway_id"]
            )
            valid_castaway_ids = set(valid_castaways["castaway_id"].dropna())

            def _split_targets(value):
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    return [None]
                text = str(value).strip()
                if not text or text.lower() in {"nan", "none"}:
                    return [None]
                parts = [part.strip() for part in text.split(",")]
                cleaned = [part for part in parts if part]
                return cleaned or [None]

            target_lists = df["played_for_id"].apply(_split_targets)
            target_name_lists_aligned: Optional[pd.Series] = None
            if "played_for" in df.columns:
                raw_target_names = df["played_for"].apply(_split_targets)
                target_name_lists_aligned = pd.Series(index=df.index, dtype=object)
                for idx in df.index:
                    target_name_lists_aligned.at[idx] = _align_list_lengths(
                        target_lists.loc[idx], raw_target_names.loc[idx]
                    )
            multi_target_mask = target_lists.apply(
                lambda values: len([val for val in values if val is not None]) > 1
            )

            co_target_lists = pd.Series(index=df.index, dtype=object)
            for idx in df.index:
                targets = target_lists.loc[idx]
                cleaned_targets = [t for t in targets if t]
                existing_value = existing_co_series.loc[idx]
                row_values: List[Optional[str]] = []
                for target in targets:
                    if target:
                        others = [other for other in cleaned_targets if other != target]
                        others_str = _stringify_ids(others)
                    else:
                        others_str = None
                    row_values.append(_merge_co_strings(existing_value, others_str))
                if not row_values:
                    row_values = [_stringify_ids(existing_value)]
                co_target_lists.at[idx] = row_values

            if multi_target_mask.any():
                logger.info(
                    "Splitting %s advantage_movement rows with multiple targets.",
                    int(multi_target_mask.sum()),
                )
                original_rows = df.loc[multi_target_mask].copy()
                original_rows["multi_target_play"] = True
                original_rows["co_castaway_ids"] = original_rows.index.map(
                    lambda idx: _stringify_ids(co_target_lists.loc[idx])
                )
                original_rows_records = original_rows.to_dict("records")
                result_rows: List[Dict[str, Any]] = []
                for idx in target_lists[multi_target_mask].index:
                    base_row = df.loc[idx].to_dict()
                    base_row["multi_target_play"] = True
                    targets = target_lists.loc[idx]
                    names = (
                        target_name_lists_aligned.loc[idx]
                        if target_name_lists_aligned is not None
                        else None
                    )
                    if names:
                        paired = list(zip_longest(targets, names, fillvalue=names[-1]))
                    else:
                        paired = [(target, None) for target in targets]
                    co_values = co_target_lists.loc[idx]
                    for position, (target, name_value) in enumerate(paired):
                        new_row = base_row.copy()
                        new_row["played_for_id"] = target
                        if name_value is not None:
                            new_row["played_for"] = name_value
                        co_value: Optional[str] = None
                        if isinstance(co_values, list) and position < len(co_values):
                            co_value = co_values[position]
                        if co_value is None:
                            co_value = _stringify_ids(base_row.get("co_castaway_ids"))
                        new_row["co_castaway_ids"] = co_value
                        result_rows.append(new_row)
                register_data_issue(
                    dataset_name,
                    "multi_target_advantage_split",
                    {
                        "rows_split": int(multi_target_mask.sum()),
                        "original_rows": original_rows_records[
                            :REMEDIATION_DETAIL_LIMIT
                        ],
                        "result_rows": result_rows[:REMEDIATION_DETAIL_LIMIT],
                    },
                )

            df.loc[multi_target_mask, "multi_target_play"] = True
            df["played_for_id"] = target_lists
            df["co_castaway_ids"] = co_target_lists
            if target_name_lists_aligned is not None:
                df["played_for"] = target_name_lists_aligned
                df = df.explode(
                    ["played_for_id", "played_for", "co_castaway_ids"]
                ).reset_index(drop=True)
            else:
                df = df.explode(["played_for_id", "co_castaway_ids"]).reset_index(
                    drop=True
                )
            df["multi_target_play"] = df["multi_target_play"].fillna(False).astype(bool)
            df["co_castaway_ids"] = df["co_castaway_ids"].apply(_stringify_ids)

            df["_pre_clean_played_for_id"] = df["played_for_id"]

            def _clean_target(value):
                if value is None:
                    return None
                text = str(value).strip()
                if not text or text.lower() in {"none", "null", "nan"}:
                    return None
                cleaned = text
                if cleaned not in valid_castaway_ids:
                    logger.warning(
                        "Dropping advantage_movement target '%s' not found in castaway_details",
                        cleaned,
                    )
                    return None
                return cleaned

            df["played_for_id"] = df["played_for_id"].apply(_clean_target)
            invalid_mask = (
                df["played_for_id"].isna() & df["_pre_clean_played_for_id"].notna()
            )
            if invalid_mask.any():
                original_invalid = (
                    df.loc[invalid_mask]
                    .assign(
                        played_for_id=df.loc[invalid_mask, "_pre_clean_played_for_id"]
                    )
                    .drop(columns=["_pre_clean_played_for_id"], errors="ignore")
                )
                result_invalid = df.loc[invalid_mask].drop(
                    columns=["_pre_clean_played_for_id"], errors="ignore"
                )
                register_data_issue(
                    dataset_name,
                    "invalid_advantage_targets",
                    {
                        "rows_affected": int(len(original_invalid)),
                        "distinct_targets": sorted(
                            set(
                                str(row["played_for_id"])
                                for row in original_invalid.to_dict("records")
                            )
                        )[:10],
                        "original_rows": original_invalid.to_dict("records"),
                        "result_rows": result_invalid.to_dict("records"),
                    },
                )
            df.drop(columns=["_pre_clean_played_for_id"], inplace=True, errors="ignore")

        if "success" in df.columns:

            def _normalize_success(value):
                if pd.isna(value):
                    return None
                text = str(value).strip()
                if not text:
                    return None
                lowered = text.lower()
                if lowered in {"na", "n/a", "none"}:
                    return None
                if lowered in {"yes", "y", "true", "t", "1", "success", "successful"}:
                    return "yes"
                if lowered in {
                    "no",
                    "n",
                    "false",
                    "f",
                    "0",
                    "fail",
                    "failed",
                    "unsuccessful",
                }:
                    return "no"
                if "not" in lowered and "need" in lowered:
                    return "not needed"
                return lowered

            df["success"] = df["success"].apply(_normalize_success)
        unique_cols = [
            col
            for col in [
                "version_season",
                "castaway_id",
                "advantage_id",
                "sequence_id",
                "played_for_id",
            ]
            if col in df.columns
        ]
        if unique_cols:
            duplicate_mask = df.duplicated(subset=unique_cols, keep=False)
            if duplicate_mask.any():
                original_rows_df = df.loc[duplicate_mask]
                rows_to_remove_mask = df.duplicated(subset=unique_cols, keep="first")
                removed_rows_df = df.loc[rows_to_remove_mask]
                kept_rows_df = df.loc[duplicate_mask & ~rows_to_remove_mask]
                before = len(df)
                df = df.drop_duplicates(subset=unique_cols).reset_index(drop=True)
                dropped = before - len(df)
                if dropped:
                    samples = (
                        original_rows_df[unique_cols]
                        .drop_duplicates()
                        .head(5)
                        .to_dict("records")
                    )
                    logger.info(
                        "Dropped %s duplicate advantage_movement rows based on %s. Sample duplicates: %s",
                        dropped,
                        unique_cols,
                        samples,
                    )
                    register_data_issue(
                        dataset_name,
                        "deduplicated_rows",
                        {
                            "rows_removed": int(dropped),
                            "subset_columns": unique_cols,
                            "before": int(before),
                            "after": int(len(df)),
                            "original_rows": original_rows_df.head(
                                REMEDIATION_DETAIL_LIMIT
                            ).to_dict("records"),
                            "removed_rows": removed_rows_df.head(
                                REMEDIATION_DETAIL_LIMIT
                            ).to_dict("records"),
                            "result_rows": kept_rows_df.head(
                                REMEDIATION_DETAIL_LIMIT
                            ).to_dict("records"),
                        },
                    )
    elif dataset_name == "challenge_results":
        _ensure_challenge_description_rows(df, conn, ingest_run_id)
    elif dataset_name == "challenge_summary":
        subset_cols = [
            col
            for col in [
                "version_season",
                "challenge_id",
                "outcome_type",
                "tribe",
                "castaway_id",
                "category",
            ]
            if col in df.columns
        ]
        if subset_cols:
            dup_mask = df.duplicated(subset=subset_cols, keep=False)
            duplicate_count = int(dup_mask.sum())
            if duplicate_count:
                sample = df.loc[dup_mask, subset_cols].head(10).to_dict("records")
                logger.warning(
                    "challenge_summary contains %s duplicate rows for key %s",
                    duplicate_count,
                    subset_cols,
                )
                register_data_issue(
                    dataset_name,
                    "duplicate_rows_detected",
                    {
                        "rows": duplicate_count,
                        "subset_columns": subset_cols,
                        "sample": sample,
                    },
                )

    if "source_dataset" in df.columns:
        df["source_dataset"] = df["source_dataset"].fillna(dataset_name)
    else:
        df["source_dataset"] = dataset_name

    return df


def _align_with_schema(df: pd.DataFrame, db_schema: Dict[str, str]) -> pd.DataFrame:
    """Drop unexpected columns and align ordering with the warehouse table."""
    db_cols = list(db_schema.keys())
    extra_cols = set(df.columns) - set(db_cols)
    if extra_cols:
        logger.info(
            "Dropping extra columns not present in target table: %s", extra_cols
        )
        df = df.drop(columns=list(extra_cols))

    ordered_cols = [col for col in db_cols if col in df.columns]
    return df[ordered_cols]


def validate_schema(
    df: pd.DataFrame, table_name: str, conn: connection
) -> SchemaValidationResult:
    """Compare dataframe columns against the database table definition."""
    schema, table = _split_table_reference(table_name)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                column_name,
                data_type,
                EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.table_name = c.table_name
                      AND tc.table_schema = c.table_schema
                      AND tc.constraint_type = 'PRIMARY KEY'
                      AND kcu.column_name = c.column_name
                ) AS is_primary_key
            FROM information_schema.columns c
            WHERE table_name = %s
              AND table_schema = %s;
            """,
            (table, schema),
        )
        db_schema_info = cur.fetchall()

    df = _normalize_column_names(df)
    sheet_cols = set(df.columns)

    db_schema: Dict[str, str] = {}
    db_pks: set[str] = set()
    for col, dtype, is_pk in db_schema_info:
        db_schema[col] = dtype
        if is_pk:
            db_pks.add(col)

    db_cols = set(db_schema.keys())
    missing = {col for col in db_cols - sheet_cols if col not in db_pks}
    extra = sheet_cols - db_cols

    type_issues: Dict[str, Tuple[str, str]] = {}
    type_map = {
        "character varying": "object",
        "text": "object",
        "boolean": "bool",
        "integer": "int",
        "bigint": "int",
        "double precision": "float",
        "numeric": "float",
        "real": "float",
        "date": "datetime",
        "timestamp without time zone": "datetime",
        "timestamp with time zone": "datetime",
        "uuid": "object",
    }
    for col in df.columns:
        if col in db_schema:
            expected = type_map.get(db_schema[col], None)
            actual = str(df[col].dtype)
            if expected and expected not in actual.lower():
                if not (expected == "datetime" and actual.lower() == "object"):
                    type_issues[col] = (expected, actual)

    is_valid = not missing and not type_issues
    return SchemaValidationResult(
        is_valid=is_valid,
        missing_columns=missing,
        extra_columns=extra,
        type_mismatches=type_issues,
        db_schema=db_schema,
    )


def load_dataset_to_table(
    dataset_name: str,
    table_name: str,
    conn: connection,
    ingest_run_id: str,
    unique_constraint_columns: Optional[List[str]] = None,
    truncate: Optional[bool] = None,
    force_refresh: bool = False,
) -> None:
    """Load a survivoR dataset into the target table with optional upsert semantics."""
    if not params.base_raw_url:
        raise ValueError("Base raw URL for GitHub source is not configured.")

    if ingest_run_id is None:
        raise ValueError(
            f"ingest_run_id is required when loading dataset '{dataset_name}'."
        )

    logger.info("Loading dataset '%s' into table '%s'", dataset_name, table_name)
    df, source_type = load_dataset(
        dataset_name,
        params.base_raw_url,
        params.json_raw_url,
        force_refresh=force_refresh,
    )
    logger.info(
        "Dataset '%s' loaded from %s source.", dataset_name, source_type.upper()
    )
    df = _apply_dataset_specific_rules(dataset_name, df, conn, ingest_run_id)
    df = _apply_unique_key_deduplication(dataset_name, table_name, df)
    db_schema = get_db_column_types(table_name, conn)
    validate_bronze_dataset(dataset_name, df, db_schema=db_schema)
    # Add ingest_run_id if needed
    if (
        ingest_run_id
        and "ingest_run_id" in db_schema
        and "ingest_run_id" not in df.columns
    ):
        df["ingest_run_id"] = str(ingest_run_id)

    # Add ingested_at if required by schema and missing
    if "ingested_at" in db_schema and "ingested_at" not in df.columns:
        df["ingested_at"] = pd.Timestamp.utcnow()

    df = preprocess_dataframe(df, db_schema, dataset_name=dataset_name)

    validation = validate_schema(df, table_name, conn)
    if validation.missing_columns or validation.type_mismatches:
        _raise_schema_mismatch(dataset_name, table_name, validation)

    if validation.extra_columns:
        _note_extra_columns(dataset_name, table_name, validation.extra_columns)

    df = _align_with_schema(df, validation.db_schema)

    boolean_columns = [
        col
        for col, dtype in validation.db_schema.items()
        if dtype == "boolean" and col in df.columns
    ]
    if boolean_columns:
        for col in boolean_columns:
            df[col] = df[col].apply(
                lambda value: None if value is None or pd.isna(value) else bool(value)
            )

    if truncate is None:
        truncate = params.truncate_on_load

    if truncate and params.environment == "prod":
        logger.warning(
            "Prod environment detected — converting truncate load for %s into append/upsert mode.",
            table_name,
        )
        truncate = False

    if truncate:
        truncate_table(table_name, conn)
        logger.info("Truncated table %s", table_name)

    if df.empty:
        logger.info("No new rows to insert for %s", table_name)
        return

    if not unique_constraint_columns and not truncate:
        logger.warning(
            "No unique constraint columns configured for %s; operating in append-only mode.",
            table_name,
        )

    try:
        inserted, updated, inserted_keys, updated_keys = _upsert_dataframe(
            conn=conn,
            table_name=table_name,
            df=df,
            conflict_columns=unique_constraint_columns or [],
        )
        _log_upsert_summary(
            table_name=table_name,
            inserted=inserted,
            updated=updated,
            inserted_keys=inserted_keys,
            updated_keys=updated_keys,
        )
    except Exception as exc:
        logger.error("Bulk load failed for %s: %s", table_name, exc)
        raise RuntimeError(f"Row insertion failed for {table_name}") from exc


def _raise_schema_mismatch(
    dataset_name: str,
    table_name: str,
    validation: SchemaValidationResult,
) -> None:
    """Log detailed schema differences and raise a descriptive exception."""
    details: List[str] = []
    remediation: List[str] = []

    if validation.missing_columns:
        missing_cols = sorted(validation.missing_columns)
        details.append(f"missing columns: {missing_cols}")
        logger.error(
            "Dataset '%s' is missing required columns for %s: %s",
            dataset_name,
            table_name,
            missing_cols,
        )
        remediation.append(
            "Verify whether survivoR renamed or removed these columns. Update the bronze DDL, 'Database/table_config.json', and downstream dbt models if the change is expected."
        )

    if validation.type_mismatches:
        details.append(f"type mismatches: {validation.type_mismatches}")
        logger.error(
            "Dataset '%s' column types differ from %s: %s",
            dataset_name,
            table_name,
            validation.type_mismatches,
        )
        remediation.append(
            "Upstream types shifted. Adjust casting in 'preprocess_dataframe' or alter the warehouse column type to match the new format."
        )

    summary = "; ".join(details) if details else "schema mismatch"
    action = " ".join(remediation) if remediation else "Review upstream changes."
    logger.error("Next steps: %s", action)

    notify_schema_event(
        event_type="schema-mismatch",
        dataset=dataset_name,
        table=table_name,
        summary=summary,
        remediation=action,
        labels=["schema-drift", "upstream-change"],
    )

    raise SchemaMismatchError(
        f"Schema mismatch detected for dataset '{dataset_name}' → table '{table_name}': {summary}"
    )


def _normalize_record_value(value):
    """Translate pandas/numpy scalars into psycopg2-friendly Python types."""
    if value is None:
        return None
    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, pd.Timedelta):
        return value.to_pytimedelta()

    if isinstance(value, np.datetime64):
        return pd.to_datetime(value).to_pydatetime()
    if isinstance(value, np.timedelta64):
        return pd.to_timedelta(value).to_pytimedelta()

    if isinstance(value, BOOL_LIKE_TYPES):
        return bool(value)
    if isinstance(value, (np.integer, np.floating)):
        return value.item()

    if isinstance(value, np.generic):
        return value.item()

    return value


def _upsert_dataframe(
    conn: connection,
    table_name: str,
    df: pd.DataFrame,
    conflict_columns: Sequence[str],
) -> Tuple[int, int, List[Tuple], List[Tuple]]:
    """Insert or update dataframe rows using ON CONFLICT logic."""
    schema, table = _split_table_reference(table_name)
    columns = list(df.columns)
    records = [
        tuple(_normalize_record_value(val) for val in row)
        for row in df.itertuples(index=False, name=None)
    ]

    with conn.cursor() as cur:
        insert_sql = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(col) for col in columns),
        )

        if conflict_columns:
            update_cols = [col for col in columns if col not in conflict_columns]
            conflict_sql = sql.SQL(", ").join(
                sql.Identifier(col) for col in conflict_columns
            )
            if update_cols:
                update_assignments = sql.SQL(", ").join(
                    sql.Composed(
                        [
                            sql.Identifier(col),
                            sql.SQL(" = EXCLUDED."),
                            sql.Identifier(col),
                        ]
                    )
                    for col in update_cols
                )
                insert_sql += sql.SQL(" ON CONFLICT ({}) DO UPDATE SET {}").format(
                    conflict_sql, update_assignments
                )
            else:
                insert_sql += sql.SQL(" ON CONFLICT ({}) DO NOTHING").format(
                    conflict_sql
                )

        returning_columns = conflict_columns if conflict_columns else [columns[0]]
        insert_sql += sql.SQL(" RETURNING {}").format(
            sql.SQL(", ").join(
                [sql.Identifier(col) for col in returning_columns]
                + [sql.SQL("(xmax = 0) AS inserted_flag")]
            )
        )

        query = insert_sql.as_string(cur)
        results = execute_values(cur, query, records, page_size=1000, fetch=True)

    conn.commit()

    inserted_keys: List[Tuple] = []
    updated_keys: List[Tuple] = []
    if results:
        key_length = len(returning_columns)
        for row in results:
            key = tuple(row[:key_length])
            inserted_flag = bool(row[-1])
            if inserted_flag:
                inserted_keys.append(key)
            else:
                updated_keys.append(key)

    return len(inserted_keys), len(updated_keys), inserted_keys, updated_keys


def _log_upsert_summary(
    table_name: str,
    inserted: int,
    updated: int,
    inserted_keys: List[Tuple],
    updated_keys: List[Tuple],
) -> None:
    """Emit structured logs describing the outcome of an upsert."""
    logger.info(
        "Upsert complete for %s — inserted: %s, updated: %s",
        table_name,
        inserted,
        updated,
    )

    if inserted_keys:
        sample = inserted_keys[:10]
        logger.debug(
            "Example newly inserted keys for %s (showing up to 10): %s",
            table_name,
            sample,
        )
    if updated_keys:
        sample = updated_keys[:10]
        logger.debug(
            "Example updated keys for %s (showing up to 10): %s",
            table_name,
            sample,
        )


def register_ingestion_run(
    conn: connection,
    environment: str,
    git_branch: Optional[str],
    git_commit: Optional[str],
    source_url: Optional[str],
) -> str:
    """Insert a new ingestion run record and return its identifier."""
    run_id = str(uuid4())
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bronze.ingestion_runs (run_id, environment, git_branch, git_commit, source_url)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (run_id, environment, git_branch, git_commit, source_url),
        )
    conn.commit()
    return run_id


def finalize_ingestion_run(
    conn: connection,
    run_id: str,
    status: str,
    notes: Optional[str] = None,
) -> None:
    """Update the ingestion run status and completion timestamp."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE bronze.ingestion_runs
               SET status = %s,
                   run_finished_at = CURRENT_TIMESTAMP,
                   notes = COALESCE(%s, notes)
             WHERE run_id = %s
            """,
            (status, notes, run_id),
        )
    conn.commit()


def run_schema_sql(conn: connection) -> None:
    """Recreate warehouse schemas using the canonical SQL definition."""
    logger.info("Creating (or refreshing) warehouse schemas from SQL script...")

    schemas_to_reset = {"bronze", "silver", "gold"}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT nspname
            FROM pg_namespace
            WHERE nspname = ANY(%s)
            """,
            (list(schemas_to_reset),),
        )
        existing = {row[0] for row in cur.fetchall()}
        for schema_name in existing:
            cur.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE;").format(
                    sql.Identifier(schema_name)
                )
            )
        conn.commit()

    schema_path = Path("Database/create_tables.sql")
    schema_sql = schema_path.read_text()

    statements = [s.strip() for s in schema_sql.split(";") if s.strip()]
    with conn.cursor() as cur:
        for stmt in statements:
            try:
                cur.execute(stmt + ";")
            except Exception as exc:
                logger.error(
                    "Failed to execute SQL statement:\n%s\nError: %s", stmt, exc
                )
                raise
        conn.commit()

    logger.info("Schema creation complete.")


def get_unique_constraint_cols_from_table_name(table_name: str) -> List[str]:
    """Return the configured unique constraint columns for a given table."""
    table_config_keys = [
        key
        for key, value in params.table_config.items()
        if isinstance(value, dict) and value.get("table_name") == table_name
    ]

    assert len(table_config_keys), (
        "There should only be one key per table in the table_config"
    )
    table_config_key = table_config_keys[0]

    return params.table_config[table_config_key]["unique_constraint_columns"]


def schema_exists(conn: connection, schema_name: str = "bronze") -> bool:
    """Check whether a schema currently has at least one table defined."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s
                LIMIT 1
            );
            """,
            (schema_name,),
        )
        return cur.fetchone()[0]


def import_table_to_df(table_name: str) -> Optional[pd.DataFrame]:
    """Import an entire table into a pandas DataFrame."""
    logger.info("Importing table %s into DataFrame", table_name)
    try:
        engine = create_sql_engine()
        schema, table = _split_table_reference(table_name)
        query = f'SELECT * FROM "{schema}"."{table}"'
        return pd.read_sql(query, con=engine)
    except Exception as exc:
        logger.error("Error importing table %s: %s", table_name, exc)
        return None


def import_query_to_df(query: str) -> Optional[pd.DataFrame]:
    """Execute an arbitrary SQL query and return the result as a DataFrame."""
    logger.info("Executing custom SQL query")
    try:
        engine = create_sql_engine()
        return pd.read_sql(query, con=engine)
    except Exception as exc:
        logger.error("Error executing query: %s", exc)
        return None
