# ruff: noqa: E402

import logging
import sys
from dataclasses import dataclass
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
from .validation import validate_bronze_dataset  # noqa: E402
from .log_utils import setup_logging  # noqa: E402
from .notifications import notify_schema_event  # noqa: E402

setup_logging(logging.INFO)
logger = logging.getLogger(__name__)

BOOL_LIKE_TYPES = tuple(
    t
    for t in (bool, getattr(np, "bool_", None), getattr(np, "bool8", None))
    if t is not None
)


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


def preprocess_dataframe(df: pd.DataFrame, db_schema: Dict[str, str]) -> pd.DataFrame:
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
        sample = indices[:10]
        suffix = "..." if len(indices) > 10 else ""
        logger.warning(
            "Coercion occurred in column '%s' for rows: %s%s", col, sample, suffix
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
    stub_df = preprocess_dataframe(stub_df, db_schema)

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
        df.rename(columns={"order": "castaways_order"}, inplace=True)
        existing_ids = fetch_existing_keys(
            f"{params.bronze_schema}.castaway_details", conn, ["castaway_id"]
        )
        if not existing_ids.empty:
            valid_ids = set(existing_ids["castaway_id"].dropna())
            before = len(df)
            df = df[df["castaway_id"].isin(valid_ids)]
            dropped = before - len(df)
            if dropped:
                logger.warning(
                    "Skipped %s castaways due to missing castaway_id in castaway_details",
                    dropped,
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
                sample_events = {
                    str(v) for v in df.loc[missing_mask, "vote_event"].fillna("unknown")
                }
                logger.warning(
                    "Vote history contains %s rows without castaway_id (vote_event=%s) — keeping them as null",  # noqa: E501
                    dropped,
                    ", ".join(sorted(sample_events)[:3]),
                )
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
        subset_cols = [
            col
            for col in [
                "version_season",
                "auction_num",
                "item",
                "castaway",
                "castaway_id",
            ]
            if col in df.columns
        ]
        if subset_cols:
            before = len(df)
            df = df.drop_duplicates(subset=subset_cols).reset_index(drop=True)
            dropped = before - len(df)
            if dropped:
                logger.info(
                    "Dropped %s duplicate auction_details rows based on %s",
                    dropped,
                    subset_cols,
                )
    elif dataset_name == "castaway_scores":
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
            before = len(df)
            df = df.drop_duplicates(subset=unique_cols).reset_index(drop=True)
            dropped = before - len(df)
            if dropped:
                logger.info(
                    "Dropped %s duplicate castaway_scores rows based on %s",
                    dropped,
                    unique_cols,
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
                castaway_lookup = {
                    (
                        str(row.version_season).strip(),
                        str(row.castaway).strip().lower(),
                    ): row.castaway_id
                    for row in reference.itertuples()
                    if row.castaway_id
                }

                def _backfill_castaway_id(row: pd.Series) -> Optional[str]:
                    version_season = str(row.get("version_season") or "").strip()
                    name = str(row.get("castaway") or "").strip().lower()
                    if not version_season or not name:
                        return None
                    return castaway_lookup.get((version_season, name))

                df.loc[missing_mask, "castaway_id"] = df.loc[missing_mask].apply(
                    _backfill_castaway_id, axis=1
                )

        if "castaway" in df.columns:
            df["castaway"] = df["castaway"].apply(
                lambda value: None
                if value is None or (isinstance(value, float) and pd.isna(value))
                else str(value).strip() or None
            )
        if "lost_vote" in df.columns:
            df["lost_vote"] = df["lost_vote"].apply(_coerce_boolean_value)
        # Drop any rows that still lack a castaway_id after backfilling.
        if "castaway_id" in df.columns:
            still_missing = df["castaway_id"].isna()
            if still_missing.any():
                dropped = int(still_missing.sum())
                logger.warning(
                    "Journeys contains %s rows without castaway_id after backfill — dropping them.",
                    dropped,
                )
                df = df[~still_missing].reset_index(drop=True)

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
        if "castaway_id" in df.columns:
            df = df[~df["castaway_id"].astype(str).str.contains(",", na=False)]
        if "played_for_id" in df.columns:
            valid_castaways = fetch_existing_keys(
                f"{params.bronze_schema}.castaway_details", conn, ["castaway_id"]
            )
            valid_castaway_ids = set(valid_castaways["castaway_id"].dropna())

            needs_split_mask = (
                df["played_for_id"].astype(str).str.contains(",", na=False)
            )
            if needs_split_mask.any():
                logger.info(
                    "Splitting %s advantage_movement rows with multiple targets",
                    int(needs_split_mask.sum()),
                )

            def _split_targets(value):
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    return [None]
                text = str(value).strip()
                if not text or text.lower() in {"nan", "none"}:
                    return [None]
                parts = [part.strip() for part in text.split(",")]
                cleaned = [part for part in parts if part]
                return cleaned or [None]

            df["played_for_id"] = df["played_for_id"].apply(_split_targets)
            df = df.explode("played_for_id").reset_index(drop=True)

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
            for col in ["version_season", "castaway_id", "advantage_id", "sequence_id"]
            if col in df.columns
        ]
        if unique_cols:
            before = len(df)
            df = df.drop_duplicates(subset=unique_cols).reset_index(drop=True)
            dropped = before - len(df)
            if dropped:
                logger.info(
                    "Dropped %s duplicate advantage_movement rows based on %s",
                    dropped,
                    unique_cols,
                )
    elif dataset_name == "challenge_results":
        _ensure_challenge_description_rows(df, conn, ingest_run_id)

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
    validate_bronze_dataset(dataset_name, df)

    db_schema = get_db_column_types(table_name, conn)
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

    df = preprocess_dataframe(df, db_schema)

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
        logger.debug("Inserted key samples for %s: %s", table_name, sample)
    if updated_keys:
        sample = updated_keys[:10]
        logger.debug("Updated key samples for %s: %s", table_name, sample)


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
