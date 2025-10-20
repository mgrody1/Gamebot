import logging
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection
from psycopg2.extras import execute_values
from sqlalchemy import create_engine

# Add the base directory to sys.path so `params` can be imported reliably
base_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(base_dir))

import params
from Utils.github_data_loader import load_dataset
from Utils.log_utils import setup_logging

setup_logging(logging.INFO)
logger = logging.getLogger(__name__)


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

            if db_col_type == "boolean":
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .map(
                        {
                            "true": True,
                            "t": True,
                            "yes": True,
                            "y": True,
                            "1": True,
                            "false": False,
                            "f": False,
                            "no": False,
                            "n": False,
                            "0": False,
                            "nan": None,
                        }
                    )
                    .astype("boolean")
                )
            elif db_col_type in {"integer", "bigint"}:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif db_col_type in {"double precision", "numeric", "real"}:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif db_col_type == "date":
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
            elif db_col_type in {"timestamp without time zone", "timestamp with time zone"}:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif db_col_type == "uuid":
                df[col] = df[col].astype(str)
            else:
                df[col] = df[col].astype(str)

            coerced_rows = original_non_null & df[col].isna()
            if coerced_rows.any():
                coerced_log[col] = df[coerced_rows].index.tolist()
        except Exception as exc:
            logger.warning("Could not convert column %s to %s: %s", col, db_col_type, exc)

    datetime_cols = df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns
    for col in datetime_cols:
        df[col] = df[col].astype(object).where(pd.notnull(df[col]), None)

    for col, indices in coerced_log.items():
        sample = indices[:10]
        suffix = "..." if len(indices) > 10 else ""
        logger.warning("Coercion occurred in column '%s' for rows: %s%s", col, sample, suffix)

    df.replace(["NaT", "nan", "NaN"], np.nan, inplace=True)
    df = df.where(pd.notnull(df), None)

    return df


def fetch_existing_keys(table_name: str, conn: connection, key_columns: Iterable[str]) -> pd.DataFrame:
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


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise dataframe column names to snake_case for warehouse alignment."""
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df


def _apply_dataset_specific_rules(dataset_name: str, df: pd.DataFrame, conn: connection) -> pd.DataFrame:
    """Apply per-dataset cleansing rules prior to schema validation."""
    df = _normalize_column_names(df)

    if dataset_name == "castaways":
        df.rename(columns={"order": "castaways_order"}, inplace=True)
        existing_ids = fetch_existing_keys(f"{params.bronze_schema}.castaway_details", conn, ["castaway_id"])
        if not existing_ids.empty:
            valid_ids = set(existing_ids["castaway_id"].dropna())
            before = len(df)
            df = df[df["castaway_id"].isin(valid_ids)]
            dropped = before - len(df)
            if dropped:
                logger.warning(
                    "Skipped %s castaways due to missing castaway_id in castaway_details", dropped
                )
    elif dataset_name == "boot_mapping":
        df.rename(columns={"order": "boot_mapping_order"}, inplace=True)
    elif dataset_name == "vote_history":
        df.rename(columns={"order": "vote_history_order"}, inplace=True)
    elif dataset_name == "advantage_movement":
        if "castaway_id" in df.columns:
            df = df[~df["castaway_id"].astype(str).str.contains(",", na=False)]

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
        logger.info("Dropping extra columns not present in target table: %s", extra_cols)
        df = df.drop(columns=list(extra_cols))

    ordered_cols = [col for col in db_cols if col in df.columns]
    return df[ordered_cols]


def validate_schema(df: pd.DataFrame, table_name: str, conn: connection) -> SchemaValidationResult:
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

    is_valid = not missing and not extra and not type_issues
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
    unique_constraint_columns: Optional[List[str]] = None,
    truncate: Optional[bool] = None,
    force_refresh: bool = False,
    ingest_run_id: Optional[str] = None,
) -> None:
    """Load a survivoR dataset into the target table with optional upsert semantics."""
    if not params.base_raw_url:
        raise ValueError("Base raw URL for GitHub source is not configured.")

    logger.info("Loading dataset '%s' into table '%s'", dataset_name, table_name)
    df = load_dataset(dataset_name, params.base_raw_url, force_refresh=force_refresh)
    df = _apply_dataset_specific_rules(dataset_name, df, conn)

    db_schema = get_db_column_types(table_name, conn)
    if ingest_run_id and "ingest_run_id" in db_schema and "ingest_run_id" not in df.columns:
        df["ingest_run_id"] = str(ingest_run_id)

    validation = validate_schema(df, table_name, conn)
    if not validation.is_valid:
        _raise_schema_mismatch(dataset_name, table_name, validation)

    df = _align_with_schema(df, validation.db_schema)
    df = preprocess_dataframe(df, validation.db_schema)

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
    details = []
    if validation.missing_columns:
        details.append(f"missing columns: {sorted(validation.missing_columns)}")
        logger.error(
            "Dataset '%s' is missing required columns for %s: %s",
            dataset_name,
            table_name,
            validation.missing_columns,
        )
    if validation.extra_columns:
        details.append(f"unexpected columns: {sorted(validation.extra_columns)}")
        logger.error(
            "Dataset '%s' contains unexpected columns for %s: %s",
            dataset_name,
            table_name,
            validation.extra_columns,
        )
    if validation.type_mismatches:
        details.append(f"type mismatches: {validation.type_mismatches}")
        logger.error(
            "Dataset '%s' column types differ from %s: %s",
            dataset_name,
            table_name,
            validation.type_mismatches,
        )
    raise SchemaMismatchError(
        f"Schema mismatch detected for dataset '{dataset_name}' → table '{table_name}': "
        + "; ".join(details)
    )


def _upsert_dataframe(
    conn: connection,
    table_name: str,
    df: pd.DataFrame,
    conflict_columns: Sequence[str],
) -> Tuple[int, int, List[Tuple], List[Tuple]]:
    """Insert or update dataframe rows using ON CONFLICT logic."""
    schema, table = _split_table_reference(table_name)
    columns = list(df.columns)
    records = [tuple(row) for row in df.itertuples(index=False, name=None)]

    with conn.cursor() as cur:
        insert_sql = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(col) for col in columns),
        )

        if conflict_columns:
            update_cols = [col for col in columns if col not in conflict_columns]
            conflict_sql = sql.SQL(", ").join(sql.Identifier(col) for col in conflict_columns)
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
                insert_sql += sql.SQL(" ON CONFLICT ({}) DO NOTHING").format(conflict_sql)

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
    run_id = str(uuid.uuid4())
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
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE;").format(sql.Identifier(schema_name))
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
                logger.error("Failed to execute SQL statement:\n%s\nError: %s", stmt, exc)
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

    assert len(table_config_keys), "There should only be one key per table in the table_config"
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
