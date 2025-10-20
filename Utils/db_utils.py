import logging
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection
from sqlalchemy import create_engine

# Add the base directory to sys.path so `params` can be imported reliably
base_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(base_dir))

import params
from Utils.github_data_loader import load_dataset
from Utils.log_utils import setup_logging

setup_logging(logging.INFO)
logger = logging.getLogger(__name__)


def _split_table_reference(table_name: str) -> Tuple[str, str]:
    if "." in table_name:
        schema, table = table_name.split(".", 1)
    else:
        schema, table = "public", table_name
    return schema, table


def connect_to_db() -> Optional[connection]:
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
    port_part = f":{params.port}" if params.port else ""
    url = f"postgresql://{params.db_user}:{params.db_pass}@{params.db_host}{port_part}/{params.db_name}"
    return create_engine(url)


def truncate_table(table_name: str, conn: connection) -> None:
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
    schema, table = _split_table_reference(table_name)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
              AND table_schema = %s;
            """,
            (table, schema),
        )
        return {row[0]: row[1] for row in cur.fetchall()}


def preprocess_dataframe(df: pd.DataFrame, db_schema: Dict[str, str]) -> pd.DataFrame:
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
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df


def _apply_dataset_specific_rules(dataset_name: str, df: pd.DataFrame, conn: connection) -> pd.DataFrame:
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
    db_cols = list(db_schema.keys())
    extra_cols = set(df.columns) - set(db_cols)
    if extra_cols:
        logger.info("Dropping extra columns not present in target table: %s", extra_cols)
        df = df.drop(columns=list(extra_cols))

    ordered_cols = [col for col in db_cols if col in df.columns]
    return df[ordered_cols]


def validate_schema(df: pd.DataFrame, table_name: str, conn: connection) -> bool:
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

    if missing:
        logger.warning("Missing columns for %s: %s", table_name, missing)
    if extra:
        logger.warning("Extra columns in dataset %s: %s", table_name, extra)

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
    }
    for col in df.columns:
        if col in db_schema:
            expected = type_map.get(db_schema[col], None)
            actual = str(df[col].dtype)
            if expected and expected not in actual.lower():
                if not (expected == "datetime" and actual.lower() == "object"):
                    type_issues[col] = (expected, actual)

    if type_issues:
        logger.warning("Data type mismatches in %s: %s", table_name, type_issues)

    return not extra and not type_issues


def load_dataset_to_table(
    dataset_name: str,
    table_name: str,
    conn: connection,
    unique_constraint_columns: Optional[List[str]] = None,
    truncate: Optional[bool] = None,
    force_refresh: bool = False,
) -> None:
    if not params.base_raw_url:
        raise ValueError("Base raw URL for GitHub source is not configured.")

    logger.info("Loading dataset '%s' into table '%s'", dataset_name, table_name)
    df = load_dataset(dataset_name, params.base_raw_url, force_refresh=force_refresh)
    df = _apply_dataset_specific_rules(dataset_name, df, conn)

    db_schema = get_db_column_types(table_name, conn)

    if not validate_schema(df, table_name, conn):
        raise ValueError(f"Schema mismatch in {table_name}, halting load.")

    df = _align_with_schema(df, db_schema)
    df = preprocess_dataframe(df, db_schema)

    if truncate is None:
        truncate = params.truncate_on_load

    if truncate:
        truncate_table(table_name, conn)
        logger.info("Truncated table %s", table_name)

    if unique_constraint_columns and not truncate:
        existing = fetch_existing_keys(table_name, conn, unique_constraint_columns)
        if not existing.empty:
            df = (
                df.merge(existing, on=unique_constraint_columns, how="left", indicator=True)
                .query("_merge == 'left_only'")
                .drop(columns=["_merge"])
            )

    if df.empty:
        logger.info("No new rows to insert for %s", table_name)
        return

    schema, table = _split_table_reference(table_name)

    try:
        engine = create_sql_engine()
        df.to_sql(
            name=table,
            schema=schema,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )
        logger.info("Inserted %s rows into %s", len(df), table_name)
    except Exception as exc:
        logger.error("Bulk insert failed for %s: %s", table_name, exc)
        raise RuntimeError(f"Row insertion failed for {table_name}") from exc


def run_schema_sql(conn: connection) -> None:
    logger.info("Creating (or refreshing) warehouse schemas from SQL script...")

    with conn.cursor() as cur:
        for schema_name in ("gold", "silver", "bronze"):
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
    table_config_keys = [
        key
        for key, value in params.table_config.items()
        if isinstance(value, dict) and value.get("table_name") == table_name
    ]

    assert len(table_config_keys), "There should only be one key per table in the table_config"
    table_config_key = table_config_keys[0]

    return params.table_config[table_config_key]["unique_constraint_columns"]


def schema_exists(conn: connection, schema_name: str = "bronze") -> bool:
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
    logger.info("Executing custom SQL query")
    try:
        engine = create_sql_engine()
        return pd.read_sql(query, con=engine)
    except Exception as exc:
        logger.error("Error executing query: %s", exc)
        return None
