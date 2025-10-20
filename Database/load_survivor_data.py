import logging
import sys
from pathlib import Path

# Add the base directory to sys.path
base_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(base_dir))

# Repo imports
import params
from Utils.db_utils import (
    connect_to_db,
    load_dataset_to_table,
    run_schema_sql,
    get_unique_constraint_cols_from_table_name,
    schema_exists
)
from Utils.log_utils import setup_logging

setup_logging(logging.DEBUG)
logger = logging.getLogger(__name__)


def main():
    conn = connect_to_db()
    if not conn:
        logger.error("Database connection failed. Exiting.")
        return

    if params.first_run or not schema_exists(conn, params.bronze_schema):
        logger.info("Initializing warehouse schemas via DDL.")
        run_schema_sql(conn)

    for dataset in params.dataset_order:
        dataset_name = dataset["dataset"]
        table_name = dataset["table_name"]
        unique_cols = get_unique_constraint_cols_from_table_name(table_name)
        truncate = dataset.get("truncate", params.truncate_on_load)
        force_refresh = dataset.get("force_refresh", False)

        logger.info(
            "Loading dataset '%s' into table '%s' (truncate=%s, force_refresh=%s)",
            dataset_name,
            table_name,
            truncate,
            force_refresh,
        )
        try:
            load_dataset_to_table(
                dataset_name=dataset_name,
                table_name=table_name,
                conn=conn,
                unique_constraint_columns=unique_cols,
                truncate=truncate,
                force_refresh=force_refresh,
            )
        except Exception as e:
            logger.error(f"Error loading table '{table_name}': {e}")
            conn.close()
            raise

    conn.close()
    logger.info("ETL process complete.")


if __name__ == "__main__":
    main()
