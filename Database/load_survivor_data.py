# ruff: noqa: E402

import logging
import sys
from pathlib import Path

# Add the base directory to sys.path
base_dir = Path(__file__).resolve().parent.parent
if str(base_dir) not in sys.path:
    sys.path.append(str(base_dir))

# Repo imports
import params  # noqa: E402
from gamebot_core.db_utils import (  # noqa: E402
    connect_to_db,
    load_dataset_to_table,
    run_schema_sql,
    get_unique_constraint_cols_from_table_name,
    register_ingestion_run,
    finalize_ingestion_run,
)
from gamebot_core.log_utils import setup_logging  # noqa: E402
from gamebot_core.env import (  # noqa: E402
    current_git_branch,
    current_git_commit,
    require_prod_on_main,
)
from gamebot_core.validation import (  # noqa: E402
    finalise_validation_reports,
    set_validation_run,
)

setup_logging(logging.DEBUG)
logger = logging.getLogger(__name__)


def main():
    """Entry point that loads survivoR datasets into the bronze schema."""
    require_prod_on_main(params.environment)
    conn = connect_to_db()
    if not conn:
        logger.error("Database connection failed. Exiting.")
        return

    # Ensure schemas exist before any table operations
    run_schema_sql(conn)

    branch = current_git_branch()
    commit = current_git_commit()

    run_id = register_ingestion_run(
        conn=conn,
        environment=params.environment,
        git_branch=branch,
        git_commit=commit,
        source_url=params.base_raw_url,
    )
    set_validation_run(run_id)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT set_config('survivor.environment', %s, true)",
            (params.environment,),
        )
        if branch:
            cur.execute("SELECT set_config('survivor.git_branch', %s, true)", (branch,))
        if commit:
            cur.execute("SELECT set_config('survivor.git_commit', %s, true)", (commit,))
    conn.commit()

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
                ingest_run_id=run_id,
            )
        except Exception:
            logger.exception(
                "Error loading dataset '%s' into '%s'", dataset_name, table_name
            )
            raise

    report_path = finalise_validation_reports(run_identifier=run_id)
    if report_path:
        logger.info("Data quality report saved to %s", report_path)

    finalize_ingestion_run(conn, run_id, "succeeded")

    conn.close()
    logger.info("ETL process complete.")
    return run_id


if __name__ == "__main__":
    main()
