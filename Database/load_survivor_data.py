import logging
import sys
from pathlib import Path
from subprocess import CalledProcessError, check_output

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
    schema_exists,
    register_ingestion_run,
    finalize_ingestion_run,
)
from Utils.log_utils import setup_logging

setup_logging(logging.DEBUG)
logger = logging.getLogger(__name__)


def _current_git_branch() -> str | None:
    """Return the current git branch or None if it cannot be determined."""
    try:
        return check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"], text=True).strip()
    except (CalledProcessError, FileNotFoundError, PermissionError, OSError):
        return None


def _current_git_commit() -> str | None:
    """Return the current git commit SHA or None if it cannot be determined."""
    try:
        return check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except (CalledProcessError, FileNotFoundError, PermissionError, OSError):
        return None


def _validate_environment() -> None:
    """Ensure environment-specific constraints (e.g., prod runs on main branch)."""
    env_name = params.environment
    logger.info("Loader running in '%s' environment.", env_name)
    if env_name == "prod":
        branch = _current_git_branch()
        if branch is None:
            raise RuntimeError(
                "Unable to determine git branch while running in prod environment. "
                "Ensure git is available inside the runtime."
            )
        if branch != "main":
            raise RuntimeError(
                f"Prod loads must be executed from the 'main' branch (current branch: {branch})."
            )


def main():
    """Entry point that loads survivoR datasets into the bronze schema."""
    _validate_environment()
    conn = connect_to_db()
    if not conn:
        logger.error("Database connection failed. Exiting.")
        return

    try:
        if params.first_run or not schema_exists(conn, params.bronze_schema):
            logger.info("Initializing warehouse schemas via DDL.")
            run_schema_sql(conn)

        run_id: str | None = None
        branch = _current_git_branch()
        commit = _current_git_commit()

        run_id = register_ingestion_run(
            conn=conn,
            environment=params.environment,
            git_branch=branch,
            git_commit=commit,
            source_url=params.base_raw_url,
        )

        with conn.cursor() as cur:
            cur.execute("SELECT set_config('survivor.environment', %s, true)", (params.environment,))
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
            except Exception as exc:
                logger.exception("Error loading dataset '%s' into '%s'", dataset_name, table_name)
                raise

        finalize_ingestion_run(conn, run_id, "succeeded")
    except Exception:
        if conn and 'run_id' in locals() and run_id:
            finalize_ingestion_run(conn, run_id, "failed")
        raise
    finally:
        conn.close()
        logger.info("ETL process complete.")


if __name__ == "__main__":
    main()
