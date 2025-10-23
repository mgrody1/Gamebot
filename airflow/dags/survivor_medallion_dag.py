# ruff: noqa: E402

import sys
from datetime import datetime, timedelta
from pathlib import Path

import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

# Ensure the repo root (where `Database/` lives) is importable both locally and in containers.
for candidate in Path(__file__).resolve().parents:
    if (candidate / "Database").exists():
        if str(candidate) not in sys.path:
            sys.path.append(str(candidate))
        break

import params  # noqa: E402

from Database.load_survivor_data import main as load_bronze_layer  # noqa: E402
from Utils.data_freshness import (  # noqa: E402
    detect_dataset_changes,
    persist_metadata,
    upsert_dataset_metadata,
)

DEFAULT_SCHEDULE = os.getenv("GAMEBOT_DAG_SCHEDULE", "0 4 * * 1")

default_args = {
    "owner": "survivor-analytics",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="survivor_medallion_pipeline",
    default_args=default_args,
    description="Survivor data Medallion architecture ETL",
    schedule=DEFAULT_SCHEDULE,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["survivor", "medallion", "ml"],
) as dag:

    def _target_allows(stage: str) -> bool:
        order = {"bronze": 0, "silver": 1, "gold": 2}
        desired = os.getenv("GAMEBOT_TARGET_LAYER", "gold").lower()
        desired = desired if desired in order else "gold"
        return order[desired] >= order[stage]

    def _detect_updates(**context) -> bool:
        dataset_names = [dataset["dataset"] for dataset in params.dataset_order]
        metadata, changed = detect_dataset_changes(
            dataset_names,
            params.base_raw_url,
            params.json_raw_url,
        )
        if changed:
            context["ti"].xcom_push(key="new_metadata", value=metadata)
            context["ti"].xcom_push(key="changed_datasets", value=list(changed.keys()))
            return True
        dag.log.info("No dataset changes detected; skipping pipeline run")
        return False

    def _log_changed_datasets(**context) -> None:
        changed = (
            context["ti"].xcom_pull(key="changed_datasets", task_ids="gate_new_data")
            or []
        )
        if changed:
            dag.log.info("Detected updates for datasets: %s", ", ".join(changed))

    def _persist_metadata_task(**context) -> None:
        metadata = (
            context["ti"].xcom_pull(key="new_metadata", task_ids="gate_new_data") or {}
        )
        if not metadata:
            return

        run_id = context["ti"].xcom_pull(task_ids="load_bronze_layer")
        persist_metadata(metadata)
        try:
            upsert_dataset_metadata(metadata, run_id)
        except Exception:
            dag.log.exception(
                "Failed to persist dataset metadata into bronze.dataset_versions"
            )

    gate_new_data = ShortCircuitOperator(
        task_id="gate_new_data",
        python_callable=_detect_updates,
    )

    log_updates = PythonOperator(
        task_id="log_new_datasets",
        python_callable=_log_changed_datasets,
    )

    load_bronze = PythonOperator(
        task_id="load_bronze_layer",
        python_callable=load_bronze_layer,
    )

    silver_gate = ShortCircuitOperator(
        task_id="gate_silver",
        python_callable=lambda: _target_allows("silver"),
    )

    dbt_build_silver = BashOperator(
        task_id="dbt_build_silver",
        bash_command="cd /opt/airflow && dbt deps --project-dir dbt --profiles-dir dbt && dbt build --project-dir dbt --profiles-dir dbt --select silver",
    )

    gold_gate = ShortCircuitOperator(
        task_id="gate_gold",
        python_callable=lambda: _target_allows("gold"),
    )

    dbt_build_gold = BashOperator(
        task_id="dbt_build_gold",
        bash_command="cd /opt/airflow && dbt build --project-dir dbt --profiles-dir dbt --select gold",
    )

    persist_metadata_op = PythonOperator(
        task_id="persist_dataset_metadata",
        python_callable=_persist_metadata_task,
    )

    (
        gate_new_data
        >> log_updates
        >> load_bronze
        >> silver_gate
        >> dbt_build_silver
        >> gold_gate
        >> dbt_build_gold
        >> persist_metadata_op
    )
