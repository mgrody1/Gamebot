import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
import os

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from Database.load_survivor_data import main as load_bronze_layer

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
    schedule="0 4 * * 1",  # Early Monday UTC to capture weekend data entry
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
    bash_command="cd /opt/airflow && pipenv run dbt deps --project-dir dbt --profiles-dir dbt && pipenv run dbt build --project-dir dbt --profiles-dir dbt --select silver",
)

gold_gate = ShortCircuitOperator(
    task_id="gate_gold",
    python_callable=lambda: _target_allows("gold"),
)

dbt_build_gold = BashOperator(
    task_id="dbt_build_gold",
    bash_command="cd /opt/airflow && pipenv run dbt build --project-dir dbt --profiles-dir dbt --select gold",
)

load_bronze >> silver_gate >> dbt_build_silver >> gold_gate >> dbt_build_gold
