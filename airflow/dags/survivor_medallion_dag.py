import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from Database.load_survivor_data import main as load_bronze_layer

SQL_DIR = REPO_ROOT / "Database" / "sql"


def _read_sql(filename: str) -> str:
    return (SQL_DIR / filename).read_text()


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
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["survivor", "medallion", "ml"],
) as dag:

    load_bronze = PythonOperator(
        task_id="load_bronze_layer",
        python_callable=load_bronze_layer,
    )

    refresh_silver_dimensions = PostgresOperator(
        task_id="refresh_silver_dimensions",
        postgres_conn_id="survivor_postgres",
        sql=_read_sql("refresh_silver_dimensions.sql"),
    )

    refresh_silver_facts = PostgresOperator(
        task_id="refresh_silver_facts",
        postgres_conn_id="survivor_postgres",
        sql=_read_sql("refresh_silver_facts.sql"),
    )

    refresh_gold_features = PostgresOperator(
        task_id="refresh_gold_features",
        postgres_conn_id="survivor_postgres",
        sql=_read_sql("refresh_gold_features.sql"),
    )

    load_bronze >> refresh_silver_dimensions >> refresh_silver_facts >> refresh_gold_features
