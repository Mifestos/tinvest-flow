"""DAG: MinIO to PostgreSQL DWH."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from tinvest_flow.processing.bronze_to_silver import load_candles_to_postgres


def run_silver_load(**context) -> None:
    ds = context["ds"]
    load_candles_to_postgres(ds=ds)


with DAG(
    dag_id="bronze_to_silver_candles",
    default_args={"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=5)},
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tbank", "silver", "postgres"],
) as dag:
    PythonOperator(task_id="upsert_candles", python_callable=run_silver_load)
