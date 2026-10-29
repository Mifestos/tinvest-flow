"""DAG: Построение витрины данных в ClickHouse (Gold Layer)."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from tinvest_flow.processing.silver_to_gold import build_market_activity_mart

with DAG(
    dag_id="silver_to_gold_marts",
    default_args={"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=5)},
    description="Aggregate Silver to ClickHouse Data Marts",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tbank", "gold", "clickhouse"],
) as dag:
    
    PythonOperator(
        task_id="build_market_activity",
        python_callable=build_market_activity_mart,
    )