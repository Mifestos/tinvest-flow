"""DAG: Построение витрины данных в ClickHouse (Gold Layer)."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from tinvest_flow.common.clickhouse_migrations import apply_clickhouse_migrations
from tinvest_flow.common.data_quality import run_candles_dq_check
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
    run_migrations = PythonOperator(
        task_id="clickhouse_migrations", python_callable=apply_clickhouse_migrations
    )

    dq_check = PythonOperator(
        task_id="dq_check_candles",
        python_callable=run_candles_dq_check,
    )

    build_mart = PythonOperator(
        task_id="build_market_activity", python_callable=build_market_activity_mart
    )

    run_migrations >> dq_check >> build_mart
