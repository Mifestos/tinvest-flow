"""DAG: Построение витрины по сделкам в ClickHouse (Gold Layer)."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from tinvest_flow.processing.trades_to_gold import build_trade_activity_mart

with DAG(
    dag_id="trades_to_gold_marts",
    default_args={"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=5)},
    description="Aggregate Trades Silver to ClickHouse Data Marts",
    schedule="0 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tbank", "gold", "clickhouse"],
) as dag:
    PythonOperator(
        task_id="build_trade_activity",
        python_callable=build_trade_activity_mart,
        op_kwargs={"days_back": 2},
    )
