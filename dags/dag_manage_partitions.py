"""DAG: Автоматическое управление партициями в PostgreSQL."""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def create_next_month_partitions(**context) -> None:
    """Создает партиции для dds_trades и dds_candles на следующий месяц."""
    pg = PostgresHook(postgres_conn_id="dwh_postgres")

    next_month = (context["data_interval_start"] + timedelta(days=32)).replace(day=1)
    month_start = next_month.strftime("%Y-%m-%d")
    month_end = (next_month + timedelta(days=32)).replace(day=1).strftime("%Y-%m-%d")
    suffix = next_month.strftime("%Y_%m")

    queries = [
        (
            f"CREATE TABLE IF NOT EXISTS dds.dds_trades_{suffix} "
            f"PARTITION OF dds.dds_trades "
            f"FOR VALUES FROM ('{month_start}') TO ('{month_end}');"
        ),
        (
            f"CREATE TABLE IF NOT EXISTS dds.dds_candles_{suffix} "
            f"PARTITION OF dds.dds_candles "
            f"FOR VALUES FROM ('{month_start}') TO ('{month_end}');"
        ),
    ]

    conn = pg.get_conn()
    try:
        with conn.cursor() as cur:
            for q in queries:
                cur.execute(q)
                logger.info(f"Executed: {q}")
        conn.commit()
    finally:
        conn.close()


with DAG(
    dag_id="manage_partitions",
    default_args={"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=5)},
    description="Создание партиций на следующий месяц",
    schedule="0 0 25 * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tbank", "maintenance", "postgres"],
) as dag:
    PythonOperator(
        task_id="create_next_month_partitions",
        python_callable=create_next_month_partitions,
    )
