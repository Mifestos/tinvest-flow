"""DAG: Автоматическое управление партициями в PostgreSQL."""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def get_next_months(start_date: datetime, n: int) -> list[tuple[datetime, datetime]]:
    """Возвращает список из n пар (начало_месяца, конец_месяца) начиная со следующего месяца."""
    months = []
    current = start_date.replace(day=1)
    for _ in range(n):
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1)
        else:
            next_month = current.replace(month=current.month + 1)
        months.append((current, next_month))
        current = next_month
    return months


def create_future_partitions(**context) -> None:
    """Создает партиции для dds_trades и dds_candles на 3 месяца вперед."""
    pg = PostgresHook(postgres_conn_id="dwh_postgres")

    start_date = context["data_interval_start"]
    months = get_next_months(start_date, 3)

    queries = []
    for month_start, month_end in months:
        start_str = month_start.strftime("%Y-%m-%d")
        end_str = month_end.strftime("%Y-%m-%d")
        suffix = month_start.strftime("%Y_%m")

        queries.extend(
            [
                f"CREATE TABLE IF NOT EXISTS dds.dds_trades_{suffix} "
                f"PARTITION OF dds.dds_trades "
                f"FOR VALUES FROM ('{start_str}') TO ('{end_str}');",
                f"CREATE TABLE IF NOT EXISTS dds.dds_candles_{suffix} "
                f"PARTITION OF dds.dds_candles "
                f"FOR VALUES FROM ('{start_str}') TO ('{end_str}');",
            ]
        )

    conn = pg.get_conn()
    try:
        with conn.cursor() as cur:
            for q in queries:
                cur.execute(q)
                logger.info(f"Executed: {q}")
        conn.commit()
        logger.info("Successfully checked/created partitions for 3 months ahead.")
    finally:
        conn.close()


with DAG(
    dag_id="manage_partitions",
    default_args={"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=5)},
    description="Создание партиций на 3 месяца вперед (запускается ежедневно)",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tbank", "maintenance", "postgres"],
) as dag:
    PythonOperator(
        task_id="create_future_partitions",
        python_callable=create_future_partitions,
    )
