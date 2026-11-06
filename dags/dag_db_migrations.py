"""DAG для применения миграций Alembic."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_alembic_upgrade() -> None:
    from alembic import command
    from alembic.config import Config

    alembic_cfg = Config("/opt/airflow/alembic.ini")
    command.upgrade(alembic_cfg, "head")


with DAG(
    dag_id="db_migrations",
    default_args={"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=2)},
    description="Apply Alembic migrations to PostgreSQL",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tbank", "maintenance", "migrations"],
) as dag:
    PythonOperator(task_id="alembic_upgrade_head", python_callable=run_alembic_upgrade)
