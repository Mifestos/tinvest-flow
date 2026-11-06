"""Runner для SQL-миграций ClickHouse."""

import logging
from pathlib import Path

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from tinvest_flow.config import settings

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path("/opt/airflow/migrations/clickhouse")


def get_ch_client() -> Client:
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password.get_secret_value(),
        database="default",
    )


def apply_clickhouse_migrations() -> None:
    """Применяет отсутствующие SQL-миграции из папки."""
    ch = get_ch_client()

    ch.command("CREATE DATABASE IF NOT EXISTS gold")
    ch.command("""
        CREATE TABLE IF NOT EXISTS gold.schema_migrations (
            version String,
            applied_at DateTime DEFAULT now()
        ) ENGINE = MergeTree ORDER BY version
    """)

    applied = {row[0] for row in ch.query("SELECT version FROM gold.schema_migrations").result_rows}

    for file in sorted(MIGRATIONS_DIR.glob("V*.sql")):
        version = file.name.split("__")[0]  # "V1"
        if version in applied:
            continue

        logger.info(f"Applying ClickHouse migration: {file.name}")
        sql_content = file.read_text(encoding="utf-8")

        for statement in sql_content.split(";"):
            if statement.strip():
                ch.command(statement)

        ch.command(f"INSERT INTO gold.schema_migrations (version) VALUES ('{version}')")
        logger.info(f"Successfully applied {version}")

    ch.close()
