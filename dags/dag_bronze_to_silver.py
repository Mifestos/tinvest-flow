"""DAG: Загрузка данных из MinIO (Bronze) в PostgreSQL (Silver)."""
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


def load_candles_to_postgres(**context) -> None:
    """Читает JSON из S3 и выполняет UPSERT в dds.dds_candles."""
    ds = context["ds"]
    figis = ["BBG004730N88", "BBG004730RP0"]
    interval = "5min"
    
    s3 = S3Hook(aws_conn_id="aws_default")
    pg = PostgresHook(postgres_conn_id="dwh_postgres")
    
    rows = []
    
    for figi in figis:
        key = f"candles/{figi}/{interval}/{ds}/data.json"
        try:
            data = json.loads(s3.read_key(key=key, bucket_name="bronze"))
            rows.extend([
                (
                    r["figi"], r["interval"], r["time"],
                    r["open"], r["high"], r["low"], r["close"],
                    r["volume"], r["is_complete"]
                )
                for r in data
            ])
        except Exception as e:
            logger.warning("S3 read failed for %s: %s", key, e)

    if not rows:
        logger.info("No data to insert for date %s", ds)
        return

    conn = pg.get_conn()
    try:
        with conn.cursor() as cur:
            # UPSERT гарантирует идемпотентность при перезапусках DAG
            execute_values(
                cur,
                """
                INSERT INTO dds.dds_candles 
                    (figi, interval, candle_time, open, high, low, close, volume, is_complete)
                VALUES %s
                ON CONFLICT (figi, interval, candle_time) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    is_complete = EXCLUDED.is_complete,
                    loaded_at = NOW();
                """,
                rows
            )
        conn.commit()
        logger.info("Upserted %d candles into dds.dds_candles", len(rows))
    except Exception as e:
        logger.error("Postgres insert failed: %s", e)
        conn.rollback()
        raise
    finally:
        conn.close()


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_to_silver_candles",
    default_args=default_args,
    description="MinIO to PostgreSQL DWH",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tbank", "silver", "postgres"],
) as dag:

    PythonOperator(
        task_id="upsert_candles",
        python_callable=load_candles_to_postgres,
    )