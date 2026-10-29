"""Слой Silver: Загрузка данных из MinIO (Bronze) в PostgreSQL."""

import json
import logging

import psycopg2
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from psycopg2.extras import execute_values
from tenacity import retry, stop_after_attempt, wait_exponential

from tinvest_flow.config import settings

logger = logging.getLogger(__name__)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def load_candles_to_postgres(ds: str) -> None:
    """Читает JSON из S3 за дату ds и выполняет UPSERT в dds.dds_candles."""
    figis = ["BBG004730N88", "BBG004730RP0"]
    interval = "5min"

    s3 = S3Hook(aws_conn_id="aws_default")

    conn = psycopg2.connect(
        host=settings.postgres_dwh_host,
        port=settings.postgres_dwh_port,
        dbname=settings.postgres_dwh_db,
        user=settings.postgres_dwh_user,
        password=settings.postgres_dwh_password.get_secret_value(),
    )

    rows_to_insert = []

    for figi in figis:
        key = f"candles/{figi}/{interval}/{ds}/data.json"
        try:
            data = json.loads(s3.read_key(key=key, bucket_name="bronze"))
            rows_to_insert.extend(
                [
                    (
                        r["figi"],
                        r["interval"],
                        r["time"],
                        r["open"],
                        r["high"],
                        r["low"],
                        r["close"],
                        r["volume"],
                        r["is_complete"],
                    )
                    for r in data
                ]
            )
        except Exception as e:
            logger.warning("S3 read failed for %s: %s", key, e)

    if not rows_to_insert:
        logger.info("No data to insert for date %s", ds)
        return

    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO dds.dds_candles 
                    (figi, interval, candle_time, open, high, low, close, volume, is_complete)
                VALUES %s
                ON CONFLICT (figi, interval, candle_time) DO UPDATE SET
                    open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                    close = EXCLUDED.close, volume = EXCLUDED.volume,
                    is_complete = EXCLUDED.is_complete, loaded_at = NOW();
                """,
                rows_to_insert,
            )
        conn.commit()
        logger.info("Upserted %d candles into dds.dds_candles", len(rows_to_insert))
    except Exception as e:
        logger.error("Postgres insert failed: %s", e)
        conn.rollback()
        raise
    finally:
        conn.close()
