"""Слой Silver: Загрузка данных из MinIO (Bronze) в PostgreSQL."""

import json
import logging

import psycopg2
from botocore.exceptions import ClientError
from psycopg2.extras import execute_values
from tenacity import retry, stop_after_attempt, wait_exponential

from tinvest_flow.common.s3 import get_s3_client
from tinvest_flow.config import settings

logger = logging.getLogger(__name__)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def load_candles_to_postgres(ds: str) -> None:
    """Читает JSON из S3 за дату ds и выполняет UPSERT в dds.dds_candles."""
    figis = ["BBG004730N88", "BBG004730RP0"]
    interval = "5min"

    s3 = get_s3_client()

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
            response = s3.get_object(Bucket=settings.minio_bucket_bronze, Key=key)
            data = json.loads(response["Body"].read().decode("utf-8"))
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
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.warning("S3 object not found: %s", key)
            else:
                logger.error("S3 read failed for %s: %s", key, e)

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
