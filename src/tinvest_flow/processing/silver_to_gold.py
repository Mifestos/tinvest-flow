"""Агрегация данных из Silver (PostgreSQL) в Gold (ClickHouse)."""

import logging

import clickhouse_connect
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

from tinvest_flow.config import settings

logger = logging.getLogger(__name__)

CHUNK_SIZE = 100_000


def build_market_activity_mart() -> None:
    """Строит витрину mart_market_activity, читая данные чанками (защита от OOM)."""
    query = """
        SELECT
            figi,
            date_trunc('day', candle_time) AS minute,
            (array_agg(open ORDER BY candle_time ASC))[1]  AS open_price,
            max(high)  AS high_price,
            min(low)   AS low_price,
            (array_agg(close ORDER BY candle_time DESC))[1] AS close_price,
            sum(volume) AS volume,
            count(*)    AS trade_count,
            avg(volume) AS avg_trade_size
        FROM dds.dds_candles
        GROUP BY figi, minute
    """

    pg_conn = psycopg2.connect(
        host=settings.postgres_dwh_host,
        port=settings.postgres_dwh_port,
        dbname=settings.postgres_dwh_db,
        user=settings.postgres_dwh_user,
        password=settings.postgres_dwh_password.get_secret_value(),
    )

    ch_client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password.get_secret_value(),
        database=settings.clickhouse_db,
    )

    try:
        with pg_conn.cursor(name="chunk_cursor", cursor_factory=RealDictCursor) as cur:
            cur.itersize = CHUNK_SIZE
            cur.execute(query)

            total_rows = 0
            while True:
                rows = cur.fetchmany(CHUNK_SIZE)
                if not rows:
                    break

                df = pd.DataFrame(rows)
                columns = [
                    "figi",
                    "minute",
                    "open_price",
                    "high_price",
                    "low_price",
                    "close_price",
                    "volume",
                    "trade_count",
                    "avg_trade_size",
                ]

                ch_client.insert_df("gold.mart_market_activity", df[columns])
                total_rows += len(rows)
                logger.info(f"Inserted chunk of {len(rows)} rows. Total: {total_rows}")

        logger.info(f"Successfully loaded {total_rows} rows into ClickHouse.")
    except Exception as e:
        logger.error(f"Failed to build Gold mart: {e}")
        raise
    finally:
        pg_conn.close()
        ch_client.close()
