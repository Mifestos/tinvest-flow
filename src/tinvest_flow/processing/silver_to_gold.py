"""Агрегация данных из Silver в Gold (ClickHouse)."""
import logging

import clickhouse_connect
from airflow.providers.postgres.hooks.postgres import PostgresHook

from tinvest_flow.config import settings

logger = logging.getLogger(__name__)


def build_market_activity_mart() -> None:
    """Строит витрину mart_market_activity в ClickHouse."""
    pg = PostgresHook(postgres_conn_id="dwh_postgres")

    query = """
        SELECT
            figi,
            date_trunc('day', candle_time) AS trading_day,
            (array_agg(open ORDER BY candle_time ASC))[1]  AS open_price,
            max(high)  AS high_price,
            min(low)   AS low_price,
            (array_agg(close ORDER BY candle_time DESC))[1] AS close_price,
            sum(volume) AS volume,
            count(*)    AS trade_count,
            avg(volume) AS avg_trade_size
        FROM dds.dds_candles
        GROUP BY figi, trading_day
    """
    df = pg.get_pandas_df(query)
    if df.empty:
        logger.info("No data in Silver layer to aggregate.")
        return

    ch = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password.get_secret_value(),
        database=settings.clickhouse_db,
    )

    df = df.rename(columns={"trading_day": "minute"})
    columns = [
        "figi", "minute", "open_price", "high_price", "low_price",
        "close_price", "volume", "trade_count", "avg_trade_size",
    ]
    
    ch.insert_df("gold.mart_market_activity", df[columns])
    logger.info("Loaded %d aggregated rows into ClickHouse.", len(df))
    ch.close()