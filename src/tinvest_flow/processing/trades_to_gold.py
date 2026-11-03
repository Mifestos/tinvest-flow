"""Агрегация данных по сделкам из Silver (PostgreSQL) в Gold (ClickHouse)."""

import logging
from datetime import UTC, datetime, timedelta

import clickhouse_connect
import pandas as pd
from sqlalchemy import create_engine

from tinvest_flow.config import settings

logger = logging.getLogger(__name__)


def build_trade_activity_mart(days_back: int = 1) -> None:
    """Строит витрину mart_trade_activity за последние N дней (для инкрементальности)."""

    start_date = (datetime.now(UTC) - timedelta(days=days_back)).strftime("%Y-%m-%d")

    query = """
        SELECT
            figi,
            DATE(trade_datetime) AS trade_date,
            SUM(quantity) AS total_volume,
            COUNT(*) AS total_trades,
            SUM(
                CASE WHEN direction IN ('DIRECTION_BUY', 'BUY') 
                THEN quantity ELSE 0 END
            ) AS buy_volume,
            SUM(
                CASE WHEN direction IN ('DIRECTION_SELL', 'SELL') 
                THEN quantity ELSE 0 END
            ) AS sell_volume,
            SUM(price_rub * quantity) / SUM(quantity) AS vwap,
            MAX(price_rub) AS max_price,
            MIN(price_rub) AS min_price
        FROM dds.dds_trades
        WHERE trade_datetime >= %(start_date)s
        GROUP BY figi, trade_date
    """

    engine = create_engine(
        f"postgresql+psycopg2://{settings.postgres_dwh_user}:"
        f"{settings.postgres_dwh_password.get_secret_value()}@"
        f"{settings.postgres_dwh_host}:{settings.postgres_dwh_port}/"
        f"{settings.postgres_dwh_db}"
    )

    try:
        df = pd.read_sql_query(query, engine, params={"start_date": start_date})
    finally:
        engine.dispose()

    if df.empty:
        logger.info("No trades found for the last %d days.", days_back)
        return

    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["vwap"] = df["vwap"].astype("float64")
    df["max_price"] = df["max_price"].astype("float64")
    df["min_price"] = df["min_price"].astype("float64")

    ch_client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password.get_secret_value(),
        database=settings.clickhouse_db,
    )

    try:
        ch_client.insert_df("gold.mart_trade_activity", df)
        logger.info("Successfully loaded %d rows into gold.mart_trade_activity", len(df))
    except Exception as e:
        logger.error("Failed to load trades to Gold: %s", e)
        raise
    finally:
        ch_client.close()
