"""DAG: Построение витрины данных в ClickHouse (Gold Layer)."""
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import clickhouse_connect
from tinvest_flow.config import settings

logger = logging.getLogger(__name__)


def build_market_activity_mart(**context) -> None:
    """Агрегирует данные из Postgres и загружает в ClickHouse."""
    pg = PostgresHook(postgres_conn_id="dwh_postgres")
    
    query = """
        SELECT 
            figi,
            date_trunc('day', candle_time) AS trading_day,
            (array_agg(open ORDER BY candle_time ASC))[1] AS open_price,
            max(high) AS high_price,
            min(low) AS low_price,
            (array_agg(close ORDER BY candle_time DESC))[1] AS close_price,
            sum(volume) AS volume,
            count(*) AS trade_count,
            avg(volume) AS avg_trade_size
        FROM dds.dds_candles
        GROUP BY figi, trading_day
    """
    
    df = pg.get_pandas_df(query)
    if df.empty:
        logger.info("No data in Silver layer to aggregate.")
        return

    ch_client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
         username="default",   
        password="",          
        database=settings.clickhouse_db
    )

    df = df.rename(columns={"trading_day": "minute"})
    columns = ["figi", "minute", "open_price", "high_price", "low_price", "close_price", "volume", "trade_count", "avg_trade_size"]
    
    ch_client.insert_df("gold.mart_market_activity", df[columns])
    logger.info(f"Loaded {len(df)} aggregated rows into ClickHouse Gold layer.")
    ch_client.close()


with DAG(
    dag_id="silver_to_gold_marts",
    default_args={"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=5)},
    description="Aggregate Silver to ClickHouse Data Marts",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tbank", "gold", "clickhouse"],
) as dag:

    PythonOperator(
        task_id="build_market_activity",
        python_callable=build_market_activity_mart,
    )