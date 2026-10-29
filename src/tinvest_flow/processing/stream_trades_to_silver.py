"""Обработка потока сделок из Kafka в PostgreSQL (слой Silver) с помощью Spark."""
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from tinvest_flow.config import settings

logger = logging.getLogger(__name__)

TRADE_SCHEMA = StructType([
    StructField("trade_id", StringType()),
    StructField("order_id", StringType()),
    StructField("account_id", StringType()),
    StructField("instrument_uid", StringType()),
    StructField("figi", StringType()),
    StructField("direction", StringType()),
    StructField("price_units", LongType()),
    StructField("price_nano", LongType()),
    StructField("quantity", LongType()),
    StructField("date_time", TimestampType()),
])


def _upsert_to_postgres(batch_df, batch_id: int) -> None:
    """Записывает микробатч в PostgreSQL, выполняя upsert (обновление или вставку)."""
    if batch_df.rdd.isEmpty():
        return

    # Вычисляем десятичную цену из units и nano
    df = batch_df.withColumn(
        "price_rub",
        (F.col("price_units") + F.col("price_nano") / F.lit(1_000_000_000)).cast(DecimalType(18, 9))
    ).drop("price_units", "price_nano")

    df.createOrReplaceTempView("stg_trades")

    # Выполняем транзакцию через psycopg2 для поддержки upsert
    import psycopg2
    
    conn = psycopg2.connect(
        host=settings.postgres_dwh_host,
        port=settings.postgres_dwh_port,
        dbname=settings.postgres_dwh_db,
        user=settings.postgres_dwh_user,
        password=settings.postgres_dwh_password.get_secret_value(),
    )
    
    try:
        with conn.cursor() as cur:
            # Создаем временную таблицу для текущего батча
            cur.execute("""
                CREATE TEMP TABLE IF NOT EXISTS temp_trades (
                    trade_id VARCHAR(64) PRIMARY KEY,
                    order_id VARCHAR(64),
                    account_id VARCHAR(64),
                    instrument_uid VARCHAR(64),
                    figi VARCHAR(32),
                    direction VARCHAR(8),
                    price_rub NUMERIC(18, 9),
                    quantity BIGINT,
                    date_time TIMESTAMPTZ
                ) ON COMMIT DROP;
            """)
            
            # Вставляем данные батча во временную таблицу
            # Примечание: В production для ускорения следует использовать COPY
            rows = df.collect()
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO temp_trades VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trade_id) DO NOTHING;
                    """,
                    (row.trade_id, row.order_id, row.account_id, row.instrument_uid,
                     row.figi, row.direction, row.price_rub, row.quantity, row.date_time)
                )
            
            # Сливаем данные из временной таблицы в основную
            cur.execute("""
                INSERT INTO dds.dds_trades
                SELECT * FROM temp_trades
                ON CONFLICT (trade_id) DO UPDATE SET
                    price_rub = EXCLUDED.price_rub,
                    quantity = EXCLUDED.quantity;
            """)
        conn.commit()
    finally:
        conn.close()


def run_streaming_job() -> None:
    """Инициализирует и запускает Spark-джобу для потоковой обработки."""
    spark = (
        SparkSession.builder
        .appName("trades_kafka_to_silver")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints/trades_silver")
        .getOrCreate()
    )

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", settings.kafka_topic_trades)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_stream = (
        raw_stream
        .select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("date_time", F.to_timestamp("date_time"))
    )

    query = (
        parsed_stream.writeStream
        .foreachBatch(_upsert_to_postgres)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    from tinvest_flow.common.logging import setup_logging
    setup_logging()
    run_streaming_job()