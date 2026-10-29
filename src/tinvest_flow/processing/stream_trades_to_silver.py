"""Обработка потока сделок из Kafka в PostgreSQL (слой Silver)."""

import logging

import psycopg2
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from tinvest_flow.common.logging import setup_logging
from tinvest_flow.config import settings

logger = logging.getLogger(__name__)

TRADE_SCHEMA = StructType(
    [
        StructField("trade_id", StringType()),
        StructField("order_id", StringType()),
        StructField("account_id", StringType()),
        StructField("instrument_uid", StringType()),
        StructField("figi", StringType()),
        StructField("direction", StringType()),
        StructField("price_units", LongType()),
        StructField("price_nano", LongType()),
        StructField("quantity", LongType()),
        StructField("date_time", StringType()),
        StructField("_corrupt_record", StringType()),
    ]
)


def _upsert_to_postgres(batch_df: DataFrame, batch_id: int) -> None:
    """Записывает микробатч в PostgreSQL с использованием изолированной staging-таблицы."""
    if batch_df.rdd.isEmpty():
        return

    df = batch_df.withColumn(
        "price_rub",
        (F.col("price_units") + F.col("price_nano") / F.lit(1_000_000_000)).cast(
            DecimalType(18, 9)
        ),
    ).drop("price_units", "price_nano")

    staging_table = f"stg_trades_{batch_id}"

    df.write.format("jdbc").option("url", settings.postgres_jdbc_url).option(
        "dbtable", staging_table
    ).option("user", settings.postgres_dwh_user).option(
        "password", settings.postgres_dwh_password.get_secret_value()
    ).option("driver", "org.postgresql.Driver").mode("overwrite").save()

    conn = psycopg2.connect(
        host=settings.postgres_dwh_host,
        port=settings.postgres_dwh_port,
        dbname=settings.postgres_dwh_db,
        user=settings.postgres_dwh_user,
        password=settings.postgres_dwh_password.get_secret_value(),
    )

    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO dds.dds_trades
                SELECT trade_id, order_id, account_id, instrument_uid, figi, 
                       direction, price_rub, quantity, date_time
                FROM {staging_table}
                ON CONFLICT (trade_id, trade_datetime) DO UPDATE SET  
                    price_rub = EXCLUDED.price_rub,
                    quantity = EXCLUDED.quantity;
            """)
            cur.execute(f"DROP TABLE {staging_table};")  # Очистка
        conn.commit()
        logger.info(f"Batch {batch_id}: upserted into dds.dds_trades.")
    except Exception as e:
        logger.error(f"Batch {batch_id}: upsert failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def run_streaming_job() -> None:
    """Запуск Spark Structured Streaming с обработкой DLQ."""
    spark = (
        SparkSession.builder.appName("trades_kafka_to_silver")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints/trades_silver")
        .getOrCreate()
    )

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", settings.kafka_topic_trades)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_stream = raw_stream.select(
        F.from_json(F.col("value").cast("string"), TRADE_SCHEMA, {"mode": "PERMISSIVE"}).alias(
            "data"
        )
    ).select("data.*")

    good_df = parsed_stream.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
    bad_df = parsed_stream.filter(F.col("_corrupt_record").isNotNull())

    good_query = (  # noqa: F841
        good_df.writeStream.foreachBatch(_upsert_to_postgres)
        .option("checkpointLocation", "/tmp/checkpoints/trades_silver_good")
        .trigger(processingTime="10 seconds")
        .start()
    )

    dlq_query = (  # noqa: F841
        bad_df.select(F.col("_corrupt_record").alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("topic", "raw_trades_dlq")
        .option("checkpointLocation", "/tmp/checkpoints/trades_silver_dlq")
        .start()
    )

    logger.info("Streaming jobs started (Good + DLQ).")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    setup_logging()
    run_streaming_job()
