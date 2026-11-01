"""DAG: Управление Spark Structured Streaming джобой через SparkSubmitOperator."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spark_streaming_manager",
    default_args=default_args,
    description="Запуск Spark Streaming джобы для обработки сделок из Kafka",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tbank", "streaming", "spark"],
) as dag:
    submit_trades_stream = SparkSubmitOperator(
        task_id="submit_trades_streaming",
        conn_id="spark_default",
        application="/opt/airflow/src/tinvest_flow/processing/stream_trades_to_silver.py",
        packages="org.postgresql:postgresql:42.7.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        conf={
            "spark.jars.ivy": "/tmp/.ivy",
            "spark.master": "spark://spark-master:7077",
        },
        execution_timeout=timedelta(minutes=15),
        verbose=True,
    )
