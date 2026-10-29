"""DAG: Загрузка исторических данных из T-Invest API в S3."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from t_tech.invest import CandleInterval

from tinvest_flow.ingestion.rest_loader import upload_batch


def load_candles_task(figi: str, interval: CandleInterval, **context) -> None:
    start = context["data_interval_start"]
    end = context["data_interval_end"]
    ds = start.strftime("%Y-%m-%d")

    upload_batch(figi=figi, interval=interval, from_date=start, to_date=end, load_date=ds)


with DAG(
    dag_id="batch_ingest_candles",
    default_args={"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=5)},
    description="Загрузка исторических данных из T-Invest API в S3 (Bronze)",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tbank", "ingestion", "bronze"],
) as dag:
    load_sber = PythonOperator(
        task_id="load_sber_candles",
        python_callable=load_candles_task,
        op_kwargs={"figi": "BBG004730N88", "interval": CandleInterval.CANDLE_INTERVAL_5_MIN},
    )
