"""DAG: Батч-загрузка исторических данных из T-Invest API в S3."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from t_tech.invest import CandleInterval

# Импорт нашего кода из src-пакета
from tinvest_flow.ingestion.rest_loader import upload_batch

def load_candles_task(figi: str, interval: CandleInterval, **context) -> None:
    """Обертка для вызова функции загрузки из tinvest_flow."""
    # В будущем здесь можно использовать context['data_interval_start'] 
    # для строгой идемпотентности батчей.
    upload_batch(figi=figi, interval=interval, days_back=7)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="batch_ingest_candles",
    default_args=default_args,
    description="Загрузка исторических свечей T-Invest API в MinIO",
    schedule=None,  # Пока запускаем только вручную
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tbank", "ingestion", "bronze"],
) as dag:

    # SBER и GAZP (FIGI из официальной документации)
    load_sber = PythonOperator(
        task_id="load_sber_candles",
        python_callable=load_candles_task,
        op_kwargs={
            "figi": "BBG004730N88", 
            "interval": CandleInterval.CANDLE_INTERVAL_5_MIN
        },
    )

    load_gazp = PythonOperator(
        task_id="load_gazp_candles",
        python_callable=load_candles_task,
        op_kwargs={
            "figi": "BBG004730RP0", 
            "interval": CandleInterval.CANDLE_INTERVAL_5_MIN
        },
    )