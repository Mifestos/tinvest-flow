"""DAG: Загрузка исторических данных из T-Invest API в S3."""

from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from t_tech.invest import CandleInterval

from tinvest_flow.ingestion.rest_loader import upload_batch


def load_candles_task(figi: str, interval: CandleInterval, **context) -> None:
    """Функция загрузки. Читает параметры из context['params']."""
    params = context["params"]

    start_str = params.get("start_date")
    end_str = params.get("end_date")
    lookback = params.get("lookback_days", 1)

    if start_str and end_str:
        start_dt = datetime.fromisoformat(start_str).replace(tzinfo=UTC)
        end_dt = datetime.fromisoformat(end_str).replace(tzinfo=UTC)
    else:
        end_dt = datetime.now(UTC)
        start_dt = end_dt - timedelta(days=lookback)

    ds = end_dt.strftime("%Y-%m-%d")

    upload_batch(figi=figi, interval=interval, from_date=start_dt, to_date=end_dt, load_date=ds)


with DAG(
    dag_id="batch_ingest_candles",
    default_args={"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=5)},
    description="Загрузка исторических данных из T-Invest API в S3 (Bronze)",
    schedule=None,  # Только ручной запуск
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tbank", "ingestion", "bronze"],
    params={
        "start_date": Param(
            None, type=["null", "string"], description="Начало периода (YYYY-MM-DD или ISO 8601)"
        ),
        "end_date": Param(
            None, type=["null", "string"], description="Конец периода (YYYY-MM-DD или ISO 8601)"
        ),
        "lookback_days": Param(
            1, type="integer", description="Сколько дней назад забрать, если даты не указаны"
        ),
    },
) as dag:
    load_sber = PythonOperator(
        task_id="load_sber_candles",
        python_callable=load_candles_task,
        op_kwargs={"figi": "BBG004730N88", "interval": CandleInterval.CANDLE_INTERVAL_5_MIN},
    )
