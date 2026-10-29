"""Загрузчик исторических данных из T-Invest API в S3."""
import json
import logging
from datetime import datetime, timedelta, timezone

from t_tech.invest import CandleInterval, Client
from t_tech.invest.constants import INVEST_GRPC_API_SANDBOX

from tinvest_flow.common.s3 import ensure_bucket, get_s3_client
from tinvest_flow.config import settings

logger = logging.getLogger(__name__)

INTERVAL_MAP = {
    CandleInterval.CANDLE_INTERVAL_1_MIN: "1min",
    CandleInterval.CANDLE_INTERVAL_5_MIN: "5min",
    CandleInterval.CANDLE_INTERVAL_HOUR: "1hour",
}


def _quotation_to_float(quotation: object) -> float:
    units = getattr(quotation, "units", 0)
    nano = getattr(quotation, "nano", 0)
    return units + nano / 1_000_000_000


def _candles_to_records(candles: list, figi: str, interval_name: str) -> list[dict]:
    return [
        {
            "figi": figi,
            "interval": interval_name,
            "time": c.time.isoformat(),
            "open": _quotation_to_float(c.open),
            "high": _quotation_to_float(c.high),
            "low": _quotation_to_float(c.low),
            "close": _quotation_to_float(c.close),
            "volume": c.volume,
            "is_complete": c.is_complete,
        }
        for c in candles
    ]


def fetch_candles(figi: str, interval: CandleInterval, days_back: int = 7) -> list:
    to_date = datetime.now(timezone.utc)
    from_date = to_date - timedelta(days=days_back)

    with Client(
        token=settings.tinkoff_sandbox_token.get_secret_value(),
        target=INVEST_GRPC_API_SANDBOX,
    ) as client:
        response = client.market_data.get_candles(
            figi=figi, from_=from_date, to=to_date, interval=interval
        )
    return response.candles


def upload_batch(
    figi: str, interval: CandleInterval, days_back: int = 7, load_date: str | None = None
) -> str:
    """Загружает свечи и сохраняет в S3. Дата фиксируется для идемпотентности бэкфиллов."""
    interval_name = INTERVAL_MAP[interval]
    if load_date is None:
        load_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    candles = fetch_candles(figi, interval, days_back)
    records = _candles_to_records(candles, figi, interval_name)

    s3 = get_s3_client()
    ensure_bucket(s3, settings.minio_bucket_bronze)

    key = f"candles/{figi}/{interval_name}/{load_date}/data.json"
    body = json.dumps(records, ensure_ascii=False, indent=2).encode("utf-8")

    s3.put_object(Bucket=settings.minio_bucket_bronze, Key=key, Body=body)
    logger.info(
        "Uploaded %d candles to s3://%s/%s",
        len(records),
        settings.minio_bucket_bronze,
        key,
    )

    return key