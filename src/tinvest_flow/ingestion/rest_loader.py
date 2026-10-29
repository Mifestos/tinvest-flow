"""Загрузчик исторических данных из T-Invest API в S3."""

import json
import logging
from datetime import datetime
from typing import Any, cast

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


def _quotation_to_float(quotation: Any) -> float:
    units = getattr(quotation, "units", 0)
    nano = getattr(quotation, "nano", 0)
    return float(units + nano / 1_000_000_000)


def _candles_to_records(candles: list[Any], figi: str, interval_name: str) -> list[dict[str, Any]]:
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


def fetch_candles(
    figi: str, interval: CandleInterval, from_date: datetime, to_date: datetime
) -> list[Any]:
    """Запрашивает исторические свечи из API за указанный интервал."""
    with Client(
        token=settings.tinkoff_sandbox_token.get_secret_value(),
        target=INVEST_GRPC_API_SANDBOX,
    ) as client:
        response = client.market_data.get_candles(
            figi=figi, from_=from_date, to=to_date, interval=interval
        )
    return cast(list[Any], response.candles)


def upload_batch(
    figi: str, interval: CandleInterval, from_date: datetime, to_date: datetime, load_date: str
) -> str:
    """Загружает свечи и сохраняет в S3. Дата фиксируется для идемпотентности."""
    interval_name = INTERVAL_MAP[interval]

    candles = fetch_candles(figi, interval, from_date, to_date)
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
