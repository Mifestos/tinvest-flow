"""Утилиты для работы с S3-хранилищем."""
import logging

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from tinvest_flow.config import settings

logger = logging.getLogger(__name__)


def get_s3_client() -> boto3.client:
    """Инициализирует клиент S3 для работы с MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key.get_secret_value(),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(s3_client: boto3.client, bucket: str) -> None:
    """Проверяет наличие бакета и создает его при отсутствии."""
    try:
        s3_client.head_bucket(Bucket=bucket)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == "404":
            logger.info(f"Создание бакета {bucket}")
            s3_client.create_bucket(Bucket=bucket)
        else:
            logger.error(f"Ошибка при проверке бакета {bucket}: {e}")
            raise