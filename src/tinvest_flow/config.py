"""Централизованное управление конфигурацией приложения."""
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Настройки приложения, загружаемые из переменных окружения."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # T-Invest API
    tinkoff_sandbox_token: SecretStr
    tinkoff_sandbox_account_id: str

    # Kafka
    kafka_bootstrap: str = "localhost:9092"
    kafka_topic_trades: str = "raw_trades"

    # S3 / MinIO
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: SecretStr = SecretStr("minioadmin")
    minio_bucket_bronze: str = "bronze"

    # PostgreSQL (DWH)
    postgres_dwh_host: str = "localhost"
    postgres_dwh_port: int = 5432
    postgres_dwh_db: str = "dwh"
    postgres_dwh_user: str = "dwh"
    postgres_dwh_password: SecretStr = SecretStr("dwh")

    @property
    def postgres_dsn(self) -> str:
        """Формирует строку подключения к PostgreSQL."""
        return (
            f"postgresql://{self.postgres_dwh_user}:"
            f"{self.postgres_dwh_password.get_secret_value()}@"
            f"{self.postgres_dwh_host}:{self.postgres_dwh_port}/"
            f"{self.postgres_dwh_db}"
        )


settings = Settings()