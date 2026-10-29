# tinvest-flow

Пайплайн обработки данных для рыночной аналитики на базе T-Invest API. Проект реализует гибридную (Lambda/Kappa) архитектуру с медальонным слоением данных (Bronze -> Silver -> Gold).

> Portfolio project. Not affiliated with T-Bank. Используется публичное API песочницы T-Invest.

## Архитектура

Платформа разделена на три логических слоя:

- **Bronze (Raw)**: Сырые данные (REST-запросы и WebSocket-стримы) сохраняются в S3-совместимое хранилище (MinIO).
- **Silver (DDS)**: Очищенные и обогащенные данные, готовые для аналитики. Хранятся в MPP-СУБД (PostgreSQL локально, Greenplum в production-окружении банка).
- **Gold (Marts)**: Агрегированные витрины данных для быстрой аналитики и BI. Реализованы на ClickHouse.

Поток данных:
T-Invest API (REST) -> MinIO (Bronze) -> Spark Batch -> PostgreSQL/Greenplum (Silver)
T-Invest API (WebSocket) -> Kafka -> Spark Streaming -> PostgreSQL/Greenplum (Silver) -> ClickHouse (Gold)

## Стек технологий

- **Ingestion**: Python, Tinkoff Invest Python SDK
- **Message Broker**: Apache Kafka
- **Data Lake**: MinIO (S3)
- **Batch & Stream Processing**: Apache Spark (PySpark)
- **DWH / Storage**: PostgreSQL (локально), ClickHouse
- **Orchestration**: Apache Airflow
- **Infrastructure**: Docker, Docker Compose

