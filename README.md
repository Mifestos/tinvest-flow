# tinvest-flow

**Hybrid Data Platform & DWH на базе T-Invest API**

[![CI](https://github.com/Mifestos/tinvest-flow/actions/workflows/ci.yml/badge.svg)](https://github.com/Mifestos/tinvest-flow/actions/workflows/ci.yml)
[![CD](https://github.com/Mifestos/tinvest-flow/actions/workflows/cd.yml/badge.svg)](https://github.com/Mifestos/tinvest-flow/actions/workflows/cd.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%20%7C%203.12-blue.svg)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/docker-ready-blue.svg)](https://www.docker.com/)

Платформа обработки данных, реализующая гибридную (Lambda) архитектуру с разделением на слои хранения (Bronze -> Silver -> Gold). Проект демонстрирует end-to-end пайплайны: от сбора сырых данных из публичного API Т-Инвестиций до построения аналитических витрин в MPP-СУБД.

## Архитектура

Платформа разделена на два независимых контура обработки данных:

1. **Batch Pipeline (исторические данные):**
   `T-Invest REST API` -> `Apache Airflow` -> `MinIO (S3)` -> `Python/psycopg2` -> `PostgreSQL (DDS)` -> `ClickHouse (Data Mart)`.
2. **Streaming Pipeline (real-time данные):**
   `T-Invest WebSocket` -> `Python Producer` -> `Apache Kafka` -> `Spark Structured Streaming` -> `PostgreSQL (DDS)`.

```mermaid
graph TD
    %% Источники данных
    API_REST["T-Invest REST API"]
    API_WS["T-Invest WebSocket"]
    
    %% Оркестрация и Обработка
    Airflow["Apache Airflow<br>(Оркестратор Batch-процессов)"]
    PyBatch["Python ETL<br>(psycopg2 / pandas)"]
    Kafka["Apache Kafka"]
    Spark_Stream["Spark Structured Streaming<br>(Изолированный контейнер)"]

    %% Слои хранения (Medallion Architecture)
    MinIO[("MinIO S3<br>(Bronze Layer)")]
    Postgres[("PostgreSQL<br>(Silver Layer)")]
    ClickHouse[("ClickHouse<br>(Gold Layer)")]

    %% Batch Flow (Управляется Airflow)
    Airflow -.->|"Запускает DAG"| PyBatch
    PyBatch -.->|"Запрос данных"| API_REST
    API_REST -->|"Сырой JSON"| MinIO
    MinIO -->|"Чтение (Bronze → Silver)"| PyBatch
    PyBatch -->|"UPSERT свечей"| Postgres
    Postgres -->|"Чтение (Silver → Gold)"| PyBatch
    PyBatch -->|"Запись витрины"| ClickHouse

    %% Streaming Flow (Работает автономно)
    API_WS -->|"Python Producer"| Kafka
    Kafka -->|"Чтение стрима"| Spark_Stream
    Spark_Stream -->|"JDBC UPSERT сделок"| Postgres
```
## Схема данных (Data Model)
```mermaid
erDiagram
    dds_candles {
        varchar figi PK
        varchar interval PK
        timestamptz candle_time PK
        numeric open
        numeric high
        numeric low
        numeric close
        bigint volume
        boolean is_complete
    }
    
    dds_trades {
        varchar trade_id PK
        timestamptz trade_datetime PK
        varchar order_id
        varchar figi
        varchar direction "VARCHAR(32)"
        numeric price_rub
        bigint quantity
    }

    mart_market_activity {
        varchar figi PK
        datetime day PK
        numeric open_price
        numeric high_price
        numeric low_price
        numeric close_price
        bigint volume
        bigint candle_count
        float avg_trade_size
    }
```
## Технологический стек
### Языки и библиотеки:
* Python 3.12+ (PySpark, Pydantic, psycopg2, boto3, aiokafka)

* SQL (PostgreSQL, ClickHouse)
### Обработка данных и инфраструктура:
* Оркестрация: Apache Airflow (CeleryExecutor)

* Потоковая обработка: Apache Spark Structured Streaming (изолированный сервис)

* Пакетная обработка: Python (Airflow PythonOperator)

* Брокер сообщений: Apache Kafka (KRaft mode)

* Data Lake: MinIO (S3-совместимое хранилище)

* Реляционная БД (DWH): PostgreSQL (с партиционированием по времени)

* Аналитическая БД (Витрины): ClickHouse
### DevOps и качество кода:
* Контейнеризация: Docker, Docker Compose

* CI/CD: GitHub Actions (Lint, Type-check, Tests, Publish Docker Images)

* Линтеры и типизация: Ruff, Mypy (strict mode)

* Тестирование: Pytest
## Структура репозитория
```text
├── src/tinvest_flow/      # Исходный код (src-layout)
│   ├── common/            # Инфраструктурные утилиты (S3, logging)
│   ├── config.py          # Валидация конфигурации (Pydantic Settings)
│   ├── ingestion/         # Модули сбора данных (REST, WebSocket, Producer)
│   └── processing/        # Batch ETL и Spark Streaming джобы
├── dags/                  # DAG-файлы Airflow (оркестрация)
├── infra/                 # DDL-скрипты инициализации PostgreSQL и ClickHouse
├── tests/                 # Pytest (Unit-тесты)
├── docker-compose.yml     # Описание локальной инфраструктуры
└── pyproject.toml         # Управление зависимостями и настройки линтеров
```
## Развертывание (Local Development)
### Требования
* Docker и Docker Compose (рекомендуется выделить не менее 8 ГБ RAM для WSL2/Docker Desktop).

* Токен песочницы T-Invest API и ID виртуального счета.
### Запуск инфраструктуры
1. Настройте переменные окружения

```bash
cp .env.example .env
# Заполните TINKOFF_SANDBOX_TOKEN и TINKOFF_SANDBOX_ACCOUNT_ID
```
2. Запустите стек (сборка кастомных образов занимает несколько минут):

```bash
docker compose up -d --build
```
3. Настройте Connections в Airflow (http://localhost:8081):

* dwh_postgres: Type=Postgres, Host=postgres_dwh, DB=dwh, Login=dwh, Password=dwh, Port=5432.

* aws_default: Type=AWS, Login=minioadmin, Password=minioadmin, Extra={"endpoint_url": "http://minio:9000"}.
### Запуск Batch-пайплайна
DAG-и связаны через TriggerDagRunOperator. Достаточно запустить первый DAG, остальные выполнятся автоматически.

1. В веб-интерфейсе Airflow запустите DAG batch_ingest_candles (Сбор исторических данных).

2. Он автоматически инициирует bronze_to_silver_candles (Загрузка в PostgreSQL).

3. Затем данные попадут в silver_to_gold_marts (Агрегация в ClickHouse).
### Запуск Streaming-пайплайна
Контур потоковой обработки изолирован от оркестратора Airflow. Spark Streaming работает как независимый долгоживущий сервис.

Запустите Spark-джобу (она читает Kafka, пишет в PostgreSQL и перезапускается при сбоях):

```bash
docker compose up -d spark-streaming
```
Для отправки тестовых данных используйте встроенный симулятор сделок:

```bash
docker compose up -d trade-simulator
```
## Контроль качества и CI/CD
* Статический анализ: ruff (линтер) и mypy (строгая проверка типов, strict = true).

* Тестирование: pytest с изоляцией переменных окружения через фикстуры.

* Pre-commit: Хуки для автоматического форматирования перед коммитом.

* CI: GitHub Actions проверяет типы, стиль кода и прогоняет тесты при каждом PR.

* CD: GitHub Actions собирает multi-platform Docker-образы (linux/amd64, linux/arm64) и публикует их в GitHub Container Registry.
## Интеграция с BI (Metabase)
Для демонстрации конечного потребления данных в проект интегрирован Metabase. Он подключается напрямую к слою Gold (ClickHouse), позволяя аналитикам строить отчеты без написания SQL-запросов. Пример агрегированного запроса по витрине mart_market_activity доступен в папке docs/images/.

Пример агрегированного запроса по витрине `mart_market_activity`: суммарный объем торгов (`volume`) в разрезе финансовых инструментов (`figi`).

![Metabase Example](docs/images/metabase-example.png)
## Ключевые инженерные решения (ADR)
* ADR-001: Паттерн JDBC UPSERT в Spark Streaming с защитой от дубликатов.
Стандартный Spark JDBC не поддерживает UPSERT. Данные батча пишутся в уникальную staging-таблицу (stg_trades_{batch_id}), после чего выполняется нативный SQL-запрос (INSERT ... ON CONFLICT DO UPDATE). Для предотвращения ошибки CardinalityViolation при дублирующихся сообщениях в Kafka применяется dropDuplicates(["trade_id", "trade_datetime"]) в DataFrame перед вставкой.

* ADR-002: Dead Letter Queue (DLQ) для потоковых данных.
В Spark Structured Streaming добавлен режим PERMISSIVE. Сообщения с нарушенной схемой (invalid JSON) перехватываются в поле _corrupt_record и отправляются в отдельный Kafka-топик (raw_trades_dlq) для последующего анализа, не нарушая работу основного пайплайна.

* ADR-003: Управление партиционированием в PostgreSQL.
Таблицы dds_trades и dds_candles партиционированы по времени (RANGE). Для предотвращения падений при поступлении данных за периоды, на которые еще нет партиций, реализован DAG manage_partitions, который ежедневно проверяет и создает партиции на 3 месяца вперед. DEFAULT-партиции не используются.

* ADR-004: Гибридная Lambda архитектура и развязка Spark Streaming.
Spark Streaming работает как независимый сервис в Docker (restart: always). Это предотвращает блокировку слотов Airflow и решает проблему ограничений Standalone-кластера Spark (отсутствие поддержки deploy-mode: cluster для Python-приложений).

* ADR-005: Чанковая агрегация в ClickHouse (защита от OOM).
При построении витрин используется потоковое чтение из PostgreSQL через серверный курсор (RealDictCursor, itersize=100_000). Таблица в ClickHouse очищается (TRUNCATE) перед полной перезаписью для обеспечения идемпотентности. Это предотвращает выгрузку всей таблицы в RAM и дублирование данных при перезапусках.

* ADR-006: Оконная пагинация при загрузке исторических данных.
В связи с лимитами T-Invest API на количество возвращаемых свечей, метод fetch_candles разбивает запрошенный период на временные окна (в зависимости от интервала), что гарантирует загрузку полного объема исторических данных без потерь.
## Автор

**Mifestos** 
- GitHub: [@Mifestos](https://github.com/Mifestos)

## Лицензия

Этот проект распространяется под лицензией MIT. Подробности смотрите в файле [LICENSE](LICENSE).