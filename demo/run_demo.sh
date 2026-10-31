#!/bin/bash
set -e

echo "=== 1. Инициализация инфраструктуры ==="
docker compose up -d kafka postgres_dwh spark-master spark-worker

echo "=== 2. Ожидание инициализации Spark ==="
sleep 15

echo "=== 3. Запуск Spark Structured Streaming (Detached) ==="
# Запускаем в фоне, чтобы скрипт не блокировался
docker exec -d spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.7.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --conf spark.jars.ivy=/tmp/.ivy \
  /opt/spark-apps/src/tinvest_flow/processing/stream_trades_to_silver.py

echo "Ожидание инициализации стриминга (10 сек)..."
sleep 10

echo "=== 4. Генерация потока событий ==="
docker compose run --rm trade-simulator

echo "Ожидание обработки батча (10 сек)..."
sleep 10

echo "=== 5. Верификация данных в PostgreSQL ==="
docker compose exec -T postgres_dwh psql -U dwh -d dwh -f /demo/verify.sql

echo "=== DEMO COMPLETED ==="