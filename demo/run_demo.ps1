$ErrorActionPreference = "Stop"

Write-Host "=== 1. Initialize infrastructure ===" -ForegroundColor Cyan
docker compose up -d kafka postgres_dwh spark-master spark-worker

Write-Host "=== 2. Wait for Spark initialization ===" -ForegroundColor Cyan
Start-Sleep -Seconds 15

Write-Host "=== 3. Start Spark Structured Streaming in background ===" -ForegroundColor Cyan
docker exec -d spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.postgresql:postgresql:42.7.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --conf spark.jars.ivy=/tmp/.ivy /opt/spark-apps/src/tinvest_flow/processing/stream_trades_to_silver.py

Write-Host "Waiting for streaming initialization..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

Write-Host "=== 4. Generate event stream ===" -ForegroundColor Cyan
docker compose run --rm trade-simulator

Write-Host "Waiting for batch processing..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

Write-Host "=== 5. Verify data in PostgreSQL ===" -ForegroundColor Green
docker compose exec -T postgres_dwh psql -U dwh -d dwh -c "SELECT figi, direction, price_rub, quantity, trade_datetime FROM dds.dds_trades ORDER BY loaded_at DESC LIMIT 5;"

Write-Host "=== DEMO COMPLETED ===" -ForegroundColor Green