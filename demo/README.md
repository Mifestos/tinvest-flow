# End-to-End Demonstration (Streaming Pipeline)

Данный сценарий предназначен для быстрой валидации работоспособности потокового контура (Kappa Architecture) без необходимости ручной генерации событий через T-Invest API.

**Архитектура контура:** `Simulator` -> `Kafka` -> `Spark Structured Streaming` -> `PostgreSQL`.

## Запуск

Требуется запущенный Docker Engine.

### Windows
```powershell
.\demo\run_demo.ps1
```
### Linux / macOS
```bash
chmod +x ./demo/run_demo.sh
./demo/run_demo.sh
```
## Ожидаемый результат
Скрипт развернет инфраструктуру и запустит Spark-джобу в фоне.

Утилита simulator.py сгенерирует 50 валидных событий и отправит их в Kafka.

Скрипт выполнит verify.sql и выведет 5 последних записей из таблицы dds.dds_trades, подтверждая сквозную обработку данных.

