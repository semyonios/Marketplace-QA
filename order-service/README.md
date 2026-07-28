# order-service

`order-service` — TO BE source of truth по жизненному циклу заказов Marketplace-QA 2.0. Реализованы инфраструктурный каркас, отдельная PostgreSQL database, Alembic migrations, operational endpoints и идемпотентный `CreateOrder` с server-side cart snapshot и transactional outbox. Kafka publisher/consumer и reservation processing пока не реализованы.

## Переменные окружения

| Variable | Development default | Назначение |
|---|---|---|
| `SERVICE_NAME` | `order-service` | имя сервиса в ответах и логах |
| `ENVIRONMENT` | `development` | имя окружения |
| `HOST` | `0.0.0.0` | HTTP bind address |
| `PORT` | `8000` | внутренний HTTP port |
| `DATABASE_URL` | `postgresql+psycopg://order_app:order_app@localhost:5434/order_db` | order-db connection URL |
| `LOG_LEVEL` | `INFO` | Python log level |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | заготовка для следующего этапа |
| `CUSTOMER_SERVICE_URL` | `http://localhost:8001` | base URL внутреннего cart snapshot API |
| `CUSTOMER_SERVICE_TIMEOUT_SECONDS` | `1` | timeout одной попытки cart snapshot |
| `READINESS_TIMEOUT_SECONDS` | `2` | DB connect timeout readiness |
| `ALEMBIC_CONFIG` | `<service>/alembic.ini` | путь к Alembic config |

Development credentials предназначены только для локального стенда. Реальные secrets должны передаваться окружением.

## Запуск через Docker Compose

Из корня репозитория:

```bash
docker compose up --build order-service
```

Compose сначала ждёт healthy `order-postgres`, затем выполняет `alembic upgrade head` в одноразовом `order-migrations` и только после успешной миграции запускает API.

- health: <http://localhost:8002/health>
- readiness: <http://localhost:8002/ready>

## Создание заказа

`POST /api/v1/orders` требует `X-Test-Role: CUSTOMER`, положительный `X-Test-Subject-ID`, совпадающий с `customer_id`, и `Idempotency-Key`. Optional `X-Correlation-ID` принимается только как UUID; отсутствующее или некорректное значение заменяется новым UUID.

```bash
curl -i http://localhost:8002/api/v1/orders \
  -H 'Content-Type: application/json' \
  -H 'X-Test-Role: CUSTOMER' \
  -H 'X-Test-Subject-ID: 1' \
  -H 'Idempotency-Key: checkout-customer-1-cart-1' \
  -d '{"customer_id":1,"cart_version":1}'
```

Первый успешный запрос возвращает `202`, replay того же key/body — `200`, тот же key с другим semantic body — `409`. В одной финальной транзакции создаются Order, Items, начальная History, completed idempotency result и outbox templates `OrderCreated`/`StockReservationRequested`.

## Локальный запуск

Из каталога `order-service` при доступной PostgreSQL на `localhost:5434`:

```bash
python3.12 -m venv .venv
. .venv/bin/activate
pip install -r requirements-dev.txt
alembic upgrade head
python -m app.main
```

## Миграции

```bash
alembic upgrade head
alembic current
alembic check
```

Приложение не использует `create_all` и не изменяет схему на startup.

## Тесты

Unit tests:

```bash
pytest -m "not integration"
```

PostgreSQL migration test:

```bash
TEST_DATABASE_URL=postgresql+psycopg://order_app:order_app@localhost:5434/order_db pytest -m integration
```

Integration test создаёт уникальную временную schema и удаляет только её после проверки.
