# order-service

`order-service` — TO BE source of truth по жизненному циклу заказов Marketplace-QA 2.0. Реализованы инфраструктурный каркас, отдельная PostgreSQL database, Alembic migrations, operational endpoints, идемпотентный `CreateOrder` с server-side cart snapshot и transactional outbox, а также outbox publisher. Kafka consumer и reservation processing пока не реализованы.

## Переменные окружения

| Variable | Development default | Назначение |
|---|---|---|
| `SERVICE_NAME` | `order-service` | имя сервиса в ответах и логах |
| `ENVIRONMENT` | `development` | имя окружения |
| `HOST` | `0.0.0.0` | HTTP bind address |
| `PORT` | `8000` | внутренний HTTP port |
| `DATABASE_URL` | `postgresql+psycopg://order_app:order_app@localhost:5434/order_db` | order-db connection URL |
| `LOG_LEVEL` | `INFO` | Python log level |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | bootstrap servers для publisher и readiness |
| `CUSTOMER_SERVICE_URL` | `http://localhost:8001` | base URL внутреннего cart snapshot API |
| `CUSTOMER_SERVICE_TIMEOUT_SECONDS` | `1` | timeout одной попытки cart snapshot |
| `OUTBOX_PUBLISHER_ENABLED` | `true` | включает Kafka readiness и отдельный publisher process |
| `OUTBOX_POLL_INTERVAL_SECONDS` | `1` | пауза polling при отсутствии due records |
| `OUTBOX_BATCH_SIZE` | `100` | maximum records одного claim |
| `OUTBOX_CLAIM_LEASE_SECONDS` | `30` | lease `IN_PROGRESS` claim |
| `OUTBOX_BASE_RETRY_DELAY_SECONDS` | `1` | начальный backoff публикации |
| `OUTBOX_MAX_RETRY_DELAY_SECONDS` | `30` | maximum backoff публикации |
| `OUTBOX_MAX_ATTEMPTS` | `10` | переход outbox row в `FAILED` |
| `KAFKA_DELIVERY_TIMEOUT_SECONDS` | `10` | ожидание broker acknowledgement |
| `KAFKA_ORDER_EVENTS_TOPIC` | `marketplace.order.events.v1` | order lifecycle events |
| `KAFKA_STOCK_COMMANDS_TOPIC` | `marketplace.stock.commands.v1` | stock commands |
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

## Чтение заказов

Customer и Supplier читают только собственные заказы через test-user context:

- `GET /api/v1/customers/{customer_id}/orders`
- `GET /api/v1/customers/{customer_id}/orders/{order_id}`
- `GET /api/v1/suppliers/{supplier_id}/orders`
- `GET /api/v1/suppliers/{supplier_id}/orders/{order_id}`

Списки поддерживают `page`, `limit` до 100, повторяемый business `status`, UTC `created_from` и `created_to`. Detail возвращает public Order representation, actor-specific `available_actions` и `ETag: "<version>"`; чужой или отсутствующий Order маскируется как `404 order_not_found`.

## Outbox publisher

Publisher запускается отдельным контейнером `order-outbox-publisher` из того же image. Он claim-ит до 100 due rows через PostgreSQL `FOR UPDATE SKIP LOCKED`, освобождает DB-транзакцию до Kafka I/O и ждёт broker acknowledgement. Kafka key — строковый `order_id`; `OrderCreated` публикуется в `marketplace.order.events.v1`, `StockReservationRequested` — в `marketplace.stock.commands.v1`.

Доставка at-least-once: retry сохраняет исходные envelope, headers и `event_id`. Backoff по умолчанию — 1/2/4/8/16/30 секунд, после десятой неуспешной попытки row становится `FAILED` и требует ручного решения. PUBLISHED rows не выбираются. Физический DLQ предназначен для consumer failures и publisher автоматически в него не пишет.

При включённом publisher `/ready` требует доступности Kafka metadata вместе с order DB и актуальной Alembic revision. `/health` Kafka не проверяет.

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

PostgreSQL integration tests:

```bash
TEST_DATABASE_URL=postgresql+psycopg://order_app:order_app@localhost:5434/order_db pytest -m integration
```

Для дополнительного реального Kafka integration-теста:

```bash
TEST_DATABASE_URL=postgresql+psycopg://order_app:order_app@localhost:5434/order_db \
TEST_KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
pytest -m integration
```

PostgreSQL-тесты создают уникальную временную schema и удаляют только её после проверки.
