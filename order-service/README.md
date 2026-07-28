# order-service

`order-service` — TO BE source of truth по жизненному циклу заказов Marketplace-QA 2.0. На текущем этапе реализованы инфраструктурный каркас, отдельная PostgreSQL database, Alembic migrations, ORM metadata и operational endpoints. Business order API и Kafka processing пока не реализованы.

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
| `CUSTOMER_SERVICE_URL` | `http://localhost:8001` | заготовка для будущего cart snapshot API |
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
