# Test strategy — Marketplace-QA 2.0

## Scope

В scope входят Customer/Supplier UI, публичные REST-контракты, ownership, order state machine, PostgreSQL persistence, Kafka-команды и события, outbox/inbox, retry/DLQ, идемпотентность, optimistic locking и восстановление компонентов.

Вне scope: регистрация/JWT, оплата, доставка, partial fulfillment, нагрузочное production-тестирование, Kubernetes и полноценный distributed tracing.

## Основные риски

- двойное создание заказа после network retry;
- потеря или повтор Kafka-события;
- overselling и частичный резерв;
- неверный переход состояния при конкурирующих действиях;
- потеря резерва при cancel/reject/late success;
- чтение или изменение чужого заказа;
- stale projection и непонятное состояние UI;
- несовместимая миграция или недоступная зависимость.

## Test levels

| Уровень | Назначение |
|---|---|
| Unit/component | mapper, state calculation, React badges/timeline/dev panel |
| Contract | REST schemas, headers, error envelopes, event parsing |
| Integration | PostgreSQL constraints, migrations, outbox/inbox, Kafka round-trip |
| API smoke | готовность полного Compose и основной reserve flow |
| UI E2E | критические Customer/Supplier сценарии |
| Manual exploratory | race conditions, outage/recovery, observability и usability |

## Environments and data

Локальное и CI-окружение поднимаются Docker Compose. Backend integration tests используют PostgreSQL, а Kafka integration tests — локальный broker. `scripts/seed.py` создаёт повторяемые данные; `make reset` возвращает известное чистое состояние.

## Functional testing

Проверяются каталог, single-supplier cart, checkout, списки/карточки заказов, confirm/reject/cancel, all-or-nothing reservation, terminal actions и формы валидации.

## Integration and contract testing

Проверяются:

- публичные DTO без внутренних полей;
- `Idempotency-Key`, `ETag`, `If-Match`, test-role headers;
- owner-scoped `404`;
- outbox publication и inbox deduplication;
- reservation/finalization/release round-trips;
- порядок истории и correlation ID;
- readiness database/migrations/Kafka.

## Concurrency and idempotency

Для одного Idempotency-Key одинаковое тело должно возвращать существующий заказ, отличающееся — `409`. Параллельные confirm/reject/cancel проверяются через version/ETag: только одна версия принимается, stale mutation получает `412`.

## Eventual consistency

Автотесты ожидают бизнес-состояние через polling/assertion, а не фиксированный sleep. Проверяется переход из pending-состояний, остановка UI polling в terminal state и отображение stale/error состояния.

## Recovery

Проверяются рестарт API/consumers/workers, повторная доставка, late success compensation и доступность frontend при временном падении backend. Kafka outage выполняется контролируемо вручную, чтобы не делать основной E2E нестабильным.

## Exit criteria

- все обязательные Compose health/readiness успешны;
- backend, frontend, smoke и Playwright suites зелёные;
- migrations соответствуют head;
- критический checklist пройден;
- отсутствуют необъяснённые P0/P1 дефекты;
- README воспроизводится на чистых volumes;
- известные ограничения явно опубликованы.
