# Critical checklist — Marketplace-QA 2.0

## Order lifecycle

- [ ] Create создаёт ровно один `PENDING_RESERVATION` заказ.
- [ ] Reservation резервирует все позиции или отклоняет весь заказ.
- [ ] Успешный reserve переводит заказ в `RESERVED`.
- [ ] Недостаточный остаток переводит заказ в `REJECTED`.
- [ ] Confirm проходит через `CONFIRMATION_PENDING` в `CONFIRMED`.
- [ ] Reject проходит через `REJECTION_PENDING`, release и `REJECTED`.
- [ ] Cancel разрешён только до `CONFIRMED`.
- [ ] Cancel с резервом освобождает остаток и заканчивается `CANCELLED`.
- [ ] Late reservation success после cancel приводит к compensation release.

## Consistency and concurrency

- [ ] Одинаковый Idempotency-Key и тело возвращают тот же order.
- [ ] Одинаковый ключ с другим телом возвращает `409`.
- [ ] Double click не создаёт два заказа.
- [ ] Mutation без `If-Match` возвращает `428`.
- [ ] Неверный формат `If-Match` возвращает `400`.
- [ ] Stale ETag возвращает `412` и UI обновляет представление.
- [ ] Конкурирующие действия не создают два terminal результата.
- [ ] Kafka duplicate пропускается через inbox/deduplication.
- [ ] Версия и append-only history согласованы.

## Failure handling

- [ ] Retry использует ограниченное число попыток.
- [ ] Неподдерживаемое/сломанное сообщение попадает в DLQ.
- [ ] DLQ metadata содержит original topic/event/correlation/failure.
- [ ] Ручной replay сохраняет original event ID.
- [ ] Timeout worker переводит исчерпавшую попытки операцию в `FAILED`.
- [ ] Рестарт consumer/worker не теряет committed state.
- [ ] Kafka outage отражается в readiness и обрабатывается после recovery.

## API, data and UI

- [ ] Customer читает только свои заказы.
- [ ] Supplier читает и изменяет только свои заказы.
- [ ] Чужой order возвращается как `404`.
- [ ] Корзина и заказ содержат товары одного supplier.
- [ ] Catalog projection допускает контролируемую задержку.
- [ ] Loading, empty, error и success состояния видимы.
- [ ] UI polling останавливается после terminal state.
- [ ] Timeline отражает все переходы и версии.
- [ ] QA/Dev Panel показывают health, ETag, correlation ID и raw data.
- [ ] Миграции order/supplier находятся на head.
- [ ] Clean reset создаёт ожидаемый seed.
