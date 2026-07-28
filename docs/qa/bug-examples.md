# Sample bug reports

Ниже приведены учебные примеры оформления дефектов. Они не утверждают, что такие дефекты фактически присутствуют в текущей версии.

## BUG-SAMPLE-001 — повтор checkout создаёт два заказа

**Severity:** Critical

**Preconditions:** в корзине Customer #1 один доступный товар.

**Steps:**

1. Отправить create order с `Idempotency-Key: demo-1`.
2. Имитировать потерю HTTP-ответа.
3. Повторить тот же запрос с тем же ключом и телом.

**Expected:** возвращается исходный `order_id`, `Idempotency-Replayed: true`.

**Hypothetical actual:** создаётся второй order с новым ID.

**Evidence:** оба HTTP response, order list, outbox rows и correlation IDs.

## BUG-SAMPLE-002 — stale Supplier action перезаписывает результат

**Severity:** Critical

**Preconditions:** две вкладки открыли один `RESERVED` order с ETag `"2"`.

**Steps:**

1. В первой вкладке выполнить Confirm.
2. Во второй выполнить Reject со старым `If-Match: "2"`.

**Expected:** второй запрос получает `412`, данные обновляются.

**Hypothetical actual:** заказ становится `REJECTED` после принятого Confirm.

**Evidence:** headers, state history, versions и order logs.

## BUG-SAMPLE-003 — cancel не освобождает резерв

**Severity:** High

**Preconditions:** order имеет `RESERVED`, available stock уменьшен.

**Steps:**

1. Customer нажимает Cancel.
2. Дождаться terminal state.
3. Сравнить reserved/available stock.

**Expected:** `CANCELLED`, reservation `RELEASED`, available stock восстановлен.

**Hypothetical actual:** заказ отменён, но reserved stock остаётся.

**Evidence:** timeline, supplier product API и stock events.

## BUG-SAMPLE-004 — Customer видит чужой заказ

**Severity:** High

**Steps:**

1. Выбрать Customer #1.
2. Открыть URL заказа Customer #2.

**Expected:** owner-scoped `404`, данные заказа не раскрываются.

**Hypothetical actual:** карточка заказа отображается полностью.

**Evidence:** request headers, response и screenshot.

## BUG-SAMPLE-005 — UI polling не останавливается

**Severity:** Medium

**Preconditions:** заказ завершён в `CONFIRMED`.

**Steps:**

1. Открыть карточку во время `CONFIRMATION_PENDING`.
2. Дождаться `CONFIRMED`.
3. Наблюдать Network panel.

**Expected:** polling прекращается после `operation_state=NONE`.

**Hypothetical actual:** GET продолжает выполняться каждые две секунды.

**Evidence:** HAR/trace, final response и frontend timestamp.
