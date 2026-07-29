# Supplier API Contract Specification — остатки

## POST /warehouses/{warehouse_id}/stocks

### Назначение

Установить фактические остатки одного или нескольких товаров на конкретном складе и вернуть затронутые Product с пересчитанными агрегатами.

### Ответственность

Проверить Warehouse и каждый Product, выполнить upsert warehouse stock, пересчитать `Product.stocks` и инициировать stock events.

### Источник данных

Path и request body; `warehouses`, `products`, `warehouse_products` в `supplier_db`.

### HTTP Method

`POST`.

### URL

`/warehouses/{warehouse_id}/stocks`.

### Path Parameters

`warehouse_id` — integer PK; ограничение `>0` отдельно не задано.

### Query Parameters

Не применяется.

### Request Body

JSON с обязательным непустым массивом `items`. Каждый элемент содержит `product_id` (integer > 0) и `stocks` (integer ≥ 0). Значение является абсолютным фактическим остатком пары warehouse–product, а не приращением.

### Response

HTTP 200 со списком обновлённых Product.

### Структура ответа

`items` содержит не более одного Product на уникальный product ID и формируется при первом появлении ID; `count` равно длине результата. При отсутствии дублей Product содержит агрегат после обработки своего item. При повторе ID сохранённый объект ответа не обновляется после последующих вхождений и может отличаться от окончательного состояния БД.

### Используемые модели

Контракт restock request и warehouse stock item, ORM `Warehouse`, `Product`, `WarehouseProduct`, публичный Product и список Product.

### Бизнес-правила

- Warehouse и каждый Product должны существовать.
- Stock пары устанавливается равным входному значению.
- Агрегированный stock равен сумме всех warehouse rows Product.
- Нулевой stock допустим.
- Повторяющийся `product_id` в одном запросе обрабатывается последовательно; последнее значение остаётся в БД, но response snapshot и событие формируются только при первом появлении ID.
- Flags Product не проверяются: restock active, inactive и archived товаров допускается реализацией.

### Проверки валидации

- `warehouse_id` должен быть integer.
- `items` должен существовать и содержать минимум один элемент.
- `product_id>0`, `stocks≥0`, оба integer.

### Ошибки

- 400 `validation_error` — неверный path/body, пустой items, неположительный product ID или отрицательный stock.
- 404 `warehouse_not_found` — Warehouse отсутствует; items не обрабатываются.
- 404 `product_not_found` — очередной Product отсутствует.
- 500 `internal_error` — необработанная DB/Kafka-side ошибка.

### Побочные эффекты

Для каждого item выполняется отдельный commit upsert, затем отдельный commit пересчёта Product. При ошибке позднего item изменения предыдущих items уже сохранены; атомарности всего request нет.

### Kafka события

Для первого появления каждого уникального product ID после пересчёта публикуется `STOCK_REPLENISHED` в `product-stock-events`: key = product ID, поля `product_id`, `total_quantity`, `event_version=1`.

### Изменяемые таблицы

- `warehouse_products`: INSERT или UPDATE для каждой обработанной пары.
- `products`: UPDATE агрегированного `stocks`.
- `warehouses`: только проверка существования.

### Acceptance Criteria

- Новый pair создаётся, существующий pair получает абсолютное входное значение.
- Aggregate после каждого обработанного item равен сумме всех складов Product.
- Нулевое значение принимается; отрицательное отклоняется до обработки.
- Без дублирующихся ID ответ возвращает Product с агрегатом после обработки item и согласованным `count`.
- При дублирующемся ID ответ и событие отражают первое вхождение, а окончательная БД — последнее.
- Для уникального Product инициируется `STOCK_REPLENISHED` после успешного пересчёта.
- При отсутствующем Warehouse изменения не выполняются.
- При отсутствующем Product предыдущие items могут остаться committed, а текущий и последующие не обрабатываются.

### QA Checklist

- Создать новый stock row и заменить существующий абсолютным значением.
- Проверить нулевой/отрицательный stock и пустой items.
- Проверить один Product на нескольких складах.
- Проверить несколько Product в одном request.
- Проверить повторяющийся product ID и различие между итоговой БД, response и событием.
- Поместить отсутствующий Product первым и после валидного item, проверить частичный commit.
- Проверить archived/inactive Product.
- Сопоставить warehouse row, aggregate, response и Kafka payload.

### Ограничения

- Название `STOCK_REPLENISHED` используется даже при уменьшении абсолютного значения.
- Операция по массиву не атомарна.
- Повторяющиеся product ID могут приводить к расхождению response/Kafka payload с окончательным состоянием БД.
- Endpoint не возвращает warehouse-level rows, только агрегированные Product.
- Нет optimistic locking или защиты от конкурентных обновлений.
- Producer не подтверждает доставку события.
