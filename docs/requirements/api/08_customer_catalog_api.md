# Customer API Contract Specification — локальный каталог

## Общая структура

Customer Product содержит `id`, `name`, nullable `description`, `price`, агрегированный `stocks`, `is_active`, `is_archived`, вычисленный `total_price`, nullable `created_at`. `supplier_id` в customer-проекции отсутствует. Данные могут отставать от supplier-service.

## GET /products

### Назначение

Получить текущую локальную проекцию supplier-каталога.

### Ответственность

Прочитать все локальные Product, вычислить total price и вернуть список по ID.

### Источник данных

Таблица `products` в `customer_db`, наполненная Kafka consumer и HTTP snapshot sync.

### HTTP Method

`GET`.

### URL

`/products`.

### Path Parameters

Не применяется.

### Query Parameters

Не применяется.

### Request Body

Не применяется.

### Response

HTTP 200.

### Структура ответа

`items` — локальные Product по ID; `count` — длина; total price = price × stocks с округлением до двух знаков.

### Используемые модели

ORM customer `Product`, публичный Product, список Product.

### Бизнес-правила

Возвращаются active, inactive и archived Product без фильтра. Customer-service не подтверждает актуальность относительно supplier-service на момент запроса.

### Проверки валидации

Не применяется.

### Ошибки

Специализированные 4xx не определены; возможен 500 `internal_error`.

### Побочные эффекты

Не применяется.

### Kafka события

Не применяется; endpoint сам событий не производит и синхронизацию не запускает.

### Изменяемые таблицы

Не применяется; `products` только читается.

### Acceptance Criteria

- Пустая проекция возвращает `items=[]`, `count=0`.
- Все локальные Product возвращаются по ID, включая archived/inactive.
- Результат отражает customer DB, даже если supplier state уже изменился.

### QA Checklist

- Проверить пустую/непустую проекцию, порядок и count.
- Проверить archived/inactive и вычисление total price.
- Сравнить с supplier API до/после доставки событий, учитывая lag.

### Ограничения

Нет live-запроса supplier-service, freshness marker, supplier ID, пагинации или filtering.

## GET /products/{product_id}

### Назначение

Получить Product из локальной проекции по ID.

### Ответственность

Вернуть локальную карточку либо ошибку отсутствия.

### Источник данных

Таблица `products` в `customer_db`.

### HTTP Method

`GET`.

### URL

`/products/{product_id}`.

### Path Parameters

`product_id` — integer PK; `>0` отдельно не задано.

### Query Parameters

Не применяется.

### Request Body

Не применяется.

### Response

HTTP 200 с локальным Product.

### Структура ответа

Полная customer-карточка с вычисленным total price.

### Используемые модели

ORM customer `Product`, публичный Product.

### Бизнес-правила

Локальный Product должен существовать; flags не запрещают чтение.

### Проверки валидации

Path должен быть integer.

### Ошибки

- 400 `validation_error` — нечисловой path.
- 404 `product_not_found` — Product отсутствует в customer projection.
- 500 `internal_error`.

### Побочные эффекты

Не применяется.

### Kafka события

Не применяется.

### Изменяемые таблицы

Не применяется; `products` только читается.

### Acceptance Criteria

- Существующий локальный ID возвращает 200 независимо от flags.
- Отсутствующий локальный ID возвращает 404, даже если Product уже есть у supplier, но ещё не синхронизирован.

### QA Checklist

- Проверить существующий, отсутствующий, отрицательный и нечисловой ID.
- Проверить archived/inactive Product.
- Наблюдать eventual consistency после supplier update.

### Ограничения

Ответ не гарантирует текущего supplier state и не содержит warehouse-level stock или supplier ID.

