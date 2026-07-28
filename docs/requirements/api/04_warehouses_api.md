# Supplier API Contract Specification — склады

## Общие структуры

Warehouse response содержит `id`, `name`, `weekday_hours`, `address`, `created_at`. List response содержит `items` и `count`. Warehouse API не возвращает связанные товары или остатки.

## POST /warehouses

### Назначение

Создать склад.

### Ответственность

Проверить описательные поля и сохранить Warehouse.

### Источник данных

Request body; сохранённая запись — `supplier_db`.

### HTTP Method

`POST`.

### URL

`/warehouses`.

### Path Parameters

Не применяется.

### Query Parameters

Не применяется.

### Request Body

Обязательный JSON: `name`, `weekday_hours`, `address`.

### Response

HTTP 201 с Warehouse.

### Структура ответа

Входные поля плюс серверные `id` и `created_at`.

### Используемые модели

Контракт создания Warehouse, публичный Warehouse, ORM `Warehouse`.

### Бизнес-правила

Специализированные бизнес-правила кроме входной валидации не реализованы.

### Проверки валидации

`name`: 2–255; `weekday_hours`: 2–255; `address`: 5–500 символов; все обязательны.

### Ошибки

- 400 `validation_error` — неверное/неполное тело.
- 500 `internal_error` — необработанная DB/системная ошибка.

### Побочные эффекты

Создаёт Warehouse; stock rows не создаются.

### Kafka события

Не применяется.

### Изменяемые таблицы

`warehouses`: INSERT.

### Acceptance Criteria

- Валидное тело создаёт одну запись и возвращает 201 с ID/timestamp.
- Невалидные длины не создают запись и возвращают 400.

### QA Checklist

- Проверить обязательность и границы каждого поля.
- Проверить ответ и строку БД.
- Проверить отсутствие stock rows и Kafka events.

### Ограничения

Уникальность, формат адреса и семантический формат графика не проверяются.

## GET /warehouses

### Назначение

Получить все склады.

### Ответственность

Прочитать Warehouse и вернуть упорядоченный list response.

### Источник данных

Таблица `warehouses`.

### HTTP Method

`GET`.

### URL

`/warehouses`.

### Path Parameters

Не применяется.

### Query Parameters

Не применяется.

### Request Body

Не применяется.

### Response

HTTP 200.

### Структура ответа

`items` — Warehouse по ID; `count` — число элементов.

### Используемые модели

ORM `Warehouse`, публичный Warehouse, список Warehouse.

### Бизнес-правила

Возвращаются все Warehouse.

### Проверки валидации

Не применяется.

### Ошибки

Специализированные 4xx не определены; возможен 500 `internal_error`.

### Побочные эффекты

Не применяется.

### Kafka события

Не применяется.

### Изменяемые таблицы

Не применяется; `warehouses` только читается.

### Acceptance Criteria

- Пустая таблица даёт `items=[]`, `count=0`.
- Непустой список отсортирован по ID, `count` совпадает с длиной.

### QA Checklist

- Проверить пустой/непустой список, порядок и count.
- Проверить отсутствие stock details, изменений и событий.

### Ограничения

Пагинация, filtering и включение остатков отсутствуют.

## GET /warehouses/{warehouse_id}

### Назначение

Получить Warehouse по ID.

### Ответственность

Вернуть существующую запись либо ошибку отсутствия.

### Источник данных

Таблица `warehouses`.

### HTTP Method

`GET`.

### URL

`/warehouses/{warehouse_id}`.

### Path Parameters

`warehouse_id` — integer PK; `>0` не задано.

### Query Parameters

Не применяется.

### Request Body

Не применяется.

### Response

HTTP 200 с Warehouse.

### Структура ответа

Полное публичное представление Warehouse.

### Используемые модели

ORM `Warehouse`, публичный Warehouse.

### Бизнес-правила

Warehouse должен существовать.

### Проверки валидации

Path должен преобразовываться в integer.

### Ошибки

- 400 `validation_error` — нечисловой ID.
- 404 `warehouse_not_found` — запись отсутствует.
- 500 `internal_error`.

### Побочные эффекты

Не применяется.

### Kafka события

Не применяется.

### Изменяемые таблицы

Не применяется; `warehouses` только читается.

### Acceptance Criteria

- Существующий ID возвращает соответствующий Warehouse.
- Отсутствующий ID возвращает 404 в общем error envelope.

### QA Checklist

- Проверить существующий, отсутствующий, отрицательный и нечисловой ID.
- Проверить отсутствие связанных stock данных в ответе.

### Ограничения

Связанные `warehouse_products` не возвращаются.

## PUT /warehouses/{warehouse_id}

### Назначение

Частично изменить описательные поля склада.

### Ответственность

Найти Warehouse, применить переданные поля и вернуть итоговое состояние.

### Источник данных

Path/body и таблица `warehouses`.

### HTTP Method

`PUT`.

### URL

`/warehouses/{warehouse_id}`.

### Path Parameters

`warehouse_id` — integer PK.

### Query Parameters

Не применяется.

### Request Body

Любое подмножество `name`, `weekday_hours`, `address`; пустой объект принимается.

### Response

HTTP 200 с итоговым Warehouse.

### Структура ответа

Полное состояние Warehouse, включая непереданные поля.

### Используемые модели

Контракт обновления Warehouse, публичный Warehouse, ORM `Warehouse`.

### Бизнес-правила

Warehouse должен существовать.

### Проверки валидации

Переданные поля имеют длины как при создании. Явный null разрешён optional-схемой, но нарушает NOT NULL и специализированно не обрабатывается.

### Ошибки

- 400 `validation_error` — неверный path/body.
- 404 `warehouse_not_found`.
- 500 `internal_error` — включая не перехваченный DB IntegrityError при null.

### Побочные эффекты

Меняет описательные поля; stock rows и Product.stocks не меняются.

### Kafka события

Не применяется.

### Изменяемые таблицы

`warehouses`: UPDATE при фактическом изменении.

### Acceptance Criteria

- Переданные поля обновляются, непереданные сохраняются.
- Пустой JSON возвращает текущее состояние.
- Операция не меняет остатки и не публикует события.

### QA Checklist

- Обновить каждое поле и комбинации.
- Проверить пустой JSON, явные null и границы длины.
- Проверить отсутствующий/нечисловой ID и неизменность stock.

### Ограничения

PUT имеет partial-update семантику. DB IntegrityError отдельно не преобразуется в доменную ошибку.

## DELETE /warehouses/{warehouse_id}

### Назначение

Физически удалить склад и исключить его остатки из агрегатов товаров.

### Ответственность

Определить затронутые Product, удалить warehouse stock rows и Warehouse, пересчитать агрегаты и опубликовать новые значения.

### Источник данных

Таблицы `warehouses`, `warehouse_products`, `products`.

### HTTP Method

`DELETE`.

### URL

`/warehouses/{warehouse_id}`.

### Path Parameters

`warehouse_id` — integer PK.

### Query Parameters

Не применяется.

### Request Body

Не применяется.

### Response

HTTP 204 без тела.

### Структура ответа

Не применяется.

### Используемые модели

ORM `Warehouse`, `WarehouseProduct`, `Product`.

### Бизнес-правила

Warehouse должен существовать. Для каждого затронутого Product агрегированный stock становится суммой оставшихся warehouse rows.

### Проверки валидации

Path должен быть integer.

### Ошибки

- 400 `validation_error` — неверный path.
- 404 `warehouse_not_found`.
- 500 `internal_error` — необработанная DB/Kafka-side ошибка до формирования ответа.

### Побочные эффекты

Сначала удаляет warehouse rows и Warehouse одним commit, затем отдельно пересчитывает каждый Product с отдельными commit внутри recalculation.

### Kafka события

Для каждого затронутого существующего Product публикуется `STOCK_REPLENISHED` с новым `total_quantity`, даже если stock уменьшился.

### Изменяемые таблицы

`warehouse_products`: DELETE; `warehouses`: DELETE; `products`: UPDATE `stocks` для затронутых товаров.

### Acceptance Criteria

- Пустой Warehouse удаляется с 204 без stock events.
- Warehouse с остатками удаляется вместе со строками; агрегаты равны сумме оставшихся складов.
- На каждый затронутый Product инициируется stock event с итоговым агрегатом.

### QA Checklist

- Удалить пустой склад и склад с одним/несколькими товарами.
- Проверить товары, имеющие остатки также на других складах.
- Проверить таблицы, агрегаты, количество/payload событий и 204 body.
- Проверить отсутствующий/нечисловой ID.

### Ограничения

Название события не различает увеличение и уменьшение stock. Удаление и последующие пересчёты/публикации не являются одной общей транзакцией.

