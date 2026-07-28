# Supplier API Contract Specification — товары

## Общие структуры

Product response содержит `id`, `supplier_id`, `name`, nullable `description`, `price`, агрегированный `stocks`, вычисленный `total_price`, `is_active`, `is_archived`, `created_at`. `total_price` не хранится и рассчитывается как `price × stocks` с округлением до двух знаков. List response содержит `items` и `count`.

## POST /products

### Назначение

Создать товар существующего поставщика.

### Ответственность

Проверить supplier и состояние товара, сохранить карточку с нулевым агрегированным остатком и инициировать синхронизацию customer-каталога.

### Источник данных

Request body и таблица `users`; созданная запись — `products`.

### HTTP Method

`POST`.

### URL

`/products`.

### Path Parameters

Не применяется.

### Query Parameters

Не применяется.

### Request Body

JSON: обязательные `supplier_id`, `name`, `price`; optional `description`; optional flags `is_active` (default true), `is_archived` (default false). `stocks` клиентом не задаётся.

### Response

HTTP 201 с Product.

### Структура ответа

Полная карточка; `stocks=0`, `total_price=0`, серверные ID и `created_at`.

### Используемые модели

Контракт создания Product, публичный Product, ORM `Product`, ORM `Supplier`.

### Бизнес-правила

Supplier существует. Архивный товар не может одновременно быть активным. Начальный агрегированный stock всегда 0.

### Проверки валидации

`supplier_id>0`; `name` 1–255 и после trim не пуст; `description` nullable, до 1000; `price>0`; flags — boolean.

### Ошибки

- 400 `validation_error` — неверное тело.
- 404 `supplier_not_found` — supplier отсутствует.
- 409 `product_archived` — переданы `is_archived=true` и `is_active=true`.
- 500 `internal_error` — необработанная ошибка.

### Побочные эффекты

Создаёт Product; складские строки не создаются.

### Kafka события

После commit публикуется `PRODUCT_CREATED` в `product-events`, key = product ID, payload = полный Product, `event_version=1`.

### Изменяемые таблицы

`products`: INSERT. `users`: только чтение.

### Acceptance Criteria

- Валидный товар существующего supplier создаётся с 201 и нулевым stock.
- Несуществующий supplier и недопустимое сочетание flags не создают товар.
- Успешная операция инициирует `PRODUCT_CREATED` после commit.

### QA Checklist

- Проверить обязательность/границы полей, trim имени и price.
- Проверить default и все сочетания flags.
- Проверить отсутствующего supplier.
- Сопоставить API, БД и Kafka payload.

### Ограничения

Уникальность name/SKU не определена; SKU и валюта отсутствуют. Delivery Kafka не подтверждается.

## GET /products

### Назначение

Получить полный supplier-каталог.

### Ответственность

Прочитать все Product, рассчитать total price и вернуть список по ID.

### Источник данных

Таблица `products` в `supplier_db`.

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

`items` — Product по возрастанию ID; `count` — число элементов.

### Используемые модели

ORM `Product`, публичный Product, список Product.

### Бизнес-правила

Возвращаются активные, неактивные и архивные товары без фильтрации.

### Проверки валидации

Не применяется.

### Ошибки

Специализированные 4xx не определены; возможен 500 `internal_error`.

### Побочные эффекты

Не применяется.

### Kafka события

Не применяется.

### Изменяемые таблицы

Не применяется; `products` только читается.

### Acceptance Criteria

- Пустой каталог возвращает `items=[]`, `count=0`.
- Все товары возвращаются по ID; total price соответствует текущим price и stocks.

### QA Checklist

- Проверить пустой/непустой каталог и порядок.
- Проверить присутствие archived/inactive.
- Проверить `count` и округление total price.

### Ограничения

Нет пагинации, фильтрации, поиска или параметров сортировки. Endpoint используется customer-service для snapshot sync.

## GET /products/{product_id}

### Назначение

Получить Product по ID.

### Ответственность

Вернуть карточку и вычисленные значения либо контролируемую ошибку отсутствия.

### Источник данных

Таблица `products`.

### HTTP Method

`GET`.

### URL

`/products/{product_id}`.

### Path Parameters

`product_id` — integer PK; `>0` отдельно не проверяется.

### Query Parameters

Не применяется.

### Request Body

Не применяется.

### Response

HTTP 200 с Product.

### Структура ответа

Полная карточка Product, включая агрегированный stock и total price.

### Используемые модели

ORM `Product`, публичный Product.

### Бизнес-правила

Product должен существовать; flags не ограничивают чтение.

### Проверки валидации

Path должен преобразовываться в integer.

### Ошибки

- 400 `validation_error` — нечисловой path.
- 404 `product_not_found` — Product отсутствует.
- 500 `internal_error`.

### Побочные эффекты

Не применяется.

### Kafka события

Не применяется.

### Изменяемые таблицы

Не применяется; `products` только читается.

### Acceptance Criteria

- Существующий ID возвращает правильный Product.
- Отсутствующий ID возвращает 404; archived Product остаётся доступен для чтения.

### QA Checklist

- Проверить существующий, отсутствующий, отрицательный и нечисловой ID.
- Проверить вычисление total price и archived Product.

### Ограничения

Складская детализация в ответ не входит.

## PUT /products/{product_id}

### Назначение

Частично изменить карточку Product без прямого изменения stock.

### Ответственность

Проверить Product, optional нового supplier, итоговое сочетание flags, сохранить переданные поля и опубликовать итоговую карточку.

### Источник данных

Path/body, таблицы `products` и при смене supplier — `users`.

### HTTP Method

`PUT`.

### URL

`/products/{product_id}`.

### Path Parameters

`product_id` — integer PK.

### Query Parameters

Не применяется.

### Request Body

Любое подмножество `supplier_id`, `name`, `description`, `price`, `is_active`, `is_archived`. `stocks` не принимается. Пустой объект принимается.

### Response

HTTP 200 с итоговым Product.

### Структура ответа

Полная карточка после операции; stock сохраняется, total price пересчитывается.

### Используемые модели

Контракт обновления Product, публичный Product, ORM `Product`, ORM `Supplier`.

### Бизнес-правила

Product и указанный supplier существуют. Итоговое состояние не может быть одновременно active и archived.

### Проверки валидации

Переданные: `supplier_id>0`; trimmed непустой `name` до 255; nullable `description` до 1000; `price>0`; flags boolean. Явный null разрешён схемой optional-полей, но может нарушить NOT NULL колонки.

### Ошибки

- 400 `validation_error` — неверный path/body.
- 404 `product_not_found` или `supplier_not_found`.
- 409 `product_archived` — недопустимое итоговое состояние.
- 500 `internal_error` — включая не перехваченные DB IntegrityError при null в NOT NULL поле.

### Побочные эффекты

Меняет переданные поля Product; складские строки и stock не меняет.

### Kafka события

После commit публикуется `PRODUCT_UPDATED` с полным итоговым Product. Пустой JSON также проходит commit/publish path.

### Изменяемые таблицы

`products`: UPDATE при фактических изменениях. `users`: только проверка.

### Acceptance Criteria

- Непереданные поля и stock сохраняются.
- Перенос к существующему supplier допускается; отсутствующий даёт 404.
- Недопустимое итоговое сочетание flags даёт 409.
- Успешная операция инициирует полное `PRODUCT_UPDATED`.

### QA Checklist

- Обновить каждое поле, комбинации и пустой JSON.
- Проверить trim name, price, смену supplier, явные null.
- Проверить переходы flags с учётом текущего состояния.
- Проверить неизменность warehouse rows/stock и Kafka payload.

### Ограничения

HTTP PUT реализован как partial update. Изменять stock этим endpoint нельзя. DB IntegrityError специально не нормализуется.

## DELETE /products/{product_id}

### Назначение

Архивировать Product без физического удаления.

### Ответственность

Установить фиксированное архивное состояние и распространить его в customer-проекцию.

### Источник данных

Таблица `products`.

### HTTP Method

`DELETE`.

### URL

`/products/{product_id}`.

### Path Parameters

`product_id` — integer PK.

### Query Parameters

Не применяется.

### Request Body

Не применяется.

### Response

HTTP 204 без тела.

### Структура ответа

Не применяется.

### Используемые модели

ORM `Product`; публичный Product используется для события.

### Бизнес-правила

Product должен существовать. Удаление означает `is_active=false`, `is_archived=true`.

### Проверки валидации

Path должен быть integer.

### Ошибки

- 400 `validation_error` — неверный path.
- 404 `product_not_found` — Product отсутствует.
- 500 `internal_error`.

### Побочные эффекты

Сохраняет Product, supplier relation и warehouse stocks; повторный DELETE повторно записывает те же flags.

### Kafka события

После commit публикуется `PRODUCT_UPDATED`, не `PRODUCT_DELETED`, с архивным состоянием.

### Изменяемые таблицы

`products`: UPDATE flags. `warehouse_products` не меняется.

### Acceptance Criteria

- Существующий Product получает flags false/true и ответ 204 без тела.
- Строка Product и складские остатки остаются.
- Customer-синхронизация получает `PRODUCT_UPDATED`.

### QA Checklist

- Архивировать active Product и проверить БД/Kafka/customer projection после доставки.
- Повторить DELETE.
- Проверить отсутствующий/нечисловой ID и отсутствие response body.
- Проверить сохранность stock rows.

### Ограничения

Endpoint не выполняет физическое удаление и не публикует `PRODUCT_DELETED`; подтверждение обновления customer-service отсутствует.

