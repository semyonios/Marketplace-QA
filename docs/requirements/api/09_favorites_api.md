# Customer API Contract Specification — избранное

## Общая структура

Favorite response содержит ID связи, user ID, текущий Product summary (`id`, `name`, `price`, `stocks`, flags) и время создания связи. Product summary строится при каждом ответе из локальной product-проекции.

## POST /favorites

### Назначение

Добавить локальный Product в избранное User или вернуть существующую связь.

### Ответственность

Проверить User/Product, обеспечить одну Favorite на пару и сериализовать текущую карточку товара.

### Источник данных

Request body; таблицы `users`, `products`, `favorites` в `customer_db`.

### HTTP Method

`POST`.

### URL

`/favorites`.

### Path Parameters

Не применяется.

### Query Parameters

Не применяется.

### Request Body

JSON с обязательными integer `user_id>0` и `product_id>0`.

### Response

HTTP 201 как при создании, так и при возврате уже существующей Favorite.

### Структура ответа

`id`, `user_id`, вложенный текущий Product summary, `created_at`.

### Используемые модели

Контракт создания/чтения Favorite, Product summary, ORM `User`, `Product`, `Favorite`.

### Бизнес-правила

User и локальный Product существуют. Пара user–product уникальна. Flags и stock не проверяются: inactive, archived и zero-stock Product можно добавить, если локальная запись существует.

### Проверки валидации

Оба ID обязательны, integer и >0. Сообщение для нарушения `>0` нормализуется как `Quantity must be greater than zero`, хотя поле является ID.

### Ошибки

- 400 `validation_error` — неверное тело.
- 404 `user_not_found` или `product_not_found`.
- 500 `internal_error`.

### Побочные эффекты

При отсутствии связи добавляет Favorite; повторный вызов БД не меняет.

### Kafka события

Не применяется.

### Изменяемые таблицы

`favorites`: INSERT только при первом добавлении. `users`, `products`: чтение.

### Acceptance Criteria

- Первая валидная операция создаёт Favorite и возвращает 201.
- Повторная операция возвращает ту же запись с 201 без дубля.
- Archived/inactive Product не блокируется.

### QA Checklist

- Добавить active, inactive, archived и zero-stock Product.
- Повторить одинаковый запрос и проверить ID/count строк.
- Проверить отсутствующие User/Product и ID ≤0.
- Изменить product-проекцию и проверить текущий summary.

### Ограничения

HTTP 201 не различает создание и возврат существующей записи. Product summary не является snapshot на момент добавления.

## GET /favorites?user_id=

### Назначение

Получить избранное User.

### Ответственность

Проверить User, прочитать Favorite по ID и дополнить каждую текущим Product summary.

### Источник данных

Таблицы `users`, `favorites`, `products`.

### HTTP Method

`GET`.

### URL

`/favorites`.

### Path Parameters

Не применяется.

### Query Parameters

Обязательный `user_id` типа integer. Ограничение `>0` явно не задано.

### Request Body

Не применяется.

### Response

HTTP 200.

### Структура ответа

`items` — Favorite по возрастанию Favorite ID; `count`; каждый item содержит текущий Product summary.

### Используемые модели

Favorite list/read, Product summary, ORM `User`, `Favorite`, `Product`.

### Бизнес-правила

User должен существовать. Flags товара не фильтруют список.

### Проверки валидации

Query parameter обязателен и должен быть integer.

### Ошибки

- 400 `validation_error` — параметр отсутствует/не integer.
- 404 `user_not_found` — User отсутствует.
- 404 `product_not_found` — связанная локальная Product отсутствует при сериализации.
- 500 `internal_error`.

### Побочные эффекты

Не применяется.

### Kafka события

Не применяется.

### Изменяемые таблицы

Не применяется; три таблицы только читаются.

### Acceptance Criteria

- Существующий User без favorites получает пустой список.
- Items отсортированы по Favorite ID, count согласован.
- Archived/inactive Product остаётся в результате.

### QA Checklist

- Проверить пустой/непустой список, порядок/count.
- Проверить отсутствие/формат user_id и неизвестного User.
- Проверить текущие price/stock/flags во вложенном Product.

### Ограничения

Пагинации нет. Утрата связанной локальной Product делает конкретный item несериализуемым и приводит к 404.

## DELETE /favorites/{product_id}?user_id=

### Назначение

Удалить Favorite по паре User–Product.

### Ответственность

Проверить User, найти связь и физически удалить её.

### Источник данных

Таблицы `users`, `favorites`.

### HTTP Method

`DELETE`.

### URL

`/favorites/{product_id}`.

### Path Parameters

`product_id` — integer; существование Product отдельно не проверяется.

### Query Parameters

Обязательный integer `user_id`.

### Request Body

Не применяется.

### Response

HTTP 204 без тела.

### Структура ответа

Не применяется.

### Используемые модели

ORM `User`, `Favorite`.

### Бизнес-правила

User и Favorite pair должны существовать.

### Проверки валидации

Path/query должны быть integer; `>0` явно не задано.

### Ошибки

- 400 `validation_error` — missing/invalid parameter.
- 404 `user_not_found`.
- 404 `favorite_not_found` — связь отсутствует.
- 500 `internal_error`.

### Побочные эффекты

Физически удаляет Favorite.

### Kafka события

Не применяется.

### Изменяемые таблицы

`favorites`: DELETE; `users`: чтение.

### Acceptance Criteria

- Существующая пара удаляется и возвращает 204 без тела.
- Повторный DELETE возвращает `favorite_not_found`.
- Product lookup не выполняется.

### QA Checklist

- Удалить существующую связь и повторить.
- Проверить неверные/отсутствующие параметры и неизвестного User.
- Проверить archived Product и отсутствие Kafka events.

### Ограничения

Product может отсутствовать, если Favorite row каким-либо образом сохранилась; endpoint ориентируется только на пару ID.

