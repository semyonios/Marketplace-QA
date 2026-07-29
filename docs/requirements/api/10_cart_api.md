# Customer API Contract Specification — корзина

## Общая структура

Cart item содержит ID, user ID, текущий Product summary, quantity, текущую `unit_price`, вычисленную `total_price`, timestamps. Cart содержит items, количество позиций `count`, сумму quantity `total_items_count` и сумму текущих line totals `total_price`. Цена не фиксируется в `cart_items`.

## GET /cart?user_id=

### Назначение

Получить текущую корзину User.

### Ответственность

Проверить User, прочитать позиции и рассчитать цены/агрегаты по текущей product-проекции.

### Источник данных

`users`, `cart_items`, `products` в `customer_db`.

### HTTP Method

`GET`.

### URL

`/cart`.

### Path Parameters

Не применяется.

### Query Parameters

Обязательный integer `user_id`; `>0` явно не задано.

### Request Body

Не применяется.

### Response

HTTP 200.

### Структура ответа

Items по CartItem ID; `count`; сумма quantities; total price с округлением до двух знаков.

### Используемые модели

Cart read/item, Product summary, ORM `User`, `CartItem`, `Product`.

### Бизнес-правила

User существует. Чтение не блокирует archived/inactive и не перепроверяет quantity против stock.

### Проверки валидации

`user_id` обязателен и должен быть integer.

### Ошибки

- 400 `validation_error` — missing/invalid query.
- 404 `user_not_found`.
- 404 `product_not_found` — связанная Product отсутствует при сериализации.
- 500 `internal_error`.

### Побочные эффекты

Не применяется.

### Kafka события

Не применяется.

### Изменяемые таблицы

Не применяется; таблицы только читаются.

### Acceptance Criteria

- Пустая корзина существующего User возвращает нулевые агрегаты.
- Непустая корзина рассчитывается по текущим локальным prices.
- Изменение product price/flags отражается при следующем чтении.

### QA Checklist

- Проверить пустую/непустую корзину, порядок и агрегаты.
- Изменить проекцию цены и проверить пересчёт.
- Проверить archived/inactive и stock ниже quantity.

### Ограничения

Ответ не является ценовым snapshot и зависит от доступности локальных Product.

## POST /cart

### Назначение

Добавить Product в cart или увеличить quantity существующей позиции.

### Ответственность

Проверить User, локальный Product, flags и локальный stock; сохранить итоговое quantity.

### Источник данных

Request body; `users`, `products`, `cart_items`.

### HTTP Method

`POST`.

### URL

`/cart`.

### Path Parameters

Не применяется.

### Query Parameters

Не применяется.

### Request Body

Обязательные integer `user_id>0`, `product_id>0`, `quantity>0`.

### Response

HTTP 201 как для новой позиции, так и для увеличения существующей.

### Структура ответа

Итоговая CartItem с текущим Product summary, unit price и line total.

### Используемые модели

Cart create/read, Product summary, ORM `User`, `Product`, `CartItem`.

### Бизнес-правила

Product должен быть не archived и active. Итоговое quantity (старое + входное) не превышает локальный Product.stocks. Stock не резервируется и не уменьшается.

### Проверки валидации

Все поля integer >0; любое нарушение `>0` получает общее сообщение `Quantity must be greater than zero`.

### Ошибки

- 400 `validation_error`.
- 404 `user_not_found` или `product_not_found`.
- 409 `product_archived`, `product_inactive`, `insufficient_stock`.
- 500 `internal_error`.

### Побочные эффекты

INSERT новой CartItem или UPDATE quantity; обновляется DB-managed `updated_at` при update.

### Kafka события

Не применяется.

### Изменяемые таблицы

`cart_items`: INSERT/UPDATE. `users`, `products`: чтение.

### Acceptance Criteria

- Новая валидная позиция создаётся; повторный POST суммирует quantity.
- Archived проверяется раньше inactive; недоступные flags дают 409.
- Итог выше локального stock даёт 409 без изменения cart.
- Supplier stock не меняется.

### QA Checklist

- Добавить новую позицию и увеличить существующую.
- Проверить границу stock и превышение суммарным quantity.
- Проверить archived, inactive, оба flags, zero stock.
- Проверить unknown IDs и ID/quantity ≤0.
- Учитывать stale local stock относительно supplier.

### Ограничения

Проверка выполняется по eventual-consistent проекции; отсутствует резервирование и supplier round-trip.

## PATCH /cart/{product_id}

### Назначение

Заменить quantity существующей CartItem.

### Ответственность

Проверить User/Product/flags, существование item и доступный локальный stock; установить новое количество.

### Источник данных

Path/body; `users`, `products`, `cart_items`.

### HTTP Method

`PATCH`.

### URL

`/cart/{product_id}`.

### Path Parameters

`product_id` — integer; `>0` отдельно не задано path-схемой.

### Query Parameters

Не применяется.

### Request Body

Обязательные integer `user_id>0`, `quantity>0`.

### Response

HTTP 200 с обновлённой CartItem.

### Структура ответа

CartItem с новым quantity и текущими product/price/total.

### Используемые модели

Cart quantity update/read, Product summary, ORM `User`, `Product`, `CartItem`.

### Бизнес-правила

User и Product проверяются до поиска CartItem. Product active/not archived. Новое quantity заменяет, а не прибавляет, и не превышает local stock.

### Проверки валидации

Body IDs/quantity >0; path integer.

### Ошибки

- 400 `validation_error`.
- 404 `user_not_found`, `product_not_found`, `cart_item_not_found`.
- 409 `product_archived`, `product_inactive`, `insufficient_stock`.
- 500 `internal_error`.

### Побочные эффекты

UPDATE quantity и `updated_at`.

### Kafka события

Не применяется.

### Изменяемые таблицы

`cart_items`: UPDATE; другие таблицы читаются.

### Acceptance Criteria

- Существующая позиция получает ровно входное quantity.
- Отсутствующая CartItem даёт 404 только после успешной проверки User/Product/flags.
- Недоступный Product или превышение stock не меняют item.

### QA Checklist

- Заменить quantity вверх/вниз и на stock boundary.
- Проверить 0/negative, превышение, stale stock.
- Проверить порядок ошибок для missing item и archived/missing Product.

### Ограничения

Нельзя установить 0 для удаления; используется DELETE. Проверка stock локальная.

## DELETE /cart/{product_id}?user_id=

### Назначение

Удалить CartItem по паре User–Product.

### Ответственность

Проверить User, найти и удалить позицию.

### Источник данных

`users`, `cart_items`.

### HTTP Method

`DELETE`.

### URL

`/cart/{product_id}`.

### Path Parameters

`product_id` — integer; Product existence/flags не проверяются.

### Query Parameters

Обязательный integer `user_id`.

### Request Body

Не применяется.

### Response

HTTP 204 без тела.

### Структура ответа

Не применяется.

### Используемые модели

ORM `User`, `CartItem`.

### Бизнес-правила

User и CartItem pair должны существовать.

### Проверки валидации

Path/query должны быть integer; `>0` явно не задано.

### Ошибки

- 400 `validation_error`.
- 404 `user_not_found` или `cart_item_not_found`.
- 500 `internal_error`.

### Побочные эффекты

Физически удаляет CartItem.

### Kafka события

Не применяется.

### Изменяемые таблицы

`cart_items`: DELETE; `users`: чтение.

### Acceptance Criteria

- Существующая pair удаляется с 204 без тела.
- Повторный вызов даёт `cart_item_not_found`.
- Product state и supplier stock не участвуют.

### QA Checklist

- Удалить item и повторить.
- Проверить unknown User, invalid/missing parameters.
- Удалить item archived/missing Product при наличии строки.

### Ограничения

Не возвращает удалённую запись и не изменяет stock.

