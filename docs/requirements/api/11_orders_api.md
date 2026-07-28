# Customer API Contract Specification — заказы

## Общая структура

Order response содержит `id`, `user_id`, `order_number`, строковый `status`, items, сумму quantity `total_items_count`, сохранённый `total_price`, `created_at`. OrderItem содержит ID, текущий Product summary, сохранённые `quantity`, `unit_price`, `total_price`, `created_at`.

Историчность смешанная: цены/quantity/line total сохранены при создании; name, current price внутри Product summary, stocks и flags читаются из текущей локальной Product. Поэтому `item.unit_price` может отличаться от `item.product.price`.

## POST /orders

### Назначение

Создать Order из явных items либо из текущей cart.

### Ответственность

Определить набор позиций, агрегировать дубли, проверить локальные Product, сохранить Order/OrderItem, очистить cart и инициировать supplier-side списание через Kafka.

### Источник данных

Request body; `users`, `products`, `cart_items`, `orders`, `order_items` в `customer_db`.

### HTTP Method

`POST`.

### URL

`/orders`.

### Path Parameters

Не применяется.

### Query Parameters

Не применяется.

### Request Body

Обязательный `user_id>0`; optional `items`. Каждый item: `product_id>0`, `quantity>0`. Если `items` имеет непустой массив, используется он. `items` отсутствующий, `null` и пустой массив обрабатываются одинаково: позиции берутся из cart.

### Response

HTTP 201 с созданным Order.

### Структура ответа

Order number формируется как `ORD-` и ID с шестью цифрами; status `created`; items по OrderItem ID; total items count — сумма quantities; total price — сохранённая сумма line totals.

### Используемые модели

Order create/item/read/list, Product summary; ORM `User`, `Product`, `CartItem`, `Order`, `OrderItem`.

### Бизнес-правила

- User существует.
- Непустые body items приоритетны над cart.
- Дубли `product_id` в body items суммируются до проверки/создания.
- При выборе cart пустая cart даёт `cart_is_empty`.
- Каждый Product существует, не archived, active, quantity ≤ local stocks.
- Цена фиксируется из local Product на момент создания.
- После успешного создания удаляются все CartItem User, даже если заказ создан из body items.
- Начальный status — `created`.

### Проверки валидации

`user_id`, `product_id`, `quantity` — integer >0. `items` должен быть массивом или null. Нарушения `>0` получают нормализованное сообщение `Quantity must be greater than zero` независимо от поля.

### Ошибки

- 400 `validation_error` — неверное тело.
- 400 `cart_is_empty` — body items отсутствуют/null/пусты и cart пуста.
- 404 `user_not_found` или `product_not_found`.
- 409 `product_archived`, `product_inactive`, `insufficient_stock`.
- 500 `internal_error` — необработанная DB/producer/serialization ошибка.

### Побочные эффекты

В одной DB transaction создаются Order/OrderItem и удаляется вся cart User. После commit producer вызывается по каждой агрегированной позиции. Order ID может быть consumed sequence даже при rollback, что является поведением PostgreSQL sequence.

### Kafka события

После DB commit публикуется отдельный `ORDER_CREATED` в `order-events` на каждый уникальный Product: новый UUID `event_id`, `product_id`, aggregated `quantity`, `event_version=1`; key = Product ID. Order ID/number/user ID/price в событии отсутствуют.

### Изменяемые таблицы

- `orders`: INSERT.
- `order_items`: INSERT по уникальному Product.
- `cart_items`: DELETE всех позиций User.
- `users`, `products`: чтение.

### Acceptance Criteria

- Непустые body items создают Order независимо от содержимого cart и агрегируют дубли.
- None/empty/missing items выбирают cart; пустая cart возвращает 400.
- Все позиции валидируются до INSERT Order.
- Успешный commit фиксирует цены, status/number и очищает всю cart.
- Kafka publish вызывается после commit и не является supplier confirmation.
- HTTP 201 не означает, что supplier stock уже списан.

### QA Checklist

- Создать из cart, body, `items=null`, `items=[]`, без поля items.
- Проверить дубли Product и суммарное quantity против stock.
- Проверить несколько Product, prices, округление, number/status.
- Проверить inactive/archived/missing/insufficient Product и пустую cart.
- При body items проверить полную очистку независимой cart.
- Сопоставить DB commit и по одному событию на уникальный Product.
- Проверить lag до обновления local stock.

### Ограничения

- Stock проверяется по eventual-consistent projection и не резервируется заранее.
- Нет общей DB/Kafka transaction или outbox.
- Если producer вызов завершится ошибкой после commit, API может вернуть 500 при уже созданном Order и очищенной cart.
- Supplier-service не подтверждает полное/частичное списание и не меняет status.
- Деньги — float.

## GET /orders?user_id=

### Назначение

Получить список Order указанного User.

### Ответственность

Проверить User, прочитать видимые Orders и собрать вложенные items с текущими Product summary.

### Источник данных

`users`, `orders`, `order_items`, `products`.

### HTTP Method

`GET`.

### URL

`/orders`.

### Path Parameters

Не применяется.

### Query Parameters

Обязательный integer `user_id`; `>0` явно не задано.

### Request Body

Не применяется.

### Response

HTTP 200.

### Структура ответа

`items` — Orders с непустым `order_number` по убыванию Order ID; `count`. Внутри OrderItem идут по возрастанию ID.

### Используемые модели

Order list/read/item, Product summary; ORM `User`, `Order`, `OrderItem`, `Product`.

### Бизнес-правила

User существует. Возвращаются его Orders с `order_number IS NOT NULL`, независимо от status.

### Проверки валидации

Query обязателен и должен быть integer.

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

- User без Orders получает пустой list.
- Orders возвращаются newest-first, count согласован.
- `created` и `cancelled` присутствуют; строки без order number скрыты.
- Stored prices сохраняются, Product summary отражает текущую проекцию.

### QA Checklist

- Проверить empty/multiple Orders, порядок/count и оба status.
- Изменить Product price/stock/flags после Order и сравнить stored/current поля.
- Проверить missing/invalid User и связанную Product.

### Ограничения

Нет пагинации/фильтра status. Чтение Order зависит от существования current Product projection.

## GET /orders/{order_id}?user_id=

### Назначение

Получить один Order, принадлежащий User.

### Ответственность

Проверить User и ownership, затем собрать Order с позициями.

### Источник данных

`users`, `orders`, `order_items`, `products`.

### HTTP Method

`GET`.

### URL

`/orders/{order_id}`.

### Path Parameters

`order_id` — integer; `>0` явно не задано.

### Query Parameters

Обязательный integer `user_id`.

### Request Body

Не применяется.

### Response

HTTP 200 с Order.

### Структура ответа

Полный Order с stored totals и current Product summaries.

### Используемые модели

Order read/item, Product summary; ORM `User`, `Order`, `OrderItem`, `Product`.

### Бизнес-правила

User существует; Order имеет указанные `id`, `user_id` и непустой order number.

### Проверки валидации

Path/query должны быть integer.

### Ошибки

- 400 `validation_error`.
- 404 `user_not_found` — User отсутствует.
- 404 `order_not_found` — Order отсутствует, принадлежит другому User или не имеет number.
- 404 `product_not_found` — Product отсутствует при сериализации.
- 500 `internal_error`.

### Побочные эффекты

Не применяется.

### Kafka события

Не применяется.

### Изменяемые таблицы

Не применяется; только чтение.

### Acceptance Criteria

- Правильная пара order/user возвращает Order.
- Чужой Order не раскрывается и даёт `order_not_found`.
- Stored item prices могут отличаться от current Product summary.

### QA Checklist

- Проверить собственный/чужой/отсутствующий Order.
- Проверить invalid/missing IDs и User precedence.
- Проверить order без number, current/stored Product fields.

### Ограничения

Ownership основан только на входном user ID без auth. Product summary не исторический.

## POST /orders/{order_id}/cancel?user_id=

### Назначение

Перевести Order User в status `cancelled`.

### Ответственность

Проверить User/ownership, запретить повторную отмену, сохранить status и вернуть Order.

### Источник данных

`users`, `orders`, `order_items`, `products`.

### HTTP Method

`POST`.

### URL

`/orders/{order_id}/cancel`.

### Path Parameters

`order_id` — integer.

### Query Parameters

Обязательный integer `user_id`.

### Request Body

Не применяется.

### Response

HTTP 200 с Order в status `cancelled`.

### Структура ответа

Полный Order; позиции/цены сохраняются, Product summaries читаются текущими.

### Используемые модели

Order read/item, Product summary; ORM `User`, `Order`, `OrderItem`, `Product`.

### Бизнес-правила

User существует; Order принадлежит User и имеет number; status не равен `cancelled`. Текущий API создаёт status `created` и переводит его в `cancelled`.

### Проверки валидации

Path/query должны быть integer.

### Ошибки

- 400 `validation_error`.
- 404 `user_not_found`.
- 404 `order_not_found` — missing/foreign/numberless Order.
- 409 `order_already_cancelled`.
- 404 `product_not_found` может возникнуть после успешного status commit при сериализации, если Product отсутствует.
- 500 `internal_error`.

### Побочные эффекты

UPDATE `orders.status`; запись логируется. Stock, cart и order items не меняются.

### Kafka события

Не применяется. Событие отмены или возврата stock не публикуется.

### Изменяемые таблицы

`orders`: UPDATE status. Остальные таблицы читаются для ответа.

### Acceptance Criteria

- Доступный Order меняет status на `cancelled` и возвращается с 200.
- Повторная отмена возвращает 409 без нового изменения.
- Supplier stock и cart не восстанавливаются; Kafka event отсутствует.
- Чужой Order возвращает `order_not_found`.

### QA Checklist

- Отменить created Order и повторить.
- Проверить foreign/missing Order, missing/invalid User.
- Проверить неизменность items, totals, cart и supplier/customer stock.
- Убедиться в отсутствии Kafka event.
- Проверить поведение при отсутствующей local Product.

### Ограничения

- Нет compensating stock flow.
- Нет supplier notification или order confirmation.
- Реализация запрещает только уже `cancelled`; другие status, если появятся в БД вне API, также будут переведены в `cancelled`.
- Status может быть committed до ошибки сериализации ответа.

