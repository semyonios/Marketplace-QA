# Customer API Contract Specification — обзор

## 1. Назначение customer-service

`customer-service` — HTTP-сервис customer-домена Marketplace-QA. Он предоставляет API на порту `8001` для покупателей, локального каталога, избранного, корзины и заказов; хранит customer-состояние в PostgreSQL; синхронизирует Product из supplier-service; публикует созданные заказы в Kafka.

Документ фиксирует только текущую реализацию AS IS. Контракты доменных endpoint находятся в соседних документах каталога.

## 2. Ответственность

Сервис отвечает за:

- создание и чтение покупателей;
- чтение локальной проекции supplier-каталога;
- хранение и выдачу favorites;
- хранение корзин и расчёт их текущей стоимости;
- создание, чтение и отмену заказов;
- фиксацию цены позиции и общей суммы заказа на момент создания;
- публикацию `ORDER_CREATED` после commit заказа;
- потребление product/stock events;
- полную HTTP-синхронизацию каталога при старте и fallback-синхронизацию;
- единый формат HTTP-ошибок;
- создание и частичную адаптацию схемы БД при старте.

## 3. Владелец данных и Source of Truth

`customer-service` является владельцем и Source of Truth для:

- покупателей (`users` customer DB);
- favorites;
- cart items;
- orders и order items;
- локального статуса заказа (`created` или `cancelled` в текущем API).

Он не является Source of Truth для supplier Product, цены, flags или stock. Таблица `products` в `customer_db` — локальная проекция состояния supplier-service. Источником истины для этих данных остаётся supplier-service / `supplier_db`.

## 4. Хранимые данные

| Область | Таблица | Содержание |
|---|---|---|
| Покупатели | `users` | Имя, уникальный email, время создания |
| Каталог | `products` | Локальная проекция карточки, цены, агрегированного stock и flags |
| Избранное | `favorites` | Уникальная связь user–product и время создания |
| Корзина | `cart_items` | Уникальная связь user–product, количество, timestamps |
| Заказы | `orders` | User, номер, строковый статус, сохранённая общая стоимость, время создания |
| Позиции заказов | `order_items` | Product ID, количество, сохранённые unit price и total price, время создания |

## 5. Данные, которые сервис не хранит

Customer-service не хранит:

- поставщиков и `supplier_id` в локальной Product;
- склады и warehouse-level stock;
- первичное supplier-состояние Product;
- результат применения заказа supplier-service;
- подтверждение или отклонение заказа supplier-service;
- возврат stock при отмене;
- платежи, доставку, auth, роли или сессии;
- audit supplier events и processed order event IDs supplier-service.

## 6. Локальная проекция supplier catalog

Customer `products` заполняется двумя способами:

1. consumer выполняет upsert по `PRODUCT_CREATED` и `PRODUCT_UPDATED` из `product-events`, а stock обновляет по `product-stock-events`;
2. при старте вызывается HTTP `GET /products` supplier-service, после чего выполняется upsert snapshot и попытка удалить локальные Product, отсутствующие у supplier.

Consumer также поддерживает `PRODUCT_DELETED`, хотя текущий supplier-service архивирует товар и публикует `PRODUCT_UPDATED`. Удалить локальный Product может быть невозможно из-за FK из favorites, cart или order items; такая ошибка откатывается и только логируется.

При stock event функция возвращает одинаковый результат `False` и для отсутствующего Product, и для уже совпадающего stock. В обоих случаях запускается полный HTTP sync, затем повторяется stock update.

## 7. Связь с supplier-service

- Customer-service читает полный supplier-каталог по HTTP при старте/fallback.
- Product и stock changes поступают асинхронно через Kafka.
- Customer-service публикует `ORDER_CREATED`; supplier-service асинхронно списывает warehouse stock.
- Прямого доступа к `supplier_db` нет.
- Supplier-service не отправляет customer-service подтверждение успешного, частичного или неуспешного списания конкретного заказа.

## 8. Связь с Kafka

Customer-service — consumer топиков `product-events` и `product-stock-events` в одной consumer group и producer топика `order-events`. Consumer использует manual offset commit и до трёх попыток; отдельного DLQ нет. Producer вызывает `produce()` и `poll(0)`, но не использует delivery callback и не ожидает подтверждение доставки.

`ORDER_CREATED` содержит UUID `event_id`, `product_id`, `quantity`, `event_type`, `event_version=1`; order ID и order number не передаются. На один Order создаётся отдельное событие для каждого уникального Product после агрегации дублей.

## 9. Связь с PostgreSQL

Customer-service работает только с `customer_db` в `customer-postgres`. При старте SQLAlchemy создаёт отсутствующие таблицы, точечно адаптирует legacy-колонки `orders` и добавляет product flags при их отсутствии. Отдельных миграций в репозитории нет.

Order, order items и очистка cart коммитятся одной DB-сессией до публикации Kafka. Общей транзакции между PostgreSQL и Kafka нет.

## 10. Границы ответственности

- API не аутентифицирует вызывающего; владение задаётся входным `user_id`.
- Сервис не резервирует stock при добавлении в cart.
- Проверки stock и flags выполняются только по локальному Product.
- Сервис создаёт Order до фактического списания supplier stock.
- Сервис не меняет status в зависимости от результата supplier consumer.
- Cancel меняет только локальный status и не публикует событие.
- Catalog API только читает проекцию; HTTP endpoint изменения Product отсутствуют.

## 11. Eventual consistency

Product, price, flags и stock могут временно отличаться от supplier-service. Поэтому:

- новый/обновлённый Product может появиться не сразу;
- cart/order могут проверить устаревший stock или flags;
- после создания Order локальный stock уменьшается только после цепочки `ORDER_CREATED` → supplier processing → `STOCK_DECREASED_BY_ORDER`;
- HTTP 201 заказа не подтверждает supplier-side списание;
- порядок snapshot sync, product events и stock events не защищён revision/version полем состояния.

## 12. Текущие ограничения

- Нет auth и авторизационной связи вызывающего с `user_id`.
- Нет удаления/обновления User через API.
- Нет пагинации/фильтрации списков.
- Stock может быть устаревшим.
- Cart не резервирует stock.
- Отмена Order не возвращает stock.
- Нет order confirmation event.
- Нет outbox и delivery callback.
- Нет отдельного DLQ.
- Health endpoint не проверяет зависимости.
- Деньги хранятся как float.
- Исторический OrderItem хранит цены, но вложенный Product summary строится из текущей product-проекции.
- Потеря локального Product может сделать сериализацию favorite/cart/order невозможной.

## 13. Общие свойства HTTP-контракта

- Base URL: `http://localhost:8001`.
- JSON используется для тел и ответов, кроме 204 без тела.
- List response: `items` + `count`.
- Error envelope: `error.code` + `error.message`.
- Validation errors: HTTP 400, `validation_error`; любое Pydantic-сообщение с фразой `greater than 0` заменяется сообщением `Quantity must be greater than zero`, даже если поле не quantity.
- Необработанные ошибки: HTTP 500, `internal_error`.

## 14. Служебный контракт: GET /health

### Назначение

Проверить ответ HTTP-процесса customer-service.

### Ответственность

Вернуть статический статус без диагностики зависимостей.

### Источник данных

Константа приложения.

### HTTP Method

`GET`.

### URL

`/health`.

### Path Parameters

Не применяется.

### Query Parameters

Не применяется.

### Request Body

Не применяется.

### Response

HTTP 200.

### Структура ответа

Объект с `status=ok`.

### Используемые модели

Отдельная именованная Pydantic-модель не используется.

### Бизнес-правила

Не применяется.

### Проверки валидации

Не применяется.

### Ошибки

Специализированные ошибки не определены; общий обработчик может вернуть 500 `internal_error`.

### Побочные эффекты

Не применяется.

### Kafka события

Не применяется.

### Изменяемые таблицы

Не применяется.

### Acceptance Criteria

- Доступный процесс возвращает 200 и `status=ok`.
- Ответ не зависит от состояния DB, Kafka или supplier-service.

### QA Checklist

- Вызвать без параметров/тела.
- Проверить 200 и точное значение status.
- Не использовать как проверку готовности зависимостей.

### Ограничения

Не проверяет consumer thread, PostgreSQL, Kafka или supplier-service.

## 15. Карта спецификации

- [Users API](07_users_api.md)
- [Customer Catalog API](08_customer_catalog_api.md)
- [Favorites API](09_favorites_api.md)
- [Cart API](10_cart_api.md)
- [Orders API](11_orders_api.md)

