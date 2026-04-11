# HTTP + Kafka Demo

Небольшой marketplace-проект для локального запуска и manual QA practice.

Проект специально сделан простым: без auth, без внешних интеграций оплаты, без сложной оркестрации. При этом в нём уже есть два домена, event-driven взаимодействие через Kafka, отдельные сценарии для `favorites`, `cart`, `orders`, продуктовые флаги `is_active` и `is_archived`, а также единый формат ошибок.

## Архитектура

- `supplier-service` — источник истины для поставщиков, товаров, складов и остатков
- `customer-service` — customer-side API с локальной копией каталога, а также `favorites`, `cart` и `orders`
- `audit-consumer` — потребитель событий поставщиков
- `Kafka` — транспорт событий между сервисами

Важно для тестирования:

- `customer-service` получает изменения по товарам и остаткам не мгновенно, а через Kafka
- между сервисами есть eventual consistency
- при ручном тестировании после изменения товара или оформления заказа стоит учитывать небольшую задержку синхронизации

## Состав сервисов

- `supplier-service`
- `customer-service`
- `audit-consumer`
- `kafka-broker`
- `kafka-ui`
- `supplier-postgres`
- `customer-postgres`

## Как запустить проект

```bash
git clone git@github.com:semyonios/Marketplace-QA.git
cd Marketplace-QA
docker compose up --build -d
```

После запуска доступны:

- Supplier service: [http://localhost:8000/docs](http://localhost:8000/docs)
- Customer service: [http://localhost:8001/docs](http://localhost:8001/docs)
- Kafka UI: [http://localhost:8080](http://localhost:8080)

## Основные API-сценарии

### `supplier-service`

#### Поставщики

- `POST /suppliers`
- `GET /suppliers`
- `GET /suppliers/{id}`
- `PUT /suppliers/{id}`
- `DELETE /suppliers/{id}`

#### Товары

- `POST /products`
- `GET /products`
- `GET /products/{id}`
- `PUT /products/{id}`
- `DELETE /products/{id}`
- `POST /warehouses/{warehouse_id}/stocks`

Поля товара:

- `name` — непустое название
- `price` — цена должна быть больше `0`
- `stocks` — агрегированный остаток, не может быть отрицательным
- `is_active` — доступен ли товар для покупки
- `is_archived` — архивный ли товар

#### Склады

- `POST /warehouses`
- `GET /warehouses`
- `GET /warehouses/{warehouse_id}`
- `PUT /warehouses/{warehouse_id}`
- `DELETE /warehouses/{warehouse_id}`

### `customer-service`

#### Покупатели

- `POST /users`
- `GET /users`
- `GET /users/{id}`

#### Избранное

- `POST /favorites`
- `GET /favorites?user_id=1`
- `DELETE /favorites/{product_id}?user_id=1`

#### Корзина

- `GET /cart?user_id=1`
- `POST /cart`
- `PATCH /cart/{product_id}`
- `DELETE /cart/{product_id}?user_id=1`

#### Заказы

- `POST /orders`
- `GET /orders?user_id=1`
- `GET /orders/{order_id}?user_id=1`
- `POST /orders/{order_id}/cancel?user_id=1`

## Как тестировать вручную

Ниже базовый happy-path, который удобно прогонять руками через Swagger или `curl`.

### Сценарий 1. Создать поставщика и товар

1. Создать поставщика через `POST /suppliers`
2. Создать склад через `POST /warehouses`
3. Создать товар через `POST /products`
4. Назначить остаток через `POST /warehouses/{warehouse_id}/stocks`
5. Проверить товар через `GET /products`

### Сценарий 2. Проверить каталог покупателя

1. Открыть `GET /products` в `customer-service`
2. Убедиться, что товар появился в локальной копии каталога
3. Если товар не появился сразу, подождать немного и повторить запрос

Это нормальное поведение для текущей схемы, потому что синхронизация идёт через Kafka.

### Сценарий 3. Проверить `favorites`

1. Создать пользователя через `POST /users`
2. Добавить товар в избранное через `POST /favorites`
3. Проверить список через `GET /favorites?user_id=...`
4. Удалить товар из избранного через `DELETE /favorites/{product_id}?user_id=...`

### Сценарий 4. Проверить `cart`

1. Добавить товар в корзину через `POST /cart`
2. Изменить количество через `PATCH /cart/{product_id}`
3. Проверить корзину через `GET /cart?user_id=...`
4. Удалить товар через `DELETE /cart/{product_id}?user_id=...`

### Сценарий 5. Проверить `orders`

1. Добавить товар в корзину
2. Создать заказ через `POST /orders`
3. Проверить заказ через `GET /orders?user_id=...`
4. Проверить конкретный заказ через `GET /orders/{order_id}?user_id=...`
5. Отменить заказ через `POST /orders/{order_id}/cancel?user_id=...`

Что стоит отдельно проверить:

- после создания заказа корзина очищается
- заказ создаётся со статусом `created`
- после отмены статус становится `cancelled`

## Негативные сценарии

### `insufficient_stock`

Проверить заказ с количеством больше доступного остатка.

Ожидаемое поведение:

- HTTP `409`
- error code `insufficient_stock`

### `product_inactive`

Проверить добавление в корзину или оформление заказа для товара с `is_active=false`.

Ожидаемое поведение:

- HTTP `409`
- error code `product_inactive`

### `product_archived`

Проверить добавление в корзину или оформление заказа для товара с `is_archived=true`.

Ожидаемое поведение:

- HTTP `409`
- error code `product_archived`

### `cart_is_empty`

Проверить `POST /orders` без `items` и с пустой корзиной пользователя.

Ожидаемое поведение:

- HTTP `400`
- error code `cart_is_empty`

### `invalid_quantity`

Проверить:

- `POST /cart` с `quantity=0`
- `PATCH /cart/{product_id}` с `quantity=0`

Ожидаемое поведение:

- HTTP `400`
- error code `validation_error`

## Формат ошибок

Во всех сервисах ошибки возвращаются в одном формате:

```json
{
  "error": {
    "code": "product_not_found",
    "message": "Product not found"
  }
}
```

Примеры:

```json
{
  "error": {
    "code": "validation_error",
    "message": "quantity: Quantity must be greater than zero"
  }
}
```

```json
{
  "error": {
    "code": "insufficient_stock",
    "message": "Insufficient stock"
  }
}
```

```json
{
  "error": {
    "code": "cart_is_empty",
    "message": "Cart is empty and no order items were provided"
  }
}
```

Типовые error codes:

- `product_not_found`
- `validation_error`
- `insufficient_stock`
- `product_inactive`
- `product_archived`
- `cart_is_empty`
- `order_not_found`
- `order_already_cancelled`

## Бизнес-правила

### Товары

- товар нельзя создать с пустым `name`
- товар нельзя создать или обновить с `price <= 0`
- `stocks` не может быть отрицательным
- архивный товар не должен одновременно быть активным
- `supplier-service` — источник истины по остаткам

### Customer-side поведение

- `favorites` не влияет на `cart` и `orders`
- неактивный товар нельзя добавить в корзину
- архивный товар нельзя добавить в корзину
- неактивный товар нельзя купить
- архивный товар нельзя купить
- нельзя оформить заказ с количеством больше доступного `stocks`
- `POST /orders` может брать товары либо из корзины, либо из переданного массива `items`

### Заказы

- заказ создаётся со статусом `created`
- заказ можно перевести в `cancelled`
- в текущей реализации отмена заказа не возвращает stock обратно в `supplier-service`

## Известные ограничения

- между `supplier-service` и `customer-service` есть eventual consistency
- сразу после создания товара или изменения stock локальная копия в `customer-service` может обновиться не мгновенно
- отмена заказа не восстанавливает stock
- проект ориентирован на локальное ручное тестирование, а не на production-ready сценарии

## Ответы API, полезные для QA

### List responses

Списки возвращаются в формате:

```json
{
  "items": [],
  "count": 0
}
```

Это относится, например, к:

- `GET /suppliers`
- `GET /warehouses`
- `GET /products`
- `GET /users`
- `GET /favorites`
- `GET /orders`

### Cart response

Корзина содержит:

- `items`
- `count`
- `total_items_count`
- `total_price`

### Order response

Заказ содержит:

- `items`
- `total_items_count`
- `total_price`
- `status`
- `order_number`

## На что обращать внимание при тестировании Kafka / eventual consistency

- после `POST /products` товар может появиться в `customer-service` не сразу
- после изменения stock в `supplier-service` остаток в `customer-service` тоже может обновиться с небольшой задержкой
- после `POST /orders` остаток сначала уменьшается в `supplier-service`, затем обновляется в `customer-service`
- для наблюдения событий удобно использовать `Kafka UI`

Практически это значит:

- если сразу после действия данные не совпадают, стоит повторить `GET` через короткий интервал
- для негативных сценариев, завязанных на stock, лучше сначала убедиться, что локальная копия товара уже синхронизировалась

## Примеры `curl`-запросов

### Add to favorites

```bash
curl -X POST http://localhost:8001/favorites \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1,
    "product_id": 1
  }'
```

```bash
curl "http://localhost:8001/favorites?user_id=1"
```

### Add to cart

```bash
curl -X POST http://localhost:8001/cart \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1,
    "product_id": 1,
    "quantity": 2
  }'
```

```bash
curl -X PATCH http://localhost:8001/cart/1 \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1,
    "quantity": 3
  }'
```

```bash
curl "http://localhost:8001/cart?user_id=1"
```

### Create order

Из корзины:

```bash
curl -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1
  }'
```

Напрямую по переданным товарам:

```bash
curl -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1,
    "items": [
      { "product_id": 1, "quantity": 1 },
      { "product_id": 2, "quantity": 2 }
    ]
  }'
```

```bash
curl "http://localhost:8001/orders?user_id=1"
```

### Cancel order

```bash
curl -X POST "http://localhost:8001/orders/1/cancel?user_id=1"
```

### Empty product name

```bash
curl -X POST http://localhost:8000/products \
  -H "Content-Type: application/json" \
  -d '{
    "supplier_id": 1,
    "name": "   ",
    "description": "Invalid product",
    "price": 100,
    "is_active": true,
    "is_archived": false
  }'
```

### Product price `<= 0`

```bash
curl -X POST http://localhost:8000/products \
  -H "Content-Type: application/json" \
  -d '{
    "supplier_id": 1,
    "name": "Broken product",
    "description": "Invalid price",
    "price": 0,
    "is_active": true,
    "is_archived": false
  }'
```

### Add inactive or archived product to cart

```bash
curl -X POST http://localhost:8001/cart \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1,
    "product_id": 1,
    "quantity": 1
  }'
```

### Update cart quantity to `0`

```bash
curl -X PATCH http://localhost:8001/cart/1 \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1,
    "quantity": 0
  }'
```

### Create order with empty cart

```bash
curl -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1
  }'
```
