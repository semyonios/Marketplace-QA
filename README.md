# HTTP + Kafka Demo

Мини-приложение с двумя простыми сервисами:

- `supplier-service` - поставщики, товары, склады и остатки
- `customer-service` - покупатели, избранное, корзина и покупка
- `audit-consumer` - аудит событий поставщиков
- `kafka-broker` - обмен событиями между сервисами
- `supplier-postgres` - база поставщика `supplier_db`
- `customer-postgres` - база покупателя `customer_db`
- `kafka-ui` - просмотр топиков и сообщений

## Контейнеры

- `supplier-service`
- `customer-service`
- `supplier-postgres`
- `customer-postgres`
- `kafka-broker`
- `kafka-ui`
- `audit-consumer`

## Базы данных

- `supplier_db` - основная база поставщика
- `customer_db` - основная база покупателя

## Подключения

- supplier-service -> `postgresql+psycopg://app:app@supplier-postgres:5432/supplier_db`
- customer-service -> `postgresql+psycopg://app:app@customer-postgres:5432/customer_db`
- audit-consumer -> `postgresql+psycopg://app:app@supplier-postgres:5432/supplier_db`
- Kafka bootstrap servers -> `kafka-broker:9092`

## Что умеет supplier-service

### Поставщики

- `POST /suppliers`
- `GET /suppliers`
- `GET /suppliers/{id}`
- `PUT /suppliers/{id}`
- `DELETE /suppliers/{id}`

### Товары

- `POST /products`
- `GET /products`
- `GET /products/{id}`
- `PUT /products/{id}`
- `POST /warehouses/{warehouse_id}/stocks`
- `DELETE /products/{id}`

## Что умеет customer-service

### Пользователи

- `POST /users`
- `GET /users`
- `GET /users/{id}`

### Избранное

- `POST /favorites`
- `GET /favorites?user_id=1`
- `DELETE /favorites/{product_id}?user_id=1`

### Корзина

- `GET /cart?user_id=1`
- `POST /cart`
- `PATCH /cart/{product_id}`
- `DELETE /cart/{product_id}?user_id=1`

### Заказы

- `POST /orders`
- `GET /orders?user_id=1`
- `GET /orders/{order_id}?user_id=1`

## Kafka topics

- `supplier-events`
- `product-events`
- `order-events`
- `product-stock-events`

## Логика остатков

- источник истины по остаткам: `supplier-service`
- остатки в проекте называются `stocks`
- у товаров возвращается `total_price = price * stocks`
- у корзины и заказов возвращается стоимость каждой позиции и общая сумма `total_price`
- `POST /products` и `PUT /products/{id}` не управляют остатками
- `POST /warehouses/{warehouse_id}/stocks` использует `warehouse_id` только в URL, а в теле принимает только массив `items`
- `POST /favorites` не влияет на корзину и заказы
- `POST /cart` не даст добавить товаров больше, чем доступно в текущих `stocks`, и будет обновлять одну запись корзины для каждого товара
- `POST /orders` можно вызвать сразу с товарами в `items` без предварительного добавления в корзину
- оформление заказа создаёт `orders` и `order_items`, очищает корзину пользователя и отправляет `ORDER_CREATED` в `order-events`
- `supplier-service` уменьшает остаток и публикует новое значение в `product-stock-events`
- `customer-service` обновляет локальную копию товаров по `product-stock-events`

## Примеры запросов

### Добавить в избранное

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

### Добавить в корзину

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

### Оформить заказ

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

## Пользовательские сценарии

### Добавить в избранное

1. Создать покупателя через `POST /users`
2. Выбрать товар из `GET /products`
3. Вызвать `POST /favorites`
4. Проверить результат через `GET /favorites?user_id=...`

### Добавить в корзину

1. Выбрать товар из `GET /products`
2. Вызвать `POST /cart` с количеством
3. При необходимости обновить количество через `PATCH /cart/{product_id}`
4. Проверить итоговую корзину через `GET /cart?user_id=...`

### Оформить заказ

1. Наполнить корзину или передать `items` сразу в `POST /orders`
2. Убедиться, что заказ создался с `order_items`
3. Проверить историю через `GET /orders?user_id=...`
4. Проверить, что корзина пользователя очищена после оформления

## Запуск

```bash
cd ~/Desktop/http-kafka-demo
docker compose up --build -d
```

## Swagger UI

- Supplier service: [http://localhost:8000/docs](http://localhost:8000/docs)
- Customer service: [http://localhost:8001/docs](http://localhost:8001/docs)
- Kafka UI: [http://localhost:8080](http://localhost:8080)
