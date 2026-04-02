# HTTP + Kafka Demo

Мини-приложение с двумя простыми сервисами:

- `supplier-service` - поставщики, товары, склады и остатки
- `customer-service` - покупатели, избранное, корзина и покупка
- `audit-consumer` - аудит пользовательских событий
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

- `POST /users`
- `GET /users`
- `GET /users/{id}`
- `PUT /users/{id}`
- `DELETE /users/{id}`

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
- `DELETE /favorites?user_id=1&product_id=1`

### Корзина

- `GET /cart?user_id=1`
- `POST /cart`
- `DELETE /cart?user_id=1&product_id=1`

### Покупка

- `GET /purchases?user_id=1`
- `POST /purchase` - можно купить товары напрямую через `items` или оформить текущую корзину

## Kafka topics

- `user-events`
- `products-events`
- `order-events`
- `stock-supplier-events`

## Логика остатков

- источник истины по остаткам: `supplier-service`
- остатки в проекте называются `stocks`
- у товаров возвращается `total_price = price * stocks`
- у корзины и покупок возвращается стоимость каждой позиции и общая сумма `total_price`
- `POST /products` и `PUT /products/{id}` не управляют остатками
- `POST /warehouses/{warehouse_id}/stocks` использует `warehouse_id` только в URL, а в теле принимает только массив `items`
- `POST /cart` не даст добавить товаров больше, чем доступно в текущих `stocks`, и будет обновлять одну запись корзины для каждого товара
- `POST /purchase` можно вызвать сразу с товарами в `items` без предварительного добавления в корзину
- покупка тоже проверяет доступные `stocks` и отправляет только `ORDER_CREATED` в `order-events`
- `supplier-service` уменьшает остаток и публикует новое значение в `stock-supplier-events`
- `customer-service` обновляет локальную копию товаров по `stock-supplier-events`

## Запуск

```bash
cd ~/Desktop/http-kafka-demo
docker compose up --build -d
```

## Swagger UI

- Supplier service: [http://localhost:8000/docs](http://localhost:8000/docs)
- Customer service: [http://localhost:8001/docs](http://localhost:8001/docs)
- Kafka UI: [http://localhost:8080](http://localhost:8080)
