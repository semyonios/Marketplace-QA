# HTTP + Kafka Demo

Simple marketplace demo for local QA practice.

- `supplier-service` manages suppliers, products, warehouses, and stock
- `customer-service` manages catalog reads, favorites, cart, and orders
- `audit-consumer` stores supplier events
- `kafka-broker` transfers product and order events between services
- `supplier-postgres` stores supplier data
- `customer-postgres` stores customer data
- `kafka-ui` lets you inspect topics and messages

## Architecture

- `supplier-service`: source of truth for products and stock
- `customer-service`: local read model for the customer side plus favorites, cart, and orders
- `Kafka`: moves product and order events so the customer side can stay in sync without calling supplier-service for every action

## Containers

- `supplier-service`
- `customer-service`
- `supplier-postgres`
- `customer-postgres`
- `kafka-broker`
- `kafka-ui`
- `audit-consumer`

## Databases

- `supplier_db` - основная база поставщика
- `customer_db` - основная база покупателя

## Connections

- supplier-service -> `postgresql+psycopg://app:app@supplier-postgres:5432/supplier_db`
- customer-service -> `postgresql+psycopg://app:app@customer-postgres:5432/customer_db`
- audit-consumer -> `postgresql+psycopg://app:app@supplier-postgres:5432/supplier_db`
- Kafka bootstrap servers -> `kafka-broker:9092`

## Supplier Service API

### Suppliers

- `POST /suppliers`
- `GET /suppliers`
- `GET /suppliers/{id}`
- `PUT /suppliers/{id}`
- `DELETE /suppliers/{id}`

### Products

- `POST /products`
- `GET /products`
- `GET /products/{id}`
- `PUT /products/{id}`
- `POST /warehouses/{warehouse_id}/stocks`
- `DELETE /products/{id}`

Product fields:

- `name` - non-empty product name
- `price` - must be greater than `0`
- `stocks` - aggregated stock, must be non-negative
- `is_active` - whether the product can be purchased
- `is_archived` - archived product flag

List endpoints return:

```json
{
  "items": [],
  "count": 0
}
```

## Customer Service API

### Users

- `POST /users`
- `GET /users`
- `GET /users/{id}`

### Favorites

- `POST /favorites`
- `GET /favorites?user_id=1`
- `DELETE /favorites/{product_id}?user_id=1`

### Cart

- `GET /cart?user_id=1`
- `POST /cart`
- `PATCH /cart/{product_id}`
- `DELETE /cart/{product_id}?user_id=1`

### Orders

- `POST /orders`
- `GET /orders?user_id=1`
- `GET /orders/{order_id}?user_id=1`
- `POST /orders/{order_id}/cancel?user_id=1`

## Kafka Topics

- `supplier-events`
- `product-events`
- `order-events`
- `product-stock-events`

## Business Rules

- stock source of truth is `supplier-service`
- stock field name is `stocks`
- products expose `total_price = price * stocks`
- cart and orders expose item totals plus full totals
- `POST /products` and `PUT /products/{id}` do not edit stock directly
- `POST /warehouses/{warehouse_id}/stocks` updates stock values through warehouse rows
- product name must not be empty
- product price must be greater than zero
- archived product cannot be active at the same time
- stock cannot be negative
- favorites do not affect cart or orders
- inactive or archived products cannot be added to cart
- cart quantity cannot exceed current stock
- orders can be created from explicit `items` or from the cart
- inactive or archived products cannot be ordered
- orders with quantity above available stock fail with `insufficient_stock`
- order creation creates `orders` and `order_items`, clears the cart, and publishes `ORDER_CREATED`
- new orders start with status `created`
- `POST /orders/{order_id}/cancel` changes status to `cancelled`
- in this simple version, order cancellation does not restore stock in supplier-service

## Error Format

All services return errors in the same JSON shape:

```json
{
  "error": {
    "code": "product_not_found",
    "message": "Product not found"
  }
}
```

Examples:

```json
{
  "error": {
    "code": "invalid_quantity",
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

## Request Examples

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

From cart:

```bash
curl -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1
  }'
```

From explicit items:

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

Cancel order:

```bash
curl -X POST "http://localhost:8001/orders/1/cancel?user_id=1"
```

## How To Test Manually

1. Create a supplier with `POST /suppliers`
2. Create a product with `POST /products`
3. Add stock through `POST /warehouses/{warehouse_id}/stocks`
4. Create a user with `POST /users`
5. Add the product to cart with `POST /cart`
6. Create an order with `POST /orders`
7. Cancel the order with `POST /orders/{order_id}/cancel?user_id=...`

## Negative Scenarios

Empty product name:

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

Product price `<= 0`:

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

Add inactive or archived product to cart:

```bash
curl -X POST http://localhost:8001/cart \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1,
    "product_id": 1,
    "quantity": 1
  }'
```

Update cart quantity to `0`:

```bash
curl -X PATCH http://localhost:8001/cart/1 \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1,
    "quantity": 0
  }'
```

Create order with empty cart:

```bash
curl -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1
  }'
```

## What To Verify In QA

- product creation and update with valid and invalid `name`, `price`, `is_active`, `is_archived`
- list responses include `items` and `count`
- cart response includes `total_items_count`
- order response includes `total_items_count`
- inactive and archived products cannot be added to cart
- inactive and archived products cannot be ordered
- `insufficient_stock` is returned as `409`
- order status changes from `created` to `cancelled`
- supplier and customer services stay in sync on stock changes through Kafka

## Запуск

```bash
cd ~/Desktop/http-kafka-demo
docker compose up --build -d
```

## Swagger UI

- Supplier service: [http://localhost:8000/docs](http://localhost:8000/docs)
- Customer service: [http://localhost:8001/docs](http://localhost:8001/docs)
- Kafka UI: [http://localhost:8080](http://localhost:8080)
