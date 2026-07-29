# Supplier API Contract Specification — обзор

## 1. Назначение supplier-service

`supplier-service` — HTTP-сервис управления поставщиками, товарами, складами и складскими остатками Marketplace-QA. Он предоставляет Supplier API на порту `8000`, хранит supplier-домен в PostgreSQL, публикует изменения в Kafka и потребляет события созданных заказов для списания остатков.

Документ фиксирует текущую реализацию AS IS. Контракты доменных endpoint приведены в соседних документах этого каталога.

## 2. Ответственность

Сервис отвечает за:

- создание, чтение, изменение и удаление поставщиков;
- создание, чтение, изменение и архивирование товаров;
- создание, чтение, изменение и удаление складов;
- установку фактического остатка товара на складе;
- расчёт и хранение агрегированного остатка товара;
- публикацию supplier, product и stock events;
- обработку `ORDER_CREATED` и списание складских остатков;
- единый формат ошибок своего HTTP API;
- создание таблиц и точечное добавление product-флагов при старте.

## 3. Владелец данных и Source of Truth

`supplier-service` является владельцем и Source of Truth для поставщиков, товаров, складов, складских остатков и агрегированного остатка. Источником сохранённого состояния служит `supplier_db` в `supplier-postgres`.

Customer-каталог является локальной проекцией и не заменяет supplier-service как источник истины. При этом заказ создаётся и хранится в customer-service; supplier-service получает из заказа только `product_id`, `quantity` и технический `event_id`.

## 4. Хранимые данные

| Область | Таблица | Содержание |
|---|---|---|
| Поставщики | `users` | Имя, телефон, email, дата рождения, город, timestamps |
| Товары | `products` | Supplier, карточка, цена, агрегированный stock, флаги, дата создания |
| Склады | `warehouses` | Название, график, адрес, дата создания |
| Остатки | `warehouse_products` | Абсолютный stock по паре warehouse–product |
| Идемпотентность order events | `processed_events` | UUID уже принятых событий и время обработки |
| Аудит supplier events | `user_events` | Записывается `audit-consumer`, а не supplier-service, но находится в той же БД |

## 5. Публикуемые данные

| Topic | Типы событий | Содержание |
|---|---|---|
| `supplier-events` | `SUPPLIER_CREATED`, `SUPPLIER_UPDATED`, `SUPPLIER_DELETED` | Полная публичная supplier-модель |
| `product-events` | `PRODUCT_CREATED`, `PRODUCT_UPDATED` | Полная публичная product-модель, включая агрегированный stock и вычисленный total price |
| `product-stock-events` | `STOCK_REPLENISHED`, `STOCK_DECREASED_BY_ORDER` | Product ID и новый агрегированный stock |

Каждое публикуемое сообщение получает `event_version=1`. Producer ставит сообщение в локальную очередь клиента Kafka через `produce()` и вызывает `poll(0)`; delivery callback и ожидание подтверждения отсутствуют.

## 6. Данные, которые сервис не хранит

`supplier-service` не хранит:

- покупателей customer-service;
- favorites и cart;
- заказы и order items как бизнес-сущности;
- статус и номер заказа;
- платежи и доставку;
- данные аутентификации, авторизации, сессий или ролей;
- локальную customer-проекцию каталога;
- подтверждение успешного или неуспешного выполнения заказа.

## 7. Зависимости

- `supplier-postgres` / `supplier_db` — обязательное хранилище.
- `kafka-broker` — транспорт исходящих событий и входящих `order-events`.
- FastAPI и Uvicorn — HTTP runtime.
- SQLAlchemy и psycopg — доступ к PostgreSQL.
- confluent-kafka — producer и consumer.
- Pydantic/email-validator — входная и выходная валидация.
- Docker Compose — локальный запуск и передача конфигурации.

По Compose сервис стартует после healthy PostgreSQL и started Kafka. Собственного container healthcheck у supplier-service нет.

## 8. Границы ответственности

- Supplier API не аутентифицирует вызывающего и не проверяет его роль.
- Сервис не создаёт заказ и не управляет его статусом.
- Проверка доступности товара при заказе выполняется customer-service по локальной проекции; supplier consumer затем применяет событие к фактическим складам.
- При недостаточном фактическом остатке consumer списывает доступную часть и не сообщает результат конкретному заказу.
- Отмена заказа не поступает в supplier-service и не возвращает stock.
- API не предоставляет чтение отдельных `warehouse_products`, `processed_events` или audit rows.
- Изменения БД и публикация Kafka не объединены общей транзакцией.

## 9. Связь с customer-service

`customer-service` получает карточки товаров через `product-events`, остатки через `product-stock-events` и дополнительно загружает полный каталог HTTP-запросом `GET /products`. В обратном направлении customer-service публикует `ORDER_CREATED` в `order-events`; встроенный consumer supplier-service уменьшает складские остатки.

Согласованность между supplier state и customer projection является eventual. Прямого доступа supplier-service к `customer_db` нет.

## 10. Связь с Kafka

Supplier-service является producer для `supplier-events`, `product-events`, `product-stock-events` и consumer для `order-events`. Consumer использует ручной commit offset, три попытки обработки и дедупликацию order event по UUID. Отдельного DLQ нет.

## 11. Связь с PostgreSQL

Все HTTP-операции домена работают с `supplier_db`. SQLAlchemy создаёт отсутствующие таблицы при старте; для существующей `products` сервис точечно добавляет `is_active` и `is_archived`, если колонок нет. Отдельной системы миграций в репозитории нет.

## 12. Общие свойства HTTP-контракта

- Base URL локального запуска: `http://localhost:8000`.
- Формат тела: JSON, кроме ответов 204 без тела.
- Списки имеют форму `items` + `count`.
- Ошибка имеет форму `error.code` + `error.message`.
- Ошибки входной валидации возвращаются с HTTP 400 и кодом `validation_error`.
- Необработанные ошибки возвращаются с HTTP 500 и кодом `internal_error`.
- Пагинация, auth headers, filtering и content negotiation в реализации не определены.

## 13. Служебный контракт: GET /health

### Назначение

Проверить, что HTTP-процесс supplier-service отвечает.

### Ответственность

Возвращает статический статус процесса; зависимости не диагностирует.

### Источник данных

Константа внутри приложения.

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

Объект с полем `status`, равным `ok`.

### Используемые модели

Отдельная именованная Pydantic-модель не используется.

### Бизнес-правила

Не применяется.

### Проверки валидации

Не применяется.

### Ошибки

Специализированные ошибки endpoint не определены; общий обработчик может вернуть `internal_error` при необработанном исключении.

### Побочные эффекты

Не применяется.

### Kafka события

Не применяется.

### Изменяемые таблицы

Не применяется.

### Acceptance Criteria

- При доступном HTTP-процессе запрос без параметров возвращает 200 и `status=ok`.
- Результат не зависит от фактической доступности PostgreSQL или Kafka.

### QA Checklist

- Вызвать endpoint без тела и проверить HTTP 200.
- Проверить точное значение `status`.
- Не интерпретировать результат как readiness зависимостей.

### Ограничения

Endpoint не проверяет БД, Kafka, consumer thread или customer-service.

## 14. Карта спецификации

- [Suppliers API](02_suppliers_api.md)
- [Products API](03_products_api.md)
- [Warehouses API](04_warehouses_api.md)
- [Stocks API](05_stocks_api.md)

