# Known limitations

- Нет регистрации, JWT, password/session management и production authorization. Роль задаётся тестовыми заголовками `X-Test-Role` и `X-Test-Subject-ID`.
- Нет оплаты, доставки, возвратов после получения, промокодов, отзывов и рекомендаций.
- Один заказ и одна корзина ограничены одним поставщиком.
- Partial fulfillment отсутствует; reservation выполняется all-or-nothing.
- UI использует polling с интервалом две секунды вместо WebSocket/SSE.
- Нет Kubernetes, service mesh, Elasticsearch/Kibana, Grafana/Tempo и полноценного distributed tracing.
- `correlation_id` и structured JSON logs полноценно реализованы в новом order/supplier lifecycle; legacy customer/audit logging проще.
- `customer-service` использует SQLAlchemy `create_all` и совместимые schema adjustments вместо Alembic. Alembic migrations есть у order/supplier сервисов.
- Склады в legacy supplier model не имеют `supplier_id`; UI показывает их как общие для стенда.
- Публичный API не отдаёт отдельные warehouse-product rows, поэтому Supplier UI показывает агрегированный source stock и выполняет абсолютную запись для выбранного склада.
- QA Panel получает полную readiness только для supplier/order; customer-service предоставляет `/health`.
- Frontend nginx использует same-origin proxy. Отдельный production CORS/deployment profile не реализован.
- Kafka auto-create topics включён для локального стенда; production governance topics не моделируется.
- DLQ replay — ручная CLI-операция. Перед replay нужно проверить inbox/deduplication; web UI для replay отсутствует.
- Seed гарантирует стабильные IDs после clean reset. В давно существующих volumes объекты ищутся по естественным ключам, поэтому IDs могут отличаться.
- Audit consumer сохраняет legacy supplier events и не является универсальным аудит-хранилищем всех Marketplace 2.0 событий.
- Интерфейс намеренно утилитарный и предназначен для QA-демонстрации, а не для production-grade UX.
