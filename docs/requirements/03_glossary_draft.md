# Черновик глоссария Marketplace-QA

Термины взяты только из текущего кода, конфигурации и README. Определения описывают фактическое употребление в проекте.

| Термин | Черновое определение |
|---|---|
| Marketplace-QA | Локальный демонстрационный marketplace-проект для ручной QA-практики |
| Supplier / Поставщик | Сущность supplier-service, владеющая товарами; хранится в supplier-таблице `users` |
| Customer / Покупатель | Пользователь customer-service, работающий с каталогом, избранным, корзиной и заказами |
| User | Имя ORM-модели покупателя в customer-service; также физическое имя таблицы для Supplier в supplier-service |
| Product / Товар | Карточка товара с названием, описанием, ценой, агрегированным остатком и флагами состояния |
| Product catalog / Каталог | Набор Product; в customer-service является локальной копией supplier-каталога |
| Local product copy / Локальная копия каталога | Customer-проекция товаров, обновляемая через Kafka и HTTP snapshot sync |
| Warehouse / Склад | Место хранения товаров с названием, строкой графика и адресом |
| WarehouseProduct | Строка фактического остатка конкретного товара на конкретном складе |
| Stock / Stocks / Остаток | Количество товара; в `WarehouseProduct` — по складу, в `Product` — агрегированное |
| Aggregated stock / Агрегированный остаток | Сумма остатков товара по всем строкам `warehouse_products` |
| Restock | Имя операции установки фактических остатков через warehouse stock API |
| Favorite / Избранное | Уникальная связь покупателя с товаром, не влияющая на корзину и заказ |
| Cart / Корзина | Набор `CartItem` одного покупателя |
| CartItem / Позиция корзины | Уникальная для пары user–product запись с количеством |
| Order / Заказ | Зафиксированная покупка customer-service с номером, статусом, итоговой ценой и позициями |
| OrderItem / Позиция заказа | Товар, количество, цена единицы и сумма, сохранённые в составе заказа |
| Order number / Номер заказа | Строка вида `ORD-000001`, формируемая из ID заказа |
| `created` | Начальный строковый статус заказа |
| `cancelled` | Статус заказа после вызова cancel API |
| `is_active` | Флаг доступности товара для покупки; false блокирует cart/order |
| `is_archived` | Флаг архивного товара; true блокирует cart/order и не допускается одновременно с active=true |
| Archive / Архивирование | Реализация DELETE товара через установку inactive и archived без удаления строки |
| Total price / Итоговая стоимость | Для товара — `price * stocks`; для cart/order item — цена на количество; для заказа — сумма позиций |
| Eventual consistency | Негарантированное мгновенное совпадение supplier-данных и customer-проекции из-за асинхронной синхронизации |
| Kafka | Транспорт событий между сервисами |
| Kafka broker | Одноузловой Apache Kafka контейнер, принимающий и выдающий события |
| Kafka topic / Топик | Именованный поток событий: `supplier-events`, `product-events`, `product-stock-events`, `order-events` |
| Kafka event / Событие Kafka | JSON-сообщение с типом события и версией, передаваемое через topic |
| Event type | Строковый тип события, например `PRODUCT_UPDATED` или `ORDER_CREATED` |
| Event version | Поле `event_version`, фактически имеющее значение 1 |
| Event ID | UUID order event, используемый supplier-service для дедупликации |
| Producer | Код, публикующий JSON-события в Kafka |
| Consumer | Фоновый обработчик, читающий Kafka topic и изменяющий локальное состояние |
| Consumer group | Kafka group ID отдельного вида consumer |
| Offset | Позиция consumer в Kafka topic, фиксируемая вручную после обработки |
| ProcessedEvent | Запись UUID уже принятого order event в supplier DB |
| Audit consumer | Отдельный consumer supplier events, сохраняющий их в `user_events` |
| SupplierEvent | Аудит-запись supplier event с типом, supplier ID, JSON payload и временем получения |
| Payload | Полезная JSON-часть Kafka event |
| Product projection / Проекция товара | Customer-модель Product, содержащая подмножество supplier-данных |
| Upsert | Создание либо обновление customer Product по входному supplier payload |
| Snapshot sync / Полная синхронизация | HTTP-загрузка всего supplier-каталога и сверка с customer-проекцией |
| Healthcheck | `GET /health` API либо проверка готовности PostgreSQL в Docker Compose |
| Supplier service | FastAPI-сервис управления supplier, product, warehouse и stock |
| Customer service | FastAPI-сервис customer-сценариев и локального каталога |
| Audit consumer | Не-HTTP сервис сохранения supplier events |
| Supplier DB | PostgreSQL `supplier_db`, используемая supplier-service и audit-consumer |
| Customer DB | PostgreSQL `customer_db`, используемая customer-service |
| Kafka UI | Веб-интерфейс просмотра локального Kafka-кластера |
| Docker Compose | Описание локального запуска приложений, БД, Kafka и Kafka UI |
| API | HTTP-интерфейс supplier-service или customer-service |
| List response | Ответ-обёртка с полями `items` и `count` |
| Error code | Машиночитаемая строка в `error.code`, например `product_not_found` |
| Validation error | Ошибка входной схемы с кодом `validation_error` и HTTP 400 |
| Business error | Ошибка предметной проверки, формируемая через `HTTPException` |
| Insufficient stock | Состояние, когда запрошенное количество выше локального customer stock |
| Dead letter | Термин из логов для сообщения, не обработанного за три попытки; отдельной сущности/топика в коде нет |
| Retry | Повторная попытка consumer обработать сообщение; максимум три внутри одного цикла |
| Idempotency / Идемпотентность | Для order events — дедупликация по UUID; для favorite — возврат существующей связи при повторном POST |

