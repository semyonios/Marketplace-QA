# Functional Requirements Specification AS IS

## 1. Назначение документа

Документ фиксирует только функциональные требования, уже реализованные в Marketplace-QA. Он переводит подтверждённое поведение API, ошибок, Kafka, данных, бизнес-правил и сценариев в атомарные проверяемые формулировки.

AS IS requirement описывает существующее состояние и не утверждает, что оно является желаемым для будущей версии.

## 2. Принципы формулирования требований

- Требование включается только при подтверждении существующей реализацией и AS IS-источниками.
- Технические ограничения и открытые вопросы не превращаются в функциональные требования.
- Каждое требование имеет уникальный ID и ссылки на BR, сценарий и технический контракт.
- Одно требование описывает одно наблюдаемое поведение или один связный функциональный результат.
- Приоритет (`High`/`Medium`) наследует QA-критичность AS IS-документов и не является будущим product priority.

## 3. Формат требования

Для каждого требования используются поля:

- **Requirement ID** — уникальный `FR-*`.
- **Формулировка** — проверяемое существующее поведение.
- **Домен** и **Приоритет**.
- **Источник** — подтверждающие AS IS-документы.
- **Связанные Business Rules** и **Связанные Scenarios**.
- **API/Event/Data references**.
- **Acceptance Criteria** — наблюдаемый результат.
- **QA Notes** — фокус проверки.
- **Ограничения** — граница реализации, не являющаяся требованием.

## 4. Supplier Functional Requirements

### FR-SUP-001 — Создание Supplier

- **Формулировка:** Система создаёт Supplier из валидных обязательных полей, нормализует телефон и возвращает созданную запись с ID/timestamps.
- **Домен:** Supplier.
- **Приоритет:** High.
- **Источник:** Supplier API, Error Contract, Data Contract.
- **Связанные Business Rules:** `BR-SUP-001`–`BR-SUP-003`.
- **Связанные Scenarios:** `SCN-SUP-001`.
- **API/Event/Data references:** `POST /suppliers`; `supplier_db.users`.
- **Acceptance Criteria:** Валидный уникальный запрос даёт 201 и одну row; duplicate email/phone даёт 409.
- **QA Notes:** Проверить обязательность, границы, email и phone normalization.
- **Ограничения:** Auth и возрастные правила отсутствуют.

### FR-SUP-002 — Чтение списка Supplier

- **Формулировка:** Система возвращает все Supplier по возрастанию ID в структуре `items`/`count`.
- **Домен:** Supplier.
- **Приоритет:** Medium.
- **Источник:** Supplier API, Data Contract.
- **Связанные Business Rules:** `BR-SUP-004`.
- **Связанные Scenarios:** `SCN-SUP-001`–`SCN-SUP-003`.
- **API/Event/Data references:** `GET /suppliers`; `supplier_db.users`.
- **Acceptance Criteria:** Пустая таблица даёт empty list/count 0; непустая — согласованный count/order.
- **QA Notes:** Проверить пустой и непустой набор.
- **Ограничения:** Пагинация/фильтры отсутствуют.

### FR-SUP-003 — Чтение Supplier по ID

- **Формулировка:** Система возвращает существующего Supplier по integer ID либо `supplier_not_found`.
- **Домен:** Supplier.
- **Приоритет:** Medium.
- **Источник:** Supplier API, Error Contract.
- **Связанные Business Rules:** `BR-SUP-004`.
- **Связанные Scenarios:** `SCN-SUP-001`–`SCN-SUP-003`.
- **API/Event/Data references:** `GET /suppliers/{supplier_id}`; `users`.
- **Acceptance Criteria:** Existing ID даёт 200; absent ID — 404; invalid type — 400.
- **QA Notes:** Проверить existing/absent/negative/non-integer.
- **Ограничения:** Связанные Product не возвращаются.

### FR-SUP-004 — Изменение Supplier

- **Формулировка:** Система применяет только переданные валидные Supplier fields и возвращает полное итоговое состояние.
- **Домен:** Supplier.
- **Приоритет:** High.
- **Источник:** Supplier API, Error/Data Contracts.
- **Связанные Business Rules:** `BR-SUP-002`, `BR-SUP-003`, `BR-SUP-005`.
- **Связанные Scenarios:** `SCN-SUP-002`.
- **API/Event/Data references:** `PUT /suppliers/{id}`; `users`.
- **Acceptance Criteria:** Непереданные поля сохраняются; unique conflict даёт 409 без успешного update.
- **QA Notes:** Проверить partial/empty body и phone normalization.
- **Ограничения:** PUT имеет partial semantics; explicit null может привести к integrity conflict.

### FR-SUP-005 — Удаление Supplier

- **Формулировка:** Система физически удаляет Supplier только при отсутствии любого связанного Product.
- **Домен:** Supplier.
- **Приоритет:** High.
- **Источник:** Supplier API, Data/Business Contracts.
- **Связанные Business Rules:** `BR-SUP-006`.
- **Связанные Scenarios:** `SCN-SUP-003`.
- **API/Event/Data references:** `DELETE /suppliers/{id}`; `users`, `products`.
- **Acceptance Criteria:** Supplier без Product удаляется с 204; active/archived Product блокирует удаление кодом `supplier_has_products`.
- **QA Notes:** Проверить оба состояния Product и repeated lookup.
- **Ограничения:** Каскад Product не выполняется.

### FR-SUP-006 — Публикация Supplier events

- **Формулировка:** После успешного commit create/update/delete Supplier система инициирует соответствующий event с полным Supplier payload.
- **Домен:** Supplier.
- **Приоритет:** High.
- **Источник:** Kafka Event Contract, Supplier API.
- **Связанные Business Rules:** `BR-SUP-007`.
- **Связанные Scenarios:** `SCN-SUP-001`–`SCN-SUP-003`.
- **API/Event/Data references:** `supplier-events`; `SUPPLIER_CREATED/UPDATED/DELETED`.
- **Acceptance Criteria:** Topic/key/type/payload/version соответствуют mutation.
- **QA Notes:** Сопоставить событие с committed row/snapshot.
- **Ограничения:** Delivery callback отсутствует; producer error возможен post-commit.

### FR-SUP-007 — Audit-результат Supplier mutation

- **Формулировка:** При успешной доставке и обработке Supplier event система сохраняет audit row с type, Supplier ID и payload.
- **Домен:** Supplier/Audit.
- **Приоритет:** High.
- **Источник:** Event/Data Contracts.
- **Связанные Business Rules:** `BR-AUD-001`–`BR-AUD-005`.
- **Связанные Scenarios:** `SCN-AUD-001`.
- **API/Event/Data references:** `supplier-events`; `supplier_db.user_events`.
- **Acceptance Criteria:** Каждое обработанное событие добавляет row с received_at.
- **QA Notes:** Проверить create/update/delete payload и duplicate delivery.
- **Ограничения:** Результат eventual; deduplication и FK отсутствуют.

## 5. Product Functional Requirements

### FR-PROD-001 — Создание Product

- **Формулировка:** Система создаёт Product для существующего Supplier из валидных name/price/flags и возвращает полную карточку.
- **Домен:** Product.
- **Приоритет:** High.
- **Источник:** Product API, Error/Data Contracts.
- **Связанные Business Rules:** `BR-PROD-001`–`BR-PROD-004`, `BR-PROD-006`.
- **Связанные Scenarios:** `SCN-PROD-001`.
- **API/Event/Data references:** `POST /products`; `supplier_db.products/users`.
- **Acceptance Criteria:** Valid request даёт 201; missing Supplier — 404; active+archived — 409.
- **QA Notes:** Проверить trim name, price и flags.
- **Ограничения:** SKU/валюта/unique name отсутствуют.

### FR-PROD-002 — Начальный stock Product

- **Формулировка:** Система присваивает новому Product агрегированный `stocks=0`, независимо от клиента.
- **Домен:** Product/Stock.
- **Приоритет:** High.
- **Источник:** Product API, Data Contract.
- **Связанные Business Rules:** `BR-PROD-005`.
- **Связанные Scenarios:** `SCN-PROD-001`.
- **API/Event/Data references:** `POST /products`; `products.stocks`.
- **Acceptance Criteria:** Response и DB нового Product содержат stock 0/total price 0.
- **QA Notes:** Убедиться, что stock не принимается create body.
- **Ограничения:** Warehouse rows не создаются.

### FR-PROD-003 — Чтение списка Product

- **Формулировка:** Supplier API возвращает все Product по ID, включая inactive/archived, с вычисленным total price.
- **Домен:** Product.
- **Приоритет:** Medium.
- **Источник:** Product API, Data Contract.
- **Связанные Business Rules:** `BR-PROD-007`.
- **Связанные Scenarios:** `SCN-PROD-001`–`SCN-PROD-003`.
- **API/Event/Data references:** `GET /products`; `supplier_db.products`.
- **Acceptance Criteria:** Count/order корректны; flags не фильтруют rows.
- **QA Notes:** Проверить empty list и archived/inactive.
- **Ограничения:** Нет pagination/search.

### FR-PROD-004 — Чтение Product по ID

- **Формулировка:** Supplier API возвращает существующий Product по ID с aggregate stock и вычисленным total price.
- **Домен:** Product.
- **Приоритет:** Medium.
- **Источник:** Product API, Error Contract.
- **Связанные Business Rules:** `BR-PROD-007`.
- **Связанные Scenarios:** `SCN-PROD-001`–`SCN-PROD-003`.
- **API/Event/Data references:** `GET /products/{id}`; `products`.
- **Acceptance Criteria:** Existing ID — 200; absent — `product_not_found`.
- **QA Notes:** Проверить flags и total price.
- **Ограничения:** Warehouse breakdown отсутствует.

### FR-PROD-005 — Изменение Product

- **Формулировка:** Система частично изменяет переданные Product fields, проверяет optional Supplier/flags и сохраняет stock.
- **Домен:** Product.
- **Приоритет:** High.
- **Источник:** Product API, Business/Data Contracts.
- **Связанные Business Rules:** `BR-PROD-002`–`BR-PROD-004`, `BR-PROD-006`, `BR-PROD-009`.
- **Связанные Scenarios:** `SCN-PROD-002`.
- **API/Event/Data references:** `PUT /products/{id}`; `products`, `users`.
- **Acceptance Criteria:** Переданные поля меняются, stock/warehouse rows сохраняются; invalid owner/flags отклоняются.
- **QA Notes:** Проверить empty body, owner change и unchanged stock.
- **Ограничения:** PUT partial; null в NOT NULL может стать 500.

### FR-PROD-006 — Архивирование Product

- **Формулировка:** DELETE Product сохраняет row и устанавливает `is_active=false`, `is_archived=true`.
- **Домен:** Product.
- **Приоритет:** High.
- **Источник:** Product API, Data/Business Contracts.
- **Связанные Business Rules:** `BR-PROD-008`.
- **Связанные Scenarios:** `SCN-PROD-003`.
- **API/Event/Data references:** `DELETE /products/{id}`; `products`.
- **Acceptance Criteria:** 204 без body; row/warehouse stock остаются; дальнейшее чтение доступно.
- **QA Notes:** Проверить repeated delete и сохранность links.
- **Ограничения:** Physical delete endpoint отсутствует.

### FR-PROD-007 — Проверка flags Product

- **Формулировка:** Supplier create/update отклоняет итоговое сочетание active=true и archived=true.
- **Домен:** Product.
- **Приоритет:** High.
- **Источник:** Product API, Error Contract.
- **Связанные Business Rules:** `BR-PROD-006`.
- **Связанные Scenarios:** `SCN-PROD-001`, `SCN-PROD-002`.
- **API/Event/Data references:** Product create/update; `product_archived`.
- **Acceptance Criteria:** Недопустимая combination даёт 409 без commit Product state.
- **QA Notes:** Проверить все flag transitions.
- **Ограничения:** DB CHECK отсутствует; правило действует через API logic.

### FR-PROD-008 — Публикация Product events

- **Формулировка:** После create система публикует `PRODUCT_CREATED`, после update/archive — `PRODUCT_UPDATED` с полным Product payload.
- **Домен:** Product/Event.
- **Приоритет:** High.
- **Источник:** Kafka Event Contract.
- **Связанные Business Rules:** `BR-PROD-010`.
- **Связанные Scenarios:** `SCN-PROD-001`–`SCN-PROD-003`.
- **API/Event/Data references:** `product-events`; key Product ID.
- **Acceptance Criteria:** Correct event type follows committed mutation; archive не создаёт `PRODUCT_DELETED`.
- **QA Notes:** Проверить payload/flags/version.
- **Ограничения:** Event ID/delivery confirmation отсутствуют.

### FR-PROD-009 — Асинхронный результат Product projection

- **Формулировка:** Customer consumer upsert-ит local Product по `PRODUCT_CREATED/UPDATED` при успешной обработке.
- **Домен:** Product/Catalog.
- **Приоритет:** High.
- **Источник:** Event/Data/Catalog Contracts.
- **Связанные Business Rules:** `BR-CAT-001`, `BR-CAT-005`, `BR-CAT-008`.
- **Связанные Scenarios:** `SCN-CAT-001`.
- **API/Event/Data references:** `product-events`; `customer_db.products`.
- **Acceptance Criteria:** После обработки projection содержит доставленные fields.
- **QA Notes:** Сопоставить source/payload/projection и lag.
- **Ограничения:** Результат eventual, без revision guard.

## 6. Warehouse Functional Requirements

### FR-WH-001 — Создание Warehouse

- **Формулировка:** Система создаёт Warehouse из валидных name, weekday_hours и address.
- **Домен:** Warehouse.
- **Приоритет:** Medium.
- **Источник:** Warehouse API, Data Contract.
- **Связанные Business Rules:** `BR-WH-001`, `BR-WH-002`.
- **Связанные Scenarios:** `SCN-WH-001`.
- **API/Event/Data references:** `POST /warehouses`; `warehouses`.
- **Acceptance Criteria:** Valid body даёт 201/row; duplicate name/address разрешён.
- **QA Notes:** Проверить длины и duplicates.
- **Ограничения:** Semantic address/schedule validation отсутствует.

### FR-WH-002 — Чтение списка Warehouse

- **Формулировка:** Система возвращает все Warehouse по ID в `items`/`count`.
- **Домен:** Warehouse.
- **Приоритет:** Medium.
- **Источник:** Warehouse API.
- **Связанные Business Rules:** `BR-WH-003`.
- **Связанные Scenarios:** `SCN-WH-001`–`SCN-WH-003`.
- **API/Event/Data references:** `GET /warehouses`; `warehouses`.
- **Acceptance Criteria:** Empty/nonempty list count/order корректны.
- **QA Notes:** Проверить отсутствие stock details.
- **Ограничения:** Нет pagination/filter.

### FR-WH-003 — Чтение Warehouse по ID

- **Формулировка:** Система возвращает Warehouse по ID либо `warehouse_not_found`.
- **Домен:** Warehouse.
- **Приоритет:** Medium.
- **Источник:** Warehouse API, Error Contract.
- **Связанные Business Rules:** `BR-WH-003`.
- **Связанные Scenarios:** `SCN-WH-001`–`SCN-WH-003`.
- **API/Event/Data references:** `GET /warehouses/{id}`.
- **Acceptance Criteria:** Existing ID — 200; missing — 404.
- **QA Notes:** Проверить invalid/missing ID.
- **Ограничения:** Product/stock rows не возвращаются.

### FR-WH-004 — Изменение Warehouse

- **Формулировка:** Система применяет только переданные Warehouse fields и не изменяет stock.
- **Домен:** Warehouse.
- **Приоритет:** High.
- **Источник:** Warehouse API, Business Contract.
- **Связанные Business Rules:** `BR-WH-004`.
- **Связанные Scenarios:** `SCN-WH-002`.
- **API/Event/Data references:** `PUT /warehouses/{id}`; `warehouses`.
- **Acceptance Criteria:** Metadata обновлена; WarehouseProduct/Product.stocks прежние.
- **QA Notes:** Проверить partial/empty body и stock invariance.
- **Ограничения:** updated_at отсутствует.

### FR-WH-005 — Удаление Warehouse

- **Формулировка:** Система удаляет Warehouse и его WarehouseProduct rows.
- **Домен:** Warehouse.
- **Приоритет:** High.
- **Источник:** Warehouse API, Data Contract.
- **Связанные Business Rules:** `BR-WH-005`.
- **Связанные Scenarios:** `SCN-WH-003`.
- **API/Event/Data references:** `DELETE /warehouses/{id}`; `warehouses`, `warehouse_products`.
- **Acceptance Criteria:** Existing Warehouse/rows отсутствуют после 204.
- **QA Notes:** Проверить empty/nonempty Warehouse.
- **Ограничения:** Warehouse-specific event не публикуется.

### FR-WH-006 — Пересчёт stock после удаления Warehouse

- **Формулировка:** После удаления Warehouse система пересчитывает aggregate каждого затронутого Product и инициирует stock event.
- **Домен:** Warehouse/Stock.
- **Приоритет:** High.
- **Источник:** Warehouse API, Event/Data Contracts.
- **Связанные Business Rules:** `BR-WH-005`, `BR-WH-006`, `BR-STOCK-004`, `BR-STOCK-009`.
- **Связанные Scenarios:** `SCN-WH-003`.
- **API/Event/Data references:** `products.stocks`; `STOCK_REPLENISHED`.
- **Acceptance Criteria:** Aggregate равен сумме оставшихся rows; event содержит итог.
- **QA Notes:** Проверить несколько Product/Warehouse.
- **Ограничения:** Пересчёты выполняются после delete commit и могут завершиться частично.

## 7. Stock Functional Requirements

### FR-STOCK-001 — Установка Warehouse stock

- **Формулировка:** Система создаёт или заменяет абсолютный stock пары Warehouse–Product валидным non-negative значением.
- **Домен:** Stock.
- **Приоритет:** High.
- **Источник:** Stock API, Data Contract.
- **Связанные Business Rules:** `BR-STOCK-001`–`BR-STOCK-003`.
- **Связанные Scenarios:** `SCN-STOCK-001`.
- **API/Event/Data references:** `POST /warehouses/{id}/stocks`; `warehouse_products`.
- **Acceptance Criteria:** Existing pair заменяется, new pair создаётся; invalid Warehouse/Product/value отклоняются.
- **QA Notes:** Проверить replace, zero/negative и missing entities.
- **Ограничения:** Archived/inactive Product допускается.

### FR-STOCK-002 — Расчёт aggregate stock

- **Формулировка:** После изменения WarehouseProduct система сохраняет `Product.stocks` как сумму всех warehouse rows Product.
- **Домен:** Stock.
- **Приоритет:** High.
- **Источник:** Stock API, Data Contract.
- **Связанные Business Rules:** `BR-STOCK-004`.
- **Связанные Scenarios:** `SCN-STOCK-001`, `SCN-WH-003`, `SCN-STOCK-002`.
- **API/Event/Data references:** `warehouse_products`, `products.stocks`.
- **Acceptance Criteria:** Saved aggregate равен SQL-сумме rows после успешного пересчёта.
- **QA Notes:** Проверить zero/no rows и несколько Warehouse.
- **Ограничения:** DB trigger/check согласованности отсутствует.

### FR-STOCK-003 — Multi-warehouse stock

- **Формулировка:** Система хранит отдельный stock одного Product на нескольких Warehouse и агрегирует их.
- **Домен:** Stock.
- **Приоритет:** High.
- **Источник:** Data/Stock Contracts.
- **Связанные Business Rules:** `BR-STOCK-005`.
- **Связанные Scenarios:** `SCN-STOCK-001`.
- **API/Event/Data references:** Composite PK `warehouse_products`.
- **Acceptance Criteria:** Две warehouse rows одного Product сосуществуют, aggregate равен сумме.
- **QA Notes:** Изменять каждый Warehouse независимо.
- **Ограничения:** История/резервы отсутствуют.

### FR-STOCK-004 — Публикация stock events

- **Формулировка:** После успешного пересчёта система публикует новый aggregate в `product-stock-events`.
- **Домен:** Stock/Event.
- **Приоритет:** High.
- **Источник:** Kafka Event Contract.
- **Связанные Business Rules:** `BR-STOCK-009`.
- **Связанные Scenarios:** `SCN-STOCK-001`, `SCN-WH-003`, `SCN-STOCK-002`.
- **API/Event/Data references:** `STOCK_REPLENISHED`, `STOCK_DECREASED_BY_ORDER`.
- **Acceptance Criteria:** Event key/product_id/total_quantity/version соответствуют committed aggregate.
- **QA Notes:** Проверить увеличение и уменьшение.
- **Ограничения:** Event ID/delivery confirmation отсутствуют; название replenished используется при снижении.

### FR-STOCK-005 — Обработка ORDER_CREATED

- **Формулировка:** Supplier consumer дедуплицирует `ORDER_CREATED` по event ID и уменьшает WarehouseProduct последовательно по Warehouse ID.
- **Домен:** Stock/Order.
- **Приоритет:** High.
- **Источник:** Event/Data Contracts.
- **Связанные Business Rules:** `BR-STOCK-010`.
- **Связанные Scenarios:** `SCN-STOCK-002`.
- **API/Event/Data references:** `order-events`; `processed_events`, `warehouse_products`.
- **Acceptance Criteria:** First UUID применяет stock один раз; duplicate UUID не применяет повторно.
- **QA Notes:** Проверить порядок Warehouse и duplicate/no ID.
- **Ограничения:** Event без ID обрабатывается без дедупликации.

### FR-STOCK-006 — Partial decrease

- **Формулировка:** Если supplier stock меньше ordered quantity, consumer уменьшает весь доступный stock без фиксации remainder как ошибки.
- **Домен:** Stock/Order.
- **Приоритет:** High.
- **Источник:** Event/Business Contracts.
- **Связанные Business Rules:** `BR-STOCK-011`, `BR-CONS-007`.
- **Связанные Scenarios:** `SCN-STOCK-002`.
- **API/Event/Data references:** Supplier order consumer; inventory tables.
- **Acceptance Criteria:** Rows не уходят ниже нуля; aggregate становится нулём/остатком; Order status не меняется.
- **QA Notes:** Проверить requested > aggregate и missing rows.
- **Ограничения:** Результат не связан с Order entity/confirmation.

### FR-STOCK-007 — Customer stock projection

- **Формулировка:** Customer consumer применяет `total_quantity` (или fallback `stocks`) к local Product и при missing/no-op запускает snapshot sync.
- **Домен:** Stock/Catalog.
- **Приоритет:** High.
- **Источник:** Event/Catalog/Data Contracts.
- **Связанные Business Rules:** `BR-CAT-007`, `BR-CAT-008`.
- **Связанные Scenarios:** `SCN-CAT-002`.
- **API/Event/Data references:** `product-stock-events`; `customer_db.products`.
- **Acceptance Criteria:** Changed Product получает event aggregate; missing/no-op вызывает snapshot path.
- **QA Notes:** Проверить fallback field и HTTP sync failure.
- **Ограничения:** Event type/range/revision не валидируются.

## 8. Customer/User Functional Requirements

### FR-CUST-001 — Создание User

- **Формулировка:** Customer API создаёт User из валидных full_name/email и возвращает ID/created_at.
- **Домен:** Customer.
- **Приоритет:** High.
- **Источник:** Users API, Error/Data Contracts.
- **Связанные Business Rules:** `BR-CUST-001`, `BR-CUST-002`.
- **Связанные Scenarios:** Основной путь покупателя в Scenario Specification.
- **API/Event/Data references:** `POST /users`; `customer_db.users`.
- **Acceptance Criteria:** Unique email — 201/row; duplicate — `user_conflict`.
- **QA Notes:** Проверить name/email validation.
- **Ограничения:** Auth credentials не создаются.

### FR-CUST-002 — Чтение списка User

- **Формулировка:** Customer API возвращает всех User по ID в `items`/`count`.
- **Домен:** Customer.
- **Приоритет:** Medium.
- **Источник:** Users API.
- **Связанные Business Rules:** `BR-CUST-005`.
- **Связанные Scenarios:** Пользовательские пути Customer.
- **API/Event/Data references:** `GET /users`; `users`.
- **Acceptance Criteria:** Empty/nonempty list count/order корректны.
- **QA Notes:** Проверить отсутствие связанных данных в response.
- **Ограничения:** Пагинация/filter отсутствуют.

### FR-CUST-003 — Чтение User по ID

- **Формулировка:** Customer API возвращает User по ID либо `user_not_found`.
- **Домен:** Customer.
- **Приоритет:** Medium.
- **Источник:** Users API, Error Contract.
- **Связанные Business Rules:** `BR-CUST-005`.
- **Связанные Scenarios:** Все Customer scenarios используют User.
- **API/Event/Data references:** `GET /users/{id}`.
- **Acceptance Criteria:** Existing ID — 200; absent — 404.
- **QA Notes:** Проверить invalid/missing ID.
- **Ограничения:** Update/delete endpoints отсутствуют.

### FR-CUST-004 — Ownership по user_id

- **Формулировка:** Favorites, cart и orders выбираются/изменяются по входному `user_id`; чужой Order возвращается как `order_not_found`.
- **Домен:** Customer/Ownership.
- **Приоритет:** High.
- **Источник:** Customer API, Error/Business Contracts.
- **Связанные Business Rules:** `BR-CUST-003`, `BR-CUST-004`.
- **Связанные Scenarios:** `SCN-FAV-001`, `SCN-CART-001`–`003`, `SCN-ORD-001`–`003`.
- **API/Event/Data references:** Customer domain endpoints; user FKs.
- **Acceptance Criteria:** Operations affect only rows указанного User; foreign Order не раскрывается.
- **QA Notes:** Проверить два User и crossed IDs.
- **Ограничения:** Auth не подтверждает caller identity.

## 9. Catalog Projection Functional Requirements

### FR-CAT-001 — Чтение customer catalog

- **Формулировка:** Customer API возвращает все local Product projection rows, включая inactive/archived.
- **Домен:** Catalog.
- **Приоритет:** Medium.
- **Источник:** Catalog API, Data Contract.
- **Связанные Business Rules:** `BR-CAT-001`–`BR-CAT-004`.
- **Связанные Scenarios:** `SCN-CAT-001`.
- **API/Event/Data references:** `GET /products`; `customer_db.products`.
- **Acceptance Criteria:** List отражает local DB, count/order/total price корректны.
- **QA Notes:** Сравнить с supplier before/after lag.
- **Ограничения:** Supplier ID/freshness marker отсутствуют.

### FR-CAT-002 — Чтение projection по ID

- **Формулировка:** Customer API возвращает local Product по ID либо `product_not_found`.
- **Домен:** Catalog.
- **Приоритет:** Medium.
- **Источник:** Catalog API, Error Contract.
- **Связанные Business Rules:** `BR-CAT-001`, `BR-CAT-003`.
- **Связанные Scenarios:** `SCN-CAT-001`.
- **API/Event/Data references:** `GET /products/{id}`.
- **Acceptance Criteria:** Existing local row — 200; отсутствующая projection — 404 независимо от source.
- **QA Notes:** Проверить lag после supplier create.
- **Ограничения:** Live supplier lookup не выполняется.

### FR-CAT-003 — Upsert projection через Product events

- **Формулировка:** Customer consumer создаёт/обновляет local Product по `PRODUCT_CREATED/UPDATED`.
- **Домен:** Catalog/Event.
- **Приоритет:** High.
- **Источник:** Kafka Event Contract.
- **Связанные Business Rules:** `BR-CAT-005`.
- **Связанные Scenarios:** `SCN-CAT-001`.
- **API/Event/Data references:** `product-events`; customer Product.
- **Acceptance Criteria:** Доставленный payload приводит projection к его fields; identical state — no-op.
- **QA Notes:** Проверить create/update/unknown type.
- **Ограничения:** `PRODUCT_DELETED` поддерживается, но source не публикует.

### FR-CAT-004 — Обновление projection stock

- **Формулировка:** Customer consumer обновляет `products.stocks` по stock event.
- **Домен:** Catalog/Stock.
- **Приоритет:** High.
- **Источник:** Event/Data Contracts.
- **Связанные Business Rules:** `BR-CAT-007`.
- **Связанные Scenarios:** `SCN-CAT-002`.
- **API/Event/Data references:** `product-stock-events`.
- **Acceptance Criteria:** Event aggregate сохраняется для существующего Product.
- **QA Notes:** Проверить оба current event type.
- **Ограничения:** Старое event может перезаписать новое.

### FR-CAT-005 — Snapshot synchronization

- **Формулировка:** На startup и stock fallback customer-service запрашивает полный Supplier catalog, upsert-ит rows и пытается удалить stale Product.
- **Домен:** Catalog.
- **Приоритет:** High.
- **Источник:** Customer Overview, Event/Data Contracts.
- **Связанные Business Rules:** `BR-CAT-006`, `BR-CAT-007`.
- **Связанные Scenarios:** `SCN-CAT-001`, `SCN-CAT-002`.
- **API/Event/Data references:** Supplier `GET /products`; customer `products`.
- **Acceptance Criteria:** Available snapshot upsert-ит source items; stale deletion выполняется при отсутствии FK conflict.
- **QA Notes:** Проверить startup/fallback и unavailable Supplier.
- **Ограничения:** FK может оставить stale row; failure только логируется.

### FR-CAT-006 — Eventual consistency projection

- **Формулировка:** Customer reads и availability checks используют последнее сохранённое local Product state, которое обновляется асинхронно.
- **Домен:** Catalog/Consistency.
- **Приоритет:** High.
- **Источник:** System/Event/Data Contracts.
- **Связанные Business Rules:** `BR-CAT-008`, `BR-CONS-001`, `BR-CONS-005`.
- **Связанные Scenarios:** `SCN-CAT-001`, `SCN-CAT-002`.
- **API/Event/Data references:** Kafka/HTTP sync; customer Product.
- **Acceptance Criteria:** До обработки change Customer API может возвращать предыдущее состояние; после обработки — новое.
- **QA Notes:** Наблюдать lag и ordering между product/stock topics.
- **Ограничения:** SLA/revision не определены.

## 10. Favorite Functional Requirements

### FR-FAV-001 — Добавление Favorite

- **Формулировка:** Система создаёт Favorite для существующих User/local Product.
- **Домен:** Favorite.
- **Приоритет:** High.
- **Источник:** Favorites API.
- **Связанные Business Rules:** `BR-FAV-001`, `BR-FAV-002`.
- **Связанные Scenarios:** `SCN-FAV-001`.
- **API/Event/Data references:** `POST /favorites`; `favorites`.
- **Acceptance Criteria:** Valid pair создаёт row/201; missing entity — 404.
- **QA Notes:** Проверить unique pair.
- **Ограничения:** Auth отсутствует.

### FR-FAV-002 — Повторное добавление Favorite

- **Формулировка:** Повторный POST той же pair возвращает существующую Favorite без duplicate row.
- **Домен:** Favorite.
- **Приоритет:** Medium.
- **Источник:** Favorites API, Data Contract.
- **Связанные Business Rules:** `BR-FAV-002`, `BR-FAV-003`.
- **Связанные Scenarios:** `SCN-FAV-001`.
- **API/Event/Data references:** UNIQUE pair.
- **Acceptance Criteria:** ID/created_at прежние, row count неизменен, HTTP 201.
- **QA Notes:** Повторить одинаковый request.
- **Ограничения:** Status не различает create/existing.

### FR-FAV-003 — Чтение Favorites

- **Формулировка:** Система возвращает Favorites указанного User по Favorite ID с current Product summary.
- **Домен:** Favorite.
- **Приоритет:** Medium.
- **Источник:** Favorites API.
- **Связанные Business Rules:** `BR-FAV-006`.
- **Связанные Scenarios:** `SCN-FAV-001`.
- **API/Event/Data references:** `GET /favorites?user_id=`.
- **Acceptance Criteria:** Items/count/order корректны; summary соответствует current projection.
- **QA Notes:** Изменить Product после Favorite.
- **Ограничения:** Missing Product делает item несериализуемым.

### FR-FAV-004 — Удаление Favorite

- **Формулировка:** Система удаляет Favorite по user ID и product ID.
- **Домен:** Favorite.
- **Приоритет:** Medium.
- **Источник:** Favorites API, Error Contract.
- **Связанные Business Rules:** `BR-FAV-005`.
- **Связанные Scenarios:** `SCN-FAV-001`.
- **API/Event/Data references:** `DELETE /favorites/{product_id}`.
- **Acceptance Criteria:** Existing pair — 204/delete; absent — `favorite_not_found`.
- **QA Notes:** Проверить repeat delete.
- **Ограничения:** Product existence не проверяется.

### FR-FAV-005 — Favorite для недоступного Product

- **Формулировка:** Система разрешает Favorite для archived, inactive и zero-stock local Product.
- **Домен:** Favorite.
- **Приоритет:** Medium.
- **Источник:** Favorites API, Business Contract.
- **Связанные Business Rules:** `BR-FAV-004`.
- **Связанные Scenarios:** `SCN-FAV-001`.
- **API/Event/Data references:** `POST /favorites`; Product flags.
- **Acceptance Criteria:** Все три состояния создают/возвращают Favorite без flag/stock conflict.
- **QA Notes:** Проверить flags отдельно и вместе.
- **Ограничения:** Намерение поведения не определено; фиксируется AS IS.

### FR-FAV-006 — Текущий Product summary Favorite

- **Формулировка:** Favorite response строит вложенный Product summary из текущей projection.
- **Домен:** Favorite/Catalog.
- **Приоритет:** Medium.
- **Источник:** Favorites API, Data Contract.
- **Связанные Business Rules:** `BR-FAV-006`.
- **Связанные Scenarios:** `SCN-FAV-001`.
- **API/Event/Data references:** Favorite serializer; customer Product.
- **Acceptance Criteria:** Изменение local price/stock/flags отражается в следующем response.
- **QA Notes:** Проверить неисторичность summary.
- **Ограничения:** Snapshot момента добавления не хранится.

## 11. Cart Functional Requirements

### FR-CART-001 — Чтение Cart

- **Формулировка:** Система возвращает Cart User с items/count/total_items_count/total_price.
- **Домен:** Cart.
- **Приоритет:** High.
- **Источник:** Cart API, Data Contract.
- **Связанные Business Rules:** `BR-CART-009`, `BR-CART-010`.
- **Связанные Scenarios:** `SCN-CART-001`–`SCN-CART-003`.
- **API/Event/Data references:** `GET /cart`; `cart_items`, `products`.
- **Acceptance Criteria:** Empty cart имеет нули; totals согласованы с current projection.
- **QA Notes:** Проверить item ordering и агрегаты.
- **Ограничения:** Read не перепроверяет flags/stock.

### FR-CART-002 — Добавление CartItem

- **Формулировка:** Система создаёт CartItem для существующих User/Product при допустимых flags/quantity/local stock.
- **Домен:** Cart.
- **Приоритет:** High.
- **Источник:** Cart API, Error Contract.
- **Связанные Business Rules:** `BR-CART-001`–`BR-CART-004`.
- **Связанные Scenarios:** `SCN-CART-001`.
- **API/Event/Data references:** `POST /cart`; `cart_items`.
- **Acceptance Criteria:** Valid request — 201; invalid flags/stock — соответствующий 409.
- **QA Notes:** Проверить boundary и error precedence.
- **Ограничения:** Проверка по local projection.

### FR-CART-003 — Increment повторным POST

- **Формулировка:** Повторный POST existing pair увеличивает quantity на входное значение.
- **Домен:** Cart.
- **Приоритет:** High.
- **Источник:** Cart API.
- **Связанные Business Rules:** `BR-CART-005`.
- **Связанные Scenarios:** `SCN-CART-001`.
- **API/Event/Data references:** `POST /cart`; unique pair.
- **Acceptance Criteria:** New quantity = old + input при достаточном local stock.
- **QA Notes:** Проверить суммарное превышение.
- **Ограничения:** Response status остаётся 201 для update.

### FR-CART-004 — Замена quantity через PATCH

- **Формулировка:** PATCH existing CartItem заменяет quantity целиком.
- **Домен:** Cart.
- **Приоритет:** High.
- **Источник:** Cart API.
- **Связанные Business Rules:** `BR-CART-006`.
- **Связанные Scenarios:** `SCN-CART-002`.
- **API/Event/Data references:** `PATCH /cart/{product_id}`.
- **Acceptance Criteria:** Result quantity равно body quantity; missing item — 404.
- **QA Notes:** Проверить increase/decrease/0.
- **Ограничения:** Ноль не означает delete.

### FR-CART-005 — Удаление CartItem

- **Формулировка:** DELETE удаляет CartItem pair указанного User/Product.
- **Домен:** Cart.
- **Приоритет:** Medium.
- **Источник:** Cart API.
- **Связанные Business Rules:** `BR-CART-007`.
- **Связанные Scenarios:** `SCN-CART-003`.
- **API/Event/Data references:** `DELETE /cart/{product_id}`.
- **Acceptance Criteria:** Existing item — 204/delete; repeat — `cart_item_not_found`.
- **QA Notes:** Проверить без Product lookup.
- **Ограничения:** Stock не меняется.

### FR-CART-006 — Проверка flags Cart

- **Формулировка:** Cart add/update отклоняет archived Product, затем inactive Product.
- **Домен:** Cart/Product.
- **Приоритет:** High.
- **Источник:** Cart API, Error Contract.
- **Связанные Business Rules:** `BR-CART-002`.
- **Связанные Scenarios:** `SCN-CART-001`, `SCN-CART-002`.
- **API/Event/Data references:** `product_archived`, `product_inactive`.
- **Acceptance Criteria:** Correct 409 code возвращается для каждого state.
- **QA Notes:** Проверить both flags и precedence.
- **Ограничения:** Favorite/Cart read не используют эту блокировку.

### FR-CART-007 — Проверка local stock Cart

- **Формулировка:** Cart add/update отклоняет итоговую quantity выше local `Product.stocks`.
- **Домен:** Cart/Stock.
- **Приоритет:** High.
- **Источник:** Cart API, Business Contract.
- **Связанные Business Rules:** `BR-CART-004`, `BR-CART-008`, `BR-CONS-005`.
- **Связанные Scenarios:** `SCN-CART-001`, `SCN-CART-002`.
- **API/Event/Data references:** `insufficient_stock`; customer Product.
- **Acceptance Criteria:** Equal stock допускается; above — 409/no cart mutation.
- **QA Notes:** Сравнить stale local/source stock.
- **Ограничения:** Stock не резервируется.

### FR-CART-008 — Расчёт Cart totals

- **Формулировка:** Система вычисляет unit/line/cart totals по текущей local Product price при чтении.
- **Домен:** Cart/Pricing.
- **Приоритет:** High.
- **Источник:** Cart API, Data Contract.
- **Связанные Business Rules:** `BR-CART-009`.
- **Связанные Scenarios:** `SCN-CART-001`, `SCN-CART-002`.
- **API/Event/Data references:** Cart serializers; `products.price`.
- **Acceptance Criteria:** Изменение local price меняет subsequent totals без CartItem update.
- **QA Notes:** Проверить rounding и quantity sums.
- **Ограничения:** Цена CartItem не фиксируется.

## 12. Order Functional Requirements

### FR-ORD-001 — Создание Order из Cart

- **Формулировка:** При отсутствии непустых body items система создаёт Order из CartItems User.
- **Домен:** Order.
- **Приоритет:** High.
- **Источник:** Orders API, Business Scenarios.
- **Связанные Business Rules:** `BR-ORD-001`–`BR-ORD-004`, `BR-ORD-007`, `BR-ORD-008`.
- **Связанные Scenarios:** `SCN-ORD-001`.
- **API/Event/Data references:** `POST /orders`; `cart_items`, `products`.
- **Acceptance Criteria:** Непустая валидная cart создаёт Order; пустая — `cart_is_empty`.
- **QA Notes:** Проверить missing/null/empty items.
- **Ограничения:** Проверки используют local projection.

### FR-ORD-002 — Создание Order из body items

- **Формулировка:** При непустых body items система создаёт Order из них независимо от состава Cart.
- **Домен:** Order.
- **Приоритет:** High.
- **Источник:** Orders API, Scenarios.
- **Связанные Business Rules:** `BR-ORD-002`, `BR-ORD-005`, `BR-ORD-007`, `BR-ORD-008`.
- **Связанные Scenarios:** `SCN-ORD-002`.
- **API/Event/Data references:** `POST /orders`; `order_items`.
- **Acceptance Criteria:** OrderItems соответствуют body items после агрегации, не cart contents.
- **QA Notes:** Проверить body priority при непустой cart.
- **Ограничения:** Вся cart всё равно очищается.

### FR-ORD-003 — Семантика missing/null/empty items

- **Формулировка:** Отсутствующее поле `items`, `null` и `[]` одинаково выбирают Cart как источник.
- **Домен:** Order.
- **Приоритет:** High.
- **Источник:** Orders API, Business Contract.
- **Связанные Business Rules:** `BR-ORD-003`, `BR-ORD-004`.
- **Связанные Scenarios:** `SCN-ORD-001`.
- **API/Event/Data references:** Order request builder.
- **Acceptance Criteria:** Три формы дают одинаковый Order при одинаковой cart и одинаковый error при empty cart.
- **QA Notes:** Сравнить response/DB/events.
- **Ограничения:** Явный empty list не означает empty Order.

### FR-ORD-004 — Агрегация duplicate Product

- **Формулировка:** Система суммирует quantities повторяющихся `product_id` body items до проверки и создания OrderItems.
- **Домен:** Order.
- **Приоритет:** High.
- **Источник:** Orders API, Data/Business Contracts.
- **Связанные Business Rules:** `BR-ORD-006`.
- **Связанные Scenarios:** `SCN-ORD-002`.
- **API/Event/Data references:** Order aggregation; `order_items`.
- **Acceptance Criteria:** Один unique Product создаёт один OrderItem/event с summed quantity.
- **QA Notes:** Проверить суммарное превышение stock.
- **Ограничения:** DB сама не имеет unique order/product constraint.

### FR-ORD-005 — Price snapshot Order

- **Формулировка:** При создании система сохраняет quantity, unit price и line total в OrderItem и сумму в Order.
- **Домен:** Order/Pricing.
- **Приоритет:** High.
- **Источник:** Orders API, Data Contract.
- **Связанные Business Rules:** `BR-ORD-009`.
- **Связанные Scenarios:** `SCN-ORD-001`, `SCN-ORD-002`.
- **API/Event/Data references:** `orders.total_price`, `order_items.unit_price/total_price`.
- **Acceptance Criteria:** Последующее изменение Product price не меняет stored totals.
- **QA Notes:** Проверить rounding и multi-item sum.
- **Ограничения:** Money хранится float.

### FR-ORD-006 — Текущий Product summary Order

- **Формулировка:** При чтении Order система дополняет OrderItem текущим local Product summary.
- **Домен:** Order/Catalog.
- **Приоритет:** High.
- **Источник:** Orders API, Data Contract.
- **Связанные Business Rules:** `BR-ORD-010`.
- **Связанные Scenarios:** `SCN-ORD-001`–`SCN-ORD-003`.
- **API/Event/Data references:** Order serializer; customer Product.
- **Acceptance Criteria:** Current price/stock/flags изменяются в summary, stored unit price остаётся.
- **QA Notes:** Проверить historical/current mix.
- **Ограничения:** Missing Product может сделать Order response 404.

### FR-ORD-007 — Номер Order

- **Формулировка:** Созданный Order получает number `ORD-` + decimal ID с минимальной шириной шесть цифр.
- **Домен:** Order.
- **Приоритет:** High.
- **Источник:** Orders API, Business Contract.
- **Связанные Business Rules:** `BR-ORD-011`.
- **Связанные Scenarios:** `SCN-ORD-001`, `SCN-ORD-002`.
- **API/Event/Data references:** `orders.order_number`.
- **Acceptance Criteria:** ID 1 даёт `ORD-000001`; number unique/non-null для API-created Order.
- **QA Notes:** Проверить границу >999999 без truncation.
- **Ограничения:** Number не несёт другой business semantics.

### FR-ORD-008 — Initial status Order

- **Формулировка:** Новый Order создаётся со status `created`.
- **Домен:** Order.
- **Приоритет:** High.
- **Источник:** Orders API, Data Contract.
- **Связанные Business Rules:** `BR-ORD-011`.
- **Связанные Scenarios:** `SCN-ORD-001`, `SCN-ORD-002`.
- **API/Event/Data references:** `orders.status`.
- **Acceptance Criteria:** После create DB/response status = created.
- **QA Notes:** Проверить до/после supplier stock processing.
- **Ограничения:** Supplier result status не меняет.

### FR-ORD-009 — Очистка Cart после Order

- **Формулировка:** В transaction создания Order система удаляет все CartItems User.
- **Домен:** Order/Cart.
- **Приоритет:** High.
- **Источник:** Orders API, Data Contract.
- **Связанные Business Rules:** `BR-ORD-005`, `BR-ORD-012`.
- **Связанные Scenarios:** `SCN-ORD-001`, `SCN-ORD-002`.
- **API/Event/Data references:** `cart_items`; create Order transaction.
- **Acceptance Criteria:** После successful commit cart пуста для обоих source modes.
- **QA Notes:** Body Order с unrelated cart.
- **Ограничения:** Producer failure после commit не восстанавливает cart.

### FR-ORD-010 — Публикация ORDER_CREATED

- **Формулировка:** После Order commit система публикует отдельный `ORDER_CREATED` на каждый unique Product.
- **Домен:** Order/Event.
- **Приоритет:** High.
- **Источник:** Kafka Event Contract.
- **Связанные Business Rules:** `BR-ORD-013`.
- **Связанные Scenarios:** `SCN-ORD-001`, `SCN-ORD-002`, `SCN-STOCK-002`.
- **API/Event/Data references:** `order-events`; event ID/product ID/quantity/version.
- **Acceptance Criteria:** Event count/payload соответствуют unique OrderItems.
- **QA Notes:** Проверить отсутствие order/user/price fields.
- **Ограничения:** Multi-event publication может быть частичной.

### FR-ORD-011 — Чтение Orders

- **Формулировка:** Система возвращает list/detail Orders указанного User с non-null number и вложенными items.
- **Домен:** Order.
- **Приоритет:** Medium.
- **Источник:** Orders API, Error Contract.
- **Связанные Business Rules:** `BR-CUST-004`, `BR-ORD-010`.
- **Связанные Scenarios:** `SCN-ORD-001`–`SCN-ORD-003`.
- **API/Event/Data references:** `GET /orders`, `GET /orders/{id}`.
- **Acceptance Criteria:** List newest-first; own detail доступна; foreign/missing — `order_not_found`.
- **QA Notes:** Проверить ownership, numberless row, current summary.
- **Ограничения:** Нет pagination/status filter.

### FR-ORD-012 — Cancel Order

- **Формулировка:** Система переводит принадлежащий User non-cancelled Order в status `cancelled` и запрещает повторный cancel.
- **Домен:** Order.
- **Приоритет:** High.
- **Источник:** Orders API, Error Contract.
- **Связанные Business Rules:** `BR-ORD-015`.
- **Связанные Scenarios:** `SCN-ORD-003`.
- **API/Event/Data references:** `POST /orders/{id}/cancel`; `orders.status`.
- **Acceptance Criteria:** First cancel — 200/cancelled; repeat — 409 `order_already_cancelled`.
- **QA Notes:** Проверить own/foreign/missing Order.
- **Ограничения:** Другой DB status тоже будет заменён на cancelled.

### FR-ORD-013 — Отсутствие stock compensation при cancel

- **Формулировка:** Cancel изменяет только local status и не возвращает stock, не восстанавливает Cart, не меняет OrderItems/totals.
- **Домен:** Order/Consistency.
- **Приоритет:** High.
- **Источник:** Orders/Data Contracts.
- **Связанные Business Rules:** `BR-ORD-016`.
- **Связанные Scenarios:** `SCN-ORD-003`.
- **API/Event/Data references:** Cancel path; customer/supplier stock и Cart tables.
- **Acceptance Criteria:** После cancel изменён только `orders.status`; остальные перечисленные данные прежние.
- **QA Notes:** Сравнить stock/cart/items/totals до и после.
- **Ограничения:** Это граница AS IS, не предписание будущего поведения.

### FR-ORD-014 — Отсутствие cancel event

- **Формулировка:** Cancel Order не публикует Kafka event и не уведомляет supplier-service.
- **Домен:** Order/Event.
- **Приоритет:** High.
- **Источник:** Orders/Kafka Event Contracts.
- **Связанные Business Rules:** `BR-ORD-017`.
- **Связанные Scenarios:** `SCN-ORD-003`.
- **API/Event/Data references:** Cancel path; отсутствие producer topic.
- **Acceptance Criteria:** После успешного cancel в прикладных topics отсутствует событие отмены.
- **QA Notes:** Наблюдать все четыре topic во время cancel.
- **Ограничения:** Отсутствие события фиксируется как AS IS behavior.

### FR-ORD-015 — Отсутствие supplier confirmation

- **Формулировка:** Create Order возвращает HTTP 201 после customer DB flow, не ожидая подтверждения supplier stock processing.
- **Домен:** Order/Consistency.
- **Приоритет:** High.
- **Источник:** Orders/Event/Scenario Contracts.
- **Связанные Business Rules:** `BR-ORD-014`, `BR-CONS-007`.
- **Связанные Scenarios:** `SCN-ORD-001`, `SCN-ORD-002`, `SCN-STOCK-002`.
- **API/Event/Data references:** `POST /orders`; `order-events`; supplier consumer.
- **Acceptance Criteria:** HTTP 201 возможен до stock decrease; supplier result не меняет Order status.
- **QA Notes:** Сравнить время response, order row и inventory effect.
- **Ограничения:** Confirmation/rejection event отсутствует.

## 13. Audit Functional Requirements

### FR-AUD-001 — Потребление supplier-events

- **Формулировка:** Audit-consumer читает сообщения `supplier-events` в своей consumer group и retry-ит exception до трёх раз.
- **Домен:** Audit.
- **Приоритет:** High.
- **Источник:** Kafka Event Contract.
- **Связанные Business Rules:** `BR-AUD-001`.
- **Связанные Scenarios:** `SCN-AUD-001`.
- **API/Event/Data references:** `audit-supplier-events-consumer-group`.
- **Acceptance Criteria:** Valid event обрабатывается и offset коммитится после handler.
- **QA Notes:** Проверить retry/manual commit.
- **Ограничения:** Dead letter существует только в логах.

### FR-AUD-002 — Запись audit payload

- **Формулировка:** Для обработанного supplier event система вставляет `user_events` row с type, Supplier ID, полным payload и received_at.
- **Домен:** Audit/Data.
- **Приоритет:** High.
- **Источник:** Event/Data Contracts.
- **Связанные Business Rules:** `BR-AUD-002`, `BR-AUD-003`, `BR-AUD-005`.
- **Связанные Scenarios:** `SCN-AUD-001`.
- **API/Event/Data references:** `supplier_db.user_events`.
- **Acceptance Criteria:** Row fields соответствуют message; delete audit сохраняется без Supplier FK.
- **QA Notes:** Проверить все три type.
- **Ограничения:** Отдельной audit DB нет.

### FR-AUD-003 — Повторная audit-запись

- **Формулировка:** Повторная обработка одинакового supplier event создаёт дополнительную audit row.
- **Домен:** Audit.
- **Приоритет:** Medium.
- **Источник:** Data/Event Contracts.
- **Связанные Business Rules:** `BR-AUD-004`, `BR-AUD-006`.
- **Связанные Scenarios:** `SCN-AUD-001`.
- **API/Event/Data references:** `user_events`; отсутствие event ID/unique.
- **Acceptance Criteria:** Две обработки дают две rows.
- **QA Notes:** Смоделировать duplicate/commit failure.
- **Ограничения:** API чтения audit отсутствует; проверка через DB.

## 14. Cross-service Functional Requirements

### FR-CONS-001 — Kafka synchronization

- **Формулировка:** Supplier Product/stock changes синхронизируются в customer projection через product и stock topics.
- **Домен:** Cross-service.
- **Приоритет:** High.
- **Источник:** Kafka Event/System/Data Contracts.
- **Связанные Business Rules:** `BR-CONS-001`, `BR-CAT-005`, `BR-CAT-007`.
- **Связанные Scenarios:** `SCN-CAT-001`, `SCN-CAT-002`.
- **API/Event/Data references:** `product-events`, `product-stock-events`.
- **Acceptance Criteria:** После consumer processing projection отражает delivered state.
- **QA Notes:** Проверить topic/key/effect.
- **Ограничения:** Eventual, без SLA/revision.

### FR-CONS-002 — Manual consumer processing

- **Формулировка:** Consumers коммитят offset вручную после successful/skipped handler и retry-ят exception до трёх раз.
- **Домен:** Cross-service/Event.
- **Приоритет:** High.
- **Источник:** Kafka Event Contract.
- **Связанные Business Rules:** `BR-AUD-001`; техническая реализация consumers.
- **Связанные Scenarios:** `SCN-CAT-001`, `SCN-STOCK-002`, `SCN-CAT-002`, `SCN-AUD-001`.
- **API/Event/Data references:** Все три consumer groups.
- **Acceptance Criteria:** Commit/retry behavior соответствует handler result.
- **QA Notes:** Проверить unknown type/no-op/failure.
- **Ограничения:** Отдельного dead-letter topic нет.

### FR-CONS-003 — Post-commit публикация

- **Формулировка:** Mutating flows публикуют Kafka events после соответствующего DB commit.
- **Домен:** Cross-service/Consistency.
- **Приоритет:** High.
- **Источник:** Event/Error Contracts.
- **Связанные Business Rules:** `BR-CONS-002`–`BR-CONS-004`.
- **Связанные Scenarios:** Все Kafka-producing scenarios.
- **API/Event/Data references:** Supplier/Product/Stock/Order producers.
- **Acceptance Criteria:** При наблюдаемом publish committed state уже существует.
- **QA Notes:** Инъецировать producer exception.
- **Ограничения:** Distributed transaction/outbox отсутствуют.

### FR-CONS-004 — Сохранение post-commit effects при ошибке

- **Формулировка:** Если операция после DB commit завершается producer/serialization error, ранее committed data остаётся сохранённым.
- **Домен:** Cross-service/Consistency.
- **Приоритет:** High.
- **Источник:** Error/Event/Data Contracts.
- **Связанные Business Rules:** `BR-CONS-004`, `BR-CONS-006`.
- **Связанные Scenarios:** `SCN-SUP-001`–`003`, `SCN-WH-003`, `SCN-STOCK-001`, `SCN-ORD-001`–`003`, `SCN-STOCK-002`.
- **API/Event/Data references:** Multiple commit/publish paths.
- **Acceptance Criteria:** Error response не откатывает подтверждённый предшествующий commit.
- **QA Notes:** Всегда проверять DB side effects после 500/404 post-commit.
- **Ограничения:** Safe retry semantics не определена.

## 15. Functional requirements catalogue

Сокращения ссылок: `API-S`/`API-C` — Supplier/Customer API contracts; `ERR` — Error Contract; `EVT` — Kafka Event Contract; `DATA` — Data Contract.

| Requirement ID | Requirement | Domain | Priority | Related BR | Related Scenario | API/Event/Data reference | Acceptance summary |
|---|---|---|---|---|---|---|---|
| FR-SUP-001 | Создать валидного уникального Supplier | Supplier | High | BR-SUP-001..003 | SCN-SUP-001 | API-S POST suppliers; users | 201 и row; duplicate 409 |
| FR-SUP-002 | Вернуть список Supplier | Supplier | Medium | BR-SUP-004 | SCN-SUP-001..003 | API-S GET suppliers | items/count/order |
| FR-SUP-003 | Вернуть Supplier по ID | Supplier | Medium | BR-SUP-004 | SCN-SUP-001..003 | API-S GET supplier; ERR | 200 либо 404 |
| FR-SUP-004 | Частично изменить Supplier | Supplier | High | BR-SUP-002,003,005 | SCN-SUP-002 | API-S PUT supplier | Только переданные fields |
| FR-SUP-005 | Удалить Supplier без Product | Supplier | High | BR-SUP-006 | SCN-SUP-003 | API-S DELETE; DATA | 204 либо has-products 409 |
| FR-SUP-006 | Опубликовать Supplier event | Supplier | High | BR-SUP-007 | SCN-SUP-001..003 | EVT supplier-events | Type/payload после commit |
| FR-SUP-007 | Сохранить audit-результат Supplier event | Supplier/Audit | High | BR-AUD-001..005 | SCN-AUD-001 | EVT; DATA user_events | Event создаёт audit row |
| FR-PROD-001 | Создать Product существующего Supplier | Product | High | BR-PROD-001..004,006 | SCN-PROD-001 | API-S POST products | 201 либо validation/business error |
| FR-PROD-002 | Установить initial stock 0 | Product/Stock | High | BR-PROD-005 | SCN-PROD-001 | DATA products.stocks | DB/response stocks=0 |
| FR-PROD-003 | Вернуть Supplier Product list | Product | Medium | BR-PROD-007 | SCN-PROD-001..003 | API-S GET products | Все flags, items/count |
| FR-PROD-004 | Вернуть Supplier Product detail | Product | Medium | BR-PROD-007 | SCN-PROD-001..003 | API-S GET product | 200 либо 404 |
| FR-PROD-005 | Частично изменить Product без stock | Product | High | BR-PROD-002..004,006,009 | SCN-PROD-002 | API-S PUT product | Fields изменены, stock прежний |
| FR-PROD-006 | Архивировать Product | Product | High | BR-PROD-008 | SCN-PROD-003 | API-S DELETE; DATA | Row остаётся false/true |
| FR-PROD-007 | Запретить active+archived | Product | High | BR-PROD-006 | SCN-PROD-001,002 | ERR product_archived | 409 без state change |
| FR-PROD-008 | Опубликовать Product event | Product/Event | High | BR-PROD-010 | SCN-PROD-001..003 | EVT product-events | Correct type/payload |
| FR-PROD-009 | Upsert customer Product projection | Product/Catalog | High | BR-CAT-001,005,008 | SCN-CAT-001 | EVT; DATA customer products | Projection получает payload |
| FR-WH-001 | Создать Warehouse | Warehouse | Medium | BR-WH-001,002 | SCN-WH-001 | API-S POST warehouses | 201/row |
| FR-WH-002 | Вернуть Warehouse list | Warehouse | Medium | BR-WH-003 | SCN-WH-001..003 | API-S GET warehouses | items/count/order |
| FR-WH-003 | Вернуть Warehouse detail | Warehouse | Medium | BR-WH-003 | SCN-WH-001..003 | API-S GET warehouse | 200 либо 404 |
| FR-WH-004 | Частично изменить Warehouse | Warehouse | High | BR-WH-004 | SCN-WH-002 | API-S PUT warehouse | Metadata без stock effect |
| FR-WH-005 | Удалить Warehouse и rows | Warehouse | High | BR-WH-005 | SCN-WH-003 | API-S DELETE; DATA | Warehouse/rows отсутствуют |
| FR-WH-006 | Пересчитать stock после Warehouse delete | Warehouse/Stock | High | BR-WH-005,006; BR-STOCK-004,009 | SCN-WH-003 | EVT; DATA products | Aggregate/event обновлены |
| FR-STOCK-001 | Установить абсолютный warehouse stock | Stock | High | BR-STOCK-001..003 | SCN-STOCK-001 | API-S stock; DATA | Pair insert/replace |
| FR-STOCK-002 | Пересчитать aggregate stock | Stock | High | BR-STOCK-004 | SCN-STOCK-001,002; SCN-WH-003 | DATA | Aggregate = сумма rows |
| FR-STOCK-003 | Хранить stock нескольких Warehouse | Stock | High | BR-STOCK-005 | SCN-STOCK-001 | DATA composite PK | Несколько rows агрегируются |
| FR-STOCK-004 | Опубликовать stock event | Stock/Event | High | BR-STOCK-009 | SCN-STOCK-001,002; SCN-WH-003 | EVT stock topic | Event содержит aggregate |
| FR-STOCK-005 | Применить ORDER_CREATED идемпотентно | Stock/Order | High | BR-STOCK-010 | SCN-STOCK-002 | EVT; processed_events | UUID применяется один раз |
| FR-STOCK-006 | Выполнить partial decrease | Stock/Order | High | BR-STOCK-011; BR-CONS-007 | SCN-STOCK-002 | Supplier inventory | Списывается доступное |
| FR-STOCK-007 | Обновить customer stock projection | Stock/Catalog | High | BR-CAT-007,008 | SCN-CAT-002 | EVT; customer products | Aggregate либо snapshot fallback |
| FR-CUST-001 | Создать User | Customer | High | BR-CUST-001,002 | Customer path | API-C POST users | 201 либо duplicate 409 |
| FR-CUST-002 | Вернуть User list | Customer | Medium | BR-CUST-005 | Customer path | API-C GET users | items/count/order |
| FR-CUST-003 | Вернуть User detail | Customer | Medium | BR-CUST-005 | Customer path | API-C GET user | 200 либо 404 |
| FR-CUST-004 | Ограничить данные входным user_id | Ownership | High | BR-CUST-003,004 | SCN-FAV/CART/ORD | API-C; DATA FKs | Только строки указанного User |
| FR-CAT-001 | Прочитать customer catalog | Catalog | Medium | BR-CAT-001..004 | SCN-CAT-001 | API-C GET products | Local items/count |
| FR-CAT-002 | Прочитать projection по ID | Catalog | Medium | BR-CAT-001,003 | SCN-CAT-001 | API-C GET product | 200 либо local 404 |
| FR-CAT-003 | Upsert projection через Product events | Catalog/Event | High | BR-CAT-005 | SCN-CAT-001 | EVT product-events | Payload сохранён/no-op |
| FR-CAT-004 | Обновить projection через stock event | Catalog/Stock | High | BR-CAT-007 | SCN-CAT-002 | EVT stock-events | stocks обновлён |
| FR-CAT-005 | Выполнить snapshot sync | Catalog | High | BR-CAT-006,007 | SCN-CAT-001,002 | Supplier GET products; DATA | Upsert + stale delete attempt |
| FR-CAT-006 | Использовать eventual local state | Catalog/Consistency | High | BR-CAT-008; BR-CONS-001,005 | SCN-CAT-001,002 | EVT/DATA | До/после consumer видны разные states |
| FR-FAV-001 | Добавить Favorite | Favorite | High | BR-FAV-001,002 | SCN-FAV-001 | API-C POST favorites | 201/row либо 404 |
| FR-FAV-002 | Вернуть existing Favorite повторно | Favorite | Medium | BR-FAV-002,003 | SCN-FAV-001 | API-C; UNIQUE pair | Нет duplicate row |
| FR-FAV-003 | Прочитать Favorites | Favorite | Medium | BR-FAV-006 | SCN-FAV-001 | API-C GET favorites | items/count/current summary |
| FR-FAV-004 | Удалить Favorite | Favorite | Medium | BR-FAV-005 | SCN-FAV-001 | API-C DELETE favorite | 204 либо 404 |
| FR-FAV-005 | Разрешить недоступный Product в Favorite | Favorite | Medium | BR-FAV-004 | SCN-FAV-001 | API-C; Product flags | Archived/inactive/zero accepted |
| FR-FAV-006 | Показывать current Product summary | Favorite/Catalog | Medium | BR-FAV-006 | SCN-FAV-001 | Serializer/DATA | Summary меняется с projection |
| FR-CART-001 | Прочитать Cart и totals | Cart | High | BR-CART-009,010 | SCN-CART-001..003 | API-C GET cart | Items/count/totals |
| FR-CART-002 | Добавить CartItem | Cart | High | BR-CART-001..004 | SCN-CART-001 | API-C POST cart | 201 либо business error |
| FR-CART-003 | Increment CartItem повторным POST | Cart | High | BR-CART-005 | SCN-CART-001 | API-C POST cart | New = old + input |
| FR-CART-004 | Replace quantity PATCH | Cart | High | BR-CART-006 | SCN-CART-002 | API-C PATCH cart | Result = body quantity |
| FR-CART-005 | Удалить CartItem | Cart | Medium | BR-CART-007 | SCN-CART-003 | API-C DELETE cart | 204/delete |
| FR-CART-006 | Блокировать archived/inactive в Cart | Cart/Product | High | BR-CART-002 | SCN-CART-001,002 | ERR | Correct 409 code |
| FR-CART-007 | Проверить local stock Cart | Cart/Stock | High | BR-CART-004,008; BR-CONS-005 | SCN-CART-001,002 | Customer Product | Equal accepted, above 409 |
| FR-CART-008 | Вычислять Cart totals текущей ценой | Cart/Pricing | High | BR-CART-009 | SCN-CART-001,002 | Serializer/DATA | Price change меняет totals |
| FR-ORD-001 | Создать Order из Cart | Order | High | BR-ORD-001..004,007,008 | SCN-ORD-001 | API-C POST orders | Valid cart создаёт Order |
| FR-ORD-002 | Создать Order из body items | Order | High | BR-ORD-002,005,007,008 | SCN-ORD-002 | API-C POST orders | Items соответствуют body |
| FR-ORD-003 | Treat missing/null/empty items одинаково | Order | High | BR-ORD-003,004 | SCN-ORD-001 | Order builder | Все три выбирают Cart |
| FR-ORD-004 | Агрегировать duplicate Product | Order | High | BR-ORD-006 | SCN-ORD-002 | Order builder/DATA | Один item/event на Product |
| FR-ORD-005 | Сохранить price snapshot | Order/Pricing | High | BR-ORD-009 | SCN-ORD-001,002 | DATA orders/order_items | Stored totals неизменны |
| FR-ORD-006 | Показывать current Product summary | Order/Catalog | High | BR-ORD-010 | SCN-ORD-001..003 | Serializer/DATA | Historical/current mix |
| FR-ORD-007 | Сформировать Order number | Order | High | BR-ORD-011 | SCN-ORD-001,002 | orders.order_number | ORD + min 6-digit ID |
| FR-ORD-008 | Установить status created | Order | High | BR-ORD-011 | SCN-ORD-001,002 | orders.status | Created после commit |
| FR-ORD-009 | Очистить всю Cart | Order/Cart | High | BR-ORD-005,012 | SCN-ORD-001,002 | DATA transaction | Cart пуста после Order |
| FR-ORD-010 | Опубликовать ORDER_CREATED | Order/Event | High | BR-ORD-013 | SCN-ORD-001,002; SCN-STOCK-002 | EVT order-events | Event на unique Product |
| FR-ORD-011 | Прочитать list/detail Orders | Order | Medium | BR-CUST-004; BR-ORD-010 | SCN-ORD-001..003 | API-C GET orders | Ownership/order/current summary |
| FR-ORD-012 | Отменить Order | Order | High | BR-ORD-015 | SCN-ORD-003 | API-C cancel; orders.status | First 200, repeat 409 |
| FR-ORD-013 | Не компенсировать stock/cart при cancel | Order/Consistency | High | BR-ORD-016 | SCN-ORD-003 | Cancel/Data | Меняется только status |
| FR-ORD-014 | Не публиковать cancel event | Order/Event | High | BR-ORD-017 | SCN-ORD-003 | Kafka producer paths | В topics нет cancel event |
| FR-ORD-015 | Не ожидать supplier confirmation для 201 | Order/Consistency | High | BR-ORD-014; BR-CONS-007 | SCN-ORD-001,002; SCN-STOCK-002 | API-C/EVT | 201 до supplier result |
| FR-AUD-001 | Потреблять supplier-events | Audit | High | BR-AUD-001 | SCN-AUD-001 | EVT consumer group | Retry/commit valid event |
| FR-AUD-002 | Сохранить audit payload | Audit/Data | High | BR-AUD-002,003,005 | SCN-AUD-001 | DATA user_events | Row соответствует event |
| FR-AUD-003 | Сохранять duplicate audit delivery | Audit | Medium | BR-AUD-004,006 | SCN-AUD-001 | DATA no unique | Две обработки — две rows |
| FR-CONS-001 | Синхронизировать Product/stock через Kafka | Cross-service | High | BR-CONS-001; BR-CAT-005,007 | SCN-CAT-001,002 | EVT/DATA | Projection получает delivered state |
| FR-CONS-002 | Выполнять manual consumer commit/retry | Cross-service | High | BR-AUD-001 | SCN-CAT-001,002; SCN-STOCK-002; SCN-AUD-001 | EVT consumers | Commit после handler, 3 retry |
| FR-CONS-003 | Публиковать после DB commit | Cross-service | High | BR-CONS-002..004 | Kafka-producing SCN | EVT/ERR | State exists до publish |
| FR-CONS-004 | Сохранять committed effect при поздней ошибке | Cross-service | High | BR-CONS-004,006 | Post-commit SCN | ERR/EVT/DATA | Error не откатывает prior commit |

## 16. Open questions

Следующие темы не стали функциональными требованиями, поскольку текущая реализация не определяет желаемый ответ:

1. Что является подтверждением Order и нужен ли отдельный confirmation/rejection result?
2. Каков полный lifecycle Order и набор допустимых status/transitions?
3. Должен ли cancel возвращать stock, восстанавливать Cart или создавать compensation?
4. Кто является организационным владельцем audit data и нужна ли другая граница хранения?
5. Каков допустимый SLA lag customer projection и какие ordering guarantees ожидаются?
6. Должны ли Favorite запрещать archived/inactive Product? AS IS они разрешены.
7. Как клиент безопасно повторяет request после post-commit producer failure?
8. Как отражать partial supplier stock decrease в Customer Order?
9. Какие отличия должны появиться в future 2.0, из AS IS источников не определяется.
