# Business Rules Specification AS IS

## 1. Назначение документа

Документ централизованно фиксирует бизнес-правила, фактически реализованные в Marketplace-QA. Правила собраны из кода, README и AS IS контрактов API, ошибок, событий и данных.

Документ не задаёт будущие требования, не оценивает корректность правил и не предлагает изменения. Rule ID служат только для трассировки текущего поведения.

## 2. Принципы фиксации правил

- Правило включается только при прямом подтверждении кодом или ранее проверенной AS IS документацией.
- Open questions не преобразуются в правила или требования.
- Бизнес-правила отделяются от технических ограничений; ограничения указываются, когда они меняют наблюдаемое поведение правила.
- Если правило реализовано частично, документ фиксирует именно фактическую границу реализации.
- Слова «должен», «нельзя» и аналогичные внутри формулировок означают текущую проверку кода, а не целевое будущее состояние.
- QA priority в каталоге означает приоритет проверки текущего поведения: `High` — влияет на сохранённое состояние, доступность покупки, деньги или межсервисные эффекты; `Medium` — остальные подтверждённые контракты. Это классификация документа, не свойство системы.

## 3. Supplier rules

### Правила

- **BR-SUP-001.** Для создания Supplier обязательны `full_name`, `phone_number`, `email`, `birth_date`, `city`; действуют ограничения длины и формата входной схемы.
- **BR-SUP-002.** Телефон очищается от нецифровых символов; вход с начальной `7` или `8` и 11 цифрами сохраняется в формате `+7` и 10 последующих цифр.
- **BR-SUP-003.** Email и нормализованный phone уникальны в `supplier_db.users`; конфликт create/update возвращается как `supplier_conflict`.
- **BR-SUP-004.** Supplier list/get читают все существующие записи; list сортируется по ID и не имеет пагинации.
- **BR-SUP-005.** `PUT /suppliers/{id}` применяет только переданные поля, то есть фактически выполняет partial update; непереданные поля сохраняются.
- **BR-SUP-006.** Supplier можно удалить только при отсутствии любого связанного Product. Archived Product остаётся строкой Product и также блокирует удаление.
- **BR-SUP-007.** Успешные create/update/delete после DB commit инициируют соответственно `SUPPLIER_CREATED`, `SUPPLIER_UPDATED`, `SUPPLIER_DELETED`; delete физически удаляет Supplier.

### Связанные ошибки, API и события

- API: `/suppliers`, Product create/update при проверке owner.
- Ошибки: `validation_error`, `supplier_not_found`, `supplier_conflict`, `supplier_has_products`, `internal_error`.
- События: три supplier events в `supplier-events`.
- Таблицы: `supplier_db.users`, проверка `products`; audit side effect — `user_events` асинхронно.

### QA checks

- Проверить обязательность/границы полей, email и варианты phone normalization.
- Проверить unique conflicts при create/update.
- Проверить partial update, пустое тело и неизменность непереданных полей.
- Проверить delete Supplier без Product, с active Product и с archived Product.
- Сопоставить DB commit и supplier event payload.

## 4. Product rules

### Правила

- **BR-PROD-001.** Для создания Product обязательны `supplier_id`, `name`, `price`; description optional, flags имеют defaults active=true и archived=false.
- **BR-PROD-002.** Указанный Supplier должен существовать при create и при смене `supplier_id` через update.
- **BR-PROD-003.** Price во входном create/update должен быть больше нуля.
- **BR-PROD-004.** Name обрезается по краям и после trim не может быть пустым; действует максимальная длина 255.
- **BR-PROD-005.** Новый Product всегда создаётся с агрегированным `stocks=0`; stock не принимается create body.
- **BR-PROD-006.** Состояние `is_active=true` одновременно с `is_archived=true` отклоняется как `product_archived`.
- **BR-PROD-007.** Active, inactive и archived Product остаются читаемыми в Supplier list/get; flags не фильтруют чтение.
- **BR-PROD-008.** DELETE Product означает archive: устанавливает active=false и archived=true; физическая Product row и warehouse stock rows сохраняются.
- **BR-PROD-009.** PUT Product имеет partial-update semantics и не принимает/не меняет stock; total price после ответа пересчитывается из сохранённых price/stocks.
- **BR-PROD-010.** Create публикует `PRODUCT_CREATED`; update и archive публикуют `PRODUCT_UPDATED` после commit. Supplier-service не публикует `PRODUCT_DELETED`.

### Связанные ошибки, API и события

- API: `/products`, stock endpoint косвенно меняет aggregate.
- Ошибки: `validation_error`, `supplier_not_found`, `product_not_found`, `product_archived`, `internal_error`.
- События: `PRODUCT_CREATED`, `PRODUCT_UPDATED`.
- Таблицы: `supplier_db.products`, `users`, `warehouse_products`.

### QA checks

- Проверить Supplier existence, price boundaries и name trim.
- Проверить defaults и переходы всех combinations flags.
- Проверить initial stock, невозможность stock update через Product PUT.
- Проверить чтение inactive/archived и archive без физического удаления.
- Проверить тип/payload события для create, update и delete.

## 5. Warehouse rules

### Правила

- **BR-WH-001.** Для создания Warehouse обязательны name, weekday_hours и address с ограничениями длины.
- **BR-WH-002.** Уникальность Warehouse name/address не проверяется ни входной схемой, ни DB constraint.
- **BR-WH-003.** List/get возвращают существующие Warehouse без stock details; list сортируется по ID.
- **BR-WH-004.** PUT Warehouse применяет только переданные описательные поля и не меняет warehouse/product stock.
- **BR-WH-005.** DELETE Warehouse физически удаляет его `warehouse_products`, затем Warehouse, и пересчитывает `Product.stocks` затронутых Product по оставшимся складам.
- **BR-WH-006.** Warehouse create/update не публикуют events; delete публикует `STOCK_REPLENISHED` на каждый затронутый Product после пересчёта, но собственного Warehouse event нет.

### Связанные ошибки, API и события

- API: `/warehouses`, `/warehouses/{id}/stocks`.
- Ошибки: `validation_error`, `warehouse_not_found`, `internal_error`.
- События: только stock events как side effect delete.
- Таблицы: `warehouses`, `warehouse_products`, `products`.

### QA checks

- Проверить field boundaries и duplicate name/address.
- Проверить partial update без stock effect.
- Удалить пустой Warehouse и Warehouse с Product на одном/нескольких складах.
- Проверить удаление rows, aggregate и количество stock events.

## 6. Stock rules

### Правила

- **BR-STOCK-001.** Stock endpoint устанавливает абсолютное значение пары Warehouse–Product, а не прибавляет delta.
- **BR-STOCK-002.** Входной `stocks` должен быть integer `>=0`; request `items` должен быть непустым.
- **BR-STOCK-003.** Warehouse проверяется до items; каждый Product должен существовать на момент своей обработки.
- **BR-STOCK-004.** `Product.stocks` хранится как сумма всех `warehouse_products.stocks` данного Product.
- **BR-STOCK-005.** Один Product может иметь отдельные stock rows на нескольких Warehouse; пара Warehouse–Product уникальна составным PK.
- **BR-STOCK-006.** Duplicate product ID в одном stock request обрабатывается последовательно; последнее значение остаётся в DB, но response snapshot/event формируются только при первом вхождении.
- **BR-STOCK-007.** Multi-item stock request не атомарен: для каждого item выполняются отдельные commit upsert и aggregate recalculation; ошибка позднего item не откатывает предыдущие.
- **BR-STOCK-008.** Restock inactive/archived Product допускается: flags не проверяются.
- **BR-STOCK-009.** `STOCK_REPLENISHED` публикуется после пересчёта и используется как при увеличении, так и при уменьшении aggregate, включая delete Warehouse.
- **BR-STOCK-010.** Supplier order consumer списывает stock последовательно по возрастанию `warehouse_id`.
- **BR-STOCK-011.** Если ordered quantity выше фактического supplier stock, consumer списывает всё доступное, не фиксирует remainder как ошибку и не меняет Customer Order status.

### Связанные ошибки, API и события

- API: `POST /warehouses/{warehouse_id}/stocks`; Customer cart/order проверяют только local aggregate.
- Ошибки: `validation_error`, `warehouse_not_found`, `product_not_found`, `insufficient_stock` только на customer side.
- События: `STOCK_REPLENISHED`, `ORDER_CREATED`, `STOCK_DECREASED_BY_ORDER`.
- Таблицы: `warehouse_products`, supplier/customer `products`, `processed_events`.

### QA checks

- Проверить absolute replace, zero/negative stock и несколько Warehouse.
- Проверить duplicate Product и расхождение first response/event с final DB.
- Поместить invalid Product после valid item и проверить partial commit.
- Restock archived/inactive Product.
- Проверить order decrease по Warehouse ID, достаточный/недостаточный/нулевой stock.

## 7. Customer/User rules

### Правила

- **BR-CUST-001.** Для создания User обязательны `full_name` длиной 3–255 и валидный email.
- **BR-CUST-002.** Email User уникален; duplicate create возвращает `user_conflict`.
- **BR-CUST-003.** Auth/roles отсутствуют; API не связывает фактического caller с User.
- **BR-CUST-004.** Входной `user_id` определяет ownership favorites, cart и orders; чужой Order скрывается как `order_not_found`.
- **BR-CUST-005.** Customer API предоставляет create/list/get User; update/delete User отсутствуют.

### Связанные ошибки, API и события

- API: `/users`; все favorites/cart/orders используют User validation.
- Ошибки: `validation_error`, `user_not_found`, `user_conflict`.
- Kafka events для User отсутствуют.
- Таблица: `customer_db.users`.

### QA checks

- Проверить required/length/email/duplicate.
- Проверить list/get и unknown User.
- Проверить ownership разных User для favorites/cart/orders.
- Подтвердить отсутствие update/delete endpoints и auth check.

## 8. Catalog projection rules

### Правила

- **BR-CAT-001.** Customer Product — локальная проекция Supplier Product с тем же Product ID.
- **BR-CAT-002.** Customer-service не является Source of Truth для Product/price/flags/stock; source — supplier-service/`supplier_db`.
- **BR-CAT-003.** Customer catalog list/get возвращает active, inactive и archived Product без фильтрации.
- **BR-CAT-004.** `supplier_id` и входной Product `total_price` не сохраняются в customer projection; total price вычисляется локально.
- **BR-CAT-005.** `PRODUCT_CREATED`/`PRODUCT_UPDATED` выполняют upsert projection; consumer поддерживает `PRODUCT_DELETED`, хотя current supplier его не публикует.
- **BR-CAT-006.** На startup customer-service делает HTTP snapshot sync всего supplier catalog и пытается удалить stale local Product.
- **BR-CAT-007.** Stock event обновляет local aggregate; при missing Product или stock no-op запускается snapshot fallback и повторный update.
- **BR-CAT-008.** Projection согласуется eventual и может быть stale; revision/source timestamp для упорядочивания состояния не используется.

### Связанные API и события

- API: Supplier/Customer `GET /products`; customer catalog read-only.
- Ошибка: `product_not_found` означает отсутствие local projection для Customer API.
- События: Product/stock events.
- Таблица: `customer_db.products`.

### QA checks

- Сопоставить supplier state и customer projection до/после event lag.
- Проверить inactive/archived reads и отсутствие supplier ID.
- Проверить startup sync, unknown Product stock fallback и no-op fallback.
- Проверить stale deletion при FK из favorite/cart/order item.

## 9. Favorite rules

### Правила

- **BR-FAV-001.** Для добавления Favorite должны существовать User и local Product.
- **BR-FAV-002.** Пара user–product уникальна в `favorites`.
- **BR-FAV-003.** Repeated POST возвращает существующую Favorite с тем же HTTP 201 и не создаёт duplicate row.
- **BR-FAV-004.** Archived, inactive и zero-stock Product можно добавить в Favorite; flags/stock не проверяются.
- **BR-FAV-005.** DELETE удаляет Favorite по `user_id` + `product_id`; отсутствие Product отдельно не проверяется.
- **BR-FAV-006.** Product summary в Favorite response строится из текущей local Product и не является snapshot момента добавления.

### Связанные ошибки, API и таблицы

- API: `/favorites`.
- Ошибки: `validation_error`, `user_not_found`, `product_not_found`, `favorite_not_found`.
- Kafka events отсутствуют.
- Таблицы: `favorites`, `users`, `products`.

### QA checks

- Добавить active/inactive/archived/zero-stock Product.
- Повторить POST и проверить один row/тот же ID.
- Проверить list/delete, unknown User/Product и repeated delete.
- Изменить Product и проверить current summary.

## 10. Cart rules

### Правила

- **BR-CART-001.** Для cart create/update должны существовать User и local Product.
- **BR-CART-002.** Product должен быть not archived и active; archived check выполняется раньше inactive.
- **BR-CART-003.** Quantity во входном body должна быть integer `>0`.
- **BR-CART-004.** Итоговая quantity не может превышать `stocks` local Product projection.
- **BR-CART-005.** Repeated POST для существующей pair увеличивает quantity на входное значение.
- **BR-CART-006.** PATCH заменяет quantity целиком, а не добавляет.
- **BR-CART-007.** DELETE физически удаляет CartItem pair и не проверяет Product flags/existence.
- **BR-CART-008.** Добавление/изменение cart не резервирует и не уменьшает supplier/customer stock.
- **BR-CART-009.** Цена в CartItem не хранится; unit/line/cart totals рассчитываются по текущей local Product при чтении.
- **BR-CART-010.** Cart read не блокирует/не скрывает item после archive/inactive и не перепроверяет quantity против изменившегося stock.

### Связанные ошибки, API и таблицы

- API: `/cart`.
- Ошибки: `validation_error`, `user_not_found`, `product_not_found`, `product_archived`, `product_inactive`, `insufficient_stock`, `cart_item_not_found`.
- Kafka events отсутствуют.
- Таблицы: `cart_items`, `users`, `products`.

### QA checks

- Проверить POST add/increment, PATCH replace, DELETE/repeated delete.
- Проверить quantity 0, boundary/equal/above local stock.
- Проверить archived/inactive/both flags и порядок error.
- Изменить Product price/stock/flags после add и проверить cart read.
- Сравнить local stock с supplier stock при lag.

## 11. Order rules

### Правила

- **BR-ORD-001.** User должен существовать до построения Order.
- **BR-ORD-002.** Непустые body `items` используются как источник Order; иначе используются CartItem User.
- **BR-ORD-003.** Missing, `null` и пустой `items` обрабатываются одинаково как выбор cart.
- **BR-ORD-004.** Если выбрана cart и она пуста, возвращается `cart_is_empty`, Order не создаётся.
- **BR-ORD-005.** Непустые body items имеют приоритет над cart, но успешный Order всё равно очищает всю cart User.
- **BR-ORD-006.** Duplicate `product_id` в body items агрегируются суммированием quantity до проверки и insert.
- **BR-ORD-007.** Каждый local Product должен существовать, быть not archived и active.
- **BR-ORD-008.** Aggregated requested quantity каждого Product должна быть `<=` local Product.stocks.
- **BR-ORD-009.** OrderItem сохраняет quantity, текущий local `unit_price` и line `total_price`; Order сохраняет сумму line totals.
- **BR-ORD-010.** Product summary в Order response берётся из текущей projection; historical prices могут отличаться от `product.price` summary.
- **BR-ORD-011.** Новый Order получает status `created` и number `ORD-` + decimal ID с минимальной шириной шесть цифр (`ORD-000001`; большие ID не обрезаются).
- **BR-ORD-012.** Order, OrderItems и полная очистка cart коммитятся до публикации Kafka.
- **BR-ORD-013.** После commit публикуется отдельный `ORDER_CREATED` на каждый уникальный Product; event не содержит Order ID/number/User/price.
- **BR-ORD-014.** Supplier-service не подтверждает Order; HTTP 201 не означает, что supplier stock уже списан полностью или вообще списан.
- **BR-ORD-015.** Cancel принадлежащего User Order меняет status на `cancelled`; повторный cancel возвращает `order_already_cancelled`.
- **BR-ORD-016.** Cancel не возвращает stock, не восстанавливает cart и не изменяет OrderItems/totals.
- **BR-ORD-017.** Cancel не публикует Kafka event; supplier-service о нём не уведомляется.

### Связанные ошибки, API и события

- API: `/orders`, `/orders/{id}/cancel`.
- Ошибки: `validation_error`, `user_not_found`, `product_not_found`, `cart_is_empty`, `product_archived`, `product_inactive`, `insufficient_stock`, `order_not_found`, `order_already_cancelled`, `internal_error`.
- События: `ORDER_CREATED`; downstream `STOCK_DECREASED_BY_ORDER` не является confirmation конкретного Order.
- Таблицы: `orders`, `order_items`, `cart_items`, `products`, supplier inventory/processed events асинхронно.

### QA checks

- Создать Order из body/cart/missing/null/empty items.
- Проверить body priority, duplicate aggregation и полную cart cleanup.
- Проверить product flags, local stock boundary и stale projection.
- Сверить stored prices/totals с current Product summary после изменения price.
- Проверить number/status и по одному event на unique Product.
- Проверить HTTP 201 до stock update, partial supplier decrease, cancel/repeated cancel и отсутствие cancel event/stock return.

## 12. Audit rules

### Правила

- **BR-AUD-001.** Audit-consumer подписан на `supplier-events` и работает отдельным процессом.
- **BR-AUD-002.** Каждое успешно обработанное supplier event создаёт row `supplier_db.user_events` с type, supplier ID, полным payload и received timestamp.
- **BR-AUD-003.** Audit writer использует `supplier_db`; отдельной audit DB нет.
- **BR-AUD-004.** Supplier events/audit rows не имеют event ID и deduplication; повторная обработка создаёт duplicate row.
- **BR-AUD-005.** `user_events.user_id` не является FK на supplier `users`; audit delete payload сохраняется после удаления Supplier.
- **BR-AUD-006.** HTTP API чтения, изменения или удаления audit rows отсутствует.

### Связанные events/tables

- Events: `SUPPLIER_CREATED`, `SUPPLIER_UPDATED`, `SUPPLIER_DELETED`.
- Таблица: `supplier_db.user_events`.
- Consumer group: `audit-supplier-events-consumer-group`.

### QA checks

- Сопоставить каждый supplier event с audit row/payload.
- Проверить delete Supplier и сохранность audit row.
- Повторить событие/offset commit failure и проверить duplicate.
- Подтвердить отсутствие FK и audit HTTP API.

## 13. Cross-service consistency rules

### Правила и фактические ограничения

- **BR-CONS-001.** `supplier_db` является Source of Truth Supplier/Product/Warehouse/stock; `customer_db.products` согласуется с ним eventual.
- **BR-CONS-002.** Между `supplier_db`, Kafka и `customer_db` нет distributed transaction.
- **BR-CONS-003.** Outbox отсутствует; producer вызывается после DB commit.
- **BR-CONS-004.** Producer failure после commit может вернуть HTTP 500 при сохранённых данных; для Order cart уже может быть очищена.
- **BR-CONS-005.** Cart/Order availability checks используют потенциально stale local projection.
- **BR-CONS-006.** Multi-commit flows допускают partial side effects: stock request, delete Warehouse recalculations и multi-event Order publication.
- **BR-CONS-007.** По текущим данным нельзя полностью восстановить fulfillment конкретного Order: supplier не хранит Order ID/status/result, а partial decrease не возвращает confirmation.

### QA checks

- Наблюдать lag каждого product/stock flow.
- Инъецировать producer failure и проверить post-commit DB state.
- Проверить partial commit stock request и частичную публикацию multi-product Order.
- Сравнить customer validation со supplier stock при рассинхроне.
- Проверить невозможность связать ProcessedEvent с Order entity по данным supplier DB.

## 14. Rule catalogue table

Источники сокращены: `S-API` — Supplier API contract; `C-API` — Customer API contract; `EVT` — Kafka event contract; `DATA` — Data contract; имена таблиц указывают хранилище. Priority относится только к QA-проверке AS IS.

| Rule ID | Rule | Domain | Source/API/Event/Table | Error code if applicable | QA priority |
|---|---|---|---|---|---|
| BR-SUP-001 | Create Supplier требует пять обязательных полей и валидные форматы | Supplier | S-API `/suppliers`; `supplier_db.users` | `validation_error` | High |
| BR-SUP-002 | Phone нормализуется в `+7XXXXXXXXXX` | Supplier | S-API schemas; `users.phone_number` | `validation_error` | High |
| BR-SUP-003 | Supplier email и phone уникальны | Supplier | DATA UNIQUE; POST/PUT suppliers | `supplier_conflict` | High |
| BR-SUP-004 | Supplier читаются все, list идёт по ID | Supplier | GET suppliers | — | Medium |
| BR-SUP-005 | Supplier PUT обновляет только переданные поля | Supplier | PUT supplier | `validation_error` | Medium |
| BR-SUP-006 | Supplier с любым Product не удаляется | Supplier | DELETE supplier; `products` | `supplier_has_products` | High |
| BR-SUP-007 | Supplier mutations публикуют соответствующие events после commit | Supplier | `supplier-events`; EVT | `internal_error` | High |
| BR-PROD-001 | Product create требует supplier ID, name, price; flags default | Product | POST products | `validation_error` | High |
| BR-PROD-002 | Supplier owner Product должен существовать | Product | POST/PUT products; `users` | `supplier_not_found` | High |
| BR-PROD-003 | Input Product price > 0 | Product | Product schemas | `validation_error` | High |
| BR-PROD-004 | Product name trim и non-empty | Product | Product schemas | `validation_error` | Medium |
| BR-PROD-005 | Новый Product имеет stocks=0 | Product | POST products; `products` | — | High |
| BR-PROD-006 | Active+archived Product запрещён | Product | POST/PUT products | `product_archived` | High |
| BR-PROD-007 | Все состояния Product остаются читаемыми | Product | GET products | — | Medium |
| BR-PROD-008 | DELETE архивирует, не удаляет Product | Product | DELETE product; `products` | `product_not_found` | High |
| BR-PROD-009 | Product PUT не меняет stock | Product | PUT product | — | High |
| BR-PROD-010 | Create/update/archive публикуют Product events | Product | `product-events`; EVT | `internal_error` | High |
| BR-WH-001 | Warehouse create требует name/hours/address | Warehouse | POST warehouses | `validation_error` | Medium |
| BR-WH-002 | Warehouse name/address не уникальны | Warehouse | DATA `warehouses` | — | Medium |
| BR-WH-003 | Warehouse reads не включают stock details | Warehouse | GET warehouses | `warehouse_not_found` | Medium |
| BR-WH-004 | Warehouse PUT не меняет stock | Warehouse | PUT warehouse | `warehouse_not_found` | High |
| BR-WH-005 | Delete Warehouse удаляет rows и пересчитывает aggregates | Warehouse | DELETE warehouse; DATA | `warehouse_not_found` | High |
| BR-WH-006 | Warehouse events отсутствуют; delete даёт stock events | Warehouse | EVT stock topic | `internal_error` | High |
| BR-STOCK-001 | Stock input — абсолютное значение | Stock | POST warehouse stocks | — | High |
| BR-STOCK-002 | Input stocks >=0 и items непустой | Stock | Stock schemas | `validation_error` | High |
| BR-STOCK-003 | Warehouse и каждый Product существуют | Stock | Stock endpoint | `warehouse_not_found`, `product_not_found` | High |
| BR-STOCK-004 | Aggregate Product.stocks = сумма warehouse rows | Stock | `products`, `warehouse_products` | — | High |
| BR-STOCK-005 | Product может находиться на нескольких Warehouse | Stock | Composite PK; DATA | — | High |
| BR-STOCK-006 | Duplicate Product request: final DB последнее, response/event первое | Stock | Stock endpoint | — | High |
| BR-STOCK-007 | Multi-item stock request не атомарен | Stock | Multiple commits | `internal_error` | High |
| BR-STOCK-008 | Restock inactive/archived допускается | Stock | Stock endpoint | — | Medium |
| BR-STOCK-009 | STOCK_REPLENISHED используется при росте/снижении | Stock | `product-stock-events` | — | High |
| BR-STOCK-010 | Order stock списывается по Warehouse ID | Stock | Supplier order consumer | — | High |
| BR-STOCK-011 | Недостаток supplier stock приводит к partial decrease | Stock | Order consumer | — | High |
| BR-CUST-001 | User create требует name/email | Customer | POST users | `validation_error` | Medium |
| BR-CUST-002 | User email уникален | Customer | `customer_db.users` | `user_conflict` | High |
| BR-CUST-003 | Auth и roles отсутствуют | Customer | C-API/system context | — | High |
| BR-CUST-004 | Input user ID задаёт ownership | Customer | Favorites/cart/orders | `user_not_found`, `order_not_found` | High |
| BR-CUST-005 | User доступен только create/list/get | Customer | Users API | — | Medium |
| BR-CAT-001 | Customer Product — projection по тому же ID | Catalog | `customer_db.products`; EVT | `product_not_found` | High |
| BR-CAT-002 | Source of Truth Product — supplier-service | Catalog | DATA ownership | — | High |
| BR-CAT-003 | Catalog читает active/inactive/archived | Catalog | GET products | — | Medium |
| BR-CAT-004 | Supplier ID не хранится; total price вычисляется | Catalog | Customer Product model | — | Medium |
| BR-CAT-005 | Product events upsert; delete type только поддерживается | Catalog | `product-events` | — | High |
| BR-CAT-006 | Startup выполняет HTTP snapshot sync | Catalog | Customer startup | — | High |
| BR-CAT-007 | Missing/no-op stock запускает snapshot fallback | Catalog | Stock consumer | — | High |
| BR-CAT-008 | Projection eventual и без revision guard | Catalog | EVT/DATA | — | High |
| BR-FAV-001 | Favorite требует существующих User/Product | Favorite | POST favorites | `user_not_found`, `product_not_found` | High |
| BR-FAV-002 | Favorite pair уникальна | Favorite | UNIQUE constraint | — | High |
| BR-FAV-003 | Repeated POST возвращает existing row с 201 | Favorite | POST favorites | — | Medium |
| BR-FAV-004 | Flags/zero stock не блокируют Favorite | Favorite | POST favorites | — | Medium |
| BR-FAV-005 | Favorite удаляется по user/product pair | Favorite | DELETE favorite | `favorite_not_found` | Medium |
| BR-FAV-006 | Favorite Product summary текущий | Favorite | Serializer; `products` | `product_not_found` | Medium |
| BR-CART-001 | Cart mutation требует User/Product | Cart | POST/PATCH cart | `user_not_found`, `product_not_found` | High |
| BR-CART-002 | Cart блокирует archived/inactive Product | Cart | POST/PATCH cart | `product_archived`, `product_inactive` | High |
| BR-CART-003 | Cart quantity >0 | Cart | Cart schemas | `validation_error` | High |
| BR-CART-004 | Итоговая quantity <= local stock | Cart | Cart business check | `insufficient_stock` | High |
| BR-CART-005 | Repeated POST прибавляет quantity | Cart | POST cart | — | High |
| BR-CART-006 | PATCH заменяет quantity | Cart | PATCH cart | `cart_item_not_found` | High |
| BR-CART-007 | DELETE удаляет pair без Product check | Cart | DELETE cart | `cart_item_not_found` | Medium |
| BR-CART-008 | Cart не резервирует stock | Cart | C-API/DATA | — | High |
| BR-CART-009 | Cart prices/totals вычисляются текущими | Cart | Serializer | `product_not_found` | High |
| BR-CART-010 | Cart read не перепроверяет flags/stock | Cart | GET cart | — | Medium |
| BR-ORD-001 | Order требует существующего User | Order | POST orders | `user_not_found` | High |
| BR-ORD-002 | Непустые body items иначе cart | Order | Order builder | — | High |
| BR-ORD-003 | Missing/null/empty items одинаковы | Order | Order builder | — | High |
| BR-ORD-004 | Пустая выбранная cart отклоняется | Order | POST orders | `cart_is_empty` | High |
| BR-ORD-005 | Body items приоритетны, но вся cart очищается | Order | POST orders | — | High |
| BR-ORD-006 | Duplicate Product агрегируются | Order | Order aggregation | — | High |
| BR-ORD-007 | Order Product существует/not archived/active | Order | Availability checks | `product_not_found`, `product_archived`, `product_inactive` | High |
| BR-ORD-008 | Order quantity <= local stock | Order | Availability check | `insufficient_stock` | High |
| BR-ORD-009 | OrderItem/Order сохраняют price totals | Order | `order_items`, `orders` | — | High |
| BR-ORD-010 | Product summary в Order текущий | Order | Order serializer | `product_not_found` | High |
| BR-ORD-011 | Initial status created, number ORD + ID шириной минимум 6 | Order | POST orders | — | High |
| BR-ORD-012 | Order/items/cart cleanup commit вместе до Kafka | Order | DB transaction | `internal_error` | High |
| BR-ORD-013 | ORDER_CREATED по unique Product после commit | Order | `order-events` | `internal_error` | High |
| BR-ORD-014 | HTTP 201 не подтверждает supplier decrease | Order | EVT/consistency | — | High |
| BR-ORD-015 | Cancel ставит cancelled, повторный запрещён | Order | Cancel API | `order_already_cancelled` | High |
| BR-ORD-016 | Cancel не возвращает stock/cart | Order | Cancel API/DATA | — | High |
| BR-ORD-017 | Cancel не публикует event | Order | Kafka producer paths | — | High |
| BR-AUD-001 | Audit consumer слушает supplier-events | Audit | EVT | — | Medium |
| BR-AUD-002 | Supplier payload сохраняется в user_events | Audit | `supplier_db.user_events` | — | High |
| BR-AUD-003 | Audit rows хранятся в supplier_db | Audit | Compose/DATA | — | Medium |
| BR-AUD-004 | Audit не имеет event-ID deduplication | Audit | EVT/DATA | — | High |
| BR-AUD-005 | Audit supplier ID не FK | Audit | DATA `user_events` | — | Medium |
| BR-AUD-006 | Audit read API отсутствует | Audit | API inventory | — | Medium |
| BR-CONS-001 | Supplier source и customer projection согласуются eventual | Consistency | DATA/EVT | — | High |
| BR-CONS-002 | Distributed transaction отсутствует | Consistency | Architecture | — | High |
| BR-CONS-003 | Outbox отсутствует; publish после commit | Consistency | Producer paths | — | High |
| BR-CONS-004 | Producer failure возможен после persisted side effects | Consistency | Error/EVT contracts | `internal_error` | High |
| BR-CONS-005 | Customer availability checks используют stale projection | Consistency | Cart/Order rules | `insufficient_stock` и flags | High |
| BR-CONS-006 | Multi-commit flows допускают partial effects | Consistency | Stock/Warehouse/Order flows | `internal_error` | High |
| BR-CONS-007 | Fulfillment конкретного Order полностью не реконструируется | Consistency | Order event/DATA | — | High |

## 15. Open questions

Следующие вопросы не являются правилами текущей реализации:

1. Каков полный жизненный цикл Order и существуют ли предметные status кроме `created`/`cancelled`?
2. Что считается подтверждением Order: customer DB commit, публикация Kafka или supplier inventory effect?
3. Должна ли отмена возвращать stock и/или восстанавливать cart?
4. Как отражать partial supplier stock decrease и неуспешное fulfillment?
5. Должны ли Favorite блокировать archived/inactive Product? Сейчас не блокируют.
6. Нужна ли история warehouse stock и причины изменения?
7. Нужна ли отдельная audit DB и каков lifecycle audit rows?
8. Должен ли Order хранить полный historical Product snapshot?
9. Какова допустимая задержка/рассинхронизация projection?
10. Должны ли PUT Supplier/Product/Warehouse оставаться partial updates?
11. Как интерпретировать повтор запроса после post-commit producer failure?
12. Какие правила и constraints должны применяться в future 2.0, текущая реализация не определяет.
