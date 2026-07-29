# Error Contract Specification AS IS

## 1. Назначение документа

Документ централизованно описывает фактический контракт ошибок двух HTTP API Marketplace-QA: `supplier-service` и `customer-service`. Он основан на зарегистрированных обработчиках исключений, местах формирования ошибок и API Contract Specification в этом каталоге.

Здесь фиксируется текущее поведение AS IS. Документ не вводит новые коды, не исправляет реализацию и не задаёт будущий контракт.

## 2. Общий формат ошибки

Оба сервиса возвращают контролируемые ошибки в JSON envelope:

```json
{
  "error": {
    "code": "...",
    "message": "..."
  }
}
```

- `error.code` — строковый машиночитаемый код категории ошибки.
- `error.message` — строковое человекочитаемое сообщение.
- HTTP status определяется типом и местом возникновения ошибки; одинаковый envelope используется для 400, 404, 409 и 500.
- Успешные ответы HTTP 204 не содержат body и не используют error envelope.
- В error response нет stack trace, имени класса исключения, SQL или внутреннего payload Kafka.

## 3. Общие обработчики ошибок

### 3.1. `validation_error`

Оба API перехватывают FastAPI `RequestValidationError` и Pydantic `ValidationError`. Ответ имеет HTTP 400 и `error.code=validation_error`. Сообщения всех найденных нарушений объединяются через `; `. Для каждого нарушения указывается location без префикса `body`, затем исходный Pydantic message.

Это отличается от стандартного FastAPI response с HTTP 422: текущие сервисы явно преобразуют request validation в HTTP 400 и собственный envelope.

### 3.2. `internal_error`

Любое исключение, не обработанное более специализированным handler, логируется вместе со stack trace на стороне сервиса и возвращается клиенту как HTTP 500:

```json
{
  "error": {
    "code": "internal_error",
    "message": "Internal server error"
  }
}
```

Внутренняя причина клиенту не раскрывается.

### 3.3. Wrapper для `HTTPException`

Оба API перехватывают FastAPI `HTTPException`, сохраняя заданный HTTP status и нормализуя `detail`:

- string detail становится `error.code`; message берётся из локального словаря либо строится из кода заменой `_` на пробел;
- dictionary detail может задать `code` и `message`; если `code` отсутствует, fallback — `business_error`;
- detail другого типа даёт `business_error` / `Business rule conflict`.

Текущие endpoint используют string detail из каталогов ниже. `business_error` является fallback обработчика, но подтверждённого endpoint-сценария, который его возвращает, в текущем коде нет; поэтому он не включён в доменные каталоги как фактически используемый код.

### 3.4. Различия сервисов

- Supplier-service оставляет сообщения Pydantic без предметной подмены.
- Customer-service заменяет любое Pydantic message, содержащее текст `greater than 0`, на `Quantity must be greater than zero`.
- Такая подмена действует не только для `quantity`, но также для `user_id` и `product_id` с ограничением `>0`.
- При этом `error.code` остаётся `validation_error`. Строка `invalid_quantity` присутствует в словаре сообщений customer-service, но не возвращается текущим обработчиком как самостоятельный `error.code`.

## 4. Supplier API error catalogue

| Error code | HTTP status | Где возникает | Причина | Пример сценария | Затрагиваемые endpoint |
|---|---:|---|---|---|---|
| `validation_error` | 400 | Request/Pydantic validation handler | Неверный тип, отсутствующее обязательное поле, нарушение длины/формата/диапазона, неверный path | `price=0`, пустой `items`, нечисловой ID | Все Supplier endpoint с path/body; фактически неприменим к обычному вызову list/health без параметров |
| `internal_error` | 500 | Общий handler `Exception` | Необработанная DB, Kafka, serialization или другая runtime-ошибка | Kafka producer выбрасывает исключение после DB commit | Потенциально любой endpoint; особенно mutating endpoint с DB/Kafka |
| `supplier_not_found` | 404 | Supplier lookup | Supplier с указанным ID отсутствует | Создание Product для неизвестного supplier | `GET/PUT/DELETE /suppliers/{supplier_id}`, `POST /products`, `PUT /products/{product_id}` при смене supplier |
| `supplier_conflict` | 409 | Перехваченный `IntegrityError` при create/update Supplier | Нарушена уникальность email/phone либо другой integrity constraint в этом commit | Повторный email; явный null в NOT NULL поле при PUT также может попасть сюда | `POST /suppliers`, `PUT /suppliers/{supplier_id}` |
| `supplier_has_products` | 409 | Проверка перед удалением Supplier | Существует хотя бы один связанный Product | Удаление supplier с активным или архивным товаром | `DELETE /suppliers/{supplier_id}` |
| `product_not_found` | 404 | Product lookup | Product отсутствует | Получение неизвестного Product; stock item с неизвестным product ID | `GET/PUT/DELETE /products/{product_id}`, `POST /warehouses/{warehouse_id}/stocks` |
| `product_archived` | 409 | Проверка итогового состояния Product | `is_archived=true` одновременно с `is_active=true` | Создание active+archived Product | `POST /products`, `PUT /products/{product_id}` |
| `warehouse_not_found` | 404 | Warehouse lookup | Warehouse отсутствует | Restock неизвестного склада | `GET/PUT/DELETE /warehouses/{warehouse_id}`, `POST /warehouses/{warehouse_id}/stocks` |

Других string error codes в Supplier endpoint текущего `main.py` не обнаружено.

## 5. Customer API error catalogue

| Error code | HTTP status | Где возникает | Причина | Пример сценария | Затрагиваемые endpoint |
|---|---:|---|---|---|---|
| `validation_error` | 400 | Request/Pydantic validation handler | Неверный/missing path, query или body; нарушение `>0`, email/length/type | `quantity=0`, отсутствующий `user_id` query | Все Customer endpoint с параметрами/body |
| `internal_error` | 500 | Общий handler | Необработанная DB, Kafka, serialization или runtime-ошибка | Ошибка producer после commit Order | Потенциально любой endpoint |
| `user_not_found` | 404 | Общая проверка User | User отсутствует | Cart запрошена для неизвестного user ID | `GET /users/{user_id}`; все favorites/cart/orders endpoint |
| `user_conflict` | 409 | Перехваченный `IntegrityError` при create User | Email уже существует либо иной integrity conflict в commit | Повторный email | `POST /users` |
| `product_not_found` | 404 | Lookup локальной Product, включая сериализацию вложенного summary | Product отсутствует именно в customer projection | Supplier Product ещё не синхронизирован; связанная Product отсутствует при чтении Order | `GET /products/{product_id}`, `POST/GET /favorites`, `GET/POST/PATCH /cart`, `POST/GET orders`, cancel response serialization |
| `favorite_not_found` | 404 | Lookup Favorite pair | Связь user–product отсутствует | Повторное удаление Favorite | `DELETE /favorites/{product_id}?user_id=` |
| `cart_item_not_found` | 404 | Lookup CartItem pair | Позиция user–product отсутствует | PATCH/DELETE товара вне cart | `PATCH /cart/{product_id}`, `DELETE /cart/{product_id}?user_id=` |
| `cart_is_empty` | 400 | Сбор Order items из cart | Body items отсутствуют, `null` или `[]`, а cart пуста | `POST /orders` только с user ID и пустой cart | `POST /orders` |
| `insufficient_stock` | 409 | Сравнение requested quantity с локальным stock | Запрошенное/суммарное количество выше `Product.stocks` customer projection | Добавление 3 единиц при local stock 2 | `POST /cart`, `PATCH /cart/{product_id}`, `POST /orders` |
| `product_archived` | 409 | Проверка возможности покупки | Local Product имеет `is_archived=true` | Добавление архивного товара в cart | `POST /cart`, `PATCH /cart/{product_id}`, `POST /orders` |
| `product_inactive` | 409 | Проверка возможности покупки после archived check | Local Product имеет `is_active=false` и не был раньше отклонён как archived | Заказ неактивного товара | `POST /cart`, `PATCH /cart/{product_id}`, `POST /orders` |
| `order_not_found` | 404 | Lookup Order с фильтром ID, user ID и non-null number | Order отсутствует, принадлежит другому User или не имеет number | User запрашивает чужой Order | `GET /orders/{order_id}?user_id=`, `POST /orders/{order_id}/cancel?user_id=` |
| `order_already_cancelled` | 409 | Проверка status перед cancel | Status уже равен `cancelled` | Повторная отмена | `POST /orders/{order_id}/cancel?user_id=` |

`invalid_quantity` не является отдельным фактически возвращаемым кодом: его значение используется только как replacement message внутри `validation_error`.

## 6. Validation behavior

### 6.1. Какие ошибки становятся `validation_error`

- отсутствующие required body fields;
- отсутствующие required query parameters;
- path/query/body значения неверного типа;
- невалидный JSON/body shape;
- invalid email;
- нарушение min/max length;
- нарушение числовых `gt`/`ge` constraints;
- пустой stock `items`, где задана минимальная длина;
- whitespace-only Product name после пользовательского validator;
- Pydantic `ValidationError`, возникший при построении response/helper model и дошедший до handler.

### 6.2. Required fields

Если обязательное поле отсутствует, операция не входит в endpoint body и возвращает HTTP 400 `validation_error`. Message содержит location поля и Pydantic-текст ошибки. Точная формулировка зависит от Pydantic.

### 6.3. Path, query и body

- Нечисловой integer path/query преобразуется в HTTP 400, не 404.
- Отсутствующий обязательный query parameter даёт HTTP 400.
- Integer path/query без явного `gt=0` принимает ноль/отрицательное значение как синтаксически валидное, после чего lookup обычно даёт доменный 404.
- Body IDs/quantities с `gt=0` отклоняются validation handler до бизнес-логики.

### 6.4. Особенность customer quantity message

Customer-service проверяет raw Pydantic message на подстроку `greater than 0`. При совпадении message становится `Quantity must be greater than zero`. Поэтому возможны ответы вида `user_id: Quantity must be greater than zero` или `product_id: Quantity must be greater than zero`. Код при этом всегда `validation_error`, а не `invalid_quantity`.

### 6.5. Отличие от business errors

Validation error означает, что вход не прошёл структурную/декларативную проверку или построение Pydantic-модели. Business errors формируются явным `HTTPException` после начала endpoint-логики: сущность не найдена, состояние конфликтует, cart пуста или stock недостаточен. Business error сохраняет назначенный endpoint HTTP status и предметный code.

## 7. Conflict behavior

Все перечисленные ниже коды фактически возвращаются с HTTP 409 в указанных путях:

| Code | Условие 409 | Особенность |
|---|---|---|
| `supplier_conflict` | `IntegrityError` при commit create/update Supplier | Может объединять уникальность и иной integrity conflict, а message всегда говорит об email/phone |
| `supplier_has_products` | У Supplier найден связанный Product | Архивный Product также блокирует удаление |
| `user_conflict` | `IntegrityError` при commit create User | Message интерпретирует conflict как duplicate email |
| `product_archived` | Supplier: active+archived state; Customer: попытка cart/order для archived projection | Один code имеет разные контексты в двух API |
| `product_inactive` | Customer Product не active при cart/order | Archived check выполняется раньше inactive check |
| `insufficient_stock` | Quantity выше local customer stock | Проверка может использовать устаревшую проекцию |
| `order_already_cancelled` | Повторный cancel status `cancelled` | Другие возможные DB status явно не запрещены кодом |

`cart_is_empty` является business error, но имеет HTTP 400, не 409. `product_archived` в Supplier API не означает попытку покупки: там он обозначает недопустимое сочетание flags.

## 8. Not Found behavior

Все подтверждённые not-found codes используют HTTP 404:

| Code | Что скрывается за 404 |
|---|---|
| `supplier_not_found` | Supplier lookup не дал запись; также неизвестный owner Product |
| `product_not_found` | Supplier Product отсутствует либо Customer local Product отсутствует; в customer чтении может возникнуть при сериализации связанной сущности |
| `warehouse_not_found` | Warehouse отсутствует перед CRUD/restock |
| `user_not_found` | Customer User отсутствует; проверка часто выполняется первой |
| `favorite_not_found` | Favorite pair отсутствует; Product existence при DELETE отдельно не проверяется |
| `cart_item_not_found` | CartItem pair отсутствует; в PATCH сначала проверяются User/Product/flags |
| `order_not_found` | Order отсутствует, принадлежит другому User или имеет null `order_number` |

`order_not_found` намеренно не различает отсутствующий и чужой Order на уровне фактического ответа. Это скрывает ownership результата, хотя auth в системе отсутствует. Порядок проверок влияет на возвращаемый код: например, неизвестный User даёт `user_not_found` до проверки Order.

## 9. Internal errors and limitations

### 9.1. Источники `internal_error`

Под общий HTTP 500 могут попасть:

- неперехваченные SQLAlchemy/DB exceptions;
- Kafka producer exceptions;
- response serialization failures, не представленные как `HTTPException`/Pydantic validation;
- ошибки соединения с DB во время HTTP-запроса;
- неожиданные программные ошибки.

### 9.2. `IntegrityError`

Supplier create/update Supplier перехватывает любой `IntegrityError` как 409 `supplier_conflict`; customer create User — как 409 `user_conflict`. На других endpoint `IntegrityError` специально не перехватывается и станет 500. Подтверждённые по структуре кода примеры возможных путей:

- явный `null` в optional update Product/Warehouse, если он записывается в NOT NULL колонку;
- race между check и INSERT уникальной Favorite/CartItem;
- constraint failure при создании Order/OrderItem или restock.

Точный DB exception зависит от данных и конкурентного выполнения; код не предоставляет отдельного error code для этих случаев.

### 9.3. Kafka errors после commit

Producer вызывается после DB commit в следующих HTTP-потоках:

- Supplier create/update/delete Supplier;
- Supplier create/update/archive Product;
- Supplier delete Warehouse и restock при публикации stock events;
- Customer create Order.

Если producer выбрасывает исключение, общий handler может вернуть 500 `internal_error`, хотя основное изменение БД уже сохранено. Для Customer Order к этому моменту также очищена cart. Повтор запроса способен создать новое изменение, поскольку HTTP error не означает rollback ранее выполненного commit.

Producer не использует delivery callback и не делает `flush()`, поэтому успешный HTTP response также не является подтверждением фактической доставки сообщения broker.

### 9.4. Ошибки после commit, не равные 500

При cancel Order status коммитится до сериализации ответа. Если связанная локальная Product отсутствует, helper формирует `HTTPException` с 404 `product_not_found`; клиент получает ошибку, хотя status уже изменён на `cancelled`.

Аналогично общий принцип API не гарантирует, что любой error response означает отсутствие side effects: код содержит несколько commit до последующих операций.

### 9.5. Ограничения текущего контракта

- Нет одного машинно-проверяемого реестра, общего для двух приложений и документации.
- `message` формируется из словаря, Pydantic message или fallback; его стабильность явно не версионируется.
- Один code может объединять разные причины (`supplier_conflict`, `user_conflict`, `product_not_found`).
- Нет correlation/request/error ID в response.
- Нет details array для нескольких validation violations; сообщения склеены в строку.
- `invalid_quantity` выглядит как код словаря, но фактически служит только message template.
- Нет отдельного кода для Kafka publish/delivery failure или post-commit failure.
- HTTP 500 скрывает причину от клиента; диагностическая информация доступна только в логах.
- Health endpoints не выявляют недоступность зависимостей заранее.

## 10. QA Checklist

### Общий envelope

- Проверить JSON content type и наличие ровно верхнего объекта `error`.
- Проверить строковые `error.code` и `error.message`.
- Сопоставить HTTP status с каталогом.
- Убедиться, что stack trace/SQL/internal exception не попадают клиенту.
- Проверить, что успешный HTTP 204 имеет пустое body.

### Validation

- Пропустить каждое required body/query поле.
- Передать неверные типы path/query/body и malformed body.
- Проверить min/max length, email, `gt=0`, `ge=0`, пустые arrays.
- Проверить объединение нескольких сообщений через `; `.
- В customer-service проверить message для quantity, user ID и product ID ≤0 при сохранении code `validation_error`.

### Business, conflict и not found

- Проверить каждый 404 на отсутствующей сущности и порядок precedence зависимостей.
- Проверить duplicate supplier email/phone и user email.
- Проверить удаление Supplier с active/archived Product.
- Проверить active+archived Supplier Product.
- Проверить customer archived/inactive/insufficient-stock отдельно.
- Проверить пустую cart при создании Order.
- Проверить повторный cancel и чужой Order как `order_not_found`.

### Side effects и internal errors

- Для error response проверить фактическое состояние БД, cart и Kafka, а не предполагать rollback.
- При контролируемом producer failure проверить post-commit состояние.
- Проверить отсутствие внутренней причины в HTTP 500 и наличие server-side log.
- Сверить error code/message между повторными идентичными запросами.

## 11. Open questions

1. Является ли `error.message` стабильной частью публичного контракта или только диагностическим текстом?
2. Какой артефакт должен считаться единым source of truth каталога: код каждого сервиса, централизованная схема или документация? Текущий репозиторий этого не определяет.
3. Нужно ли предметно различать validation codes по полям и типам нарушений? Сейчас все объединены в `validation_error`.
4. Является ли применение quantity message к `user_id`/`product_id` намеренным? По коду определить нельзя.
5. Как клиент должен интерпретировать Kafka producer error после успешного DB commit и безопасно ли повторять запрос?
6. Как различать DB failure до commit, post-commit failure и недоставленное асинхронное событие? Текущий envelope этого не показывает.
7. Должны ли conflict codes отражать точный constraint, а not-found codes — отсутствие локальной проекции отдельно от отсутствия source entity?
8. Какие error codes потребуются для future 2.0, неизвестно; текущий код и этот AS IS документ их не определяют.

