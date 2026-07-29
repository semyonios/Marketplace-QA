# Non-Functional Requirements Specification AS IS

## 1. Назначение документа

Нефункциональные требования описывают не отдельные бизнес-функции, а наблюдаемые свойства и эксплуатационные ограничения системы: способ развертывания, доступность, производительность, масштабируемость, согласованность, надежность, безопасность, наблюдаемость и технические контракты.

Этот документ фиксирует только свойства текущей реализации Marketplace-QA. Формулировка свойства как NFR AS IS означает, что оно подтверждается репозиторием, но не означает, что оно является целевым или рекомендуемым состоянием.

## 2. Общие принципы

- Каждая характеристика выводится из исходного кода, `docker-compose.yml`, Dockerfile, зависимостей, README и ранее составленных AS IS-контрактов.
- Отсутствие механизма, гарантии или измеримого показателя также фиксируется как характеристика, если отсутствие подтверждается составом репозитория и конфигурацией.
- Неопределенные значения не подменяются предположениями. Они перечислены в разделе открытых вопросов.
- AS IS-описание не задает направление изменений и не является оценкой качества архитектуры.
- Приоритет в каталоге означает значимость свойства для проверки текущего поведения: `Высокий`, `Средний` или `Низкий`. Это не приоритет разработки.

## 3. Архитектурные требования

- **NFR-ARCH-001.** Система состоит из двух HTTP-сервисов: `supplier-service` и `customer-service`.
- **NFR-ARCH-002.** Аудит событий поставщиков выполняется отдельным процессом `audit-consumer`, не предоставляющим HTTP API.
- **NFR-ARCH-003.** Асинхронное межсервисное взаимодействие реализовано через Kafka и четыре прикладных топика.
- **NFR-ARCH-004.** Данные доменов физически разделены между `supplier_db` и `customer_db`; `audit-consumer` разделяет `supplier_db` с `supplier-service`.
- **NFR-ARCH-005.** `customer-service` хранит локальную проекцию товаров и остатков, получаемую из supplier-контура.
- **NFR-ARCH-006.** Компоновка компонентов задана Docker Compose; иной оркестратор в репозитории не определен.

## 4. Требования к развертыванию

- **NFR-DEPLOY-001.** Полный локальный контур запускается через Docker Compose и включает семь компонентов: два API-сервиса, `audit-consumer`, две PostgreSQL, Kafka и Kafka UI.
- **NFR-DEPLOY-002.** Python-компоненты собираются из образа `python:3.12-slim`; API запускаются Uvicorn, audit — Python-модулем.
- **NFR-DEPLOY-003.** Адреса БД, Kafka, топиков, consumer groups и supplier API передаются через переменные окружения с Compose-значениями по умолчанию.
- **NFR-DEPLOY-004.** Compose ожидает healthcheck PostgreSQL перед стартом зависимых компонентов, но для API, Kafka и audit прикладные healthcheck не заданы.
- **NFR-DEPLOY-005.** ORM-таблицы создаются при старте через `create_all`; оба API также выполняют отдельные runtime-проверки/`ALTER TABLE` для части полей совместимости.
- **NFR-DEPLOY-006.** Версионированный механизм миграций БД в репозитории отсутствует.
- **NFR-DEPLOY-007.** Данные PostgreSQL сохраняются в именованных Docker volumes `postgres_data` и `customer_postgres_data`.

## 5. Требования к доступности

- **NFR-AVAIL-001.** SLA, SLO, допустимое время простоя и целевые показатели доступности не определены.
- **NFR-AVAIL-002.** Compose запускает по одному экземпляру каждого сервиса, БД и broker; failover и резервные экземпляры не сконфигурированы.
- **NFR-AVAIL-003.** `GET /health` обоих API возвращает статический статус `ok` и не проверяет PostgreSQL, Kafka, соседний сервис или consumer thread.
- **NFR-AVAIL-004.** Недоступность PostgreSQL при выполнении API-операции приводит к необработанной инфраструктурной ошибке, оборачиваемой общим HTTP 500 `internal_error`; отдельный режим деградации не определен.
- **NFR-AVAIL-005.** При недоступности Kafka consumers повторяют подключение/обработку, а producer не предоставляет HTTP-клиенту подтверждение доставки; недоступность supplier API при startup snapshot логируется `customer-service`, после чего запуск продолжается.

## 6. Требования к производительности

- **NFR-PERF-001.** Ограничения времени ответа, пропускной способности, lag и объема нагрузки, а также результаты benchmark/load test в репозитории отсутствуют.
- **NFR-PERF-002.** HTTP-обработка и обращения к SQLAlchemy выполняются синхронно в рамках запроса.
- **NFR-PERF-003.** Межсервисное распространение изменений вынесено в асинхронную обработку Kafka и не входит в синхронное завершение большинства HTTP-операций.
- **NFR-PERF-004.** Списочные API не имеют пагинации; snapshot каталога запрашивает полный список supplier products.
- **NFR-PERF-005.** Rate limiting и явно настроенный прикладной cache отсутствуют; customer product projection является сохраняемой доменной копией, а не временным cache-слоем.

## 7. Требования к масштабируемости

- **NFR-SCALE-001.** Текущее Compose-развертывание задает один экземпляр каждого приложения и каждой БД.
- **NFR-SCALE-002.** Kafka работает как один broker/controller с replication factor `1` для внутренних Kafka-топиков.
- **NFR-SCALE-003.** Consumer supplier/order и customer/product/stock запускаются daemon threads внутри соответствующих API-процессов; audit consumer работает отдельным процессом.
- **NFR-SCALE-004.** Реплики, autoscaling, load balancer и конфигурация горизонтального масштабирования в репозитории отсутствуют.
- **NFR-SCALE-005.** Явная стратегия числа partitions, распределения ключей по partitions и масштабирования топиков не определена; топики создаются автоматически broker-ом.

## 8. Требования к консистентности

- **NFR-CONS-001.** Изменения, выполненные в одной транзакции одной PostgreSQL, получают транзакционную согласованность этой БД.
- **NFR-CONS-002.** Supplier-данные и customer projection согласуются eventual consistency через Kafka и startup snapshot.
- **NFR-CONS-003.** Прикладные события публикуются после commit доменной транзакции, поэтому фиксация в БД предшествует попытке публикации.
- **NFR-CONS-004.** Распределенная транзакция, outbox и saga между PostgreSQL и Kafka отсутствуют.
- **NFR-CONS-005.** Некоторые составные операции используют несколько commit, поэтому допускают частично зафиксированное состояние при ошибке между этапами.
- **NFR-CONS-006.** Создание заказа не ожидает подтверждения supplier-service; отмена заказа не инициирует компенсационное восстановление supplier stock.

## 9. Требования к надежности

- **NFR-REL-001.** Consumer выполняет до трех попыток обработки одного сообщения с паузой между попытками.
- **NFR-REL-002.** Автоматический commit отключен; offset фиксируется вручную после признанной успешной обработки.
- **NFR-REL-003.** После общей ошибки consumer loop логирует сбой, ожидает и повторно создает/продолжает потребление.
- **NFR-REL-004.** Идемпотентность обработки `ORDER_CREATED` обеспечивается таблицей `processed_events` по `event_id`.
- **NFR-REL-005.** Supplier, product, stock и audit events не имеют общего `event_id` и не дедуплицируются текущими consumers.
- **NFR-REL-006.** После исчерпания трех попыток сообщение обозначается в логе как dead letter, но отдельный dead-letter topic и хранилище отсутствуют.
- **NFR-REL-007.** Producers вызывают `produce()` и `poll(0)` без delivery callback и `flush()`; подтверждение доставки не контролируется, а ошибка после DB commit может оставить сохраненное изменение без доставленного события.

## 10. Требования к безопасности

- **NFR-SEC-001.** HTTP API не аутентифицируют клиента.
- **NFR-SEC-002.** RBAC, JWT и OAuth в приложениях отсутствуют.
- **NFR-SEC-003.** Доступ к customer-ресурсам ограничивается переданным клиентом `user_id`, а не подтвержденной identity.
- **NFR-SEC-004.** HTTPS/TLS для HTTP и шифрование Kafka не настроены; broker использует PLAINTEXT listeners.
- **NFR-SEC-005.** Учетные данные PostgreSQL заданы открытыми значениями Compose; интеграция с secrets manager отсутствует.
- **NFR-SEC-006.** Pydantic и бизнес-логика валидируют входные данные, однако ограничения доступа к Kafka UI, маскирование payload/персональных данных в логах и отдельные security headers конфигурацией не определены.

## 11. Требования к наблюдаемости

- **NFR-OBS-001.** Компоненты используют стандартное Python logging с уровнем `INFO` и сообщениями об обработке, retry и исключениях.
- **NFR-OBS-002.** В ряде producer/consumer путей в журнал выводятся payload событий; централизованная политика редактирования чувствительных полей не определена.
- **NFR-OBS-003.** Kafka UI предоставляет локальный просмотр broker, топиков и consumer state на порту `8080`.
- **NFR-OBS-004.** PostgreSQL имеет container healthcheck, а API имеют только статические HTTP health endpoints без dependency probes.
- **NFR-OBS-005.** Prometheus-метрики, Grafana dashboards и прикладные счетчики/гистограммы в репозитории отсутствуют.
- **NFR-OBS-006.** Distributed tracing, correlation ID, централизованный сбор логов и alerting не сконфигурированы.

## 12. Требования к данным

- **NFR-DATA-001.** Supplier-домен и customer-домен хранятся в двух отдельных PostgreSQL БД с раздельным владением, кроме audit rows в `supplier_db`.
- **NFR-DATA-002.** Product и stock дублируются в `customer_db` как асинхронно обновляемая проекция supplier source of truth.
- **NFR-DATA-003.** Денежные значения хранятся и вычисляются с использованием `float`/SQL `FLOAT`.
- **NFR-DATA-004.** `OrderItem` сохраняет price snapshot, тогда как вложенное описание Product при чтении заказа строится из текущей customer projection.
- **NFR-DATA-005.** Отдельная audit DB отсутствует; `audit-consumer` сохраняет `user_events` в `supplier_db`.
- **NFR-DATA-006.** Схема создается и частично изменяется приложениями во время запуска; миграционные версии не хранятся.
- **NFR-DATA-007.** История warehouse stock и полные исторические снимки Product не сохраняются; revision/timestamp для упорядочивания состояния проекции отсутствуют.

## 13. Требования к API

- **NFR-API-001.** Оба API реализованы на FastAPI как HTTP-интерфейсы с JSON request/response.
- **NFR-API-002.** Ошибки обоих API возвращаются в envelope `{"error":{"code":"...","message":"..."}}` для обработанных validation, business и internal errors.
- **NFR-API-003.** Ошибки Pydantic-проверки path, query и body преобразуются в HTTP 400 с кодом `validation_error`.
- **NFR-API-004.** Успешные операции со статусом HTTP 204 не возвращают response body.
- **NFR-API-005.** FastAPI предоставляет генерируемые OpenAPI schema и Swagger UI; отдельная вручную поддерживаемая формальная API schema в репозитории не используется runtime.
- **NFR-API-006.** URL API не содержат версии; negotiation/deprecation policy, pagination и rate limiting не определены.

## 14. Требования к Kafka

- **NFR-KAFKA-001.** Система использует четыре прикладных топика: `supplier-events`, `product-events`, `product-stock-events`, `order-events`.
- **NFR-KAFKA-002.** Supplier-service публикует supplier/product/stock events и потребляет order events; customer-service потребляет product/stock events и публикует order events; audit-consumer потребляет supplier events.
- **NFR-KAFKA-003.** Consumers используют именованные группы и `auto.offset.reset=earliest` при отсутствии ранее сохраненного offset.
- **NFR-KAFKA-004.** Consumers отключают auto commit и вручную фиксируют offset после обработки.
- **NFR-KAFKA-005.** Ошибка обработки сообщения повторяется до трех раз; физического DLQ нет.
- **NFR-KAFKA-006.** Межсервисная проекция обновляется асинхронно и допускает lag/stale state; SLA lag не установлен.
- **NFR-KAFKA-007.** Формальные event schemas, Schema Registry и единый строгий envelope отсутствуют; совместимость опирается на JSON-поля и `event_version` в payload.
- **NFR-KAFKA-008.** Broker разрешает auto topic creation; явные настройки прикладных topic partitions, retention и replication в Compose отсутствуют.

## 15. Ограничения

Текущая реализация имеет следующие подтвержденные ограничения:

- один экземпляр каждого компонента и один Kafka broker без настроенного failover;
- отсутствие SLA/SLO для доступности, производительности и Kafka lag;
- статические API health endpoints, не отражающие состояние зависимостей;
- отсутствие auth, RBAC, TLS и управляемого хранения secrets;
- отсутствие миграций и выполнение части изменений схемы при старте;
- отсутствие distributed transaction, outbox, saga и supplier confirmation заказа;
- публикация после DB commit без delivery callback/flush и гарантии, видимой HTTP-клиенту;
- отсутствие DLQ и общей дедупликации событий;
- eventual consistency и возможность stale customer projection;
- частичные побочные эффекты в multi-commit и post-commit сценариях;
- отсутствие метрик, tracing, централизованных логов и alerting;
- отсутствие rate limiting, пагинации и формальной политики API versioning;
- отсутствие формальных Kafka schemas и Schema Registry;
- хранение денег как floating-point значений;
- смешение исторической цены и текущего Product summary в Order response;
- отсутствие истории остатков и revision-механизма для проекций;
- audit rows хранятся в supplier DB, отдельная audit DB отсутствует.

## 16. Каталог NFR

| ID | Requirement | Домен | Evidence | QA relevance |
|---|---|---|---|---|
| NFR-ARCH-001 | Два HTTP-сервиса разделяют supplier и customer функции | Архитектура | Compose; FastAPI entrypoints | Высокий: проверить независимые API |
| NFR-ARCH-002 | Audit работает отдельным процессом без HTTP API | Архитектура | Compose; audit entrypoint | Средний: проверить автономное потребление |
| NFR-ARCH-003 | Межсервисный async transport — Kafka с четырьмя топиками | Архитектура | Compose; producer/consumer code | Высокий: проверить сквозные потоки |
| NFR-ARCH-004 | Две БД разделены, audit пишет в supplier DB | Архитектура | DB URLs; ORM models | Высокий: проверить границы данных |
| NFR-ARCH-005 | Customer хранит supplier Product projection | Архитектура | Customer Product; consumers/snapshot | Высокий: проверить eventual consistency |
| NFR-ARCH-006 | Оркестрация задана только Docker Compose | Архитектура | `docker-compose.yml`; отсутствие иных manifests | Средний: проверка воспроизводимости запуска |
| NFR-DEPLOY-001 | Локальный контур содержит семь Compose-компонентов | Развертывание | `docker-compose.yml` | Высокий: smoke test полного контура |
| NFR-DEPLOY-002 | Python 3.12-slim; Uvicorn для API, module runner для audit | Развертывание | Dockerfile/commands | Средний: проверить процессы контейнеров |
| NFR-DEPLOY-003 | Runtime-конфигурация передается env variables | Развертывание | Compose environment; settings | Высокий: проверить подстановку адресов/топиков |
| NFR-DEPLOY-004 | Compose ждет DB health, но app healthchecks не заданы | Развертывание | depends_on/healthcheck | Высокий: проверить порядок старта |
| NFR-DEPLOY-005 | Таблицы и runtime ALTER выполняются при старте | Развертывание | startup/create_all/schema helpers | Высокий: проверить чистую и существующую БД |
| NFR-DEPLOY-006 | Механизм миграций отсутствует | Развертывание | Состав репозитория | Средний: учитывать при проверке schema lifecycle |
| NFR-DEPLOY-007 | PostgreSQL использует named volumes | Развертывание | Compose volumes | Средний: проверить сохранность после restart |
| NFR-AVAIL-001 | SLA/SLO доступности не определены | Доступность | README/config/docs | Средний: нет числового oracle |
| NFR-AVAIL-002 | Failover и резервные instances не сконфигурированы | Доступность | Compose replica count | Высокий: проверить single-point outage |
| NFR-AVAIL-003 | API health статичен и не проверяет зависимости | Доступность | Оба `/health` handlers | Высокий: проверить при отключенных зависимостях |
| NFR-AVAIL-004 | DB runtime failure отображается как 500 internal_error | Доступность | Exception handlers; DB operations | Высокий: negative infrastructure test |
| NFR-AVAIL-005 | Consumers retry Kafka; producer delivery не подтверждается; snapshot failure не останавливает customer | Доступность | Consumer loops; producer; startup snapshot | Высокий: outage tests Kafka/supplier |
| NFR-PERF-001 | Performance SLA и benchmark отсутствуют | Производительность | Состав репозитория | Низкий: нет числового acceptance threshold |
| NFR-PERF-002 | HTTP и SQL выполняются синхронно | Производительность | Route/session implementation | Средний: учитывать blocking behavior |
| NFR-PERF-003 | Межсервисная синхронизация асинхронна | Производительность | Kafka flow | Высокий: измерять отдельно HTTP и propagation |
| NFR-PERF-004 | List API и snapshot не пагинированы | Производительность | Routes; snapshot HTTP request | Средний: проверить объемные выборки без SLA |
| NFR-PERF-005 | Rate limiting и явный app cache отсутствуют | Производительность | Middleware/dependencies | Средний: проверить фактическое отсутствие 429 |
| NFR-SCALE-001 | Compose запускает по одному instance приложений и БД | Масштабируемость | Compose services | Средний: фиксировать baseline topology |
| NFR-SCALE-002 | Kafka состоит из одного broker/controller | Масштабируемость | Kafka Compose config | Высокий: broker outage влияет на весь event flow |
| NFR-SCALE-003 | API consumers работают daemon threads; audit — отдельный process | Масштабируемость | Service startup/consumer entrypoints | Высокий: проверить lifecycle consumers |
| NFR-SCALE-004 | Replicas/autoscaling/load balancer не настроены | Масштабируемость | Состав репозитория | Средний: масштабирование не имеет AS IS test target |
| NFR-SCALE-005 | Partition strategy не определена, topics auto-created | Масштабируемость | Kafka config/producer keys | Средний: не предполагать partition count |
| NFR-CONS-001 | Одна DB transaction обеспечивает локальную atomicity | Консистентность | Session commit/rollback | Высокий: transaction rollback tests |
| NFR-CONS-002 | Межбазовая согласованность eventual | Консистентность | Kafka consumers; snapshot | Высокий: проверить временный рассинхрон |
| NFR-CONS-003 | События публикуются после DB commit | Консистентность | API/service control flow | Высокий: post-commit failure test |
| NFR-CONS-004 | Distributed transaction/outbox/saga отсутствуют | Консистентность | Transaction/event implementation | Высокий: учитывать независимые failures |
| NFR-CONS-005 | Multi-commit операции допускают partial state | Консистентность | Stock/warehouse/order flows | Высокий: fault injection между commit |
| NFR-CONS-006 | Нет supplier confirmation и cancel compensation | Консистентность | Order/event flows | Высокий: проверить фактическое конечное состояние |
| NFR-REL-001 | Consumer делает максимум три попытки обработки | Надежность | Consumer retry loops | Высокий: проверить число retries |
| NFR-REL-002 | Offset commit выполняется вручную после success | Надежность | Consumer config/commit | Высокий: проверить replay до commit |
| NFR-REL-003 | Consumer loop восстанавливается после общей ошибки | Надежность | Outer exception loop | Высокий: Kafka restart test |
| NFR-REL-004 | ORDER_CREATED дедуплицируется по processed_events | Надежность | Supplier order consumer/model | Высокий: duplicate order event test |
| NFR-REL-005 | Остальные event streams не имеют общей deduplication | Надежность | Event contracts/consumers | Высокий: duplicate side-effect tests |
| NFR-REL-006 | Dead letter существует только как log, DLQ отсутствует | Надежность | Consumer retry exhaustion | Высокий: poison message test |
| NFR-REL-007 | Producer delivery не подтверждается; post-commit gap возможен | Надежность | `produce`/`poll(0)`; нет callback/flush | Высокий: producer failure test |
| NFR-SEC-001 | API не аутентифицируют клиента | Безопасность | Routes/dependencies | Высокий: запросы без credentials проходят |
| NFR-SEC-002 | RBAC/JWT/OAuth отсутствуют | Безопасность | Dependencies/middleware | Высокий: нет role-based denial |
| NFR-SEC-003 | Ownership задается клиентским user_id | Безопасность | Customer routes/queries | Высокий: ownership negative cases |
| NFR-SEC-004 | HTTP/Kafka TLS не настроен, Kafka PLAINTEXT | Безопасность | Compose/listeners | Высокий: подтвердить transport config |
| NFR-SEC-005 | DB credentials открыто заданы Compose | Безопасность | Compose environment | Высокий: configuration review |
| NFR-SEC-006 | Есть input validation, но UI access/redaction/security headers не определены | Безопасность | Schemas, logging, middleware, Compose | Высокий: validation и exposure checks |
| NFR-OBS-001 | Используется INFO-level Python logging | Наблюдаемость | logging configuration | Средний: проверить ключевые lifecycle logs |
| NFR-OBS-002 | Event payload может журналироваться без общей redaction policy | Наблюдаемость | Producer/consumer logs | Высокий: проверить содержание логов |
| NFR-OBS-003 | Kafka UI доступен как средство локального наблюдения | Наблюдаемость | Compose kafka-ui | Средний: проверить topics/groups |
| NFR-OBS-004 | DB healthcheck есть, API dependency probes отсутствуют | Наблюдаемость | Compose; health handlers | Высокий: dependency outage test |
| NFR-OBS-005 | Prometheus/Grafana/app metrics отсутствуют | Наблюдаемость | Состав репозитория | Средний: метрики не являются test oracle |
| NFR-OBS-006 | Tracing/correlation/central logs/alerts отсутствуют | Наблюдаемость | Состав репозитория | Средний: диагностика опирается на container logs |
| NFR-DATA-001 | Два DB ownership, audit rows находятся в supplier DB | Данные | Models/DB configuration | Высокий: проверить физическое размещение |
| NFR-DATA-002 | Customer Product/stock — дублированная projection | Данные | Customer models/consumers | Высокий: projection consistency tests |
| NFR-DATA-003 | Деньги используют floating-point | Данные | ORM/Pydantic arithmetic | Высокий: проверить rounding artifacts |
| NFR-DATA-004 | Order сочетает price snapshot и current Product summary | Данные | Order serialization/models | Высокий: изменить Product после order |
| NFR-DATA-005 | Audit DB отсутствует; user_events хранится в supplier DB | Данные | Audit model/DB URL | Средний: проверить место записи |
| NFR-DATA-006 | Schema создается/изменяется runtime без migration versions | Данные | create_all/ALTER; no migrations | Высокий: startup schema tests |
| NFR-DATA-007 | Нет stock history, full Product snapshot и projection revision | Данные | Data models/event payloads | Высокий: историческое состояние не восстанавливается |
| NFR-API-001 | API используют FastAPI, HTTP и JSON | API | App/routes | Высокий: content-type/serialization tests |
| NFR-API-002 | Обработанные ошибки имеют единый error envelope | API | Exception handlers | Высокий: contract tests |
| NFR-API-003 | Pydantic validation возвращает 400 validation_error | API | Validation handlers | Высокий: invalid path/query/body tests |
| NFR-API-004 | HTTP 204 не содержит body | API | Delete/cancel responses | Высокий: exact response test |
| NFR-API-005 | OpenAPI/Swagger генерируются FastAPI | API | FastAPI defaults | Средний: schema/docs reachability |
| NFR-API-006 | URL versioning/pagination/rate limiting отсутствуют | API | Routes/middleware | Средний: не ожидать этих механизмов |
| NFR-KAFKA-001 | Используются четыре прикладных topics | Kafka | Compose/settings/event code | Высокий: topic routing test |
| NFR-KAFKA-002 | Producer/consumer роли распределены между тремя процессами | Kafka | Event code | Высокий: end-to-end event paths |
| NFR-KAFKA-003 | Именованные groups используют earliest для нового offset | Kafka | Consumer configuration | Высокий: fresh group replay test |
| NFR-KAFKA-004 | Consumers используют manual commit | Kafka | Consumer configuration | Высокий: commit behavior test |
| NFR-KAFKA-005 | Три retries, отдельного DLQ нет | Kafka | Consumer loops/Compose | Высокий: poison event test |
| NFR-KAFKA-006 | Projection допускает lag без заданного SLA | Kafka | Async flow; no SLA | Высокий: eventual consistency test |
| NFR-KAFKA-007 | Нет formal schemas/Schema Registry/strict envelope | Kafka | Event payload builders/Compose | Высокий: compatibility negative tests |
| NFR-KAFKA-008 | Auto topic creation включен; app topic config не задан | Kafka | Broker Compose config | Средний: clean-broker startup test |

Всего в каталоге: **74 NFR AS IS**.

## 17. QA Checklist

### Развертывание и topology

- Запустить чистый контур через Docker Compose и подтвердить старт семи компонентов.
- Проверить создание схем на пустых volumes и повторный старт на уже заполненных volumes.
- Перезапустить PostgreSQL-контейнеры и подтвердить сохранность данных в named volumes.
- Проверить фактическую подстановку DB/Kafka/topic/group env variables.
- Подтвердить, что API health endpoints остаются `ok` при недоступных зависимостях.

### Отказы и надежность

- Отключить Kafka и отдельно проверить producer HTTP-flow, consumer reconnect и startup snapshot behavior.
- Перезапустить PostgreSQL во время HTTP-запроса и проверить envelope `internal_error` там, где исключение доходит до общего handler.
- Подать consumer-у сообщение, обработка которого завершается ошибкой, и проверить три попытки, отсутствие commit до success и отсутствие физического DLQ.
- Повторно подать одинаковый `ORDER_CREATED` и проверить `processed_events` и отсутствие повторного списания.
- Повторно подать supplier/product/stock event и зафиксировать фактические side effects при отсутствии общей дедупликации.
- Смоделировать producer failure после DB commit и сравнить состояние БД с наличием события.
- Проверить составные операции при ошибке между commit и зафиксировать partial state.

### Консистентность и данные

- Измерить фактическую задержку supplier → customer без интерпретации ее как SLA.
- Прочитать customer catalog непосредственно после supplier change и подтвердить возможность stale projection.
- Проверить startup snapshot после временной недоступности supplier API.
- Проверить aggregate stock по нескольким warehouses и customer stock после Kafka event.
- Изменить Product после создания Order и проверить сохраненную цену и текущий Product summary.
- Проверить floating-point вычисления цен на значениях, чувствительных к двоичному округлению.

### API и безопасность

- Проверить JSON content type, общий error envelope, HTTP status и отсутствие stack trace.
- Проверить отсутствие body у всех успешных HTTP 204.
- Проверить invalid path/query/body и HTTP 400 `validation_error`.
- Выполнить API-запросы без credentials и подтвердить отсутствие auth challenge.
- Проверить ownership-сценарии с чужим `user_id` в пределах фактического API-контракта.
- Проверить доступность OpenAPI/Swagger и отсутствие version prefix в URL.
- Подтвердить отсутствие rate limiting серией запросов, не делая выводов о допустимой нагрузке.

### Наблюдаемость и Kafka

- Проверить наличие lifecycle/error/retry logs и отсутствие stack trace в HTTP response.
- Проверить, какие payload и персональные поля фактически попадают в logs.
- Через Kafka UI проверить topics, keys, consumer groups, offsets и lag.
- Проверить event type/version/payload и routing по четырем topics.
- Проверить manual offset commit после успешной обработки и replay до commit.
- Проверить работу consumer после Kafka restart.

## 18. Open Questions

По текущему коду и конфигурации невозможно определить:

- целевые SLA/SLO доступности, времени ответа, throughput и Kafka lag;
- ожидаемые объемы данных, число одновременных клиентов и профиль нагрузки;
- допустимые RPO/RTO и регламент backup/restore PostgreSQL;
- среду развертывания помимо локального Docker Compose и требования к production orchestration;
- политику секретов, сетевых границ, TLS, доступа к Kafka UI и PostgreSQL вне локального контура;
- формальную политику хранения, ротации, централизации и маскирования логов;
- допустимый уровень раскрытия supplier contact data в Kafka payload, audit rows и logs;
- политику API versioning, backward compatibility и deprecation;
- допустимую точность/округление денежных значений;
- Kafka guarantees, которые считаются обязательными для бизнеса: допустимость потерь, дублей и нарушения порядка;
- требуемое число partitions, replication factor прикладных топиков, retention и максимальный размер сообщения;
- поведение deployment при несовместимой существующей схеме БД;
- критерий готовности API к трафику и необходимость различать liveness/readiness;
- правила эксплуатации poison messages после записи строки `dead letter` в лог;
- допустимость частично выполненного списания stock и способ внешней диагностики результата заказа.
