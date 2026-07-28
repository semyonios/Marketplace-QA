# Документация анализа Marketplace-QA

## Назначение каталога

Каталог содержит результаты изучения существующей реализации Marketplace-QA. Документы описывают только то, что подтверждается исходным кодом, конфигурацией и README. Требования к будущему состоянию системы в этот комплект не входят.

## Состав

1. [Анализ проекта](01_project_analysis.md) — назначение системы и сервисов, структура данных, API, Kafka, Docker, связи, сценарии, правила, ограничения и реализованные архитектурные решения.
2. [Открытые вопросы](02_open_questions.md) — сведения, которые нельзя однозначно восстановить по репозиторию, и наблюдаемые неоднозначности реализации.
3. [Черновик глоссария](03_glossary_draft.md) — термины, фактически используемые в коде, конфигурации и README.
4. [Системный контекст AS IS](04_system_context_as_is.md) — текущие границы системы, акторы, компоненты, взаимодействия и потоки данных.
5. [Supplier API Contract Specification](api/01_supplier_overview.md) — обзор и инженерные контракты текущего Supplier API по поставщикам, товарам, складам и остаткам.
6. [Customer API Contract Specification](api/06_customer_overview.md) — обзор и инженерные контракты текущего Customer API по покупателям, локальному каталогу, избранному, корзине и заказам.
7. [Error Contract Specification AS IS](api/12_error_contract.md) — единый формат, централизованные каталоги кодов и фактическое поведение ошибок Supplier API и Customer API.
8. [Kafka Event Contract Specification AS IS](events/01_kafka_event_contract_as_is.md) — текущие Kafka topics, события, producers/consumers, обработка, побочные эффекты и ограничения.
9. [Data Contract Specification AS IS](data/01_data_contract_as_is.md) — текущие PostgreSQL-схемы, владение данными, связи, вычисляемые значения, жизненный цикл и согласованность.
10. [Business Rules Specification AS IS](business/01_business_rules_as_is.md) — централизованный трассируемый каталог фактически реализованных бизнес-правил по всем доменам.
11. [Business Scenario Specification AS IS](scenarios/01_business_scenarios_as_is.md) — 21 сквозной сценарий текущего поведения с API, Kafka, таблицами, правилами и QA-проверками.
12. [Functional Requirements Specification AS IS](functional/01_functional_requirements_as_is.md) — атомарные реализованные функциональные требования с трассировкой к правилам, сценариям, API, событиям и данным.
13. [Requirements Traceability Matrix AS IS](traceability/01_requirements_traceability_matrix_as_is.md) — центральная прямая и обратная карта связей BR, FR, сценариев, API, Kafka, таблиц, ошибок и QA.
14. [Non-Functional Requirements Specification AS IS](non-functional/01_non_functional_requirements_as_is.md) — 74 подтвержденные нефункциональные характеристики архитектуры, развертывания, доступности, производительности, надежности, безопасности, наблюдаемости, данных, API и Kafka.

## Возможные разделы будущей документации

- карта заинтересованных сторон и ролей;
- описание границ системы и внешнего контекста;
- каталог функциональных требований;
- спецификация API-контрактов;
- спецификация Kafka-событий;
- модель данных и правила хранения;
- описание бизнес-сценариев и состояний;
- трассировка требований к API, событиям и данным;
- критерии приёмки и тестовые сценарии.
