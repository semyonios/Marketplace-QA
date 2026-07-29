# Marketplace-QA 2.0 — архитектурные диаграммы

Диаграммы отражают реализованный MVP. Детальные решения и контракты остаются в документах [TO BE](../to-be/01-vision-and-architecture-principles.md).

## System context

```mermaid
flowchart LR
    Customer["Покупатель"] --> Frontend["Frontend"]
    Supplier["Поставщик"] --> Frontend
    Frontend --> CustomerAPI["Customer API"]
    Frontend --> SupplierAPI["Supplier API"]
    Frontend --> OrderAPI["Order API"]
    CustomerAPI --> CustomerDB[("Customer DB")]
    SupplierAPI --> SupplierDB[("Supplier DB")]
    OrderAPI --> OrderDB[("Order DB")]
    CustomerAPI <--> Kafka["Kafka"]
    SupplierAPI <--> Kafka
    OrderAPI <--> Kafka
    Kafka --> Audit["Audit consumer"]
    QA["QA-инженер"] --> Frontend
    QA --> Swagger["Swagger / Postman / Kafka UI"]
```

## Containers and services

```mermaid
flowchart TB
    subgraph UI["Presentation"]
        FE["React + TypeScript frontend"]
        KUI["Kafka UI"]
    end
    subgraph CustomerDomain["Customer domain"]
        CS["customer-service"]
        CDB[("customer-postgres")]
        CS --> CDB
    end
    subgraph OrderDomain["Order domain"]
        OS["order-service"]
        OP["order-outbox-publisher"]
        OC["order-stock-events-consumer"]
        TW["order-timeout-worker"]
        ODB[("order-postgres")]
        OS --> ODB
        OP --> ODB
        OC --> ODB
        TW --> ODB
    end
    subgraph SupplierDomain["Supplier domain"]
        SS["supplier-service"]
        SP["supplier-outbox-publisher"]
        SC["supplier-reservation-consumer"]
        SDB[("supplier-postgres")]
        SS --> SDB
        SP --> SDB
        SC --> SDB
    end
    FE --> CS
    FE --> OS
    FE --> SS
    OS --> CS
    OP --> K["Kafka"]
    K --> OC
    K --> SC
    SP --> K
    CS <--> K
    K --> AC["audit-consumer"]
    KUI --> K
```

## Order creation and reservation

```mermaid
sequenceDiagram
    actor Customer
    participant FE as Frontend
    participant CS as customer-service
    participant OS as order-service
    participant ODB as order-db
    participant K as Kafka
    participant SS as supplier consumer
    participant SDB as supplier-db

    Customer->>FE: Checkout
    FE->>CS: GET cart + cart_version
    FE->>OS: POST order + Idempotency-Key
    OS->>CS: GET internal cart snapshot
    OS->>ODB: Order + history + outbox
    OS-->>FE: 202 PENDING_RESERVATION + ETag
    ODB-->>K: StockReservationRequested
    K-->>SS: Reservation command
    SS->>SDB: Inbox + all-or-nothing reserve + outbox
    SDB-->>K: StockReservationSucceeded/Failed
    K-->>OS: Reservation result
    OS->>ODB: Inbox + RESERVED/REJECTED + history
    FE->>OS: Poll order
    OS-->>FE: Terminal/current representation
```

## Supplier confirm

```mermaid
sequenceDiagram
    actor Supplier
    participant FE as Frontend
    participant OS as order-service
    participant ODB as order-db
    participant K as Kafka
    participant SS as supplier consumer
    participant SDB as supplier-db

    Supplier->>FE: Confirm
    FE->>OS: POST confirm + If-Match
    OS->>ODB: CONFIRMATION_PENDING + outbox
    OS-->>FE: 202 + new ETag
    ODB-->>K: StockFinalizationRequested
    K-->>SS: Finalize command
    SS->>SDB: Commit reserved stock + outbox
    SDB-->>K: StockFinalized
    K-->>OS: Finalization result
    OS->>ODB: CONFIRMED + history
    FE->>OS: Poll until operation_state=NONE
```

## Cancel with late reservation success

```mermaid
sequenceDiagram
    actor Customer
    participant FE as Frontend
    participant OS as order-service
    participant ODB as order-db
    participant K as Kafka
    participant SS as supplier consumer

    Customer->>FE: Cancel before CONFIRMED
    FE->>OS: POST cancel + If-Match
    OS->>ODB: CANCELLATION_PENDING + release outbox
    OS-->>FE: 202
    K-->>OS: Late StockReservationSucceeded
    OS->>ODB: Keep cancellation + schedule release
    ODB-->>K: StockReleaseRequested
    K-->>SS: Release command
    SS-->>K: StockReleased
    K-->>OS: Release result
    OS->>ODB: CANCELLED + RELEASED + history
    FE->>OS: Poll until CANCELLED
```

## Order state machine

Единственный нормативный вариант state machine находится в
[05-order-state-machine.md](../to-be/05-order-state-machine.md). Диаграмма здесь не дублируется, чтобы изменения правил не расходились между документами.
