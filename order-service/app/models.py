from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    CHAR,
    CheckConstraint,
    DateTime,
    Enum as SAEnum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    SmallInteger,
    String,
    UniqueConstraint,
    event,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .database import Base
from .enums import (
    ActorType,
    BusinessStatus,
    FailurePhase,
    IdempotencyState,
    InboxStatus,
    OperationState,
    OrderStatus,
    OutboxStatus,
    ReservationState,
    TargetTerminalStatus,
)


def enum_type(enum_class, name: str, length: int = 32) -> SAEnum:
    return SAEnum(
        enum_class,
        name=name,
        native_enum=False,
        create_constraint=True,
        validate_strings=True,
        length=length,
    )


class Order(Base):
    __tablename__ = "orders"
    __table_args__ = (
        UniqueConstraint("customer_id", "cart_version", name="uq_orders_customer_cart_version"),
        UniqueConstraint("reservation_request_id", name="uq_orders_reservation_request_id"),
        CheckConstraint("customer_id > 0", name="orders_customer_id_positive"),
        CheckConstraint("supplier_id > 0", name="orders_supplier_id_positive"),
        CheckConstraint("cart_version > 0", name="orders_cart_version_positive"),
        CheckConstraint("total_amount > 0", name="orders_total_amount_positive"),
        CheckConstraint("currency = 'RUB'", name="orders_currency_rub"),
        CheckConstraint("version >= 1", name="orders_version_positive"),
        CheckConstraint("reservation_attempt_count >= 0", name="orders_reservation_attempt_count_non_negative"),
        CheckConstraint("release_attempt_count >= 0", name="orders_release_attempt_count_non_negative"),
        CheckConstraint(
            "(operation_state <> 'NONE') OR target_terminal_status IS NULL",
            name="orders_none_operation_has_no_target",
        ),
        CheckConstraint(
            """
            operation_state NOT IN ('CANCELLATION_PENDING', 'REJECTION_PENDING')
            OR (
                reservation_state = 'RELEASE_REQUESTED'
                AND release_request_id IS NOT NULL
                AND (
                    (operation_state = 'CANCELLATION_PENDING' AND target_terminal_status = 'CANCELLED')
                    OR (operation_state = 'REJECTION_PENDING' AND target_terminal_status = 'REJECTED')
                )
            )
            """,
            name="orders_pending_release_target",
        ),
        CheckConstraint(
            "business_status <> 'CONFIRMED' OR (operation_state = 'NONE' AND reservation_state = 'RESERVED')",
            name="orders_confirmed_state",
        ),
        CheckConstraint(
            "business_status <> 'CANCELLED' OR (operation_state = 'NONE' AND reservation_state = 'RELEASED')",
            name="orders_cancelled_state",
        ),
        CheckConstraint(
            """
            business_status <> 'REJECTED'
            OR (operation_state = 'NONE' AND reservation_state IN ('NOT_RESERVED', 'RELEASED'))
            """,
            name="orders_rejected_state",
        ),
        CheckConstraint(
            "operation_state <> 'FAILED' OR (failure_phase IS NOT NULL AND failure_reason_code IS NOT NULL)",
            name="orders_failed_has_reason",
        ),
        CheckConstraint(
            """
            (reservation_requested_at IS NULL AND reservation_deadline_at IS NULL)
            OR (
                reservation_requested_at IS NOT NULL
                AND reservation_deadline_at > reservation_requested_at
                AND reservation_attempt_count >= 1
            )
            """,
            name="orders_reservation_deadline_pair",
        ),
        CheckConstraint(
            """
            (release_requested_at IS NULL AND release_deadline_at IS NULL)
            OR (
                release_requested_at IS NOT NULL
                AND release_deadline_at > release_requested_at
                AND release_attempt_count >= 1
            )
            """,
            name="orders_release_deadline_pair",
        ),
        Index("ix_orders_customer_created_id", "customer_id", text("created_at DESC"), text("id DESC")),
        Index("ix_orders_supplier_created_id", "supplier_id", text("created_at DESC"), text("id DESC")),
        Index(
            "ix_orders_business_operation_created",
            "business_status",
            "operation_state",
            text("created_at DESC"),
        ),
        Index(
            "ix_orders_reservation_deadline_pending",
            "reservation_deadline_at",
            postgresql_where=text(
                "business_status = 'PENDING_RESERVATION' AND reservation_state IN ('REQUESTED', 'UNKNOWN')"
            ),
        ),
        Index(
            "ix_orders_release_deadline_pending",
            "release_deadline_at",
            postgresql_where=text(
                "operation_state IN ('CANCELLATION_PENDING', 'REJECTION_PENDING', 'FAILED')"
                " AND release_deadline_at IS NOT NULL"
            ),
        ),
        Index(
            "uq_orders_release_request_id",
            "release_request_id",
            unique=True,
            postgresql_where=text("release_request_id IS NOT NULL"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    supplier_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    cart_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    cart_version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    business_status: Mapped[BusinessStatus] = mapped_column(
        enum_type(BusinessStatus, "business_status_enum"),
        nullable=False,
    )
    operation_state: Mapped[OperationState] = mapped_column(
        enum_type(OperationState, "operation_state_enum"),
        nullable=False,
        default=OperationState.NONE,
        server_default=OperationState.NONE.value,
    )
    reservation_state: Mapped[ReservationState] = mapped_column(
        enum_type(ReservationState, "reservation_state_enum"),
        nullable=False,
    )
    target_terminal_status: Mapped[TargetTerminalStatus | None] = mapped_column(
        enum_type(TargetTerminalStatus, "target_terminal_status_enum", length=16),
        nullable=True,
    )
    rejection_reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    rejection_reason_text: Mapped[str | None] = mapped_column(String(500), nullable=True)
    cancellation_reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    cancellation_reason_text: Mapped[str | None] = mapped_column(String(500), nullable=True)
    total_amount: Mapped[Decimal] = mapped_column(Numeric(19, 2), nullable=False)
    currency: Mapped[str] = mapped_column(CHAR(3), nullable=False, default="RUB", server_default="RUB")
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, default=1, server_default="1")
    correlation_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    client_request_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    reservation_request_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    reservation_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    release_request_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    reservation_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reservation_deadline_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reservation_attempt_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    reservation_last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    release_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    release_deadline_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    release_attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    release_last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    failure_phase: Mapped[FailurePhase | None] = mapped_column(
        enum_type(FailurePhase, "failure_phase_enum", length=16),
        nullable=True,
    )
    failure_reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    failure_reason_text: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    items: Mapped[list["OrderItem"]] = relationship(back_populates="order")
    history: Mapped[list["OrderStatusHistory"]] = relationship(back_populates="order")
    idempotency_records: Mapped[list["OrderIdempotency"]] = relationship(back_populates="order")

    @property
    def status(self) -> OrderStatus:
        if self.operation_state != OperationState.NONE:
            return OrderStatus(self.operation_state.value)
        return OrderStatus(self.business_status.value)


class OrderItem(Base):
    __tablename__ = "order_items"
    __table_args__ = (
        UniqueConstraint("order_id", "product_id", name="uq_order_items_order_product"),
        CheckConstraint("product_id > 0", name="order_items_product_id_positive"),
        CheckConstraint(
            "btrim(product_name_snapshot) <> ''",
            name="order_items_product_name_non_blank",
        ),
        CheckConstraint("quantity > 0", name="order_items_quantity_positive"),
        CheckConstraint("unit_price >= 0", name="order_items_unit_price_non_negative"),
        CheckConstraint("line_total >= 0", name="order_items_line_total_non_negative"),
        CheckConstraint("supplier_id_snapshot > 0", name="order_items_supplier_id_positive"),
        CheckConstraint("currency = 'RUB'", name="order_items_currency_rub"),
        Index("ix_order_items_order_id", "order_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("orders.id", ondelete="RESTRICT"),
        nullable=False,
    )
    product_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    product_name_snapshot: Mapped[str] = mapped_column(String(255), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    unit_price: Mapped[Decimal] = mapped_column(Numeric(19, 2), nullable=False)
    line_total: Mapped[Decimal] = mapped_column(Numeric(19, 2), nullable=False)
    currency: Mapped[str] = mapped_column(CHAR(3), nullable=False)
    supplier_id_snapshot: Mapped[int] = mapped_column(BigInteger, nullable=False)

    order: Mapped[Order] = relationship(back_populates="items")


@event.listens_for(OrderItem, "before_update", propagate=True)
def prevent_order_item_update(_mapper, _connection, _target) -> None:
    raise ValueError("order item snapshots are immutable")


class OrderStatusHistory(Base):
    __tablename__ = "order_status_history"
    __table_args__ = (
        UniqueConstraint("order_id", "version_after", name="uq_order_status_history_order_version"),
        CheckConstraint("actor_id IS NULL OR actor_id > 0", name="order_status_history_actor_id_positive"),
        CheckConstraint("version_before >= 0", name="order_status_history_version_before_non_negative"),
        CheckConstraint("version_after >= 1", name="order_status_history_version_after_positive"),
        CheckConstraint("version_after > version_before", name="order_status_history_version_increases"),
        CheckConstraint(
            """
            (
                version_before = 0
                AND business_status_before IS NULL
                AND operation_state_before IS NULL
                AND reservation_state_before IS NULL
            )
            OR (
                version_before >= 1
                AND business_status_before IS NOT NULL
                AND operation_state_before IS NOT NULL
                AND reservation_state_before IS NOT NULL
            )
            """,
            name="order_status_history_before_state_consistency",
        ),
        Index("ix_order_status_history_order_created_id", "order_id", "created_at", "id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("orders.id", ondelete="RESTRICT"),
        nullable=False,
    )
    business_status_before: Mapped[BusinessStatus | None] = mapped_column(
        enum_type(BusinessStatus, "history_business_status_before_enum"),
        nullable=True,
    )
    business_status_after: Mapped[BusinessStatus] = mapped_column(
        enum_type(BusinessStatus, "history_business_status_after_enum"),
        nullable=False,
    )
    operation_state_before: Mapped[OperationState | None] = mapped_column(
        enum_type(OperationState, "history_operation_state_before_enum"),
        nullable=True,
    )
    operation_state_after: Mapped[OperationState] = mapped_column(
        enum_type(OperationState, "history_operation_state_after_enum"),
        nullable=False,
    )
    reservation_state_before: Mapped[ReservationState | None] = mapped_column(
        enum_type(ReservationState, "history_reservation_state_before_enum"),
        nullable=True,
    )
    reservation_state_after: Mapped[ReservationState] = mapped_column(
        enum_type(ReservationState, "history_reservation_state_after_enum"),
        nullable=False,
    )
    trigger: Mapped[str] = mapped_column(String(64), nullable=False)
    actor_type: Mapped[ActorType] = mapped_column(
        enum_type(ActorType, "actor_type_enum"),
        nullable=False,
    )
    actor_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    event_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    correlation_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    version_before: Mapped[int] = mapped_column(BigInteger, nullable=False)
    version_after: Mapped[int] = mapped_column(BigInteger, nullable=False)
    reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    reason_text: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    order: Mapped[Order] = relationship(back_populates="history")


@event.listens_for(OrderStatusHistory, "before_update", propagate=True)
def prevent_order_status_history_update(_mapper, _connection, _target) -> None:
    raise ValueError("order status history is append-only")


class OrderIdempotency(Base):
    __tablename__ = "order_idempotency"
    __table_args__ = (
        UniqueConstraint(
            "customer_id",
            "operation_type",
            "key_hash",
            name="uq_order_idempotency_customer_operation_key",
        ),
        CheckConstraint("customer_id > 0", name="order_idempotency_customer_id_positive"),
        CheckConstraint("operation_type = 'CREATE_ORDER'", name="order_idempotency_operation_type"),
        CheckConstraint("key_hash ~ '^[0-9a-f]{64}$'", name="order_idempotency_key_hash_format"),
        CheckConstraint(
            "request_fingerprint ~ '^[0-9a-f]{64}$'",
            name="order_idempotency_fingerprint_format",
        ),
        CheckConstraint(
            "response_status IS NULL OR response_status BETWEEN 100 AND 599",
            name="order_idempotency_response_status_range",
        ),
        Index("ix_order_idempotency_state_lease", "state", "lease_expires_at"),
        Index("ix_order_idempotency_expires_at", "expires_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    operation_type: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="CREATE_ORDER",
        server_default="CREATE_ORDER",
    )
    key_hash: Mapped[str] = mapped_column(CHAR(64), nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(CHAR(64), nullable=False)
    state: Mapped[IdempotencyState] = mapped_column(
        enum_type(IdempotencyState, "idempotency_state_enum"),
        nullable=False,
        default=IdempotencyState.IN_PROGRESS,
        server_default=IdempotencyState.IN_PROGRESS.value,
    )
    owner_token: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    order_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("orders.id", ondelete="RESTRICT"),
        nullable=True,
    )
    response_status: Mapped[int | None] = mapped_column(SmallInteger, nullable=True)
    last_error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    order: Mapped[Order | None] = relationship(back_populates="idempotency_records")


class OrderOutbox(Base):
    __tablename__ = "order_outbox"
    __table_args__ = (
        CheckConstraint("aggregate_type = 'ORDER'", name="order_outbox_aggregate_type"),
        CheckConstraint("event_version >= 1", name="order_outbox_event_version_positive"),
        CheckConstraint("attempt_count >= 0", name="order_outbox_attempt_count_non_negative"),
        Index("ix_order_outbox_status_next_created", "status", "next_attempt_at", "created_at"),
        Index("ix_order_outbox_aggregate_created", "aggregate_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    aggregate_type: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="ORDER",
        server_default="ORDER",
    )
    aggregate_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    event_version: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=1, server_default="1")
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    headers: Mapped[dict] = mapped_column(JSONB, nullable=False)
    status: Mapped[OutboxStatus] = mapped_column(
        enum_type(OutboxStatus, "outbox_status_enum", length=16),
        nullable=False,
        default=OutboxStatus.PENDING,
        server_default=OutboxStatus.PENDING.value,
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    locked_by: Mapped[str | None] = mapped_column(String(100), nullable=True)


class OrderInbox(Base):
    __tablename__ = "order_inbox"
    __table_args__ = (
        CheckConstraint("attempt_count >= 1", name="order_inbox_attempt_count_positive"),
        CheckConstraint("payload_hash ~ '^[0-9a-f]{64}$'", name="order_inbox_payload_hash_format"),
        Index("ix_order_inbox_status_received", "status", "received_at"),
    )

    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    consumer_name: Mapped[str] = mapped_column(String(100), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    aggregate_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    payload_hash: Mapped[str] = mapped_column(CHAR(64), nullable=False)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[InboxStatus] = mapped_column(
        enum_type(InboxStatus, "inbox_status_enum", length=16),
        nullable=False,
        default=InboxStatus.PROCESSING,
        server_default=InboxStatus.PROCESSING.value,
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    last_error: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    correlation_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
