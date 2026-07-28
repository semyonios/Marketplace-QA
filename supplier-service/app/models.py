import uuid
from datetime import date, datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


class Supplier(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    phone_number: Mapped[str] = mapped_column(String(30), nullable=False, unique=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    birth_date: Mapped[date] = mapped_column(Date, nullable=False)
    city: Mapped[str] = mapped_column(String(120), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class Product(Base):
    __tablename__ = "products"
    __table_args__ = (
        CheckConstraint("stocks >= 0", name="products_stocks_non_negative"),
        CheckConstraint("reserved_stocks >= 0", name="products_reserved_stocks_non_negative"),
        CheckConstraint("reserved_stocks <= stocks", name="products_reserved_not_above_total"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    supplier_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    stocks: Mapped[int] = mapped_column(Integer, nullable=False)
    reserved_stocks: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    is_active: Mapped[bool] = mapped_column(default=True, nullable=False)
    is_archived: Mapped[bool] = mapped_column(default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    @property
    def available_stocks(self) -> int:
        return self.stocks - self.reserved_stocks


class Warehouse(Base):
    __tablename__ = "warehouses"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    weekday_hours: Mapped[str] = mapped_column(String(255), nullable=False)
    address: Mapped[str] = mapped_column(String(500), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class WarehouseProduct(Base):
    __tablename__ = "warehouse_products"
    __table_args__ = (
        CheckConstraint("stocks >= 0", name="warehouse_products_stocks_non_negative"),
    )

    warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id"), primary_key=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"), primary_key=True)
    stocks: Mapped[int] = mapped_column(Integer, nullable=False)


class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class SupplierInbox(Base):
    __tablename__ = "supplier_inbox"
    __table_args__ = (
        CheckConstraint("attempt_count >= 1", name="supplier_inbox_attempt_count_positive"),
        CheckConstraint("payload_hash ~ '^[0-9a-f]{64}$'", name="supplier_inbox_payload_hash_format"),
        CheckConstraint(
            "status IN ('PROCESSING','PROCESSED','FAILED_RETRYABLE','DLQ')",
            name="supplier_inbox_status",
        ),
        Index("ix_supplier_inbox_status_received", "status", "received_at"),
    )

    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    consumer_name: Mapped[str] = mapped_column(String(100), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    aggregate_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    correlation_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(1000), nullable=True)


class StockReservation(Base):
    __tablename__ = "stock_reservations"
    __table_args__ = (
        UniqueConstraint("reservation_request_id", name="uq_stock_reservations_request"),
        UniqueConstraint("order_id", name="uq_stock_reservations_order"),
        CheckConstraint("supplier_id > 0", name="stock_reservations_supplier_positive"),
        CheckConstraint(
            "status IN ('RESERVED','REJECTED')",
            name="stock_reservations_status",
        ),
        CheckConstraint(
            """
            (status = 'RESERVED' AND failure_reason IS NULL)
            OR (status = 'REJECTED' AND failure_reason IS NOT NULL)
            """,
            name="stock_reservations_failure_by_status",
        ),
        Index("ix_stock_reservations_supplier_created", "supplier_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    reservation_request_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    order_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    supplier_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    correlation_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    failure_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
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


class StockReservationItem(Base):
    __tablename__ = "stock_reservation_items"
    __table_args__ = (
        CheckConstraint("product_id > 0", name="stock_reservation_items_product_positive"),
        CheckConstraint(
            "requested_quantity > 0",
            name="stock_reservation_items_requested_positive",
        ),
        CheckConstraint(
            "reserved_quantity >= 0",
            name="stock_reservation_items_reserved_non_negative",
        ),
        CheckConstraint(
            "reserved_quantity <= requested_quantity",
            name="stock_reservation_items_reserved_not_above_requested",
        ),
    )

    reservation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("stock_reservations.id", ondelete="CASCADE"),
        primary_key=True,
    )
    product_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    requested_quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    reserved_quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    available_quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)


class SupplierOutbox(Base):
    __tablename__ = "supplier_outbox"
    __table_args__ = (
        CheckConstraint("aggregate_type = 'ORDER'", name="supplier_outbox_aggregate_type"),
        CheckConstraint("event_version >= 1", name="supplier_outbox_event_version_positive"),
        CheckConstraint("attempt_count >= 0", name="supplier_outbox_attempt_count_non_negative"),
        CheckConstraint(
            "status IN ('PENDING','IN_PROGRESS','PUBLISHED','FAILED')",
            name="supplier_outbox_status",
        ),
        Index("ix_supplier_outbox_status_next_created", "status", "next_attempt_at", "created_at"),
        Index("ix_supplier_outbox_aggregate_created", "aggregate_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    aggregate_type: Mapped[str] = mapped_column(String(32), nullable=False, default="ORDER", server_default="ORDER")
    aggregate_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    event_version: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=1, server_default="1")
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    headers: Mapped[dict] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="PENDING", server_default="PENDING")
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
