"""Add reliable supplier reservation storage.

Revision ID: 20260728_0001
Revises:
Create Date: 2026-07-28
"""

from __future__ import annotations

from collections.abc import Callable

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260728_0001"
down_revision = None
branch_labels = None
depends_on = None


def _table_names() -> set[str]:
    return set(sa.inspect(op.get_bind()).get_table_names())


def _ensure_legacy_tables() -> None:
    tables = _table_names()
    if "users" not in tables:
        op.create_table(
            "users",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("full_name", sa.String(255), nullable=False),
            sa.Column("phone_number", sa.String(30), nullable=False),
            sa.Column("email", sa.String(255), nullable=False),
            sa.Column("birth_date", sa.Date(), nullable=False),
            sa.Column("city", sa.String(120), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("phone_number", name="uq_users_phone_number"),
        )
        op.create_index("ix_users_id", "users", ["id"])
        op.create_index("ix_users_email", "users", ["email"], unique=True)

    tables = _table_names()
    if "products" not in tables:
        op.create_table(
            "products",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("supplier_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=False),
            sa.Column("name", sa.String(255), nullable=False),
            sa.Column("description", sa.String(1000), nullable=True),
            sa.Column("price", sa.Float(), nullable=False),
            sa.Column("stocks", sa.Integer(), nullable=False),
            sa.Column("reserved_stocks", sa.Integer(), server_default="0", nullable=False),
            sa.Column("is_active", sa.Boolean(), server_default=sa.true(), nullable=False),
            sa.Column("is_archived", sa.Boolean(), server_default=sa.false(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.CheckConstraint("stocks >= 0", name="ck_products_products_stocks_non_negative"),
            sa.CheckConstraint(
                "reserved_stocks >= 0",
                name="ck_products_products_reserved_stocks_non_negative",
            ),
            sa.CheckConstraint(
                "reserved_stocks <= stocks",
                name="ck_products_products_reserved_not_above_total",
            ),
        )
        op.create_index("ix_products_id", "products", ["id"])
        op.create_index("ix_products_supplier_id", "products", ["supplier_id"])
    else:
        columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("products")}
        if "reserved_stocks" not in columns:
            op.add_column(
                "products",
                sa.Column("reserved_stocks", sa.Integer(), server_default="0", nullable=False),
            )
        constraint_names = {
            constraint["name"]
            for constraint in sa.inspect(op.get_bind()).get_check_constraints("products")
        }
        constraints: tuple[tuple[str, str], ...] = (
            ("ck_products_products_stocks_non_negative", "stocks >= 0"),
            ("ck_products_products_reserved_stocks_non_negative", "reserved_stocks >= 0"),
            ("ck_products_products_reserved_not_above_total", "reserved_stocks <= stocks"),
        )
        for name, condition in constraints:
            if name not in constraint_names:
                op.create_check_constraint(name, "products", condition)

    tables = _table_names()
    if "warehouses" not in tables:
        op.create_table(
            "warehouses",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("name", sa.String(255), nullable=False),
            sa.Column("weekday_hours", sa.String(255), nullable=False),
            sa.Column("address", sa.String(500), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        )
        op.create_index("ix_warehouses_id", "warehouses", ["id"])

    tables = _table_names()
    if "warehouse_products" not in tables:
        op.create_table(
            "warehouse_products",
            sa.Column("warehouse_id", sa.Integer(), sa.ForeignKey("warehouses.id"), primary_key=True),
            sa.Column("product_id", sa.Integer(), sa.ForeignKey("products.id"), primary_key=True),
            sa.Column("stocks", sa.Integer(), nullable=False),
            sa.CheckConstraint(
                "stocks >= 0",
                name="ck_warehouse_products_warehouse_products_stocks_non_negative",
            ),
        )
    else:
        constraint_names = {
            constraint["name"]
            for constraint in sa.inspect(op.get_bind()).get_check_constraints("warehouse_products")
        }
        name = "ck_warehouse_products_warehouse_products_stocks_non_negative"
        if name not in constraint_names:
            op.create_check_constraint(name, "warehouse_products", "stocks >= 0")

    if "processed_events" not in _table_names():
        op.create_table(
            "processed_events",
            sa.Column("event_id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column("processed_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        )


def _create_if_missing(table_name: str, creator: Callable[[], None]) -> None:
    if table_name not in _table_names():
        creator()


def upgrade() -> None:
    _ensure_legacy_tables()

    def create_inbox() -> None:
        op.create_table(
            "supplier_inbox",
            sa.Column("event_id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column("consumer_name", sa.String(100), primary_key=True),
            sa.Column("event_type", sa.String(64), nullable=False),
            sa.Column("aggregate_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("payload_hash", sa.String(64), nullable=False),
            sa.Column("status", sa.String(20), nullable=False),
            sa.Column("attempt_count", sa.Integer(), server_default="1", nullable=False),
            sa.Column("received_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_error", sa.String(1000), nullable=True),
            sa.CheckConstraint(
                "attempt_count >= 1",
                name="ck_supplier_inbox_supplier_inbox_attempt_count_positive",
            ),
            sa.CheckConstraint(
                "payload_hash ~ '^[0-9a-f]{64}$'",
                name="ck_supplier_inbox_supplier_inbox_payload_hash_format",
            ),
            sa.CheckConstraint(
                "status IN ('PROCESSING','PROCESSED','FAILED_RETRYABLE','DLQ')",
                name="ck_supplier_inbox_supplier_inbox_status",
            ),
        )
        op.create_index(
            "ix_supplier_inbox_status_received",
            "supplier_inbox",
            ["status", "received_at"],
        )

    _create_if_missing("supplier_inbox", create_inbox)

    def create_reservations() -> None:
        op.create_table(
            "stock_reservations",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column("reservation_request_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("order_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("supplier_id", sa.BigInteger(), nullable=False),
            sa.Column("status", sa.String(16), nullable=False),
            sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("request_hash", sa.String(64), nullable=False),
            sa.Column("failure_reason", sa.String(64), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
            sa.UniqueConstraint(
                "reservation_request_id",
                name="uq_stock_reservations_request",
            ),
            sa.UniqueConstraint("order_id", name="uq_stock_reservations_order"),
            sa.CheckConstraint(
                "supplier_id > 0",
                name="ck_stock_reservations_stock_reservations_supplier_positive",
            ),
            sa.CheckConstraint(
                "status IN ('RESERVED','REJECTED')",
                name="ck_stock_reservations_stock_reservations_status",
            ),
            sa.CheckConstraint(
                """
                (status = 'RESERVED' AND failure_reason IS NULL)
                OR (status = 'REJECTED' AND failure_reason IS NOT NULL)
                """,
                name="ck_stock_reservations_stock_reservations_failure_by_status",
            ),
        )
        op.create_index(
            "ix_stock_reservations_supplier_created",
            "stock_reservations",
            ["supplier_id", "created_at"],
        )

    _create_if_missing("stock_reservations", create_reservations)

    def create_reservation_items() -> None:
        op.create_table(
            "stock_reservation_items",
            sa.Column(
                "reservation_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("stock_reservations.id", ondelete="CASCADE"),
                primary_key=True,
            ),
            sa.Column("product_id", sa.BigInteger(), primary_key=True),
            sa.Column("requested_quantity", sa.Integer(), nullable=False),
            sa.Column("reserved_quantity", sa.Integer(), server_default="0", nullable=False),
            sa.Column("available_quantity", sa.Integer(), nullable=True),
            sa.CheckConstraint(
                "product_id > 0",
                name="ck_stock_reservation_items_stock_reservation_items_product_positive",
            ),
            sa.CheckConstraint(
                "requested_quantity > 0",
                name="ck_stock_reservation_items_stock_reservation_items_requested_positive",
            ),
            sa.CheckConstraint(
                "reserved_quantity >= 0",
                name="ck_stock_reservation_items_stock_reservation_items_reserved_non_negative",
            ),
            sa.CheckConstraint(
                "reserved_quantity <= requested_quantity",
                name="ck_stock_reservation_items_stock_reservation_items_reserved_not_above_requested",
            ),
        )

    _create_if_missing("stock_reservation_items", create_reservation_items)

    def create_outbox() -> None:
        op.create_table(
            "supplier_outbox",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column("aggregate_type", sa.String(32), server_default="ORDER", nullable=False),
            sa.Column("aggregate_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("event_type", sa.String(64), nullable=False),
            sa.Column("event_version", sa.SmallInteger(), server_default="1", nullable=False),
            sa.Column("payload", postgresql.JSONB(), nullable=False),
            sa.Column("headers", postgresql.JSONB(), nullable=False),
            sa.Column("status", sa.String(16), server_default="PENDING", nullable=False),
            sa.Column("attempt_count", sa.Integer(), server_default="0", nullable=False),
            sa.Column("next_attempt_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_error", sa.String(1000), nullable=True),
            sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("locked_by", sa.String(100), nullable=True),
            sa.CheckConstraint(
                "aggregate_type = 'ORDER'",
                name="ck_supplier_outbox_supplier_outbox_aggregate_type",
            ),
            sa.CheckConstraint(
                "event_version >= 1",
                name="ck_supplier_outbox_supplier_outbox_event_version_positive",
            ),
            sa.CheckConstraint(
                "attempt_count >= 0",
                name="ck_supplier_outbox_supplier_outbox_attempt_count_non_negative",
            ),
            sa.CheckConstraint(
                "status IN ('PENDING','IN_PROGRESS','PUBLISHED','FAILED')",
                name="ck_supplier_outbox_supplier_outbox_status",
            ),
        )
        op.create_index(
            "ix_supplier_outbox_status_next_created",
            "supplier_outbox",
            ["status", "next_attempt_at", "created_at"],
        )
        op.create_index(
            "ix_supplier_outbox_aggregate_created",
            "supplier_outbox",
            ["aggregate_id", "created_at"],
        )

    _create_if_missing("supplier_outbox", create_outbox)


def downgrade() -> None:
    for table_name in (
        "supplier_outbox",
        "stock_reservation_items",
        "stock_reservations",
        "supplier_inbox",
    ):
        if table_name in _table_names():
            op.drop_table(table_name)

    if "products" in _table_names():
        constraints = {
            constraint["name"]
            for constraint in sa.inspect(op.get_bind()).get_check_constraints("products")
        }
        for name in (
            "ck_products_products_reserved_not_above_total",
            "ck_products_products_reserved_stocks_non_negative",
        ):
            if name in constraints:
                op.drop_constraint(name, "products", type_="check")
        columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("products")}
        if "reserved_stocks" in columns:
            op.drop_column("products", "reserved_stocks")
