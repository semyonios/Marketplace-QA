"""add stock finalization and release lifecycle

Revision ID: 20260728_0002
Revises: 20260728_0001
Create Date: 2026-07-28
"""

from alembic import op
import sqlalchemy as sa


revision = "20260728_0002"
down_revision = "20260728_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "ck_stock_reservations_stock_reservations_status",
        "stock_reservations",
        type_="check",
    )
    op.drop_constraint(
        "ck_stock_reservations_stock_reservations_failure_by_status",
        "stock_reservations",
        type_="check",
    )
    op.add_column(
        "stock_reservations",
        sa.Column("finalization_request_id", sa.UUID(), nullable=True),
    )
    op.add_column(
        "stock_reservations",
        sa.Column("release_request_id", sa.UUID(), nullable=True),
    )
    op.add_column(
        "stock_reservations",
        sa.Column("finalized_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "stock_reservations",
        sa.Column("released_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        "ck_stock_reservations_stock_reservations_status",
        "stock_reservations",
        "status IN ('RESERVED','REJECTED','FINALIZED','RELEASED')",
    )
    op.create_check_constraint(
        "ck_stock_reservations_stock_reservations_failure_by_status",
        "stock_reservations",
        """
        (status = 'REJECTED' AND failure_reason IS NOT NULL)
        OR (status <> 'REJECTED' AND failure_reason IS NULL)
        """,
    )
    op.create_index(
        "uq_stock_reservations_finalization_request",
        "stock_reservations",
        ["finalization_request_id"],
        unique=True,
        postgresql_where=sa.text("finalization_request_id IS NOT NULL"),
    )
    op.create_index(
        "uq_stock_reservations_release_request",
        "stock_reservations",
        ["release_request_id"],
        unique=True,
        postgresql_where=sa.text("release_request_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_stock_reservations_release_request",
        table_name="stock_reservations",
    )
    op.drop_index(
        "uq_stock_reservations_finalization_request",
        table_name="stock_reservations",
    )
    op.drop_constraint(
        "ck_stock_reservations_stock_reservations_failure_by_status",
        "stock_reservations",
        type_="check",
    )
    op.drop_constraint(
        "ck_stock_reservations_stock_reservations_status",
        "stock_reservations",
        type_="check",
    )
    op.drop_column("stock_reservations", "released_at")
    op.drop_column("stock_reservations", "finalized_at")
    op.drop_column("stock_reservations", "release_request_id")
    op.drop_column("stock_reservations", "finalization_request_id")
    op.create_check_constraint(
        "ck_stock_reservations_stock_reservations_status",
        "stock_reservations",
        "status IN ('RESERVED','REJECTED')",
    )
    op.create_check_constraint(
        "ck_stock_reservations_stock_reservations_failure_by_status",
        "stock_reservations",
        """
        (status = 'RESERVED' AND failure_reason IS NULL)
        OR (status = 'REJECTED' AND failure_reason IS NOT NULL)
        """,
    )
