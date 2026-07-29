"""add asynchronous order lifecycle operations

Revision ID: 0002
Revises: 0001
Create Date: 2026-07-28
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint("operation_state_enum", "orders", type_="check")
    op.drop_constraint(
        "history_operation_state_before_enum",
        "order_status_history",
        type_="check",
    )
    op.drop_constraint(
        "history_operation_state_after_enum",
        "order_status_history",
        type_="check",
    )
    op.drop_constraint("failure_phase_enum", "orders", type_="check")

    op.create_check_constraint(
        "operation_state_enum",
        "orders",
        "operation_state IN "
        "('NONE','CONFIRMATION_PENDING','CANCELLATION_PENDING','REJECTION_PENDING','FAILED')",
    )
    op.create_check_constraint(
        "history_operation_state_before_enum",
        "order_status_history",
        "operation_state_before IS NULL OR operation_state_before IN "
        "('NONE','CONFIRMATION_PENDING','CANCELLATION_PENDING','REJECTION_PENDING','FAILED')",
    )
    op.create_check_constraint(
        "history_operation_state_after_enum",
        "order_status_history",
        "operation_state_after IN "
        "('NONE','CONFIRMATION_PENDING','CANCELLATION_PENDING','REJECTION_PENDING','FAILED')",
    )
    op.create_check_constraint(
        "failure_phase_enum",
        "orders",
        "failure_phase IS NULL OR failure_phase IN "
        "('RESERVATION','CONFIRMATION','RELEASE')",
    )

    op.add_column("orders", sa.Column("finalization_request_id", sa.UUID(), nullable=True))
    op.add_column(
        "orders",
        sa.Column("finalization_requested_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "orders",
        sa.Column("finalization_deadline_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "orders",
        sa.Column(
            "finalization_attempt_count",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
    )
    op.add_column(
        "orders",
        sa.Column("finalization_last_attempt_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        "ck_orders_orders_finalization_attempt_count_non_negative",
        "orders",
        "finalization_attempt_count >= 0",
    )
    op.create_check_constraint(
        "ck_orders_orders_pending_confirmation",
        "orders",
        """
        operation_state <> 'CONFIRMATION_PENDING'
        OR (
            business_status = 'RESERVED'
            AND reservation_state = 'RESERVED'
            AND finalization_request_id IS NOT NULL
            AND target_terminal_status IS NULL
        )
        """,
    )
    op.create_check_constraint(
        "ck_orders_orders_finalization_deadline_pair",
        "orders",
        """
        (finalization_requested_at IS NULL AND finalization_deadline_at IS NULL)
        OR (
            finalization_requested_at IS NOT NULL
            AND finalization_deadline_at > finalization_requested_at
            AND finalization_attempt_count >= 1
        )
        """,
    )
    op.create_index(
        "ix_orders_finalization_deadline_pending",
        "orders",
        ["finalization_deadline_at"],
        postgresql_where=sa.text(
            "operation_state = 'CONFIRMATION_PENDING'"
            " AND finalization_deadline_at IS NOT NULL"
        ),
    )
    op.create_index(
        "uq_orders_finalization_request_id",
        "orders",
        ["finalization_request_id"],
        unique=True,
        postgresql_where=sa.text("finalization_request_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_orders_finalization_request_id", table_name="orders")
    op.drop_index("ix_orders_finalization_deadline_pending", table_name="orders")
    op.drop_constraint(
        "ck_orders_orders_finalization_deadline_pair",
        "orders",
        type_="check",
    )
    op.drop_constraint(
        "ck_orders_orders_pending_confirmation",
        "orders",
        type_="check",
    )
    op.drop_constraint(
        "ck_orders_orders_finalization_attempt_count_non_negative",
        "orders",
        type_="check",
    )
    op.drop_column("orders", "finalization_last_attempt_at")
    op.drop_column("orders", "finalization_attempt_count")
    op.drop_column("orders", "finalization_deadline_at")
    op.drop_column("orders", "finalization_requested_at")
    op.drop_column("orders", "finalization_request_id")

    op.drop_constraint("operation_state_enum", "orders", type_="check")
    op.drop_constraint(
        "history_operation_state_before_enum",
        "order_status_history",
        type_="check",
    )
    op.drop_constraint(
        "history_operation_state_after_enum",
        "order_status_history",
        type_="check",
    )
    op.drop_constraint("failure_phase_enum", "orders", type_="check")

    old_values_sql = "('NONE','CANCELLATION_PENDING','REJECTION_PENDING','FAILED')"
    for table_name, column_name, constraint_name, nullable in (
        ("orders", "operation_state", "operation_state_enum", False),
        (
            "order_status_history",
            "operation_state_before",
            "history_operation_state_before_enum",
            True,
        ),
        (
            "order_status_history",
            "operation_state_after",
            "history_operation_state_after_enum",
            False,
        ),
    ):
        null_clause = f"{column_name} IS NULL OR " if nullable else ""
        op.create_check_constraint(
            constraint_name,
            table_name,
            f"{null_clause}{column_name} IN {old_values_sql}",
        )
    op.create_check_constraint(
        "failure_phase_enum",
        "orders",
        "failure_phase IS NULL OR failure_phase IN ('RESERVATION','RELEASE')",
    )
