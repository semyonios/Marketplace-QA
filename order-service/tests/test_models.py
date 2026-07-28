from sqlalchemy import CheckConstraint, Numeric, PrimaryKeyConstraint, UniqueConstraint

from app.database import Base
from app.enums import OrderStatus
from app.models import Order, OrderIdempotency, OrderInbox, OrderItem, OrderStatusHistory


EXPECTED_TABLES = {
    "orders",
    "order_items",
    "order_status_history",
    "order_idempotency",
    "order_outbox",
    "order_inbox",
}


def test_metadata_contains_six_contract_tables() -> None:
    assert set(Base.metadata.tables) == EXPECTED_TABLES


def test_public_order_status_enum_contains_expected_values() -> None:
    assert {status.value for status in OrderStatus} == {
        "PENDING_RESERVATION",
        "RESERVED",
        "CONFIRMED",
        "REJECTION_PENDING",
        "REJECTED",
        "CANCELLATION_PENDING",
        "CANCELLED",
        "FAILED",
    }


def test_money_columns_use_numeric_not_float() -> None:
    for column in (
        Order.__table__.c.total_amount,
        OrderItem.__table__.c.unit_price,
        OrderItem.__table__.c.line_total,
    ):
        assert isinstance(column.type, Numeric)
        assert column.type.precision == 19
        assert column.type.scale == 2
        assert column.type.asdecimal is True


def test_idempotency_has_contract_unique_constraint() -> None:
    unique_columns = {
        tuple(constraint.columns.keys())
        for constraint in OrderIdempotency.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert ("customer_id", "operation_type", "key_hash") in unique_columns
    assert "idempotency_key" not in OrderIdempotency.__table__.c


def test_inbox_composite_primary_key_provides_deduplication() -> None:
    primary_key = next(
        constraint
        for constraint in OrderInbox.__table__.constraints
        if isinstance(constraint, PrimaryKeyConstraint)
    )
    assert tuple(primary_key.columns.keys()) == ("event_id", "consumer_name")


def test_version_has_default_and_positive_check_constraint() -> None:
    version = Order.__table__.c.version
    assert version.default is not None
    assert version.default.arg == 1
    assert version.server_default is not None

    checks = {
        constraint.name: str(constraint.sqltext)
        for constraint in Order.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert any("version >= 1" in expression for expression in checks.values())


def test_snapshot_name_and_history_initial_state_have_contract_checks() -> None:
    item_checks = {
        str(constraint.sqltext)
        for constraint in OrderItem.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert any("btrim(product_name_snapshot)" in expression for expression in item_checks)

    history_checks = {
        str(constraint.sqltext)
        for constraint in OrderStatusHistory.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert any("version_before = 0" in expression for expression in history_checks)
