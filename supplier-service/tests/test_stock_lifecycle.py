from __future__ import annotations

import uuid
from datetime import date, datetime, timezone

import pytest
from sqlalchemy import func, select

from app.models import (
    Product,
    StockReservation,
    StockReservationItem,
    Supplier,
    SupplierInbox,
    SupplierOutbox,
)
from app.services.stock_lifecycle import (
    StockLifecycleService,
    parse_stock_lifecycle_command,
)
from app.services import stock_lifecycle

NOW = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
CONSUMER = "supplier-service-stock-commands-v1"


def _seed(factory, *, stocks: int = 50, reserved: int = 3):
    order_id = uuid.uuid4()
    reservation_id = uuid.uuid4()
    request_id = uuid.uuid4()
    correlation_id = uuid.uuid4()
    with factory.begin() as session:
        session.add(
            Supplier(
                id=201,
                full_name="Supplier",
                phone_number="+79990000201",
                email="lifecycle@example.test",
                birth_date=date(1990, 1, 1),
                city="Moscow",
                created_at=NOW,
                updated_at=NOW,
            )
        )
        session.flush()
        session.add(
            Product(
                id=1001,
                supplier_id=201,
                name="Lifecycle product",
                price=10,
                stocks=stocks,
                reserved_stocks=reserved,
                is_active=True,
                is_archived=False,
                created_at=NOW,
            )
        )
        session.add(
            StockReservation(
                id=reservation_id,
                reservation_request_id=request_id,
                order_id=order_id,
                supplier_id=201,
                status="RESERVED",
                correlation_id=correlation_id,
                request_hash="a" * 64,
                created_at=NOW,
                updated_at=NOW,
            )
        )
        session.add(
            StockReservationItem(
                reservation_id=reservation_id,
                product_id=1001,
                requested_quantity=reserved,
                reserved_quantity=reserved,
                available_quantity=stocks - reserved,
            )
        )
    return order_id, reservation_id, request_id, correlation_id


def _command(
    seeded,
    *,
    event_type: str,
    event_id: uuid.UUID | None = None,
    operation_id: uuid.UUID | None = None,
    reservation_id: uuid.UUID | None | object = ...,
    supplier_id: int = 201,
):
    order_id, actual_reservation_id, request_id, correlation_id = seeded
    operation_id = operation_id or uuid.uuid4()
    operation_field = (
        "finalization_request_id"
        if event_type == "StockFinalizationRequested"
        else "release_request_id"
    )
    selected_reservation = (
        actual_reservation_id if reservation_id is ... else reservation_id
    )
    payload = {
        "order_id": str(order_id),
        "supplier_id": supplier_id,
        "reservation_request_id": str(request_id),
        "reservation_id": (
            str(selected_reservation) if selected_reservation is not None else None
        ),
        operation_field: str(operation_id),
        "correlation_id": str(correlation_id),
    }
    if event_type == "StockReleaseRequested":
        payload["reason"] = "CUSTOMER_CANCELLED"
    envelope = {
        "event_id": str(event_id or uuid.uuid4()),
        "event_type": event_type,
        "event_version": 1,
        "occurred_at": "2026-07-28T12:00:00.000Z",
        "producer": "order-service",
        "aggregate_type": "ORDER",
        "aggregate_id": str(order_id),
        "correlation_id": str(correlation_id),
        "causation_id": str(uuid.uuid4()),
        "payload": payload,
    }
    headers = {
        "event_id": envelope["event_id"],
        "event_type": event_type,
        "event_version": "1",
        "correlation_id": envelope["correlation_id"],
        "causation_id": envelope["causation_id"],
        "producer": "order-service",
        "content_type": "application/json",
    }
    return parse_stock_lifecycle_command(envelope, headers=headers)


def _service(factory):
    return StockLifecycleService(
        session_factory=factory,
        consumer_name=CONSUMER,
        clock=lambda: NOW,
    )


@pytest.mark.integration
def test_finalization_decrements_physical_and_reserved_stock(
    supplier_session_factory,
) -> None:
    seeded = _seed(supplier_session_factory)
    command = _command(seeded, event_type="StockFinalizationRequested")

    result = _service(supplier_session_factory).process(command)

    assert result.result == "FINALIZED"
    with supplier_session_factory() as session:
        product = session.get(Product, 1001)
        reservation = session.get(StockReservation, seeded[1])
        assert (product.stocks, product.reserved_stocks, product.available_stocks) == (
            47,
            0,
            47,
        )
        assert reservation.status == "FINALIZED"
        assert reservation.finalization_request_id == command.finalization_request_id
        assert session.scalar(select(func.count()).select_from(SupplierInbox)) == 1
        assert session.scalar(select(SupplierOutbox.event_type)) == "StockFinalized"


@pytest.mark.integration
def test_release_keeps_physical_stock_and_is_logically_idempotent(
    supplier_session_factory,
) -> None:
    seeded = _seed(supplier_session_factory)
    operation_id = uuid.uuid4()
    service = _service(supplier_session_factory)
    first = _command(
        seeded,
        event_type="StockReleaseRequested",
        operation_id=operation_id,
    )
    logical_duplicate = _command(
        seeded,
        event_type="StockReleaseRequested",
        operation_id=operation_id,
    )

    assert service.process(first).result == "RELEASED"
    assert service.process(first).result == "DUPLICATE_EVENT"
    assert service.process(logical_duplicate).result == "ALREADY_RELEASED"
    with supplier_session_factory() as session:
        product = session.get(Product, 1001)
        reservation = session.get(StockReservation, seeded[1])
        assert (product.stocks, product.reserved_stocks, product.available_stocks) == (
            50,
            0,
            50,
        )
        assert reservation.status == "RELEASED"


@pytest.mark.integration
def test_release_without_reservation_returns_no_reservation(
    supplier_session_factory,
) -> None:
    seeded = (uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), uuid.uuid4())
    command = _command(
        seeded,
        event_type="StockReleaseRequested",
        reservation_id=None,
    )

    result = _service(supplier_session_factory).process(command)

    assert result.result == "NO_RESERVATION"
    with supplier_session_factory() as session:
        outbox = session.scalar(select(SupplierOutbox))
        assert outbox.payload["payload"]["result"] == "NO_RESERVATION"


@pytest.mark.integration
def test_supplier_mismatch_has_no_stock_effect(supplier_session_factory) -> None:
    seeded = _seed(supplier_session_factory)
    command = _command(
        seeded,
        event_type="StockReleaseRequested",
        supplier_id=202,
    )

    result = _service(supplier_session_factory).process(command)

    assert result.result == "FAILED"
    assert result.reason_code == "SUPPLIER_MISMATCH"
    with supplier_session_factory() as session:
        product = session.get(Product, 1001)
        assert (product.stocks, product.reserved_stocks) == (50, 3)
        assert session.scalar(select(SupplierOutbox.event_type)) == "StockReleaseFailed"


@pytest.mark.integration
def test_lifecycle_rolls_back_stock_when_outbox_write_fails(
    supplier_session_factory,
    monkeypatch,
) -> None:
    seeded = _seed(supplier_session_factory)
    command = _command(seeded, event_type="StockFinalizationRequested")

    def fail(**_kwargs):
        raise RuntimeError("outbox failed")

    monkeypatch.setattr(stock_lifecycle, "_build_result_outbox", fail)
    with pytest.raises(RuntimeError, match="outbox failed"):
        _service(supplier_session_factory).process(command)

    with supplier_session_factory() as session:
        product = session.get(Product, 1001)
        reservation = session.get(StockReservation, seeded[1])
        assert (product.stocks, product.reserved_stocks) == (50, 3)
        assert reservation.status == "RESERVED"
        assert session.scalar(select(func.count()).select_from(SupplierInbox)) == 0
        assert session.scalar(select(func.count()).select_from(SupplierOutbox)) == 0
