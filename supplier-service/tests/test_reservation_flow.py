from __future__ import annotations

import copy
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

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
from app.services.reservation import (
    ReservationService,
    parse_reservation_command,
)

NOW = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
CONSUMER_NAME = "supplier-service-stock-commands-v1"


def _envelope(
    *,
    event_id: uuid.UUID | None = None,
    order_id: uuid.UUID | None = None,
    request_id: uuid.UUID | None = None,
    supplier_id: int = 201,
    items: list[dict] | None = None,
    deadline: datetime | None = None,
) -> tuple[dict, dict[str, str | None]]:
    event_id = event_id or uuid.uuid4()
    order_id = order_id or uuid.uuid4()
    request_id = request_id or uuid.uuid4()
    correlation_id = uuid.uuid4()
    envelope = {
        "event_id": str(event_id),
        "event_type": "StockReservationRequested",
        "event_version": 1,
        "occurred_at": _timestamp(NOW),
        "producer": "order-service",
        "aggregate_type": "ORDER",
        "aggregate_id": str(order_id),
        "correlation_id": str(correlation_id),
        "causation_id": None,
        "payload": {
            "order_id": str(order_id),
            "customer_id": 101,
            "supplier_id": supplier_id,
            "order_version": 1,
            "reservation_request_id": str(request_id),
            "items": items or [{"product_id": 1001, "quantity": 1}],
            "reservation_deadline_at": _timestamp(deadline or NOW + timedelta(seconds=30)),
            "correlation_id": str(correlation_id),
        },
    }
    headers = {
        "event_id": str(event_id),
        "event_type": "StockReservationRequested",
        "event_version": "1",
        "correlation_id": str(correlation_id),
        "causation_id": None,
        "producer": "order-service",
        "content_type": "application/json",
    }
    return envelope, headers


def _command(**kwargs):
    envelope, headers = _envelope(**kwargs)
    return parse_reservation_command(envelope, headers=headers)


def _seed_products(
    factory,
    *,
    products: list[dict] | None = None,
) -> None:
    if products is None:
        products = [
            {
                "id": 1001,
                "supplier_id": 201,
                "stocks": 10,
                "is_active": True,
                "is_archived": False,
            }
        ]
    supplier_ids = sorted({item["supplier_id"] for item in products} | {201, 202})
    with factory.begin() as session:
        for supplier_id in supplier_ids:
            session.add(
                Supplier(
                    id=supplier_id,
                    full_name=f"Supplier {supplier_id}",
                    phone_number=f"+7999000{supplier_id:04d}",
                    email=f"supplier-{supplier_id}@example.test",
                    birth_date=date(1990, 1, 1),
                    city="Moscow",
                    created_at=NOW,
                    updated_at=NOW,
                )
            )
        session.flush()
        for item in products:
            session.add(
                Product(
                    id=item["id"],
                    supplier_id=item["supplier_id"],
                    name=f"Product {item['id']}",
                    description=None,
                    price=100.0,
                    stocks=item["stocks"],
                    reserved_stocks=item.get("reserved_stocks", 0),
                    is_active=item.get("is_active", True),
                    is_archived=item.get("is_archived", False),
                    created_at=NOW,
                )
            )


def _service(factory, **kwargs) -> ReservationService:
    return ReservationService(
        session_factory=factory,
        consumer_name=CONSUMER_NAME,
        clock=lambda: NOW,
        **kwargs,
    )


@pytest.mark.integration
def test_successfully_reserves_one_product(supplier_session_factory) -> None:
    _seed_products(supplier_session_factory)
    command = _command(items=[{"product_id": 1001, "quantity": 3}])

    result = _service(supplier_session_factory).process(command)

    assert result.result == "RESERVED"
    with supplier_session_factory() as session:
        product = session.get(Product, 1001)
        assert product.stocks == 10
        assert product.reserved_stocks == 3
        assert product.available_stocks == 7


@pytest.mark.integration
def test_successfully_reserves_multiple_products(supplier_session_factory) -> None:
    _seed_products(
        supplier_session_factory,
        products=[
            {"id": 1001, "supplier_id": 201, "stocks": 5},
            {"id": 1002, "supplier_id": 201, "stocks": 8},
        ],
    )
    command = _command(
        items=[
            {"product_id": 1002, "quantity": 4},
            {"product_id": 1001, "quantity": 2},
        ]
    )

    result = _service(supplier_session_factory).process(command)

    assert result.result == "RESERVED"
    with supplier_session_factory() as session:
        assert session.get(Product, 1001).reserved_stocks == 2
        assert session.get(Product, 1002).reserved_stocks == 4


@pytest.mark.integration
def test_insufficient_stock_creates_business_rejection(supplier_session_factory) -> None:
    _seed_products(
        supplier_session_factory,
        products=[{"id": 1001, "supplier_id": 201, "stocks": 1}],
    )

    result = _service(supplier_session_factory).process(
        _command(items=[{"product_id": 1001, "quantity": 2}])
    )

    assert result.result == "REJECTED"
    assert result.reason_code == "INSUFFICIENT_STOCK"
    with supplier_session_factory() as session:
        assert session.get(Product, 1001).reserved_stocks == 0


@pytest.mark.integration
def test_one_insufficient_item_rolls_back_entire_reservation(supplier_session_factory) -> None:
    _seed_products(
        supplier_session_factory,
        products=[
            {"id": 1001, "supplier_id": 201, "stocks": 10},
            {"id": 1002, "supplier_id": 201, "stocks": 1},
        ],
    )
    command = _command(
        items=[
            {"product_id": 1001, "quantity": 4},
            {"product_id": 1002, "quantity": 2},
        ]
    )

    result = _service(supplier_session_factory).process(command)

    assert result.reason_code == "INSUFFICIENT_STOCK"
    with supplier_session_factory() as session:
        assert session.get(Product, 1001).reserved_stocks == 0
        assert session.get(Product, 1002).reserved_stocks == 0


@pytest.mark.integration
@pytest.mark.parametrize(
    ("products", "supplier_id", "product_id", "expected_reason"),
    [
        ([], 201, 9999, "PRODUCT_NOT_FOUND"),
        (
            [{"id": 1001, "supplier_id": 201, "stocks": 5, "is_active": False}],
            201,
            1001,
            "PRODUCT_INACTIVE",
        ),
        (
            [
                {
                    "id": 1001,
                    "supplier_id": 201,
                    "stocks": 5,
                    "is_active": False,
                    "is_archived": True,
                }
            ],
            201,
            1001,
            "PRODUCT_ARCHIVED",
        ),
        (
            [{"id": 1001, "supplier_id": 202, "stocks": 5}],
            201,
            1001,
            "SUPPLIER_MISMATCH",
        ),
    ],
)
def test_product_business_rejection_reasons(
    supplier_session_factory,
    products,
    supplier_id,
    product_id,
    expected_reason,
) -> None:
    _seed_products(supplier_session_factory, products=products)

    result = _service(supplier_session_factory).process(
        _command(
            supplier_id=supplier_id,
            items=[{"product_id": product_id, "quantity": 1}],
        )
    )

    assert result.result == "REJECTED"
    assert result.reason_code == expected_reason


@pytest.mark.integration
def test_invalid_quantity_is_business_rejection(supplier_session_factory) -> None:
    _seed_products(supplier_session_factory)

    result = _service(supplier_session_factory).process(
        _command(items=[{"product_id": 1001, "quantity": 0}])
    )

    assert result.result == "REJECTED"
    assert result.reason_code == "INVALID_REQUEST"
    with supplier_session_factory() as session:
        assert session.get(Product, 1001).reserved_stocks == 0


@pytest.mark.integration
def test_expired_deadline_is_business_rejection(supplier_session_factory) -> None:
    _seed_products(supplier_session_factory)

    result = _service(supplier_session_factory).process(
        _command(deadline=NOW - timedelta(milliseconds=1))
    )

    assert result.result == "REJECTED"
    assert result.reason_code == "RESERVATION_DEADLINE_EXPIRED"


@pytest.mark.integration
def test_duplicate_event_id_does_not_reserve_twice(supplier_session_factory) -> None:
    _seed_products(supplier_session_factory)
    command = _command(items=[{"product_id": 1001, "quantity": 2}])
    service = _service(supplier_session_factory)

    first = service.process(command)
    duplicate = service.process(command)

    assert first.result == "RESERVED"
    assert duplicate.result == "DUPLICATE_EVENT"
    with supplier_session_factory() as session:
        assert session.get(Product, 1001).reserved_stocks == 2
        assert session.scalar(select(func.count()).select_from(StockReservation)) == 1
        assert session.scalar(select(func.count()).select_from(SupplierOutbox)) == 1


@pytest.mark.integration
def test_new_event_id_with_same_request_id_does_not_reserve_twice(
    supplier_session_factory,
) -> None:
    _seed_products(supplier_session_factory)
    request_id = uuid.uuid4()
    order_id = uuid.uuid4()
    first = _command(
        request_id=request_id,
        order_id=order_id,
        items=[{"product_id": 1001, "quantity": 2}],
    )
    second = _command(
        event_id=uuid.uuid4(),
        request_id=request_id,
        order_id=order_id,
        items=[{"product_id": 1001, "quantity": 2}],
    )
    service = _service(supplier_session_factory)

    service.process(first)
    replay = service.process(second)

    assert replay.result == "DUPLICATE_RESERVATION_REQUEST"
    with supplier_session_factory() as session:
        assert session.get(Product, 1001).reserved_stocks == 2
        assert session.scalar(select(func.count()).select_from(StockReservation)) == 1
        assert session.scalar(select(func.count()).select_from(SupplierOutbox)) == 1
        assert session.scalar(select(func.count()).select_from(SupplierInbox)) == 2


@pytest.mark.integration
def test_concurrent_duplicate_event_is_processed_once(supplier_session_factory) -> None:
    _seed_products(supplier_session_factory)
    command = _command(items=[{"product_id": 1001, "quantity": 2}])
    service = _service(supplier_session_factory)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: service.process(command).result, range(2)))

    assert sorted(results) == ["DUPLICATE_EVENT", "RESERVED"]
    with supplier_session_factory() as session:
        assert session.get(Product, 1001).reserved_stocks == 2


@pytest.mark.integration
def test_two_orders_compete_for_last_item_and_only_one_succeeds(
    supplier_session_factory,
) -> None:
    _seed_products(
        supplier_session_factory,
        products=[{"id": 1001, "supplier_id": 201, "stocks": 1}],
    )
    commands = [_command(), _command()]
    service = _service(supplier_session_factory)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(service.process, commands))

    assert sorted(result.result for result in results) == ["REJECTED", "RESERVED"]
    assert next(result for result in results if result.result == "REJECTED").reason_code == "INSUFFICIENT_STOCK"
    with supplier_session_factory() as session:
        product = session.get(Product, 1001)
        assert product.reserved_stocks == 1
        assert product.available_stocks == 0


@pytest.mark.integration
def test_reservation_items_and_success_outbox_are_atomic(supplier_session_factory) -> None:
    _seed_products(
        supplier_session_factory,
        products=[
            {"id": 1001, "supplier_id": 201, "stocks": 5},
            {"id": 1002, "supplier_id": 201, "stocks": 5},
        ],
    )
    command = _command(
        items=[
            {"product_id": 1001, "quantity": 2},
            {"product_id": 1002, "quantity": 3},
        ]
    )

    result = _service(supplier_session_factory).process(command)

    with supplier_session_factory() as session:
        reservation = session.get(StockReservation, result.reservation_id)
        items = list(
            session.scalars(
                select(StockReservationItem)
                .where(StockReservationItem.reservation_id == result.reservation_id)
                .order_by(StockReservationItem.product_id)
            )
        )
        outbox = session.get(SupplierOutbox, result.result_event_id)
        inbox = session.get(SupplierInbox, (command.event_id, CONSUMER_NAME))
        assert reservation.status == "RESERVED"
        assert [(item.product_id, item.reserved_quantity) for item in items] == [
            (1001, 2),
            (1002, 3),
        ]
        assert outbox.event_type == "StockReservationSucceeded"
        assert outbox.payload["payload"]["reserved_items"] == [
            {"product_id": 1001, "quantity": 2},
            {"product_id": 1002, "quantity": 3},
        ]
        assert inbox.status == "PROCESSED"


@pytest.mark.integration
def test_rejection_record_items_and_outbox_are_atomic(supplier_session_factory) -> None:
    _seed_products(
        supplier_session_factory,
        products=[{"id": 1001, "supplier_id": 201, "stocks": 1}],
    )
    command = _command(items=[{"product_id": 1001, "quantity": 2}])

    result = _service(supplier_session_factory).process(command)

    with supplier_session_factory() as session:
        reservation = session.get(StockReservation, result.reservation_id)
        item = session.get(StockReservationItem, (result.reservation_id, 1001))
        outbox = session.get(SupplierOutbox, result.result_event_id)
        assert reservation.status == "REJECTED"
        assert reservation.failure_reason == "INSUFFICIENT_STOCK"
        assert item.requested_quantity == 2
        assert item.reserved_quantity == 0
        assert item.available_quantity == 1
        assert outbox.event_type == "StockReservationFailed"
        assert outbox.payload["payload"]["reason_code"] == "INSUFFICIENT_STOCK"


@pytest.mark.integration
def test_outbox_failure_rolls_back_inbox_reservation_items_and_stock(
    supplier_session_factory,
) -> None:
    _seed_products(supplier_session_factory)
    command = _command(items=[{"product_id": 1001, "quantity": 2}])

    def fail_outbox(**_kwargs):
        raise RuntimeError("simulated outbox failure")

    with pytest.raises(RuntimeError, match="simulated outbox failure"):
        _service(supplier_session_factory, outbox_builder=fail_outbox).process(command)

    with supplier_session_factory() as session:
        assert session.get(Product, 1001).reserved_stocks == 0
        assert session.scalar(select(func.count()).select_from(SupplierInbox)) == 0
        assert session.scalar(select(func.count()).select_from(StockReservation)) == 0
        assert session.scalar(select(func.count()).select_from(StockReservationItem)) == 0
        assert session.scalar(select(func.count()).select_from(SupplierOutbox)) == 0


@pytest.mark.integration
def test_technical_failure_is_recorded_retryable(supplier_session_factory) -> None:
    _seed_products(supplier_session_factory)
    command = _command()
    service = _service(supplier_session_factory)

    service.record_retryable_failure(
        command=command,
        safe_error="Reservation processing failed",
        attempt=2,
    )

    with supplier_session_factory() as session:
        inbox = session.get(SupplierInbox, (command.event_id, CONSUMER_NAME))
        assert inbox.status == "FAILED_RETRYABLE"
        assert inbox.attempt_count == 2
        assert inbox.processed_at is None
        assert inbox.last_error == "Reservation processing failed"


def test_envelope_and_headers_are_not_mutated() -> None:
    envelope, headers = _envelope()
    original_envelope = copy.deepcopy(envelope)
    original_headers = copy.deepcopy(headers)

    parse_reservation_command(envelope, headers=headers)

    assert envelope == original_envelope
    assert headers == original_headers


def _timestamp(value: datetime) -> str:
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")
