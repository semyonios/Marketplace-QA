from __future__ import annotations

import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import sessionmaker

from app.api.dependencies import get_db
from app.config import Settings
from app.database import Base
from app.errors import ServiceError
from app.main import create_app
from app.models import (
    Order,
    OrderIdempotency,
    OrderInbox,
    OrderItem,
    OrderOutbox,
    OrderStatusHistory,
)
from app.schemas import CartSnapshot, CartSnapshotItem, OrderResponse
from app.services import order_creation


class FakeCartClient:
    def __init__(
        self,
        snapshot: CartSnapshot | None = None,
        error: ServiceError | None = None,
    ) -> None:
        self.snapshot = snapshot or make_snapshot()
        self.error = error
        self.calls: list[dict] = []

    def get_cart_snapshot(self, **kwargs) -> CartSnapshot:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.snapshot

    def close(self) -> None:
        return None


class BlockingCartClient(FakeCartClient):
    def __init__(self) -> None:
        super().__init__()
        self.started = threading.Event()
        self.release = threading.Event()

    def get_cart_snapshot(self, **kwargs) -> CartSnapshot:
        self.calls.append(kwargs)
        self.started.set()
        assert self.release.wait(timeout=5)
        return self.snapshot


def make_snapshot(
    *,
    cart_version: int = 7,
    items: list[CartSnapshotItem] | None = None,
    supplier_id: int | None = 201,
) -> CartSnapshot:
    now = datetime.now(timezone.utc)
    return CartSnapshot(
        customer_id=101,
        cart_id=uuid.uuid4(),
        cart_version=cart_version,
        supplier_id=supplier_id,
        items=items
        if items is not None
        else [
            CartSnapshotItem(
                product_id=1001,
                product_name="QA Keyboard",
                supplier_id=201,
                quantity=2,
                unit_price=Decimal("1500.50"),
                currency="RUB",
                product_status="ACTIVE",
                projected_available_quantity=5,
                projection_updated_at=now,
            ),
            CartSnapshotItem(
                product_id=1002,
                product_name="QA Mouse",
                supplier_id=201,
                quantity=1,
                unit_price=Decimal("499.99"),
                currency="RUB",
                product_status="ACTIVE",
                projected_available_quantity=3,
                projection_updated_at=now,
            ),
        ],
        generated_at=now,
    )


@pytest.fixture
def order_session_factory():
    database_url = os.getenv("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required")

    schema_name = f"order_create_test_{uuid.uuid4().hex}"
    admin_engine = create_engine(database_url, future=True)
    with admin_engine.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))

    test_engine = create_engine(
        database_url,
        future=True,
        connect_args={"options": f"-csearch_path={schema_name}"},
    )
    Base.metadata.create_all(test_engine)
    factory = sessionmaker(bind=test_engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        test_engine.dispose()
        with admin_engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        admin_engine.dispose()


def make_client(factory, cart_client: FakeCartClient) -> TestClient:
    service_root = Path(__file__).resolve().parents[1]
    settings = Settings(
        service_name="order-service",
        environment="test",
        host="127.0.0.1",
        port=8000,
        database_url=os.environ["TEST_DATABASE_URL"],
        log_level="WARNING",
        kafka_bootstrap_servers="localhost:9092",
        customer_service_url="http://customer-service:8001",
        readiness_timeout_seconds=1.0,
        alembic_config=str(service_root / "alembic.ini"),
        customer_service_timeout_seconds=1.0,
    )
    application = create_app(settings, cart_snapshot_client=cart_client)

    def override_db():
        session = factory()
        try:
            yield session
        finally:
            session.close()

    application.dependency_overrides[get_db] = override_db
    return TestClient(application, raise_server_exceptions=False)


def create_headers(
    *,
    key: str = "checkout-101-v7-a",
    role: str = "CUSTOMER",
    subject_id: str = "101",
    correlation_id: str | None = None,
) -> dict[str, str]:
    headers = {
        "X-Test-Role": role,
        "X-Test-Subject-ID": subject_id,
        "Idempotency-Key": key,
    }
    if correlation_id is not None:
        headers["X-Correlation-ID"] = correlation_id
    return headers


def create_body(*, cart_version: int = 7, client_request_id: str | None = None) -> dict:
    body = {"customer_id": 101, "cart_version": cart_version}
    if client_request_id is not None:
        body["client_request_id"] = client_request_id
    return body


@pytest.mark.integration
def test_create_order_persists_snapshot_history_idempotency_and_outbox(order_session_factory) -> None:
    fake = FakeCartClient()
    client = make_client(order_session_factory, fake)
    correlation_id = str(uuid.uuid4())

    response = client.post(
        "/api/v1/orders",
        json=create_body(),
        headers=create_headers(correlation_id=correlation_id),
    )

    assert response.status_code == 202
    assert response.headers["Idempotency-Replayed"] == "false"
    assert response.headers["ETag"] == '"1"'
    assert response.headers["X-Correlation-ID"] == correlation_id
    parsed = OrderResponse.model_validate(response.json())
    assert parsed.status == "PENDING_RESERVATION"
    assert parsed.business_status == "PENDING_RESERVATION"
    assert parsed.operation_state == "NONE"
    assert parsed.reservation_state == "REQUESTED"
    assert parsed.total_amount == Decimal("3500.99")
    assert parsed.available_actions.can_cancel is True

    with order_session_factory() as session:
        order = session.scalar(select(Order))
        assert order is not None
        assert order.total_amount == Decimal("3500.99")
        assert order.correlation_id == uuid.UUID(correlation_id)

        items = list(session.scalars(select(OrderItem).order_by(OrderItem.product_id)))
        assert [(item.product_id, item.line_total) for item in items] == [
            (1001, Decimal("3001.00")),
            (1002, Decimal("499.99")),
        ]

        history = session.scalar(select(OrderStatusHistory))
        assert history is not None
        assert history.trigger == "CREATE_ORDER"
        assert history.actor_id == 101
        assert history.version_before == 0
        assert history.version_after == 1
        assert history.correlation_id == uuid.UUID(correlation_id)

        idempotency = session.scalar(select(OrderIdempotency))
        assert idempotency is not None
        assert idempotency.state.value == "COMPLETED"
        assert idempotency.order_id == order.id
        assert idempotency.response_status == 202
        assert idempotency.expires_at is not None

        outbox = list(session.scalars(select(OrderOutbox).order_by(OrderOutbox.event_type)))
        assert [record.event_type for record in outbox] == [
            "OrderCreated",
            "StockReservationRequested",
        ]
        for record in outbox:
            assert record.payload["event_id"] == str(record.id)
            assert record.payload["aggregate_id"] == str(order.id)
            assert record.payload["correlation_id"] == correlation_id
            assert record.headers["correlation_id"] == correlation_id
        stock_event = next(record for record in outbox if record.event_type == "StockReservationRequested")
        assert stock_event.payload["payload"]["items"] == [
            {"product_id": 1001, "quantity": 2},
            {"product_id": 1002, "quantity": 1},
        ]
        assert stock_event.payload["payload"]["reservation_deadline_at"] is None
        assert session.scalar(select(func.count()).select_from(OrderInbox)) == 0

    assert fake.calls == [
        {
            "customer_id": 101,
            "expected_version": 7,
            "correlation_id": correlation_id,
        }
    ]


@pytest.mark.integration
def test_same_key_same_payload_replays_without_new_side_effects(order_session_factory) -> None:
    fake = FakeCartClient()
    client = make_client(order_session_factory, fake)
    headers = create_headers()

    first = client.post("/api/v1/orders", json=create_body(), headers=headers)
    replay = client.post("/api/v1/orders", json=create_body(), headers=headers)

    assert first.status_code == 202
    assert replay.status_code == 200
    assert replay.headers["Idempotency-Replayed"] == "true"
    assert replay.json()["order_id"] == first.json()["order_id"]
    assert len(fake.calls) == 1
    with order_session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Order)) == 1
        assert session.scalar(select(func.count()).select_from(OrderItem)) == 2
        assert session.scalar(select(func.count()).select_from(OrderStatusHistory)) == 1
        assert session.scalar(select(func.count()).select_from(OrderOutbox)) == 2
        assert session.scalar(select(func.count()).select_from(OrderIdempotency)) == 1


@pytest.mark.integration
def test_same_key_different_payload_returns_conflict(order_session_factory) -> None:
    fake = FakeCartClient()
    client = make_client(order_session_factory, fake)
    headers = create_headers()

    assert client.post("/api/v1/orders", json=create_body(), headers=headers).status_code == 202
    conflict = client.post(
        "/api/v1/orders",
        json=create_body(client_request_id=str(uuid.uuid4())),
        headers=headers,
    )

    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "idempotency_key_conflict"
    with order_session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Order)) == 1
        assert session.scalar(select(func.count()).select_from(OrderOutbox)) == 2


@pytest.mark.integration
def test_concurrent_identical_requests_create_one_order(order_session_factory) -> None:
    fake = BlockingCartClient()
    first_client = make_client(order_session_factory, fake)
    second_client = make_client(order_session_factory, fake)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(
            first_client.post,
            "/api/v1/orders",
            json=create_body(),
            headers=create_headers(),
        )
        assert fake.started.wait(timeout=5)
        second = second_client.post(
            "/api/v1/orders",
            json=create_body(),
            headers=create_headers(),
        )
        fake.release.set()
        first = first_future.result(timeout=5)

    assert first.status_code == 202
    assert second.status_code == 409
    assert second.json()["error"]["code"] == "idempotency_request_in_progress"
    assert second.headers["Retry-After"] == "1"
    with order_session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Order)) == 1
        assert session.scalar(select(func.count()).select_from(OrderOutbox)) == 2


@pytest.mark.integration
@pytest.mark.parametrize(
    ("snapshot", "expected_code", "expected_status"),
    [
        (make_snapshot(items=[], supplier_id=None), "cart_empty", 409),
        (
            make_snapshot(
                supplier_id=None,
                items=[
                    make_snapshot().items[0],
                    CartSnapshotItem(
                        product_id=2001,
                        product_name="Other supplier item",
                        supplier_id=202,
                        quantity=1,
                        unit_price=Decimal("10.00"),
                        currency="RUB",
                        product_status="ACTIVE",
                        projected_available_quantity=5,
                        projection_updated_at=datetime.now(timezone.utc),
                    ),
                ],
            ),
            "cart_multiple_suppliers",
            409,
        ),
        (
            make_snapshot(
                items=[
                    CartSnapshotItem(
                        product_id=1001,
                        product_name="QA Keyboard",
                        supplier_id=201,
                        quantity=0,
                        unit_price=Decimal("1500.50"),
                        currency="RUB",
                        product_status="ACTIVE",
                        projected_available_quantity=5,
                        projection_updated_at=datetime.now(timezone.utc),
                    )
                ]
            ),
            "invalid_quantity",
            400,
        ),
    ],
)
def test_invalid_snapshot_creates_no_business_records(
    order_session_factory,
    snapshot,
    expected_code,
    expected_status,
) -> None:
    client = make_client(order_session_factory, FakeCartClient(snapshot=snapshot))
    response = client.post("/api/v1/orders", json=create_body(), headers=create_headers())

    assert response.status_code == expected_status
    assert response.json()["error"]["code"] == expected_code
    with order_session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Order)) == 0
        assert session.scalar(select(func.count()).select_from(OrderItem)) == 0
        assert session.scalar(select(func.count()).select_from(OrderStatusHistory)) == 0
        assert session.scalar(select(func.count()).select_from(OrderOutbox)) == 0


@pytest.mark.integration
def test_stale_cart_version_maps_controlled_conflict(order_session_factory) -> None:
    error = ServiceError(
        code="cart_version_conflict",
        category="CONFLICT",
        message="Cart version changed",
        status_code=409,
        details={"current_version": 8},
    )
    client = make_client(order_session_factory, FakeCartClient(error=error))
    response = client.post("/api/v1/orders", json=create_body(), headers=create_headers())

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "cart_version_conflict"
    assert response.json()["error"]["details"] == {"current_version": 8}


@pytest.mark.integration
@pytest.mark.parametrize(
    ("headers", "expected_code"),
    [
        (create_headers(role="SUPPLIER"), "order_access_forbidden"),
        (create_headers(subject_id="102"), "order_access_forbidden"),
        (
            {
                "X-Test-Subject-ID": "101",
                "Idempotency-Key": "checkout-101-v7-a",
            },
            "order_access_forbidden",
        ),
        (
            {
                "X-Test-Role": "CUSTOMER",
                "Idempotency-Key": "checkout-101-v7-a",
            },
            "order_access_forbidden",
        ),
        (
            {
                "X-Test-Role": "CUSTOMER",
                "X-Test-Subject-ID": "101",
            },
            "idempotency_key_required",
        ),
    ],
)
def test_context_and_idempotency_headers_are_required(order_session_factory, headers, expected_code) -> None:
    fake = FakeCartClient()
    client = make_client(order_session_factory, fake)
    response = client.post("/api/v1/orders", json=create_body(), headers=headers)

    assert response.status_code in {400, 403}
    assert response.json()["error"]["code"] == expected_code
    assert fake.calls == []


@pytest.mark.integration
def test_invalid_customer_id_uses_controlled_error(order_session_factory) -> None:
    client = make_client(order_session_factory, FakeCartClient())
    response = client.post(
        "/api/v1/orders",
        json={"customer_id": 0, "cart_version": 7},
        headers=create_headers(),
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_customer_id"


@pytest.mark.integration
@pytest.mark.parametrize(
    ("error", "expected_status", "expected_code"),
    [
        (
            ServiceError(
                code="dependency_unavailable",
                category="DEPENDENCY",
                message="customer-service is temporarily unavailable",
                status_code=503,
                details={"dependency": "customer-service"},
                retryable=True,
            ),
            503,
            "dependency_unavailable",
        ),
        (
            ServiceError(
                code="operation_timeout",
                category="TIMEOUT",
                message="customer-service operation timed out",
                status_code=504,
                details={"dependency": "customer-service"},
                retryable=True,
            ),
            504,
            "operation_timeout",
        ),
    ],
)
def test_dependency_failures_leave_retryable_key_without_order(
    order_session_factory,
    error,
    expected_status,
    expected_code,
) -> None:
    client = make_client(order_session_factory, FakeCartClient(error=error))
    response = client.post("/api/v1/orders", json=create_body(), headers=create_headers())

    assert response.status_code == expected_status
    assert response.json()["error"]["code"] == expected_code
    with order_session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Order)) == 0
        record = session.scalar(select(OrderIdempotency))
        assert record is not None
        assert record.state.value == "FAILED_RETRYABLE"
        assert record.last_error_code == expected_code


@pytest.mark.integration
def test_same_key_can_recover_after_dependency_failure(order_session_factory) -> None:
    fake = FakeCartClient(
        error=ServiceError(
            code="dependency_unavailable",
            category="DEPENDENCY",
            message="customer-service is temporarily unavailable",
            status_code=503,
            retryable=True,
        )
    )
    client = make_client(order_session_factory, fake)
    headers = create_headers()

    assert client.post("/api/v1/orders", json=create_body(), headers=headers).status_code == 503
    fake.error = None
    recovered = client.post("/api/v1/orders", json=create_body(), headers=headers)

    assert recovered.status_code == 202
    with order_session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Order)) == 1
        assert session.scalar(select(func.count()).select_from(OrderIdempotency)) == 1


@pytest.mark.integration
def test_outbox_failure_rolls_back_all_business_records(
    order_session_factory,
    monkeypatch,
) -> None:
    def fail_outbox(**_kwargs):
        raise RuntimeError("injected outbox failure")

    monkeypatch.setattr(order_creation, "build_outbox_records", fail_outbox)
    client = make_client(order_session_factory, FakeCartClient())
    response = client.post("/api/v1/orders", json=create_body(), headers=create_headers())

    assert response.status_code == 500
    assert response.json()["error"]["code"] == "internal_error"
    with order_session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Order)) == 0
        assert session.scalar(select(func.count()).select_from(OrderItem)) == 0
        assert session.scalar(select(func.count()).select_from(OrderStatusHistory)) == 0
        assert session.scalar(select(func.count()).select_from(OrderOutbox)) == 0
