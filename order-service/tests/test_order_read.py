from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.api.dependencies import get_db
from app.config import Settings
from app.database import Base
from app.enums import ActorType, BusinessStatus, OperationState, ReservationState
from app.main import create_app
from app.models import Order, OrderItem, OrderStatusHistory
from app.schemas import (
    CustomerOrderListResponse,
    OrderResponse,
    SupplierOrderListResponse,
)


class NoopCartClient:
    def close(self) -> None:
        return None


@pytest.fixture
def read_session_factory():
    database_url = os.getenv("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required")

    schema_name = f"order_read_test_{uuid.uuid4().hex}"
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


@pytest.fixture
def read_client(read_session_factory) -> TestClient:
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
    application = create_app(settings, cart_snapshot_client=NoopCartClient())

    def override_db():
        session = read_session_factory()
        try:
            yield session
        finally:
            session.close()

    application.dependency_overrides[get_db] = override_db
    return TestClient(application, raise_server_exceptions=False)


def _headers(*, role: str, subject_id: int) -> dict[str, str]:
    return {
        "X-Test-Role": role,
        "X-Test-Subject-ID": str(subject_id),
        "X-Correlation-ID": str(uuid.uuid4()),
    }


def _persist_order(
    factory,
    *,
    order_id: uuid.UUID | None = None,
    customer_id: int = 101,
    supplier_id: int = 201,
    business_status: BusinessStatus = BusinessStatus.PENDING_RESERVATION,
    operation_state: OperationState = OperationState.NONE,
    reservation_state: ReservationState = ReservationState.REQUESTED,
    version: int = 1,
    created_at: datetime | None = None,
    product_id: int = 1001,
) -> uuid.UUID:
    timestamp = created_at or datetime.now(timezone.utc)
    persisted_id = order_id or uuid.uuid4()
    order = Order(
        id=persisted_id,
        customer_id=customer_id,
        supplier_id=supplier_id,
        cart_id=uuid.uuid4(),
        cart_version=(persisted_id.int % 9_000_000_000_000_000_000) + 1,
        business_status=business_status,
        operation_state=operation_state,
        reservation_state=reservation_state,
        total_amount=Decimal("25.00"),
        currency="RUB",
        version=version,
        correlation_id=uuid.uuid4(),
        reservation_request_id=uuid.uuid4(),
        created_at=timestamp,
        updated_at=timestamp,
    )
    order.items.append(
        OrderItem(
            id=uuid.uuid4(),
            product_id=product_id,
            product_name_snapshot="Read API product",
            quantity=2,
            unit_price=Decimal("12.50"),
            line_total=Decimal("25.00"),
            currency="RUB",
            supplier_id_snapshot=supplier_id,
        )
    )
    with factory.begin() as session:
        session.add(order)
    return persisted_id


@pytest.mark.integration
def test_customer_gets_own_order(read_client, read_session_factory) -> None:
    order_id = _persist_order(read_session_factory)

    response = read_client.get(
        f"/api/v1/customers/101/orders/{order_id}",
        headers=_headers(role="CUSTOMER", subject_id=101),
    )

    assert response.status_code == 200
    assert response.json()["order_id"] == str(order_id)
    assert response.json()["customer_id"] == 101
    assert response.json()["supplier_id"] == 201
    assert response.json()["items"][0]["unit_price"] == "12.50"


@pytest.mark.integration
def test_order_detail_exposes_append_only_history(read_client, read_session_factory) -> None:
    order_id = _persist_order(read_session_factory)
    correlation_id = uuid.uuid4()
    with read_session_factory.begin() as session:
        session.add(
            OrderStatusHistory(
                id=uuid.uuid4(),
                order_id=order_id,
                business_status_before=None,
                business_status_after=BusinessStatus.PENDING_RESERVATION,
                operation_state_before=None,
                operation_state_after=OperationState.NONE,
                reservation_state_before=None,
                reservation_state_after=ReservationState.REQUESTED,
                trigger="CREATE_ORDER",
                actor_type=ActorType.CUSTOMER,
                actor_id=101,
                event_id=None,
                correlation_id=correlation_id,
                version_before=0,
                version_after=1,
            )
        )

    response = read_client.get(
        f"/api/v1/customers/101/orders/{order_id}",
        headers=_headers(role="CUSTOMER", subject_id=101),
    )

    assert response.status_code == 200
    assert response.json()["history"] == [
        {
            "history_id": response.json()["history"][0]["history_id"],
            "trigger": "CREATE_ORDER",
            "actor_type": "CUSTOMER",
            "actor_id": 101,
            "event_id": None,
            "business_status_before": None,
            "business_status_after": "PENDING_RESERVATION",
            "operation_state_before": None,
            "operation_state_after": "NONE",
            "reservation_state_before": None,
            "reservation_state_after": "REQUESTED",
            "version_before": 0,
            "version_after": 1,
            "reason": None,
            "correlation_id": str(correlation_id),
            "created_at": response.json()["history"][0]["created_at"],
        }
    ]


@pytest.mark.integration
def test_customer_cannot_get_foreign_order(read_client, read_session_factory) -> None:
    order_id = _persist_order(read_session_factory, customer_id=102)

    response = read_client.get(
        f"/api/v1/customers/101/orders/{order_id}",
        headers=_headers(role="CUSTOMER", subject_id=101),
    )

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "order_not_found"


@pytest.mark.integration
def test_supplier_gets_own_order(read_client, read_session_factory) -> None:
    order_id = _persist_order(
        read_session_factory,
        business_status=BusinessStatus.RESERVED,
        reservation_state=ReservationState.RESERVED,
        version=2,
    )

    response = read_client.get(
        f"/api/v1/suppliers/201/orders/{order_id}",
        headers=_headers(role="SUPPLIER", subject_id=201),
    )

    assert response.status_code == 200
    assert response.json()["order_id"] == str(order_id)
    assert response.json()["available_actions"]["can_confirm"] is True
    assert response.json()["available_actions"]["can_reject"] is True


@pytest.mark.integration
def test_supplier_cannot_get_foreign_order(read_client, read_session_factory) -> None:
    order_id = _persist_order(read_session_factory, supplier_id=202)

    response = read_client.get(
        f"/api/v1/suppliers/201/orders/{order_id}",
        headers=_headers(role="SUPPLIER", subject_id=201),
    )

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "order_not_found"


@pytest.mark.integration
@pytest.mark.parametrize(
    ("role", "path", "subject_id", "foreign_owner"),
    [
        ("CUSTOMER", "/api/v1/customers/101/orders", 101, {"customer_id": 102}),
        ("SUPPLIER", "/api/v1/suppliers/201/orders", 201, {"supplier_id": 202}),
    ],
)
def test_lists_exclude_foreign_orders(
    read_client,
    read_session_factory,
    role,
    path,
    subject_id,
    foreign_owner,
) -> None:
    own_id = _persist_order(read_session_factory)
    foreign_id = _persist_order(read_session_factory, product_id=1002, **foreign_owner)

    response = read_client.get(path, headers=_headers(role=role, subject_id=subject_id))

    assert response.status_code == 200
    assert [item["order_id"] for item in response.json()["items"]] == [str(own_id)]
    assert str(foreign_id) not in response.text


@pytest.mark.integration
def test_list_uses_stable_created_at_and_id_sorting(read_client, read_session_factory) -> None:
    now = datetime.now(timezone.utc)
    older_id = uuid.UUID(int=1)
    lower_id = uuid.UUID(int=2)
    higher_id = uuid.UUID(int=3)
    _persist_order(read_session_factory, order_id=older_id, created_at=now - timedelta(minutes=1))
    _persist_order(read_session_factory, order_id=lower_id, created_at=now, product_id=1002)
    _persist_order(read_session_factory, order_id=higher_id, created_at=now, product_id=1003)

    response = read_client.get(
        "/api/v1/customers/101/orders",
        headers=_headers(role="CUSTOMER", subject_id=101),
    )

    assert response.status_code == 200
    assert [item["order_id"] for item in response.json()["items"]] == [
        str(higher_id),
        str(lower_id),
        str(older_id),
    ]


@pytest.mark.integration
def test_list_pagination_returns_page_count_and_filtered_total(read_client, read_session_factory) -> None:
    now = datetime.now(timezone.utc)
    for index in range(3):
        _persist_order(
            read_session_factory,
            order_id=uuid.UUID(int=index + 1),
            created_at=now + timedelta(seconds=index),
            product_id=1001 + index,
        )

    response = read_client.get(
        "/api/v1/customers/101/orders",
        params={"page": 2, "limit": 2},
        headers=_headers(role="CUSTOMER", subject_id=101),
    )

    assert response.status_code == 200
    assert response.json()["page"] == 2
    assert response.json()["limit"] == 2
    assert response.json()["count"] == 1
    assert response.json()["total"] == 3


@pytest.mark.integration
def test_status_filter_uses_business_status(read_client, read_session_factory) -> None:
    pending_id = _persist_order(read_session_factory)
    reserved_id = _persist_order(
        read_session_factory,
        business_status=BusinessStatus.RESERVED,
        reservation_state=ReservationState.RESERVED,
        version=2,
        product_id=1002,
    )

    response = read_client.get(
        "/api/v1/customers/101/orders",
        params=[("status", "RESERVED"), ("status", "RESERVED")],
        headers=_headers(role="CUSTOMER", subject_id=101),
    )

    assert response.status_code == 200
    assert [item["order_id"] for item in response.json()["items"]] == [str(reserved_id)]
    assert str(pending_id) not in response.text


@pytest.mark.integration
def test_created_from_is_inclusive(read_client, read_session_factory) -> None:
    boundary = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    _persist_order(read_session_factory, created_at=boundary - timedelta(microseconds=1))
    boundary_id = _persist_order(read_session_factory, created_at=boundary, product_id=1002)
    later_id = _persist_order(read_session_factory, created_at=boundary + timedelta(seconds=1), product_id=1003)

    response = read_client.get(
        "/api/v1/customers/101/orders",
        params={"created_from": boundary.isoformat().replace("+00:00", "Z")},
        headers=_headers(role="CUSTOMER", subject_id=101),
    )

    assert response.status_code == 200
    assert {item["order_id"] for item in response.json()["items"]} == {
        str(boundary_id),
        str(later_id),
    }


@pytest.mark.integration
def test_created_to_is_exclusive(read_client, read_session_factory) -> None:
    boundary = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    earlier_id = _persist_order(read_session_factory, created_at=boundary - timedelta(microseconds=1))
    _persist_order(read_session_factory, created_at=boundary, product_id=1002)

    response = read_client.get(
        "/api/v1/customers/101/orders",
        params={"created_to": boundary.isoformat().replace("+00:00", "Z")},
        headers=_headers(role="CUSTOMER", subject_id=101),
    )

    assert response.status_code == 200
    assert [item["order_id"] for item in response.json()["items"]] == [str(earlier_id)]


@pytest.mark.integration
@pytest.mark.parametrize(
    (
        "actor_role",
        "business_status",
        "reservation_state",
        "expected_cancel",
        "expected_confirm",
        "expected_reject",
    ),
    [
        ("CUSTOMER", BusinessStatus.PENDING_RESERVATION, ReservationState.REQUESTED, True, False, False),
        ("CUSTOMER", BusinessStatus.RESERVED, ReservationState.RESERVED, True, False, False),
        ("CUSTOMER", BusinessStatus.CONFIRMED, ReservationState.RESERVED, False, False, False),
        ("SUPPLIER", BusinessStatus.PENDING_RESERVATION, ReservationState.REQUESTED, False, False, False),
        ("SUPPLIER", BusinessStatus.RESERVED, ReservationState.RESERVED, False, True, True),
        ("SUPPLIER", BusinessStatus.CONFIRMED, ReservationState.RESERVED, False, False, False),
    ],
)
def test_available_actions_follow_actor_and_state(
    read_client,
    read_session_factory,
    actor_role,
    business_status,
    reservation_state,
    expected_cancel,
    expected_confirm,
    expected_reject,
) -> None:
    order_id = _persist_order(
        read_session_factory,
        business_status=business_status,
        reservation_state=reservation_state,
        version=3,
    )
    owner_segment = "customers/101" if actor_role == "CUSTOMER" else "suppliers/201"
    subject_id = 101 if actor_role == "CUSTOMER" else 201

    response = read_client.get(
        f"/api/v1/{owner_segment}/orders/{order_id}",
        headers=_headers(role=actor_role, subject_id=subject_id),
    )

    actions = response.json()["available_actions"]
    assert response.status_code == 200
    assert actions == {
        "can_cancel": expected_cancel,
        "can_confirm": expected_confirm,
        "can_reject": expected_reject,
        "can_retry": False,
        "can_refresh": True,
    }


@pytest.mark.integration
def test_detail_etag_matches_order_version(read_client, read_session_factory) -> None:
    order_id = _persist_order(read_session_factory, version=9)

    response = read_client.get(
        f"/api/v1/customers/101/orders/{order_id}",
        headers=_headers(role="CUSTOMER", subject_id=101),
    )

    assert response.status_code == 200
    assert response.headers["ETag"] == '"9"'


@pytest.mark.integration
def test_detail_and_list_responses_match_public_schemas(read_client, read_session_factory) -> None:
    order_id = _persist_order(read_session_factory)
    headers = _headers(role="CUSTOMER", subject_id=101)

    detail = read_client.get(f"/api/v1/customers/101/orders/{order_id}", headers=headers)
    order_list = read_client.get("/api/v1/customers/101/orders", headers=headers)
    supplier_list = read_client.get(
        "/api/v1/suppliers/201/orders",
        headers=_headers(role="SUPPLIER", subject_id=201),
    )

    OrderResponse.model_validate(detail.json())
    CustomerOrderListResponse.model_validate(order_list.json())
    SupplierOrderListResponse.model_validate(supplier_list.json())


@pytest.mark.integration
def test_empty_list_returns_zero_count_and_total(read_client) -> None:
    response = read_client.get(
        "/api/v1/customers/101/orders",
        headers=_headers(role="CUSTOMER", subject_id=101),
    )

    assert response.status_code == 200
    assert response.json() == {
        "items": [],
        "page": 1,
        "limit": 20,
        "count": 0,
        "total": 0,
    }


@pytest.mark.integration
def test_path_subject_mismatch_is_forbidden_before_read(read_client, read_session_factory) -> None:
    order_id = _persist_order(read_session_factory)

    response = read_client.get(
        f"/api/v1/customers/101/orders/{order_id}",
        headers=_headers(role="CUSTOMER", subject_id=102),
    )

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "order_access_forbidden"


@pytest.mark.integration
@pytest.mark.parametrize(
    "params",
    [
        {"limit": 101},
        {"status": "FAILED"},
        {"unexpected": "value"},
        {"created_from": "2026-07-29T00:00:00Z", "created_to": "2026-07-28T00:00:00Z"},
    ],
)
def test_invalid_list_filters_return_controlled_error(read_client, params) -> None:
    response = read_client.get(
        "/api/v1/customers/101/orders",
        params=params,
        headers=_headers(role="CUSTOMER", subject_id=101),
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_request"
