from __future__ import annotations

import os
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
from app.enums import BusinessStatus, OperationState, ReservationState
from app.errors import ServiceError
from app.main import create_app
from app.models import Order, OrderItem, OrderOutbox, OrderStatusHistory
from app.services import order_actions
from app.services.order_actions import cancel_order, confirm_order, reject_order


class NoopCartClient:
    def close(self) -> None:
        return None


@pytest.fixture
def action_session_factory():
    database_url = os.getenv("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required")
    schema_name = f"order_action_test_{uuid.uuid4().hex}"
    admin_engine = create_engine(database_url, future=True)
    with admin_engine.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))
    engine = create_engine(
        database_url,
        future=True,
        connect_args={"options": f"-csearch_path={schema_name}"},
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        engine.dispose()
        with admin_engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        admin_engine.dispose()


@pytest.fixture
def action_client(action_session_factory) -> TestClient:
    root = Path(__file__).resolve().parents[1]
    settings = Settings(
        service_name="order-service",
        environment="test",
        host="127.0.0.1",
        port=8000,
        database_url=os.environ["TEST_DATABASE_URL"],
        log_level="WARNING",
        kafka_bootstrap_servers="localhost:9092",
        customer_service_url="http://customer-service:8001",
        readiness_timeout_seconds=1,
        alembic_config=str(root / "alembic.ini"),
        timeout_worker_enabled=False,
    )
    app = create_app(settings, cart_snapshot_client=NoopCartClient())

    def override_db():
        session = action_session_factory()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = override_db
    return TestClient(app, raise_server_exceptions=False)


def _seed_order(
    factory,
    *,
    status: BusinessStatus = BusinessStatus.RESERVED,
    reservation_state: ReservationState = ReservationState.RESERVED,
) -> uuid.UUID:
    now = datetime.now(timezone.utc)
    order_id = uuid.uuid4()
    reservation_id = uuid.uuid4() if reservation_state == ReservationState.RESERVED else None
    with factory.begin() as session:
        session.add(
            Order(
                id=order_id,
                customer_id=101,
                supplier_id=201,
                cart_id=uuid.uuid4(),
                cart_version=order_id.int % 2_000_000_000 + 1,
                business_status=status,
                operation_state=OperationState.NONE,
                reservation_state=reservation_state,
                total_amount=Decimal("30.00"),
                currency="RUB",
                version=2,
                correlation_id=uuid.uuid4(),
                reservation_request_id=uuid.uuid4(),
                reservation_id=reservation_id,
                created_at=now,
                updated_at=now,
                items=[
                    OrderItem(
                        id=uuid.uuid4(),
                        product_id=1001,
                        product_name_snapshot="Lifecycle product",
                        quantity=3,
                        unit_price=Decimal("10.00"),
                        line_total=Decimal("30.00"),
                        currency="RUB",
                        supplier_id_snapshot=201,
                    )
                ],
            )
        )
    return order_id


def _headers(role: str, subject: int, version: int = 2) -> dict[str, str]:
    return {
        "X-Test-Role": role,
        "X-Test-Subject-ID": str(subject),
        "X-Correlation-ID": str(uuid.uuid4()),
        "If-Match": f'"{version}"',
    }


@pytest.mark.integration
def test_supplier_confirm_is_pending_atomic_and_idempotent(
    action_client,
    action_session_factory,
) -> None:
    order_id = _seed_order(action_session_factory)
    url = f"/api/v1/suppliers/201/orders/{order_id}/confirm"

    first = action_client.post(url, headers=_headers("SUPPLIER", 201), json={})
    replay = action_client.post(url, headers=_headers("SUPPLIER", 201), json={})

    assert first.status_code == 202
    assert first.headers["ETag"] == '"3"'
    assert first.json()["status"] == "CONFIRMATION_PENDING"
    assert first.json()["available_actions"]["can_confirm"] is False
    assert replay.status_code == 200
    assert replay.headers["ETag"] == '"3"'
    with action_session_factory() as session:
        order = session.get(Order, order_id)
        assert order.version == 3
        assert order.finalization_request_id is not None
        assert session.scalar(
            select(func.count()).select_from(OrderStatusHistory)
        ) == 1
        assert set(session.scalars(select(OrderOutbox.event_type))) == {
            "OrderConfirmationRequested",
            "StockFinalizationRequested",
        }


@pytest.mark.integration
def test_supplier_reject_and_customer_cancel_create_release_intent(
    action_client,
    action_session_factory,
) -> None:
    rejected_id = _seed_order(action_session_factory)
    cancelled_id = _seed_order(action_session_factory)

    rejected = action_client.post(
        f"/api/v1/suppliers/201/orders/{rejected_id}/reject",
        headers=_headers("SUPPLIER", 201),
        json={"reason_code": "QUALITY_ISSUE", "reason_text": "Damaged"},
    )
    cancelled = action_client.post(
        f"/api/v1/customers/101/orders/{cancelled_id}/cancel",
        headers=_headers("CUSTOMER", 101),
        json={"reason_code": "CUSTOMER_REQUEST"},
    )

    assert rejected.status_code == 202
    assert rejected.json()["status"] == "REJECTION_PENDING"
    assert rejected.json()["reservation_state"] == "RELEASE_REQUESTED"
    assert cancelled.status_code == 202
    assert cancelled.json()["status"] == "CANCELLATION_PENDING"
    with action_session_factory() as session:
        orders = {
            order.id: order
            for order in session.scalars(select(Order).where(Order.id.in_([rejected_id, cancelled_id])))
        }
        assert orders[rejected_id].release_request_id is not None
        assert orders[cancelled_id].release_request_id is not None


@pytest.mark.integration
def test_pending_reservation_can_be_cancelled_with_default_reason(
    action_client,
    action_session_factory,
) -> None:
    order_id = _seed_order(
        action_session_factory,
        status=BusinessStatus.PENDING_RESERVATION,
        reservation_state=ReservationState.REQUESTED,
    )

    response = action_client.post(
        f"/api/v1/customers/101/orders/{order_id}/cancel",
        headers=_headers("CUSTOMER", 101),
    )

    assert response.status_code == 202
    assert response.json()["cancellation_reason"] == {
        "code": "CUSTOMER_REQUEST",
        "text": None,
    }


@pytest.mark.integration
@pytest.mark.parametrize(
    ("headers", "expected_status"),
    [
        ({"X-Test-Role": "SUPPLIER", "X-Test-Subject-ID": "201"}, 428),
        (
            {
                "X-Test-Role": "SUPPLIER",
                "X-Test-Subject-ID": "201",
                "If-Match": "2",
            },
            400,
        ),
        (
            {
                "X-Test-Role": "SUPPLIER",
                "X-Test-Subject-ID": "201",
                "If-Match": '"1"',
            },
            412,
        ),
    ],
)
def test_mutation_preconditions(
    action_client,
    action_session_factory,
    headers,
    expected_status,
) -> None:
    order_id = _seed_order(action_session_factory)
    response = action_client.post(
        f"/api/v1/suppliers/201/orders/{order_id}/confirm",
        headers=headers,
        json={},
    )
    assert response.status_code == expected_status


@pytest.mark.integration
def test_foreign_order_is_not_disclosed(action_client, action_session_factory) -> None:
    order_id = _seed_order(action_session_factory)
    response = action_client.post(
        f"/api/v1/suppliers/202/orders/{order_id}/confirm",
        headers=_headers("SUPPLIER", 202),
        json={},
    )
    assert response.status_code == 404


@pytest.mark.integration
def test_confirm_cancel_race_has_one_transition(action_session_factory) -> None:
    order_id = _seed_order(action_session_factory)

    def confirm():
        with action_session_factory() as session:
            try:
                confirm_order(
                    session=session,
                    supplier_id=201,
                    order_id=order_id,
                    expected_version=2,
                    correlation_id=uuid.uuid4(),
                )
                return "confirm"
            except ServiceError as exc:
                return exc.status_code

    def cancel():
        with action_session_factory() as session:
            try:
                cancel_order(
                    session=session,
                    customer_id=101,
                    order_id=order_id,
                    expected_version=2,
                    correlation_id=uuid.uuid4(),
                    reason_code="CUSTOMER_REQUEST",
                    reason_text=None,
                )
                return "cancel"
            except ServiceError as exc:
                return exc.status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = {pool.submit(confirm), pool.submit(cancel)}
        results = [future.result() for future in outcomes]

    assert sum(result in {"confirm", "cancel"} for result in results) == 1
    assert any(result in {409, 412} for result in results)
    with action_session_factory() as session:
        order = session.get(Order, order_id)
        assert order.version == 3
        assert session.scalar(select(func.count()).select_from(OrderStatusHistory)) == 1
        assert session.scalar(select(func.count()).select_from(OrderOutbox)) == 2


@pytest.mark.integration
def test_reject_cancel_race_has_one_transition(action_session_factory) -> None:
    order_id = _seed_order(action_session_factory)

    def reject():
        with action_session_factory() as session:
            try:
                reject_order(
                    session=session,
                    supplier_id=201,
                    order_id=order_id,
                    expected_version=2,
                    correlation_id=uuid.uuid4(),
                    reason_code="SUPPLIER_REJECTED",
                    reason_text=None,
                )
                return "reject"
            except ServiceError as exc:
                return exc.status_code

    def cancel():
        with action_session_factory() as session:
            try:
                cancel_order(
                    session=session,
                    customer_id=101,
                    order_id=order_id,
                    expected_version=2,
                    correlation_id=uuid.uuid4(),
                    reason_code="CUSTOMER_REQUEST",
                    reason_text=None,
                )
                return "cancel"
            except ServiceError as exc:
                return exc.status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = [
            future.result()
            for future in {pool.submit(reject), pool.submit(cancel)}
        ]

    assert sum(result in {"reject", "cancel"} for result in results) == 1
    assert any(result in {409, 412} for result in results)
    with action_session_factory() as session:
        assert session.get(Order, order_id).version == 3
        assert session.scalar(select(func.count()).select_from(OrderStatusHistory)) == 1
        assert session.scalar(select(func.count()).select_from(OrderOutbox)) == 2


@pytest.mark.integration
def test_duplicate_confirm_race_has_one_transition(action_session_factory) -> None:
    order_id = _seed_order(action_session_factory)

    def confirm():
        with action_session_factory() as session:
            return confirm_order(
                session=session,
                supplier_id=201,
                order_id=order_id,
                expected_version=2,
                correlation_id=uuid.uuid4(),
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = [future.result() for future in [pool.submit(confirm), pool.submit(confirm)]]

    assert sorted(result.status_code for result in results) == [200, 202]
    with action_session_factory() as session:
        assert session.get(Order, order_id).version == 3
        assert session.scalar(select(func.count()).select_from(OrderStatusHistory)) == 1
        assert session.scalar(select(func.count()).select_from(OrderOutbox)) == 2


@pytest.mark.integration
@pytest.mark.parametrize("failure_point", ["history", "outbox"])
def test_action_rolls_back_when_audit_write_fails(
    action_session_factory,
    monkeypatch,
    failure_point,
) -> None:
    order_id = _seed_order(action_session_factory)

    def fail(*_args, **_kwargs):
        raise RuntimeError(f"{failure_point} failed")

    monkeypatch.setattr(
        order_actions,
        "OrderStatusHistory" if failure_point == "history" else "_order_event",
        fail,
    )
    with action_session_factory() as session:
        with pytest.raises(RuntimeError, match=f"{failure_point} failed"):
            confirm_order(
                session=session,
                supplier_id=201,
                order_id=order_id,
                expected_version=2,
                correlation_id=uuid.uuid4(),
            )

    with action_session_factory() as session:
        order = session.get(Order, order_id)
        assert order.version == 2
        assert order.operation_state == OperationState.NONE
        assert session.scalar(select(func.count()).select_from(OrderStatusHistory)) == 0
        assert session.scalar(select(func.count()).select_from(OrderOutbox)) == 0
