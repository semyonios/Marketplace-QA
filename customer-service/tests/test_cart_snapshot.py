from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from app.database import Base
from app.main import app, get_db
from app.models import CartItem, CartState, Product, User


@pytest.fixture
def customer_session_factory():
    database_url = os.getenv("TEST_CUSTOMER_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_CUSTOMER_DATABASE_URL is required")

    schema_name = f"customer_snapshot_test_{uuid.uuid4().hex}"
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


def _client(factory) -> TestClient:
    def override_db():
        session = factory()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = override_db
    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.integration
def test_internal_cart_snapshot_returns_server_side_decimal_data(customer_session_factory) -> None:
    now = datetime.now(timezone.utc)
    cart_id = uuid.uuid4()
    with customer_session_factory.begin() as session:
        session.add(User(id=101, full_name="QA Customer", email="qa@example.test"))
        session.add(CartState(id=cart_id, user_id=101, version=7))
        session.add(
            Product(
                id=1001,
                supplier_id=201,
                name="QA Keyboard",
                description=None,
                price=Decimal("1500.50"),
                stocks=5,
                is_active=True,
                is_archived=False,
                created_at=now,
                updated_at=now,
            )
        )
        session.add(CartItem(user_id=101, product_id=1001, quantity=2))

    client = _client(customer_session_factory)
    correlation_id = str(uuid.uuid4())
    response = client.get(
        "/internal/v1/customers/101/cart/snapshot",
        params={"expected_version": 7},
        headers={
            "X-Internal-Service": "order-service",
            "X-Correlation-ID": correlation_id,
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.headers["X-Correlation-ID"] == correlation_id
    assert response.json() == {
        "customer_id": 101,
        "cart_id": str(cart_id),
        "cart_version": 7,
        "supplier_id": 201,
        "items": [
            {
                "product_id": 1001,
                "product_name": "QA Keyboard",
                "supplier_id": 201,
                "quantity": 2,
                "unit_price": "1500.50",
                "currency": "RUB",
                "product_status": "ACTIVE",
                "projected_available_quantity": 5,
                "projection_updated_at": now.isoformat().replace("+00:00", "Z"),
            }
        ],
        "generated_at": response.json()["generated_at"],
    }


@pytest.mark.integration
def test_internal_cart_snapshot_returns_explicit_empty_cart(customer_session_factory) -> None:
    cart_id = uuid.uuid4()
    with customer_session_factory.begin() as session:
        session.add(User(id=101, full_name="QA Customer", email="qa@example.test"))
        session.add(CartState(id=cart_id, user_id=101, version=1))

    client = _client(customer_session_factory)
    response = client.get(
        "/internal/v1/customers/101/cart/snapshot",
        params={"expected_version": 1},
        headers={
            "X-Internal-Service": "order-service",
            "X-Correlation-ID": str(uuid.uuid4()),
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["supplier_id"] is None
    assert response.json()["items"] == []
    assert response.json()["cart_version"] == 1


@pytest.mark.integration
def test_internal_cart_snapshot_returns_version_conflict_envelope(customer_session_factory) -> None:
    with customer_session_factory.begin() as session:
        session.add(User(id=101, full_name="QA Customer", email="qa@example.test"))
        session.add(CartState(user_id=101, version=3))

    client = _client(customer_session_factory)
    correlation_id = str(uuid.uuid4())
    response = client.get(
        "/internal/v1/customers/101/cart/snapshot",
        params={"expected_version": 2},
        headers={
            "X-Internal-Service": "order-service",
            "X-Correlation-ID": correlation_id,
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 409
    assert response.headers["X-Correlation-ID"] == correlation_id
    assert response.json()["error"]["code"] == "cart_version_conflict"
    assert response.json()["error"]["details"] == {"current_version": 3}
    assert response.json()["error"]["correlation_id"] == correlation_id


@pytest.mark.integration
def test_internal_cart_snapshot_exposes_multi_supplier_cart_explicitly(customer_session_factory) -> None:
    now = datetime.now(timezone.utc)
    with customer_session_factory.begin() as session:
        session.add(User(id=101, full_name="QA Customer", email="qa@example.test"))
        session.add(CartState(user_id=101, version=4))
        session.add_all(
            [
                Product(
                    id=1001,
                    supplier_id=201,
                    name="QA Keyboard",
                    description=None,
                    price=Decimal("1500.50"),
                    stocks=5,
                    is_active=True,
                    is_archived=False,
                    created_at=now,
                    updated_at=now,
                ),
                Product(
                    id=2001,
                    supplier_id=202,
                    name="QA Monitor",
                    description=None,
                    price=Decimal("5000.00"),
                    stocks=5,
                    is_active=True,
                    is_archived=False,
                    created_at=now,
                    updated_at=now,
                ),
                CartItem(user_id=101, product_id=1001, quantity=1),
                CartItem(user_id=101, product_id=2001, quantity=1),
            ]
        )

    client = _client(customer_session_factory)
    response = client.get(
        "/internal/v1/customers/101/cart/snapshot",
        params={"expected_version": 4},
        headers={
            "X-Internal-Service": "order-service",
            "X-Correlation-ID": str(uuid.uuid4()),
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["supplier_id"] is None
    assert {item["supplier_id"] for item in response.json()["items"]} == {201, 202}


@pytest.mark.integration
def test_cart_mutation_increments_version(customer_session_factory) -> None:
    now = datetime.now(timezone.utc)
    with customer_session_factory.begin() as session:
        session.add(User(id=101, full_name="QA Customer", email="qa@example.test"))
        session.add(CartState(user_id=101, version=1))
        session.add(
            Product(
                id=1001,
                supplier_id=201,
                name="QA Keyboard",
                description=None,
                price=Decimal("1500.50"),
                stocks=5,
                is_active=True,
                is_archived=False,
                created_at=now,
                updated_at=now,
            )
        )

    client = _client(customer_session_factory)
    response = client.post(
        "/cart",
        json={"user_id": 101, "product_id": 1001, "quantity": 1},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 201
    with customer_session_factory() as session:
        assert session.query(CartState).filter_by(user_id=101).one().version == 2
