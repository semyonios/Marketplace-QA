from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.main import app, get_db
from app.models import CartItem, CartState, Product, User


@pytest.fixture
def frontend_session_factory():
    database_url = os.getenv("TEST_CUSTOMER_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_CUSTOMER_DATABASE_URL is required")
    schema_name = f"frontend_contract_{uuid.uuid4().hex}"
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


def _client(factory) -> TestClient:
    def override_db():
        with factory() as session:
            yield session
    app.dependency_overrides[get_db] = override_db
    return TestClient(app, raise_server_exceptions=False)


def _seed(factory) -> None:
    now = datetime.now(timezone.utc)
    with factory.begin() as session:
        session.add(User(id=101, full_name="QA Customer", email="qa@example.test"))
        session.add(CartState(user_id=101, version=3))
        session.add_all(
            [
                Product(id=1001, supplier_id=201, name="Keyboard", description=None, price=Decimal("100.00"), stocks=5, is_active=True, is_archived=False, created_at=now, updated_at=now),
                Product(id=2001, supplier_id=202, name="Monitor", description=None, price=Decimal("200.00"), stocks=5, is_active=True, is_archived=False, created_at=now, updated_at=now),
            ]
        )


@pytest.mark.integration
def test_public_catalog_and_cart_expose_supplier_and_cart_version(frontend_session_factory) -> None:
    _seed(frontend_session_factory)
    with frontend_session_factory.begin() as session:
        session.add(CartItem(user_id=101, product_id=1001, quantity=1))
    client = _client(frontend_session_factory)

    catalog = client.get("/products")
    cart = client.get("/cart", params={"user_id": 101})
    app.dependency_overrides.clear()

    assert catalog.status_code == 200
    assert catalog.json()["items"][0]["supplier_id"] == 201
    assert cart.status_code == 200
    assert cart.json()["cart_version"] == 3
    assert cart.json()["supplier_id"] == 201
    assert cart.json()["items"][0]["product"]["supplier_id"] == 201


@pytest.mark.integration
def test_public_cart_rejects_second_supplier(frontend_session_factory) -> None:
    _seed(frontend_session_factory)
    with frontend_session_factory.begin() as session:
        session.add(CartItem(user_id=101, product_id=1001, quantity=1))
    client = _client(frontend_session_factory)

    response = client.post(
        "/cart",
        json={"user_id": 101, "product_id": 2001, "quantity": 1},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "multi_supplier_cart"
