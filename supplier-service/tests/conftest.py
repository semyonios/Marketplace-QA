from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.database import Base


@pytest.fixture
def supplier_session_factory():
    database_url = os.getenv("TEST_SUPPLIER_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_SUPPLIER_DATABASE_URL is required")

    schema_name = f"supplier_reservation_test_{uuid.uuid4().hex}"
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
