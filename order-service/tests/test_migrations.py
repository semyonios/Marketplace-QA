from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text

EXPECTED_TABLES = {
    "orders",
    "order_items",
    "order_status_history",
    "order_idempotency",
    "order_outbox",
    "order_inbox",
}


@pytest.mark.integration
def test_initial_migration_upgrades_empty_postgresql_schema() -> None:
    database_url = os.getenv("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL migration test")

    schema_name = f"order_migration_test_{uuid.uuid4().hex}"
    service_root = Path(__file__).resolve().parents[1]
    config = Config(str(service_root / "alembic.ini"))

    engine = create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))
            connection.commit()
            connection.execute(text(f'SET search_path TO "{schema_name}"'))
            config.attributes["connection"] = connection

            command.upgrade(config, "head")
            connection.commit()

            inspector = inspect(connection)
            assert set(inspector.get_table_names(schema=schema_name)) == EXPECTED_TABLES | {"alembic_version"}

            order_item_fks = inspector.get_foreign_keys("order_items", schema=schema_name)
            assert any(fk["referred_table"] == "orders" for fk in order_item_fks)

            idempotency_uniques = inspector.get_unique_constraints("order_idempotency", schema=schema_name)
            assert any(
                tuple(unique["column_names"]) == ("customer_id", "operation_type", "key_hash")
                for unique in idempotency_uniques
            )

            inbox_pk = inspector.get_pk_constraint("order_inbox", schema=schema_name)
            assert tuple(inbox_pk["constrained_columns"]) == ("event_id", "consumer_name")

            assert inspector.get_check_constraints("orders", schema=schema_name)
            assert inspector.get_indexes("order_outbox", schema=schema_name)
    finally:
        with engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        engine.dispose()
