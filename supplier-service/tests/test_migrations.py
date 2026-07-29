from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text

EXPECTED_TABLES = {
    "users",
    "products",
    "warehouses",
    "warehouse_products",
    "processed_events",
    "supplier_inbox",
    "stock_reservations",
    "stock_reservation_items",
    "supplier_outbox",
}


@pytest.mark.integration
def test_initial_migration_upgrades_empty_postgresql_schema() -> None:
    database_url = os.getenv("TEST_SUPPLIER_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_SUPPLIER_DATABASE_URL is required")

    schema_name = f"supplier_migration_test_{uuid.uuid4().hex}"
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
            assert set(inspector.get_table_names(schema=schema_name)) == EXPECTED_TABLES | {
                "alembic_version"
            }
            product_columns = {
                column["name"]
                for column in inspector.get_columns("products", schema=schema_name)
            }
            assert "reserved_stocks" in product_columns
            inbox_pk = inspector.get_pk_constraint(
                "supplier_inbox",
                schema=schema_name,
            )
            assert tuple(inbox_pk["constrained_columns"]) == (
                "event_id",
                "consumer_name",
            )
            reservation_uniques = inspector.get_unique_constraints(
                "stock_reservations",
                schema=schema_name,
            )
            assert any(
                tuple(constraint["column_names"]) == ("reservation_request_id",)
                for constraint in reservation_uniques
            )
            assert inspector.get_check_constraints(
                "products",
                schema=schema_name,
            )
            assert inspector.get_indexes(
                "supplier_outbox",
                schema=schema_name,
            )
    finally:
        with engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        engine.dispose()
