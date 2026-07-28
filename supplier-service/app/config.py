from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


def _positive_int(name: str, default: int, *, maximum: int | None = None) -> int:
    raw_value = os.getenv(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} must be less than or equal to {maximum}")
    return value


def _positive_float(name: str, default: float) -> float:
    raw_value = os.getenv(name, str(default))
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _boolean(name: str, default: bool) -> bool:
    raw_value = os.getenv(name, str(default)).strip().lower()
    if raw_value in {"1", "true", "yes", "on"}:
        return True
    if raw_value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")


@dataclass(frozen=True, slots=True)
class Settings:
    service_name: str
    environment: str
    database_url: str
    kafka_bootstrap_servers: str
    alembic_config: str
    log_level: str = "INFO"
    readiness_timeout_seconds: float = 2.0
    reservation_consumer_enabled: bool = True
    supplier_outbox_publisher_enabled: bool = True
    stock_commands_topic: str = "marketplace.stock.commands.v1"
    stock_events_topic: str = "marketplace.stock.events.v1"
    dlq_topic: str = "marketplace.order.dlq.v1"
    reservation_consumer_group_id: str = "supplier-service-stock-commands-v1"
    reservation_consumer_name: str = "supplier-service-stock-commands-v1"
    consumer_max_attempts: int = 3
    consumer_poll_seconds: float = 1.0
    outbox_poll_interval_seconds: float = 1.0
    outbox_batch_size: int = 100
    outbox_claim_lease_seconds: float = 30.0
    outbox_base_retry_delay_seconds: float = 1.0
    outbox_max_retry_delay_seconds: float = 30.0
    outbox_max_attempts: int = 10
    kafka_delivery_timeout_seconds: float = 10.0

    @classmethod
    def from_environment(cls) -> "Settings":
        service_root = Path(__file__).resolve().parents[1]
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        if log_level not in logging.getLevelNamesMapping():
            raise ValueError("LOG_LEVEL must be a valid Python logging level")
        return cls(
            service_name=os.getenv("SERVICE_NAME", "supplier-service"),
            environment=os.getenv("ENVIRONMENT", "development"),
            database_url=os.getenv(
                "DATABASE_URL",
                "postgresql+psycopg://app:app@localhost:5432/supplier_db",
            ),
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            alembic_config=os.getenv("ALEMBIC_CONFIG", str(service_root / "alembic.ini")),
            log_level=log_level,
            readiness_timeout_seconds=_positive_float("READINESS_TIMEOUT_SECONDS", 2.0),
            reservation_consumer_enabled=_boolean("RESERVATION_CONSUMER_ENABLED", True),
            supplier_outbox_publisher_enabled=_boolean(
                "SUPPLIER_OUTBOX_PUBLISHER_ENABLED",
                True,
            ),
            stock_commands_topic=os.getenv(
                "KAFKA_STOCK_COMMANDS_TOPIC",
                "marketplace.stock.commands.v1",
            ),
            stock_events_topic=os.getenv(
                "KAFKA_STOCK_EVENTS_TOPIC",
                "marketplace.stock.events.v1",
            ),
            dlq_topic=os.getenv("KAFKA_DLQ_TOPIC", "marketplace.order.dlq.v1"),
            reservation_consumer_group_id=os.getenv(
                "RESERVATION_CONSUMER_GROUP_ID",
                "supplier-service-stock-commands-v1",
            ),
            reservation_consumer_name=os.getenv(
                "RESERVATION_CONSUMER_NAME",
                "supplier-service-stock-commands-v1",
            ),
            consumer_max_attempts=_positive_int("CONSUMER_MAX_ATTEMPTS", 3),
            consumer_poll_seconds=_positive_float("CONSUMER_POLL_SECONDS", 1.0),
            outbox_poll_interval_seconds=_positive_float(
                "SUPPLIER_OUTBOX_POLL_INTERVAL_SECONDS",
                1.0,
            ),
            outbox_batch_size=_positive_int(
                "SUPPLIER_OUTBOX_BATCH_SIZE",
                100,
                maximum=100,
            ),
            outbox_claim_lease_seconds=_positive_float(
                "SUPPLIER_OUTBOX_CLAIM_LEASE_SECONDS",
                30.0,
            ),
            outbox_base_retry_delay_seconds=_positive_float(
                "SUPPLIER_OUTBOX_BASE_RETRY_DELAY_SECONDS",
                1.0,
            ),
            outbox_max_retry_delay_seconds=_positive_float(
                "SUPPLIER_OUTBOX_MAX_RETRY_DELAY_SECONDS",
                30.0,
            ),
            outbox_max_attempts=_positive_int("SUPPLIER_OUTBOX_MAX_ATTEMPTS", 10),
            kafka_delivery_timeout_seconds=_positive_float(
                "KAFKA_DELIVERY_TIMEOUT_SECONDS",
                10.0,
            ),
        )


@lru_cache
def get_settings() -> Settings:
    return Settings.from_environment()
