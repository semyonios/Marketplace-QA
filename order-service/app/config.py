from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


def _read_positive_int(name: str, default: int) -> int:
    raw_value = os.getenv(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _read_bounded_positive_int(name: str, default: int, maximum: int) -> int:
    value = _read_positive_int(name, default)
    if value > maximum:
        raise ValueError(f"{name} must be less than or equal to {maximum}")
    return value


def _read_positive_float(name: str, default: float) -> float:
    raw_value = os.getenv(name, str(default))
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _read_bool(name: str, default: bool) -> bool:
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
    host: str
    port: int
    database_url: str
    log_level: str
    kafka_bootstrap_servers: str
    customer_service_url: str
    readiness_timeout_seconds: float
    alembic_config: str
    customer_service_timeout_seconds: float = 1.0
    outbox_publisher_enabled: bool = True
    stock_events_consumer_enabled: bool = True
    timeout_worker_enabled: bool = True
    outbox_poll_interval_seconds: float = 1.0
    outbox_batch_size: int = 100
    outbox_claim_lease_seconds: float = 30.0
    outbox_base_retry_delay_seconds: float = 1.0
    outbox_max_retry_delay_seconds: float = 30.0
    outbox_max_attempts: int = 10
    kafka_delivery_timeout_seconds: float = 10.0
    kafka_order_events_topic: str = "marketplace.order.events.v1"
    kafka_stock_commands_topic: str = "marketplace.stock.commands.v1"
    kafka_stock_events_topic: str = "marketplace.stock.events.v1"
    kafka_dlq_topic: str = "marketplace.order.dlq.v1"
    stock_events_consumer_group_id: str = "order-service-stock-events-v1"
    stock_events_consumer_name: str = "order-service-stock-events-v1"
    stock_events_consumer_max_attempts: int = 3
    stock_events_consumer_poll_seconds: float = 1.0
    timeout_worker_poll_seconds: float = 1.0
    timeout_worker_batch_size: int = 50
    timeout_worker_retry_seconds: float = 5.0
    timeout_worker_max_attempts: int = 5

    @classmethod
    def from_environment(cls) -> "Settings":
        project_root = Path(__file__).resolve().parents[1]
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        if log_level not in logging.getLevelNamesMapping():
            raise ValueError("LOG_LEVEL must be a valid Python logging level")

        return cls(
            service_name=os.getenv("SERVICE_NAME", "order-service"),
            environment=os.getenv("ENVIRONMENT", "development"),
            host=os.getenv("HOST", "0.0.0.0"),
            port=_read_positive_int("PORT", 8000),
            database_url=os.getenv(
                "DATABASE_URL",
                "postgresql+psycopg://order_app:order_app@localhost:5434/order_db",
            ),
            log_level=log_level,
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            customer_service_url=os.getenv("CUSTOMER_SERVICE_URL", "http://localhost:8001"),
            readiness_timeout_seconds=_read_positive_float("READINESS_TIMEOUT_SECONDS", 2.0),
            alembic_config=os.getenv("ALEMBIC_CONFIG", str(project_root / "alembic.ini")),
            customer_service_timeout_seconds=_read_positive_float(
                "CUSTOMER_SERVICE_TIMEOUT_SECONDS",
                1.0,
            ),
            outbox_publisher_enabled=_read_bool("OUTBOX_PUBLISHER_ENABLED", True),
            stock_events_consumer_enabled=_read_bool(
                "STOCK_EVENTS_CONSUMER_ENABLED",
                True,
            ),
            timeout_worker_enabled=_read_bool("TIMEOUT_WORKER_ENABLED", True),
            outbox_poll_interval_seconds=_read_positive_float(
                "OUTBOX_POLL_INTERVAL_SECONDS",
                1.0,
            ),
            outbox_batch_size=_read_bounded_positive_int("OUTBOX_BATCH_SIZE", 100, 100),
            outbox_claim_lease_seconds=_read_positive_float(
                "OUTBOX_CLAIM_LEASE_SECONDS",
                30.0,
            ),
            outbox_base_retry_delay_seconds=_read_positive_float(
                "OUTBOX_BASE_RETRY_DELAY_SECONDS",
                1.0,
            ),
            outbox_max_retry_delay_seconds=_read_positive_float(
                "OUTBOX_MAX_RETRY_DELAY_SECONDS",
                30.0,
            ),
            outbox_max_attempts=_read_positive_int("OUTBOX_MAX_ATTEMPTS", 10),
            kafka_delivery_timeout_seconds=_read_positive_float(
                "KAFKA_DELIVERY_TIMEOUT_SECONDS",
                10.0,
            ),
            kafka_order_events_topic=os.getenv(
                "KAFKA_ORDER_EVENTS_TOPIC",
                "marketplace.order.events.v1",
            ),
            kafka_stock_commands_topic=os.getenv(
                "KAFKA_STOCK_COMMANDS_TOPIC",
                "marketplace.stock.commands.v1",
            ),
            kafka_stock_events_topic=os.getenv(
                "KAFKA_STOCK_EVENTS_TOPIC",
                "marketplace.stock.events.v1",
            ),
            kafka_dlq_topic=os.getenv(
                "KAFKA_DLQ_TOPIC",
                "marketplace.order.dlq.v1",
            ),
            stock_events_consumer_group_id=os.getenv(
                "STOCK_EVENTS_CONSUMER_GROUP_ID",
                "order-service-stock-events-v1",
            ),
            stock_events_consumer_name=os.getenv(
                "STOCK_EVENTS_CONSUMER_NAME",
                "order-service-stock-events-v1",
            ),
            stock_events_consumer_max_attempts=_read_positive_int(
                "STOCK_EVENTS_CONSUMER_MAX_ATTEMPTS",
                3,
            ),
            stock_events_consumer_poll_seconds=_read_positive_float(
                "STOCK_EVENTS_CONSUMER_POLL_SECONDS",
                1.0,
            ),
            timeout_worker_poll_seconds=_read_positive_float(
                "TIMEOUT_WORKER_POLL_SECONDS",
                1.0,
            ),
            timeout_worker_batch_size=_read_bounded_positive_int(
                "TIMEOUT_WORKER_BATCH_SIZE",
                50,
                100,
            ),
            timeout_worker_retry_seconds=_read_positive_float(
                "TIMEOUT_WORKER_RETRY_SECONDS",
                5.0,
            ),
            timeout_worker_max_attempts=_read_positive_int(
                "TIMEOUT_WORKER_MAX_ATTEMPTS",
                5,
            ),
        )


@lru_cache
def get_settings() -> Settings:
    return Settings.from_environment()
