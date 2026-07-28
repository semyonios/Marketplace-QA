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


def _read_positive_float(name: str, default: float) -> float:
    raw_value = os.getenv(name, str(default))
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


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
        )


@lru_cache
def get_settings() -> Settings:
    return Settings.from_environment()
