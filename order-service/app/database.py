from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass

from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from .config import Settings, get_settings

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


settings = get_settings()
engine: Engine = create_engine(
    settings.database_url,
    future=True,
    pool_pre_ping=True,
    connect_args={"connect_timeout": max(1, math.ceil(settings.readiness_timeout_seconds))},
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


@dataclass(frozen=True, slots=True)
class ReadinessState:
    database: str
    migrations: str
    kafka: str = "unknown"


class ReadinessCheckError(RuntimeError):
    def __init__(self, state: ReadinessState, message: str) -> None:
        super().__init__(message)
        self.state = state


def _expected_revision(config_path: str) -> str:
    alembic_config = Config(config_path)
    revision = ScriptDirectory.from_config(alembic_config).get_current_head()
    if revision is None:
        raise RuntimeError("Alembic head revision is not configured")
    return revision


def check_readiness(
    *,
    database_engine: Engine = engine,
    application_settings: Settings = settings,
    kafka_checker: Callable[[], None] | None = None,
) -> ReadinessState:
    try:
        with database_engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            current_revision = MigrationContext.configure(connection).get_current_revision()
    except Exception as exc:
        raise ReadinessCheckError(
            ReadinessState(database="down", migrations="unknown"),
            "order database is unavailable",
        ) from exc

    try:
        expected_revision = _expected_revision(application_settings.alembic_config)
    except Exception as exc:
        raise ReadinessCheckError(
            ReadinessState(database="up", migrations="unknown"),
            "migration metadata is unavailable",
        ) from exc

    if current_revision != expected_revision:
        raise ReadinessCheckError(
            ReadinessState(database="up", migrations="out_of_date"),
            "database migration revision is out of date",
        )

    if not application_settings.outbox_publisher_enabled:
        return ReadinessState(database="up", migrations="up_to_date", kafka="disabled")

    if kafka_checker is None:
        raise ReadinessCheckError(
            ReadinessState(database="up", migrations="up_to_date", kafka="unknown"),
            "Kafka readiness checker is not configured",
        )
    try:
        kafka_checker()
    except Exception as exc:
        raise ReadinessCheckError(
            ReadinessState(database="up", migrations="up_to_date", kafka="down"),
            "Kafka is unavailable",
        ) from exc

    return ReadinessState(database="up", migrations="up_to_date", kafka="up")
