from fastapi.testclient import TestClient

from app.config import Settings
from app.database import ReadinessCheckError, ReadinessState
from app.main import create_app


def _test_settings() -> Settings:
    return Settings(
        service_name="order-service",
        environment="test",
        host="127.0.0.1",
        port=8000,
        database_url="postgresql+psycopg://order_app:order_app@localhost:5434/order_db",
        log_level="WARNING",
        kafka_bootstrap_servers="localhost:9092",
        customer_service_url="http://customer-service:8001",
        readiness_timeout_seconds=1.0,
        alembic_config="alembic.ini",
    )


def test_health_returns_200_without_database_check() -> None:
    application = create_app(_test_settings())

    def must_not_run() -> ReadinessState:
        raise AssertionError("health endpoint must not query readiness dependencies")

    application.state.readiness_checker = must_not_run
    with TestClient(application) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "order-service"}
    assert response.headers["X-Correlation-ID"]


def test_ready_returns_200_when_database_migrations_and_kafka_are_ready() -> None:
    application = create_app(_test_settings())
    application.state.readiness_checker = lambda: ReadinessState(
        database="up",
        migrations="up_to_date",
        kafka="up",
    )

    with TestClient(application) as client:
        response = client.get("/ready")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ready",
        "service": "order-service",
        "dependencies": {
            "database": "up",
            "migrations": "up_to_date",
            "kafka": "up",
        },
    }


def test_ready_returns_503_when_database_is_unavailable() -> None:
    application = create_app(_test_settings())

    def unavailable() -> ReadinessState:
        raise ReadinessCheckError(
            ReadinessState(database="down", migrations="unknown", kafka="unknown"),
            "order database is unavailable",
        )

    application.state.readiness_checker = unavailable
    with TestClient(application) as client:
        response = client.get("/ready")

    assert response.status_code == 503
    assert response.json() == {
        "status": "not_ready",
        "service": "order-service",
        "dependencies": {
            "database": "down",
            "migrations": "unknown",
            "kafka": "unknown",
        },
    }


def test_ready_returns_503_when_kafka_is_unavailable() -> None:
    application = create_app(_test_settings())

    def unavailable() -> ReadinessState:
        raise ReadinessCheckError(
            ReadinessState(database="up", migrations="up_to_date", kafka="down"),
            "Kafka is unavailable",
        )

    application.state.readiness_checker = unavailable
    with TestClient(application) as client:
        response = client.get("/ready")

    assert response.status_code == 503
    assert response.json() == {
        "status": "not_ready",
        "service": "order-service",
        "dependencies": {
            "database": "up",
            "migrations": "up_to_date",
            "kafka": "down",
        },
    }
