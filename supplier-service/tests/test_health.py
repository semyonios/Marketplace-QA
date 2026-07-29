from fastapi.testclient import TestClient

from app.database import ReadinessCheckError, ReadinessState
from app.main import app


def test_health_does_not_call_dependency_readiness() -> None:
    def must_not_run():
        raise AssertionError("health must not check dependencies")

    app.state.readiness_checker = must_not_run
    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready_reports_database_migrations_and_kafka_up() -> None:
    app.state.readiness_checker = lambda: ReadinessState(
        database="up",
        migrations="up_to_date",
        kafka="up",
    )
    with TestClient(app) as client:
        response = client.get("/ready")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ready",
        "service": "supplier-service",
        "dependencies": {
            "database": "up",
            "migrations": "up_to_date",
            "kafka": "up",
        },
    }


def test_ready_reports_kafka_down_without_exposing_connection_details() -> None:
    def unavailable():
        raise ReadinessCheckError(
            ReadinessState(
                database="up",
                migrations="up_to_date",
                kafka="down",
            ),
            "Kafka is unavailable at internal-host:9092",
        )

    app.state.readiness_checker = unavailable
    with TestClient(app) as client:
        response = client.get("/ready")

    assert response.status_code == 503
    assert response.json() == {
        "status": "not_ready",
        "service": "supplier-service",
        "dependencies": {
            "database": "up",
            "migrations": "up_to_date",
            "kafka": "down",
        },
    }
    assert "internal-host" not in response.text
