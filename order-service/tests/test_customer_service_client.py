from __future__ import annotations

import uuid
from datetime import datetime, timezone

import httpx
import pytest

from app.clients.customer_service import CustomerServiceClient
from app.errors import ServiceError


def _client(handler) -> CustomerServiceClient:
    client = CustomerServiceClient(
        base_url="http://customer-service:8001",
        timeout_seconds=1.0,
        sleeper=lambda _delay: None,
    )
    client._client.close()
    client._client = httpx.Client(
        base_url="http://customer-service:8001",
        transport=httpx.MockTransport(handler),
    )
    return client


def test_customer_client_forwards_internal_and_correlation_headers() -> None:
    correlation_id = str(uuid.uuid4())

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["X-Internal-Service"] == "order-service"
        assert request.headers["X-Correlation-ID"] == correlation_id
        assert request.url.params["expected_version"] == "7"
        now = datetime.now(timezone.utc).isoformat()
        return httpx.Response(
            200,
            json={
                "customer_id": 101,
                "cart_id": str(uuid.uuid4()),
                "cart_version": 7,
                "supplier_id": 201,
                "items": [
                    {
                        "product_id": 1001,
                        "product_name": "QA Keyboard",
                        "supplier_id": 201,
                        "quantity": 1,
                        "unit_price": "1500.50",
                        "currency": "RUB",
                        "product_status": "ACTIVE",
                        "projected_available_quantity": 5,
                        "projection_updated_at": now,
                    }
                ],
                "generated_at": now,
            },
        )

    client = _client(handler)
    snapshot = client.get_cart_snapshot(
        customer_id=101,
        expected_version=7,
        correlation_id=correlation_id,
    )
    client.close()

    assert snapshot.customer_id == 101
    assert str(snapshot.items[0].unit_price) == "1500.50"


@pytest.mark.parametrize(
    ("exception_factory", "expected_code", "expected_status"),
    [
        (
            lambda request: httpx.ConnectError("connection failed", request=request),
            "dependency_unavailable",
            503,
        ),
        (
            lambda request: httpx.ReadTimeout("read timed out", request=request),
            "operation_timeout",
            504,
        ),
    ],
)
def test_customer_client_retries_and_maps_network_errors(
    exception_factory,
    expected_code,
    expected_status,
) -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise exception_factory(request)

    client = _client(handler)
    with pytest.raises(ServiceError) as exc_info:
        client.get_cart_snapshot(
            customer_id=101,
            expected_version=7,
            correlation_id=str(uuid.uuid4()),
        )
    client.close()

    assert attempts == 3
    assert exc_info.value.code == expected_code
    assert exc_info.value.status_code == expected_status
