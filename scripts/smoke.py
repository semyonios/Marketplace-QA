#!/usr/bin/env python3
"""Compact post-start API/Kafka smoke test using only the Python standard library."""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from typing import Any

FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")
CUSTOMER_URL = os.getenv("CUSTOMER_URL", "http://localhost:8001")
SUPPLIER_URL = os.getenv("SUPPLIER_URL", "http://localhost:8000")
ORDER_URL = os.getenv("ORDER_URL", "http://localhost:8002")
WAIT_SECONDS = int(os.getenv("SMOKE_WAIT_SECONDS", "90"))


def call(
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, Any, dict[str, str]]:
    request_headers = {"Accept": "application/json", **(headers or {})}
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        url, data=data, method=method, headers=request_headers
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            content = response.read()
            return (
                response.status,
                json.loads(content) if content else None,
                {key.lower(): value for key, value in response.headers.items()},
            )
    except urllib.error.HTTPError as exc:
        content = exc.read()
        try:
            body = json.loads(content) if content else None
        except json.JSONDecodeError:
            body = content.decode("utf-8", errors="replace")
        return exc.code, body, {
            key.lower(): value for key, value in exc.headers.items()
        }


def wait_200(url: str) -> Any:
    deadline = time.monotonic() + WAIT_SECONDS
    last: tuple[int, Any] | None = None
    while time.monotonic() < deadline:
        try:
            status, body, _ = call("GET", url)
            last = (status, body)
            if status == 200:
                return body
        except OSError:
            pass
        time.sleep(1)
    raise AssertionError(f"Timed out waiting for {url}; last={last}")


def assert_status(actual: int, expected: int, label: str, body: Any) -> None:
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected}, got {actual}: {body}")


def main() -> dict[str, Any]:
    wait_200(f"{FRONTEND_URL}/health")
    customer_health = wait_200(f"{CUSTOMER_URL}/health")
    supplier_ready = wait_200(f"{SUPPLIER_URL}/ready")
    order_ready = wait_200(f"{ORDER_URL}/ready")
    assert supplier_ready["dependencies"]["migrations"] == "up_to_date"
    assert order_ready["dependencies"]["migrations"] == "up_to_date"
    assert supplier_ready["dependencies"]["kafka"] == "up"
    assert order_ready["dependencies"]["kafka"] == "up"

    _, catalog, _ = call("GET", f"{CUSTOMER_URL}/products")
    _, source_products, _ = call("GET", f"{SUPPLIER_URL}/products")
    assert catalog["count"] >= 10
    assert source_products["count"] >= 10

    customer_id = 1
    product_id = 1
    cart_status, cart, _ = call(
        "POST",
        f"{CUSTOMER_URL}/cart",
        {"user_id": customer_id, "product_id": product_id, "quantity": 1},
    )
    if cart_status == 409:
        _, current_cart, _ = call(
            "GET", f"{CUSTOMER_URL}/cart?user_id={customer_id}"
        )
        for item in current_cart["items"]:
            status, body, _ = call(
                "DELETE",
                f"{CUSTOMER_URL}/cart/{item['product']['id']}?user_id={customer_id}",
            )
            assert_status(status, 204, "clear cart", body)
        cart_status, cart, _ = call(
            "POST",
            f"{CUSTOMER_URL}/cart",
            {"user_id": customer_id, "product_id": product_id, "quantity": 1},
        )
    assert_status(cart_status, 201, "add to cart", cart)

    _, cart_view, _ = call("GET", f"{CUSTOMER_URL}/cart?user_id={customer_id}")
    actor_headers = {
        "X-Test-Role": "CUSTOMER",
        "X-Test-Subject-ID": str(customer_id),
        "X-Correlation-ID": str(uuid.uuid4()),
        "Idempotency-Key": f"smoke-{uuid.uuid4()}",
    }
    create_status, order, create_headers = call(
        "POST",
        f"{ORDER_URL}/api/v1/orders",
        {"customer_id": customer_id, "cart_version": cart_view["cart_version"]},
        actor_headers,
    )
    assert_status(create_status, 202, "create order", order)
    assert create_headers.get("etag") == '"1"'

    detail_url = (
        f"{ORDER_URL}/api/v1/customers/{customer_id}/orders/{order['order_id']}"
    )
    read_headers = {
        "X-Test-Role": "CUSTOMER",
        "X-Test-Subject-ID": str(customer_id),
    }
    deadline = time.monotonic() + WAIT_SECONDS
    terminal = None
    while time.monotonic() < deadline:
        status, detail, response_headers = call(
            "GET", detail_url, headers=read_headers
        )
        assert_status(status, 200, "read order", detail)
        if detail["status"] in {"RESERVED", "REJECTED"}:
            terminal = detail
            assert response_headers.get("etag") == f'"{detail["version"]}"'
            break
        time.sleep(1)
    if terminal is None:
        raise AssertionError("Kafka processing did not move order to RESERVED/REJECTED")

    return {
        "frontend": "up",
        "customer_health": customer_health["status"],
        "supplier_readiness": supplier_ready["status"],
        "order_readiness": order_ready["status"],
        "catalog_count": catalog["count"],
        "source_product_count": source_products["count"],
        "order_id": terminal["order_id"],
        "order_status": terminal["status"],
        "order_version": terminal["version"],
    }


if __name__ == "__main__":
    try:
        print(json.dumps(main(), indent=2, sort_keys=True))
    except Exception as error:
        print(f"smoke failed: {error}", file=sys.stderr)
        raise
