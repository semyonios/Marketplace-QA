#!/usr/bin/env python3
"""Idempotent deterministic demo data for a local Marketplace-QA stack."""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any

SUPPLIER_URL = os.getenv("SUPPLIER_URL", "http://localhost:8000")
CUSTOMER_URL = os.getenv("CUSTOMER_URL", "http://localhost:8001")
WAIT_SECONDS = int(os.getenv("SEED_WAIT_SECONDS", "90"))

SUPPLIERS = [
    {
        "full_name": "Atlas Demo Supplier",
        "phone_number": "+79990000001",
        "email": "atlas.supplier@example.com",
        "birth_date": "1988-04-12",
        "city": "Moscow",
    },
    {
        "full_name": "Northstar Demo Supplier",
        "phone_number": "+79990000002",
        "email": "northstar.supplier@example.com",
        "birth_date": "1990-09-23",
        "city": "Saint Petersburg",
    },
]

CUSTOMERS = [
    {"full_name": "Elena QA Customer", "email": "elena.customer@example.com"},
    {"full_name": "Pavel QA Customer", "email": "pavel.customer@example.com"},
]

WAREHOUSES = [
    {
        "name": "Moscow North",
        "weekday_hours": "Mon-Fri 09:00-18:00",
        "address": "Moscow, Testovaya street, 10",
    },
    {
        "name": "Moscow South",
        "weekday_hours": "Mon-Fri 08:00-20:00",
        "address": "Moscow, Proverochnaya street, 25",
    },
    {
        "name": "Saint Petersburg Hub",
        "weekday_hours": "Mon-Sat 10:00-19:00",
        "address": "Saint Petersburg, Demo avenue, 7",
    },
]

PRODUCTS = [
    ("atlas.supplier@example.com", "QA Laptop Pro", "Notebook for order lifecycle demos", 129999.00, True, False),
    ("atlas.supplier@example.com", "Mechanical Keyboard", "Hot-swappable QA keyboard", 8990.00, True, False),
    ("atlas.supplier@example.com", "USB-C Dock", "Docking station with multiple ports", 15490.00, True, False),
    ("atlas.supplier@example.com", "Out of Stock Monitor", "Product for insufficient stock checks", 32990.00, True, False),
    ("atlas.supplier@example.com", "Archived Mouse", "Archived catalog example", 2490.00, False, True),
    ("northstar.supplier@example.com", "Noise Cancelling Headphones", "Wireless headset", 21990.00, True, False),
    ("northstar.supplier@example.com", "Web Camera 4K", "Camera for remote QA sessions", 11990.00, True, False),
    ("northstar.supplier@example.com", "Portable SSD 2TB", "Fast external storage", 18990.00, True, False),
    ("northstar.supplier@example.com", "Zero Stock Tablet", "Second unavailable product", 44990.00, True, False),
    ("northstar.supplier@example.com", "Inactive Smart Speaker", "Inactive catalog example", 6990.00, False, False),
]

STOCKS_BY_WAREHOUSE = [
    [50, 20, 5, 0, 0, 30, 10, 2, 0, 10],
    [25, 10, 0, 0, 0, 20, 5, 1, 0, 5],
    [25, 20, 5, 0, 0, 50, 10, 2, 0, 5],
]


class SeedError(RuntimeError):
    pass


def request_json(
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
) -> tuple[int, Any]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            content = response.read()
            return response.status, json.loads(content) if content else None
    except urllib.error.HTTPError as exc:
        content = exc.read()
        detail = content.decode("utf-8", errors="replace")
        raise SeedError(f"{method} {url} -> HTTP {exc.code}: {detail}") from exc


def wait_ready(url: str) -> None:
    deadline = time.monotonic() + WAIT_SECONDS
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            status, _ = request_json("GET", url)
            if status == 200:
                return
        except Exception as exc:
            last_error = exc
        time.sleep(1)
    raise SeedError(f"Service did not become ready: {url}; last_error={last_error}")


def ensure_by_field(
    base_url: str,
    list_path: str,
    create_path: str,
    field: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    _, listing = request_json("GET", f"{base_url}{list_path}")
    for item in listing["items"]:
        if item.get(field) == payload[field]:
            return item
    _, created = request_json("POST", f"{base_url}{create_path}", payload)
    return created


def seed() -> dict[str, Any]:
    wait_ready(f"{SUPPLIER_URL}/ready")
    wait_ready(f"{CUSTOMER_URL}/health")

    suppliers = {
        payload["email"]: ensure_by_field(
            SUPPLIER_URL, "/suppliers", "/suppliers", "email", payload
        )
        for payload in SUPPLIERS
    }
    customers = [
        ensure_by_field(CUSTOMER_URL, "/users", "/users", "email", payload)
        for payload in CUSTOMERS
    ]
    warehouses = [
        ensure_by_field(
            SUPPLIER_URL, "/warehouses", "/warehouses", "name", payload
        )
        for payload in WAREHOUSES
    ]

    _, product_listing = request_json("GET", f"{SUPPLIER_URL}/products")
    products_by_key = {
        (item["supplier_id"], item["name"]): item for item in product_listing["items"]
    }
    products: list[dict[str, Any]] = []
    for supplier_email, name, description, price, active, archived in PRODUCTS:
        supplier_id = suppliers[supplier_email]["id"]
        payload = {
            "supplier_id": supplier_id,
            "name": name,
            "description": description,
            "price": price,
            "is_active": active,
            "is_archived": archived,
        }
        existing = products_by_key.get((supplier_id, name))
        if existing is None:
            _, product = request_json("POST", f"{SUPPLIER_URL}/products", payload)
        else:
            _, product = request_json(
                "PUT", f"{SUPPLIER_URL}/products/{existing['id']}", payload
            )
        products.append(product)

    for warehouse, quantities in zip(warehouses, STOCKS_BY_WAREHOUSE, strict=True):
        request_json(
            "POST",
            f"{SUPPLIER_URL}/warehouses/{warehouse['id']}/stocks",
            {
                "items": [
                    {"product_id": product["id"], "stocks": quantity}
                    for product, quantity in zip(products, quantities, strict=True)
                ]
            },
        )

    deadline = time.monotonic() + WAIT_SECONDS
    projected_count = 0
    while time.monotonic() < deadline:
        _, catalog = request_json("GET", f"{CUSTOMER_URL}/products")
        projected_count = catalog["count"]
        projected_ids = {item["id"] for item in catalog["items"]}
        if projected_ids.issuperset(product["id"] for product in products):
            break
        time.sleep(1)
    else:
        raise SeedError(
            f"Customer projection did not receive all products; count={projected_count}"
        )

    return {
        "customers": {item["email"]: item["id"] for item in customers},
        "suppliers": {email: item["id"] for email, item in suppliers.items()},
        "warehouses": {item["name"]: item["id"] for item in warehouses},
        "products": {item["name"]: item["id"] for item in products},
        "customer_projection_count": projected_count,
    }


if __name__ == "__main__":
    try:
        print(json.dumps(seed(), ensure_ascii=False, indent=2, sort_keys=True))
    except Exception as error:
        print(f"seed failed: {error}", file=sys.stderr)
        raise
