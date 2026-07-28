from __future__ import annotations

import time
from collections.abc import Callable

import httpx
from pydantic import ValidationError

from ..errors import ServiceError
from ..schemas import CartSnapshot


class CustomerServiceClient:
    def __init__(
        self,
        *,
        base_url: str,
        timeout_seconds: float,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            timeout=httpx.Timeout(timeout_seconds),
        )
        self._sleeper = sleeper

    def close(self) -> None:
        self._client.close()

    def get_cart_snapshot(
        self,
        *,
        customer_id: int,
        expected_version: int,
        correlation_id: str,
    ) -> CartSnapshot:
        retry_delays = (0.0, 0.2, 0.5)
        last_was_timeout = False
        for attempt, delay in enumerate(retry_delays, start=1):
            if delay:
                self._sleeper(delay)
            try:
                response = self._client.get(
                    f"/internal/v1/customers/{customer_id}/cart/snapshot",
                    params={"expected_version": expected_version},
                    headers={
                        "X-Internal-Service": "order-service",
                        "X-Correlation-ID": correlation_id,
                    },
                )
            except httpx.TimeoutException:
                last_was_timeout = True
                if attempt < len(retry_delays):
                    continue
                break
            except httpx.RequestError:
                last_was_timeout = False
                if attempt < len(retry_delays):
                    continue
                break

            if response.status_code == 200:
                try:
                    return CartSnapshot.model_validate(response.json())
                except (ValidationError, ValueError) as exc:
                    raise ServiceError(
                        code="dependency_unavailable",
                        category="DEPENDENCY",
                        message="customer-service returned an invalid cart snapshot",
                        status_code=503,
                        details={"dependency": "customer-service"},
                        retryable=True,
                    ) from exc

            if response.status_code >= 500:
                last_was_timeout = False
                if attempt < len(retry_delays):
                    continue
                break

            self._raise_mapped_error(response)

        if last_was_timeout:
            raise ServiceError(
                code="operation_timeout",
                category="TIMEOUT",
                message="customer-service operation timed out",
                status_code=504,
                details={"dependency": "customer-service"},
                retryable=True,
            )
        raise ServiceError(
            code="dependency_unavailable",
            category="DEPENDENCY",
            message="customer-service is temporarily unavailable",
            status_code=503,
            details={"dependency": "customer-service"},
            retryable=True,
        )

    @staticmethod
    def _raise_mapped_error(response: httpx.Response) -> None:
        try:
            error = response.json().get("error", {})
        except ValueError:
            error = {}
        code = str(error.get("code", "dependency_unavailable"))
        details = error.get("details", {})
        if not isinstance(details, dict):
            details = {}

        mappings: dict[str, tuple[str, int, str, bool]] = {
            "invalid_customer_id": ("VALIDATION", 400, "Customer identifier is invalid", False),
            "customer_not_found": ("NOT_FOUND", 404, "Customer was not found", False),
            "cart_version_conflict": ("CONFLICT", 409, "Cart version changed", False),
            "cart_multiple_suppliers": ("BUSINESS", 409, "Cart contains products from multiple suppliers", False),
            "invalid_quantity": ("VALIDATION", 400, "Cart item quantity must be positive", False),
        }
        if code in mappings:
            category, status_code, message, retryable = mappings[code]
            raise ServiceError(
                code=code,
                category=category,
                message=message,
                status_code=status_code,
                details=details,
                retryable=retryable,
            )
        raise ServiceError(
            code="dependency_unavailable",
            category="DEPENDENCY",
            message="customer-service returned an unexpected response",
            status_code=503,
            details={"dependency": "customer-service"},
            retryable=True,
        )
