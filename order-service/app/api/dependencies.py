from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass

from fastapi import Depends, Header
from sqlalchemy.orm import Session

from ..database import SessionLocal
from ..errors import ServiceError


@dataclass(frozen=True, slots=True)
class TestActor:
    role: str
    subject_id: int


def get_db() -> Generator[Session, None, None]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def get_test_actor(
    x_test_role: str | None = Header(default=None, alias="X-Test-Role"),
    x_test_subject_id: str | None = Header(default=None, alias="X-Test-Subject-ID"),
) -> TestActor:
    if x_test_role is None or x_test_subject_id is None:
        raise ServiceError(
            code="order_access_forbidden",
            category="FORBIDDEN",
            message="Test user context is required",
            status_code=403,
        )

    try:
        subject_id = int(x_test_subject_id)
    except ValueError as exc:
        raise ServiceError(
            code="order_access_forbidden",
            category="FORBIDDEN",
            message="Test subject context is invalid",
            status_code=403,
        ) from exc
    if subject_id <= 0:
        raise ServiceError(
            code="order_access_forbidden",
            category="FORBIDDEN",
            message="Test subject context is invalid",
            status_code=403,
        )
    return TestActor(role=x_test_role.upper(), subject_id=subject_id)


def require_customer_actor(actor: TestActor = Depends(get_test_actor)) -> TestActor:
    if actor.role != "CUSTOMER":
        raise ServiceError(
            code="order_access_forbidden",
            category="FORBIDDEN",
            message="Customer role is required",
            status_code=403,
        )
    return actor


def require_supplier_actor(actor: TestActor = Depends(get_test_actor)) -> TestActor:
    if actor.role != "SUPPLIER":
        raise ServiceError(
            code="order_access_forbidden",
            category="FORBIDDEN",
            message="Supplier role is required",
            status_code=403,
        )
    return actor
