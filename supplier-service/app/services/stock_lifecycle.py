from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from ..models import (
    Product,
    StockReservation,
    StockReservationItem,
    SupplierInbox,
    SupplierOutbox,
)
from .reservation import IncompatibleMessageError, RetryableProcessingError, canonical_hash

FINALIZE_COMMAND = "StockFinalizationRequested"
RELEASE_COMMAND = "StockReleaseRequested"
FINALIZED_EVENT = "StockFinalized"
FINALIZATION_FAILED_EVENT = "StockFinalizationFailed"
RELEASED_EVENT = "StockReleased"
RELEASE_FAILED_EVENT = "StockReleaseFailed"
RELEASE_REASONS = {
    "CUSTOMER_CANCELLED",
    "SUPPLIER_REJECTED",
    "RESERVATION_TIMEOUT",
    "COMPENSATION",
}


@dataclass(frozen=True, slots=True)
class StockLifecycleCommand:
    event_id: uuid.UUID
    event_type: str
    order_id: uuid.UUID
    supplier_id: int
    reservation_request_id: uuid.UUID
    correlation_id: uuid.UUID
    causation_id: uuid.UUID | None
    envelope_hash: str
    reservation_id: uuid.UUID | None = None
    finalization_request_id: uuid.UUID | None = None
    release_request_id: uuid.UUID | None = None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class StockLifecycleResult:
    result: str
    reservation_id: uuid.UUID | None
    result_event_id: uuid.UUID | None
    reason_code: str | None = None


def parse_stock_lifecycle_command(
    envelope: Mapping[str, Any],
    *,
    headers: Mapping[str, str | None] | None = None,
) -> StockLifecycleCommand:
    required = {
        "event_id",
        "event_type",
        "event_version",
        "occurred_at",
        "producer",
        "aggregate_type",
        "aggregate_id",
        "correlation_id",
        "causation_id",
        "payload",
    }
    if not required.issubset(envelope):
        raise IncompatibleMessageError("invalid_envelope", "Kafka envelope is incomplete")
    event_type = envelope["event_type"]
    if event_type not in {FINALIZE_COMMAND, RELEASE_COMMAND}:
        raise IncompatibleMessageError(
            "unsupported_event_type",
            "Unsupported stock command event type",
        )
    if (
        envelope["event_version"] != 1
        or envelope["producer"] != "order-service"
        or envelope["aggregate_type"] != "ORDER"
    ):
        raise IncompatibleMessageError("invalid_envelope", "Stock command envelope is invalid")

    event_id = _uuid(envelope["event_id"], "event_id")
    order_id = _uuid(envelope["aggregate_id"], "aggregate_id")
    correlation_id = _uuid(envelope["correlation_id"], "correlation_id")
    _timestamp(envelope["occurred_at"], "occurred_at")
    causation_id = (
        None
        if envelope["causation_id"] is None
        else _uuid(envelope["causation_id"], "causation_id")
    )
    payload = envelope["payload"]
    if not isinstance(payload, Mapping):
        raise IncompatibleMessageError("invalid_payload", "Stock command payload must be an object")
    base_fields = {
        "order_id",
        "supplier_id",
        "reservation_request_id",
        "reservation_id",
        "correlation_id",
    }
    operation_field = (
        "finalization_request_id"
        if event_type == FINALIZE_COMMAND
        else "release_request_id"
    )
    if not base_fields.union({operation_field}).issubset(payload):
        raise IncompatibleMessageError("invalid_payload", "Stock command payload is incomplete")
    if _uuid(payload["order_id"], "payload.order_id") != order_id:
        raise IncompatibleMessageError("aggregate_mismatch", "Payload order does not match aggregate")
    if _uuid(payload["correlation_id"], "payload.correlation_id") != correlation_id:
        raise IncompatibleMessageError(
            "correlation_mismatch",
            "Payload correlation does not match envelope",
        )
    reservation_id = (
        None
        if payload["reservation_id"] is None
        else _uuid(payload["reservation_id"], "reservation_id")
    )
    reason = payload.get("reason")
    if event_type == RELEASE_COMMAND and reason not in RELEASE_REASONS:
        raise IncompatibleMessageError("invalid_release_reason", "Release reason is invalid")
    if headers is not None:
        _validate_headers(envelope, headers)
    return StockLifecycleCommand(
        event_id=event_id,
        event_type=event_type,
        order_id=order_id,
        supplier_id=_positive_int(payload["supplier_id"], "supplier_id"),
        reservation_request_id=_uuid(
            payload["reservation_request_id"],
            "reservation_request_id",
        ),
        correlation_id=correlation_id,
        causation_id=causation_id,
        envelope_hash=canonical_hash(envelope),
        reservation_id=reservation_id,
        finalization_request_id=(
            _uuid(payload[operation_field], operation_field)
            if event_type == FINALIZE_COMMAND
            else None
        ),
        release_request_id=(
            _uuid(payload[operation_field], operation_field)
            if event_type == RELEASE_COMMAND
            else None
        ),
        reason=reason,
    )


class StockLifecycleService:
    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        consumer_name: str,
        clock: Callable[[], datetime] | None = None,
        event_id_factory: Callable[[], uuid.UUID] = uuid.uuid4,
    ) -> None:
        self._session_factory = session_factory
        self._consumer_name = consumer_name
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._event_id_factory = event_id_factory

    def process(self, command: StockLifecycleCommand) -> StockLifecycleResult:
        now = self._clock()
        with self._session_factory.begin() as session:
            inbox, duplicate = self._claim_inbox(session, command, now)
            if duplicate:
                return StockLifecycleResult("DUPLICATE_EVENT", None, None)

            reservation = session.scalar(
                select(StockReservation)
                .where(StockReservation.order_id == command.order_id)
                .with_for_update()
            )
            if command.event_type == FINALIZE_COMMAND:
                result, reason = self._finalize(session, reservation, command, now)
            else:
                result, reason = self._release(session, reservation, command, now)

            result_event_id = self._event_id_factory()
            session.add(
                _build_result_outbox(
                    command=command,
                    reservation=reservation,
                    event_id=result_event_id,
                    occurred_at=now,
                    result=result,
                    reason_code=reason,
                )
            )
            _mark_processed(inbox, now)

        return StockLifecycleResult(
            result=result,
            reservation_id=reservation.id if reservation is not None else None,
            result_event_id=result_event_id,
            reason_code=reason,
        )

    def record_retryable_failure(
        self,
        *,
        command: StockLifecycleCommand,
        safe_error: str,
        attempt: int,
    ) -> None:
        self._record_failure(command, safe_error, attempt, "FAILED_RETRYABLE")

    def mark_dlq(
        self,
        *,
        command: StockLifecycleCommand,
        safe_error: str,
        attempt: int,
    ) -> None:
        self._record_failure(command, safe_error, attempt, "DLQ")

    def _finalize(
        self,
        session: Session,
        reservation: StockReservation | None,
        command: StockLifecycleCommand,
        now: datetime,
    ) -> tuple[str, str | None]:
        mismatch = _reservation_mismatch(reservation, command, require_reservation=True)
        if mismatch is not None:
            return "FAILED", mismatch
        assert reservation is not None
        if reservation.status == "FINALIZED":
            if reservation.finalization_request_id == command.finalization_request_id:
                return "ALREADY_FINALIZED", None
            return "FAILED", "RESERVATION_ALREADY_TERMINAL"
        if reservation.status != "RESERVED":
            return "FAILED", "RESERVATION_NOT_ACTIVE"

        items, products = _lock_items_and_products(session, reservation.id)
        if not items:
            return "FAILED", "STOCK_INVARIANT_VIOLATION"
        for item in items:
            product = products.get(item.product_id)
            if (
                product is None
                or product.supplier_id != command.supplier_id
                or item.reserved_quantity <= 0
                or product.stocks < item.reserved_quantity
                or product.reserved_stocks < item.reserved_quantity
            ):
                return "FAILED", "STOCK_INVARIANT_VIOLATION"
        for item in items:
            product = products[item.product_id]
            product.stocks -= item.reserved_quantity
            product.reserved_stocks -= item.reserved_quantity
            item.reserved_quantity = 0
        reservation.status = "FINALIZED"
        reservation.finalization_request_id = command.finalization_request_id
        reservation.finalized_at = now
        reservation.updated_at = now
        return "FINALIZED", None

    def _release(
        self,
        session: Session,
        reservation: StockReservation | None,
        command: StockLifecycleCommand,
        now: datetime,
    ) -> tuple[str, str | None]:
        if reservation is None:
            return "NO_RESERVATION", None
        mismatch = _reservation_mismatch(
            reservation,
            command,
            require_reservation=command.reservation_id is not None,
        )
        if mismatch is not None:
            return "FAILED", mismatch
        if reservation.status == "RELEASED":
            return "ALREADY_RELEASED", None
        if reservation.status == "REJECTED":
            return "NO_RESERVATION", None
        if reservation.status == "FINALIZED":
            return "FAILED", "UNKNOWN_RESERVATION_STATE"
        if reservation.status != "RESERVED":
            return "FAILED", "UNKNOWN_RESERVATION_STATE"

        items, products = _lock_items_and_products(session, reservation.id)
        if not items:
            return "FAILED", "STOCK_INVARIANT_VIOLATION"
        for item in items:
            product = products.get(item.product_id)
            if (
                product is None
                or product.supplier_id != command.supplier_id
                or item.reserved_quantity <= 0
                or product.reserved_stocks < item.reserved_quantity
            ):
                return "FAILED", "STOCK_INVARIANT_VIOLATION"
        for item in items:
            products[item.product_id].reserved_stocks -= item.reserved_quantity
            item.reserved_quantity = 0
        reservation.status = "RELEASED"
        reservation.release_request_id = command.release_request_id
        reservation.released_at = now
        reservation.updated_at = now
        return "RELEASED", None

    def _claim_inbox(
        self,
        session: Session,
        command: StockLifecycleCommand,
        now: datetime,
    ) -> tuple[SupplierInbox, bool]:
        inserted = session.scalar(
            insert(SupplierInbox)
            .values(
                event_id=command.event_id,
                consumer_name=self._consumer_name,
                event_type=command.event_type,
                aggregate_id=command.order_id,
                correlation_id=command.correlation_id,
                payload_hash=command.envelope_hash,
                status="PROCESSING",
                attempt_count=1,
                received_at=now,
            )
            .on_conflict_do_nothing(
                index_elements=[SupplierInbox.event_id, SupplierInbox.consumer_name]
            )
            .returning(SupplierInbox.event_id)
        )
        inbox = session.scalar(
            select(SupplierInbox)
            .where(
                SupplierInbox.event_id == command.event_id,
                SupplierInbox.consumer_name == self._consumer_name,
            )
            .with_for_update()
        )
        if inbox is None:
            raise RetryableProcessingError("Inbox claim was not persisted")
        if inserted is not None:
            return inbox, False
        if inbox.payload_hash != command.envelope_hash:
            raise IncompatibleMessageError(
                "event_payload_conflict",
                "Event ID was reused with another payload",
            )
        if inbox.status in {"PROCESSED", "DLQ"}:
            return inbox, True
        if inbox.status == "FAILED_RETRYABLE":
            inbox.status = "PROCESSING"
            inbox.attempt_count += 1
            inbox.last_error = None
            return inbox, False
        raise RetryableProcessingError("Inbox event is already processing")

    def _record_failure(
        self,
        command: StockLifecycleCommand,
        safe_error: str,
        attempt: int,
        status: str,
    ) -> None:
        now = self._clock()
        values: dict[str, Any] = {
            "status": status,
            "attempt_count": attempt,
            "last_error": safe_error,
        }
        if status == "DLQ":
            values["processed_at"] = now
        with self._session_factory.begin() as session:
            session.execute(
                insert(SupplierInbox)
                .values(
                    event_id=command.event_id,
                    consumer_name=self._consumer_name,
                    event_type=command.event_type,
                    aggregate_id=command.order_id,
                    correlation_id=command.correlation_id,
                    payload_hash=command.envelope_hash,
                    status=status,
                    attempt_count=attempt,
                    received_at=now,
                    processed_at=values.get("processed_at"),
                    last_error=safe_error,
                )
                .on_conflict_do_update(
                    index_elements=[SupplierInbox.event_id, SupplierInbox.consumer_name],
                    set_=values,
                    where=SupplierInbox.status != "PROCESSED",
                )
            )


def _lock_items_and_products(
    session: Session,
    reservation_id: uuid.UUID,
) -> tuple[tuple[StockReservationItem, ...], dict[int, Product]]:
    items = tuple(
        session.scalars(
            select(StockReservationItem)
            .where(StockReservationItem.reservation_id == reservation_id)
            .order_by(StockReservationItem.product_id)
            .with_for_update()
        )
    )
    product_ids = [item.product_id for item in items]
    products = tuple(
        session.scalars(
            select(Product)
            .where(Product.id.in_(product_ids))
            .order_by(Product.id)
            .with_for_update()
        )
    )
    return items, {product.id: product for product in products}


def _reservation_mismatch(
    reservation: StockReservation | None,
    command: StockLifecycleCommand,
    *,
    require_reservation: bool,
) -> str | None:
    if reservation is None:
        return "RESERVATION_NOT_FOUND" if require_reservation else None
    if reservation.supplier_id != command.supplier_id:
        return "SUPPLIER_MISMATCH"
    if reservation.order_id != command.order_id:
        return "ORDER_MISMATCH"
    if reservation.reservation_request_id != command.reservation_request_id:
        return "RESERVATION_REQUEST_MISMATCH"
    if command.reservation_id is not None and reservation.id != command.reservation_id:
        return "RESERVATION_MISMATCH"
    return None


def _build_result_outbox(
    *,
    command: StockLifecycleCommand,
    reservation: StockReservation | None,
    event_id: uuid.UUID,
    occurred_at: datetime,
    result: str,
    reason_code: str | None,
) -> SupplierOutbox:
    if command.event_type == FINALIZE_COMMAND:
        event_type = FINALIZED_EVENT if reason_code is None else FINALIZATION_FAILED_EVENT
        payload: dict[str, Any] = {
            "order_id": str(command.order_id),
            "supplier_id": command.supplier_id,
            "reservation_request_id": str(command.reservation_request_id),
            "reservation_id": (
                str(reservation.id)
                if reservation is not None
                else (str(command.reservation_id) if command.reservation_id else None)
            ),
            "finalization_request_id": str(command.finalization_request_id),
        }
        if reason_code is None:
            payload.update(result=result, finalized_at=_timestamp_string(occurred_at))
        else:
            payload.update(
                reason_code=reason_code,
                retryable=False,
                occurred_at=_timestamp_string(occurred_at),
            )
    else:
        event_type = RELEASED_EVENT if reason_code is None else RELEASE_FAILED_EVENT
        payload = {
            "order_id": str(command.order_id),
            "release_request_id": str(command.release_request_id),
            "reservation_id": (
                str(reservation.id)
                if reservation is not None
                else (str(command.reservation_id) if command.reservation_id else None)
            ),
            "reservation_request_id": str(command.reservation_request_id),
            "supplier_id": command.supplier_id,
        }
        if reason_code is None:
            payload.update(result=result, released_at=_timestamp_string(occurred_at))
        else:
            payload.update(
                reason_code=reason_code,
                retryable=reason_code in {"TEMPORARY_FAILURE", "INTERNAL_ERROR"},
                occurred_at=_timestamp_string(occurred_at),
            )
    envelope = {
        "event_id": str(event_id),
        "event_type": event_type,
        "event_version": 1,
        "occurred_at": _timestamp_string(occurred_at),
        "producer": "supplier-service",
        "aggregate_type": "ORDER",
        "aggregate_id": str(command.order_id),
        "correlation_id": str(command.correlation_id),
        "causation_id": str(command.event_id),
        "payload": payload,
    }
    headers = {
        "event_id": str(event_id),
        "event_type": event_type,
        "event_version": "1",
        "correlation_id": str(command.correlation_id),
        "causation_id": str(command.event_id),
        "producer": "supplier-service",
        "content_type": "application/json",
    }
    return SupplierOutbox(
        id=event_id,
        aggregate_type="ORDER",
        aggregate_id=command.order_id,
        event_type=event_type,
        event_version=1,
        payload=envelope,
        headers=headers,
        status="PENDING",
        attempt_count=0,
        next_attempt_at=occurred_at,
        created_at=occurred_at,
    )


def _mark_processed(inbox: SupplierInbox, now: datetime) -> None:
    inbox.status = "PROCESSED"
    inbox.processed_at = now
    inbox.last_error = None


def _validate_headers(
    envelope: Mapping[str, Any],
    headers: Mapping[str, str | None],
) -> None:
    expected = {
        "event_id": str(envelope["event_id"]),
        "event_type": str(envelope["event_type"]),
        "event_version": str(envelope["event_version"]),
        "correlation_id": str(envelope["correlation_id"]),
        "causation_id": (
            None if envelope["causation_id"] is None else str(envelope["causation_id"])
        ),
        "producer": str(envelope["producer"]),
        "content_type": "application/json",
    }
    if not set(expected).issubset(headers) or any(
        headers[name] != value for name, value in expected.items()
    ):
        raise IncompatibleMessageError("header_mismatch", "Kafka headers do not match envelope")


def _uuid(value: Any, field: str) -> uuid.UUID:
    try:
        return uuid.UUID(str(value))
    except (ValueError, TypeError, AttributeError) as exc:
        raise IncompatibleMessageError("invalid_uuid", f"{field} must be UUID") from exc


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise IncompatibleMessageError("invalid_integer", f"{field} must be positive")
    return value


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise IncompatibleMessageError("invalid_timestamp", f"{field} must be a timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise IncompatibleMessageError("invalid_timestamp", f"{field} must be a timestamp") from exc
    if parsed.tzinfo is None:
        raise IncompatibleMessageError(
            "invalid_timestamp",
            f"{field} must include timezone",
        )
    return parsed.astimezone(timezone.utc)


def _timestamp_string(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
