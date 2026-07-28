from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from ..models import (
    Product,
    StockReservation,
    StockReservationItem,
    SupplierInbox,
    SupplierOutbox,
)

SUCCESS_EVENT = "StockReservationSucceeded"
FAILURE_EVENT = "StockReservationFailed"
BUSINESS_REASONS = {
    "INSUFFICIENT_STOCK",
    "PRODUCT_NOT_FOUND",
    "PRODUCT_INACTIVE",
    "PRODUCT_ARCHIVED",
    "INVALID_REQUEST",
    "SUPPLIER_MISMATCH",
    "RESERVATION_DEADLINE_EXPIRED",
}


class IncompatibleMessageError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.safe_message = message


class RetryableProcessingError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class RequestedItem:
    product_id: int
    quantity: int


@dataclass(frozen=True, slots=True)
class ReservationCommand:
    event_id: uuid.UUID
    order_id: uuid.UUID
    customer_id: int
    supplier_id: int
    order_version: int
    reservation_request_id: uuid.UUID
    items: tuple[RequestedItem, ...]
    reservation_deadline_at: datetime
    correlation_id: uuid.UUID
    causation_id: uuid.UUID | None
    envelope_hash: str
    request_hash: str
    semantic_error: str | None = None


@dataclass(frozen=True, slots=True)
class ReservationProcessingResult:
    result: str
    reservation_id: uuid.UUID | None
    result_event_id: uuid.UUID | None
    reason_code: str | None = None


def canonical_hash(value: Mapping[str, Any]) -> str:
    canonical = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def parse_reservation_command(
    envelope: Mapping[str, Any],
    *,
    headers: Mapping[str, str | None] | None = None,
) -> ReservationCommand:
    required_envelope = {
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
    if not required_envelope.issubset(envelope):
        raise IncompatibleMessageError("invalid_envelope", "Kafka envelope is incomplete")
    if envelope["event_type"] != "StockReservationRequested":
        raise IncompatibleMessageError("unsupported_event_type", "Unsupported stock command event type")
    if envelope["event_version"] != 1:
        raise IncompatibleMessageError("unsupported_event_version", "Unsupported stock command event version")
    if envelope["producer"] != "order-service" or envelope["aggregate_type"] != "ORDER":
        raise IncompatibleMessageError("invalid_envelope", "Kafka envelope producer or aggregate is invalid")

    event_id = _uuid(envelope["event_id"], "event_id")
    order_id = _uuid(envelope["aggregate_id"], "aggregate_id")
    correlation_id = _uuid(envelope["correlation_id"], "correlation_id")
    causation_id = (
        None
        if envelope["causation_id"] is None
        else _uuid(envelope["causation_id"], "causation_id")
    )
    _timestamp(envelope["occurred_at"], "occurred_at")

    payload = envelope["payload"]
    if not isinstance(payload, Mapping):
        raise IncompatibleMessageError("invalid_payload", "Reservation payload must be an object")
    required_payload = {
        "order_id",
        "customer_id",
        "supplier_id",
        "order_version",
        "reservation_request_id",
        "items",
        "reservation_deadline_at",
        "correlation_id",
    }
    if not required_payload.issubset(payload):
        raise IncompatibleMessageError("invalid_payload", "Reservation payload is incomplete")
    if _uuid(payload["order_id"], "payload.order_id") != order_id:
        raise IncompatibleMessageError("aggregate_mismatch", "Payload order does not match aggregate")
    if _uuid(payload["correlation_id"], "payload.correlation_id") != correlation_id:
        raise IncompatibleMessageError("correlation_mismatch", "Payload correlation does not match envelope")

    customer_id = _positive_int(payload["customer_id"], "customer_id")
    supplier_id = _positive_int(payload["supplier_id"], "supplier_id")
    order_version = _positive_int(payload["order_version"], "order_version")
    reservation_request_id = _uuid(
        payload["reservation_request_id"],
        "reservation_request_id",
    )
    deadline = _timestamp(payload["reservation_deadline_at"], "reservation_deadline_at")
    raw_items = payload["items"]
    if not isinstance(raw_items, list) or not raw_items:
        raise IncompatibleMessageError("invalid_items", "Reservation items must be a non-empty array")

    items: list[RequestedItem] = []
    semantic_error: str | None = None
    seen_product_ids: set[int] = set()
    for raw_item in raw_items:
        if not isinstance(raw_item, Mapping):
            raise IncompatibleMessageError("invalid_items", "Reservation item must be an object")
        if not {"product_id", "quantity"}.issubset(raw_item):
            raise IncompatibleMessageError("invalid_items", "Reservation item is incomplete")
        product_id = _integer(raw_item["product_id"], "product_id")
        quantity = _integer(raw_item["quantity"], "quantity")
        if product_id <= 0 or quantity <= 0 or product_id in seen_product_ids:
            semantic_error = "INVALID_REQUEST"
        items.append(RequestedItem(product_id=product_id, quantity=quantity))
        seen_product_ids.add(product_id)

    if headers is not None:
        _validate_headers(envelope=envelope, headers=headers)

    return ReservationCommand(
        event_id=event_id,
        order_id=order_id,
        customer_id=customer_id,
        supplier_id=supplier_id,
        order_version=order_version,
        reservation_request_id=reservation_request_id,
        items=tuple(items),
        reservation_deadline_at=deadline,
        correlation_id=correlation_id,
        causation_id=causation_id,
        envelope_hash=canonical_hash(envelope),
        request_hash=canonical_hash(
            {
                name: value
                for name, value in payload.items()
                if name != "correlation_id"
            }
        ),
        semantic_error=semantic_error,
    )


class ReservationService:
    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        consumer_name: str,
        clock: Callable[[], datetime] | None = None,
        event_id_factory: Callable[[], uuid.UUID] = uuid.uuid4,
        outbox_builder: Callable[..., SupplierOutbox] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._consumer_name = consumer_name
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._event_id_factory = event_id_factory
        self._outbox_builder = outbox_builder or build_result_outbox

    def process(self, command: ReservationCommand) -> ReservationProcessingResult:
        now = self._clock()
        with self._session_factory.begin() as session:
            inbox, duplicate = self._claim_inbox(session=session, command=command, now=now)
            if duplicate:
                return ReservationProcessingResult(
                    result="DUPLICATE_EVENT",
                    reservation_id=None,
                    result_event_id=None,
                )

            _lock_logical_request(session, command.reservation_request_id)
            existing = session.scalar(
                select(StockReservation)
                .where(
                    StockReservation.reservation_request_id
                    == command.reservation_request_id
                )
                .with_for_update()
            )
            if existing is not None:
                if existing.request_hash != command.request_hash:
                    raise IncompatibleMessageError(
                        "reservation_request_conflict",
                        "Reservation request ID was reused with another payload",
                    )
                _mark_inbox_processed(inbox, now)
                return ReservationProcessingResult(
                    result="DUPLICATE_RESERVATION_REQUEST",
                    reservation_id=existing.id,
                    result_event_id=None,
                    reason_code=existing.failure_reason,
                )

            products = self._lock_products(session=session, command=command)
            reason_code = _business_rejection_reason(
                command=command,
                products=products,
                now=now,
            )
            reservation_id = uuid.uuid4()
            reservation = StockReservation(
                id=reservation_id,
                reservation_request_id=command.reservation_request_id,
                order_id=command.order_id,
                supplier_id=command.supplier_id,
                status="REJECTED" if reason_code else "RESERVED",
                correlation_id=command.correlation_id,
                request_hash=command.request_hash,
                failure_reason=reason_code,
                created_at=now,
                updated_at=now,
                expires_at=command.reservation_deadline_at,
            )
            session.add(reservation)

            items_can_be_persisted = (
                command.semantic_error is None
                and len({item.product_id for item in command.items}) == len(command.items)
            )
            if items_can_be_persisted:
                for item in command.items:
                    product = products.get(item.product_id)
                    available = product.available_stocks if product is not None else None
                    reserved_quantity = item.quantity if reason_code is None else 0
                    session.add(
                        StockReservationItem(
                            reservation_id=reservation_id,
                            product_id=item.product_id,
                            requested_quantity=item.quantity,
                            reserved_quantity=reserved_quantity,
                            available_quantity=available,
                        )
                    )

            if reason_code is None:
                for item in command.items:
                    products[item.product_id].reserved_stocks += item.quantity

            result_event_id = self._event_id_factory()
            outbox = self._outbox_builder(
                command=command,
                reservation=reservation,
                products=products,
                event_id=result_event_id,
                occurred_at=now,
            )
            session.add(outbox)
            _mark_inbox_processed(inbox, now)

        return ReservationProcessingResult(
            result="REJECTED" if reason_code else "RESERVED",
            reservation_id=reservation_id,
            result_event_id=result_event_id,
            reason_code=reason_code,
        )

    def record_retryable_failure(
        self,
        *,
        command: ReservationCommand,
        safe_error: str,
        attempt: int,
    ) -> None:
        now = self._clock()
        with self._session_factory.begin() as session:
            statement = (
                insert(SupplierInbox)
                .values(
                    event_id=command.event_id,
                    consumer_name=self._consumer_name,
                    event_type="StockReservationRequested",
                    aggregate_id=command.order_id,
                    correlation_id=command.correlation_id,
                    payload_hash=command.envelope_hash,
                    status="FAILED_RETRYABLE",
                    attempt_count=attempt,
                    received_at=now,
                    last_error=safe_error,
                )
                .on_conflict_do_update(
                    index_elements=[
                        SupplierInbox.event_id,
                        SupplierInbox.consumer_name,
                    ],
                    set_={
                        "status": "FAILED_RETRYABLE",
                        "attempt_count": attempt,
                        "last_error": safe_error,
                    },
                    where=SupplierInbox.status != "PROCESSED",
                )
            )
            session.execute(statement)

    def mark_dlq(
        self,
        *,
        command: ReservationCommand,
        safe_error: str,
        attempt: int,
    ) -> None:
        now = self._clock()
        with self._session_factory.begin() as session:
            statement = (
                insert(SupplierInbox)
                .values(
                    event_id=command.event_id,
                    consumer_name=self._consumer_name,
                    event_type="StockReservationRequested",
                    aggregate_id=command.order_id,
                    correlation_id=command.correlation_id,
                    payload_hash=command.envelope_hash,
                    status="DLQ",
                    attempt_count=attempt,
                    received_at=now,
                    processed_at=now,
                    last_error=safe_error,
                )
                .on_conflict_do_update(
                    index_elements=[
                        SupplierInbox.event_id,
                        SupplierInbox.consumer_name,
                    ],
                    set_={
                        "status": "DLQ",
                        "attempt_count": attempt,
                        "processed_at": now,
                        "last_error": safe_error,
                    },
                    where=SupplierInbox.status != "PROCESSED",
                )
            )
            session.execute(statement)

    def _claim_inbox(
        self,
        *,
        session: Session,
        command: ReservationCommand,
        now: datetime,
    ) -> tuple[SupplierInbox, bool]:
        inserted = session.scalar(
            insert(SupplierInbox)
            .values(
                event_id=command.event_id,
                consumer_name=self._consumer_name,
                event_type="StockReservationRequested",
                aggregate_id=command.order_id,
                correlation_id=command.correlation_id,
                payload_hash=command.envelope_hash,
                status="PROCESSING",
                attempt_count=1,
                received_at=now,
            )
            .on_conflict_do_nothing(
                index_elements=[
                    SupplierInbox.event_id,
                    SupplierInbox.consumer_name,
                ]
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

    @staticmethod
    def _lock_products(
        *,
        session: Session,
        command: ReservationCommand,
    ) -> dict[int, Product]:
        product_ids = sorted({item.product_id for item in command.items if item.product_id > 0})
        if not product_ids:
            return {}
        products = session.scalars(
            select(Product)
            .where(Product.id.in_(product_ids))
            .order_by(Product.id)
            .with_for_update()
        )
        return {product.id: product for product in products}


def build_result_outbox(
    *,
    command: ReservationCommand,
    reservation: StockReservation,
    products: Mapping[int, Product],
    event_id: uuid.UUID,
    occurred_at: datetime,
) -> SupplierOutbox:
    if reservation.status == "RESERVED":
        event_type = SUCCESS_EVENT
        business_payload: dict[str, Any] = {
            "order_id": str(command.order_id),
            "reservation_request_id": str(command.reservation_request_id),
            "reservation_id": str(reservation.id),
            "supplier_id": command.supplier_id,
            "reserved_items": [
                {
                    "product_id": item.product_id,
                    "quantity": item.quantity,
                }
                for item in command.items
            ],
            "reserved_at": _timestamp_string(occurred_at),
        }
    else:
        event_type = FAILURE_EVENT
        business_payload = {
            "order_id": str(command.order_id),
            "reservation_request_id": str(command.reservation_request_id),
            "supplier_id": command.supplier_id,
            "failure_category": "BUSINESS",
            "reason_code": reservation.failure_reason,
            "failed_items": [
                {
                    "product_id": item.product_id,
                    "requested_quantity": item.quantity,
                    "available_quantity": (
                        products[item.product_id].available_stocks
                        if item.product_id in products
                        else None
                    ),
                }
                for item in command.items
            ],
            "occurred_at": _timestamp_string(occurred_at),
            "retryable": False,
        }

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
        "payload": business_payload,
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


def _business_rejection_reason(
    *,
    command: ReservationCommand,
    products: Mapping[int, Product],
    now: datetime,
) -> str | None:
    if command.semantic_error is not None:
        return command.semantic_error
    if now >= command.reservation_deadline_at:
        return "RESERVATION_DEADLINE_EXPIRED"
    for item in command.items:
        product = products.get(item.product_id)
        if product is None:
            return "PRODUCT_NOT_FOUND"
        if product.supplier_id != command.supplier_id:
            return "SUPPLIER_MISMATCH"
        if product.is_archived:
            return "PRODUCT_ARCHIVED"
        if not product.is_active:
            return "PRODUCT_INACTIVE"
        if product.available_stocks < item.quantity:
            return "INSUFFICIENT_STOCK"
    return None


def _mark_inbox_processed(inbox: SupplierInbox, now: datetime) -> None:
    inbox.status = "PROCESSED"
    inbox.processed_at = now
    inbox.last_error = None


def _lock_logical_request(session: Session, reservation_request_id: uuid.UUID) -> None:
    lock_key = int.from_bytes(reservation_request_id.bytes[:8], byteorder="big", signed=True)
    session.execute(text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": lock_key})


def _validate_headers(
    *,
    envelope: Mapping[str, Any],
    headers: Mapping[str, str | None],
) -> None:
    required = {
        "event_id",
        "event_type",
        "event_version",
        "correlation_id",
        "causation_id",
        "producer",
        "content_type",
    }
    if not required.issubset(headers):
        raise IncompatibleMessageError("invalid_headers", "Kafka headers are incomplete")
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
    if any(headers[name] != value for name, value in expected.items()):
        raise IncompatibleMessageError("header_mismatch", "Kafka headers do not match envelope")


def _uuid(value: Any, field: str) -> uuid.UUID:
    try:
        return uuid.UUID(str(value))
    except (ValueError, TypeError, AttributeError) as exc:
        raise IncompatibleMessageError("invalid_uuid", f"{field} must be UUID") from exc


def _integer(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise IncompatibleMessageError("invalid_integer", f"{field} must be an integer")
    return value


def _positive_int(value: Any, field: str) -> int:
    parsed = _integer(value, field)
    if parsed <= 0:
        raise IncompatibleMessageError("invalid_integer", f"{field} must be positive")
    return parsed


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise IncompatibleMessageError("invalid_timestamp", f"{field} must be a timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise IncompatibleMessageError("invalid_timestamp", f"{field} must be a timestamp") from exc
    if parsed.tzinfo is None:
        raise IncompatibleMessageError("invalid_timestamp", f"{field} must include timezone")
    return parsed.astimezone(timezone.utc)


def _timestamp_string(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
