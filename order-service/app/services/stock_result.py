from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from ..enums import (
    ActorType,
    BusinessStatus,
    InboxStatus,
    OperationState,
    OutboxStatus,
    ReservationState,
)
from ..models import (
    Order,
    OrderInbox,
    OrderItem,
    OrderOutbox,
    OrderStatusHistory,
)

SUCCESS_EVENT = "StockReservationSucceeded"
FAILURE_EVENT = "StockReservationFailed"
BUSINESS_FAILURE_REASONS = {
    "INSUFFICIENT_STOCK",
    "PRODUCT_NOT_FOUND",
    "PRODUCT_INACTIVE",
    "PRODUCT_ARCHIVED",
    "INVALID_REQUEST",
    "SUPPLIER_MISMATCH",
    "RESERVATION_DEADLINE_EXPIRED",
}


class IncompatibleStockResultError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.safe_message = message


class RetryableStockResultError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ReservedItem:
    product_id: int
    quantity: int


@dataclass(frozen=True, slots=True)
class FailedItem:
    product_id: int
    requested_quantity: int
    available_quantity: int | None


@dataclass(frozen=True, slots=True)
class StockResultCommand:
    event_id: uuid.UUID
    event_type: str
    occurred_at: datetime
    order_id: uuid.UUID
    reservation_request_id: uuid.UUID
    supplier_id: int
    correlation_id: uuid.UUID
    envelope_hash: str
    reservation_id: uuid.UUID | None = None
    reserved_items: tuple[ReservedItem, ...] = ()
    reason_code: str | None = None
    failed_items: tuple[FailedItem, ...] = ()


@dataclass(frozen=True, slots=True)
class StockResultProcessingResult:
    result: str
    order_id: uuid.UUID
    version_before: int | None
    version_after: int | None
    resulting_event_id: uuid.UUID | None = None


def canonical_hash(value: Mapping[str, Any]) -> str:
    canonical = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def parse_stock_result(
    envelope: Mapping[str, Any],
    *,
    headers: Mapping[str, str | None] | None = None,
    key: str | None = None,
) -> StockResultCommand:
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
        raise IncompatibleStockResultError(
            "invalid_envelope",
            "Kafka envelope is incomplete",
        )

    event_type = envelope["event_type"]
    if event_type not in {SUCCESS_EVENT, FAILURE_EVENT}:
        raise IncompatibleStockResultError(
            "unsupported_event_type",
            "Unsupported stock result event type",
        )
    if envelope["event_version"] != 1:
        raise IncompatibleStockResultError(
            "unsupported_event_version",
            "Unsupported stock result event version",
        )
    if envelope["producer"] != "supplier-service" or envelope["aggregate_type"] != "ORDER":
        raise IncompatibleStockResultError(
            "invalid_envelope",
            "Kafka envelope producer or aggregate is invalid",
        )

    event_id = _uuid(envelope["event_id"], "event_id")
    order_id = _uuid(envelope["aggregate_id"], "aggregate_id")
    correlation_id = _uuid(envelope["correlation_id"], "correlation_id")
    _uuid(envelope["causation_id"], "causation_id")
    occurred_at = _timestamp(envelope["occurred_at"], "occurred_at")
    if key is not None and key != str(order_id):
        raise IncompatibleStockResultError(
            "kafka_key_mismatch",
            "Kafka key does not match aggregate order",
        )

    payload = envelope["payload"]
    if not isinstance(payload, Mapping):
        raise IncompatibleStockResultError(
            "invalid_payload",
            "Stock result payload must be an object",
        )
    required_payload = {
        "order_id",
        "reservation_request_id",
        "supplier_id",
    }
    if not required_payload.issubset(payload):
        raise IncompatibleStockResultError(
            "invalid_payload",
            "Stock result payload is incomplete",
        )
    if _uuid(payload["order_id"], "payload.order_id") != order_id:
        raise IncompatibleStockResultError(
            "aggregate_mismatch",
            "Payload order does not match aggregate",
        )

    reservation_request_id = _uuid(
        payload["reservation_request_id"],
        "reservation_request_id",
    )
    supplier_id = _positive_int(payload["supplier_id"], "supplier_id")
    if headers is not None:
        _validate_headers(envelope=envelope, headers=headers)

    if event_type == SUCCESS_EVENT:
        reservation_id = _uuid(payload.get("reservation_id"), "reservation_id")
        _timestamp(payload.get("reserved_at"), "reserved_at")
        reserved_items = _parse_reserved_items(payload.get("reserved_items"))
        return StockResultCommand(
            event_id=event_id,
            event_type=event_type,
            occurred_at=occurred_at,
            order_id=order_id,
            reservation_request_id=reservation_request_id,
            supplier_id=supplier_id,
            correlation_id=correlation_id,
            envelope_hash=canonical_hash(envelope),
            reservation_id=reservation_id,
            reserved_items=reserved_items,
        )

    if payload.get("failure_category") != "BUSINESS" or payload.get("retryable") is not False:
        raise IncompatibleStockResultError(
            "invalid_failure_category",
            "Stock reservation failure must be a non-retryable business result",
        )
    reason_code = payload.get("reason_code")
    if reason_code not in BUSINESS_FAILURE_REASONS:
        raise IncompatibleStockResultError(
            "unsupported_failure_reason",
            "Stock reservation failure reason is not supported",
        )
    _timestamp(payload.get("occurred_at"), "payload.occurred_at")
    failed_items = _parse_failed_items(payload.get("failed_items"))
    return StockResultCommand(
        event_id=event_id,
        event_type=event_type,
        occurred_at=occurred_at,
        order_id=order_id,
        reservation_request_id=reservation_request_id,
        supplier_id=supplier_id,
        correlation_id=correlation_id,
        envelope_hash=canonical_hash(envelope),
        reason_code=reason_code,
        failed_items=failed_items,
    )


class StockResultService:
    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        consumer_name: str,
        clock: Callable[[], datetime] | None = None,
        event_id_factory: Callable[[], uuid.UUID] = uuid.uuid4,
        history_builder: Callable[..., OrderStatusHistory] | None = None,
        outbox_builder: Callable[..., OrderOutbox] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._consumer_name = consumer_name
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._event_id_factory = event_id_factory
        self._history_builder = history_builder or build_result_history
        self._outbox_builder = outbox_builder or build_result_outbox

    def process(self, command: StockResultCommand) -> StockResultProcessingResult:
        now = self._clock()
        with self._session_factory.begin() as session:
            inbox, duplicate = self._claim_inbox(
                session=session,
                command=command,
                now=now,
            )
            if duplicate:
                return StockResultProcessingResult(
                    result="DUPLICATE_EVENT",
                    order_id=command.order_id,
                    version_before=None,
                    version_after=None,
                )

            order = session.scalar(
                select(Order)
                .where(Order.id == command.order_id)
                .with_for_update()
            )
            if order is None:
                raise IncompatibleStockResultError(
                    "order_not_found",
                    "Stock result references an unknown order",
                )

            order_items = tuple(
                session.scalars(
                    select(OrderItem)
                    .where(OrderItem.order_id == order.id)
                    .order_by(OrderItem.product_id)
                )
            )
            self._validate_order_link(
                order=order,
                order_items=order_items,
                command=command,
            )

            no_op_result = self._no_op_result(order=order, command=command)
            if no_op_result is not None:
                _mark_inbox_processed(inbox, now)
                return StockResultProcessingResult(
                    result=no_op_result,
                    order_id=order.id,
                    version_before=order.version,
                    version_after=order.version,
                )

            if not self._can_transition(order):
                raise IncompatibleStockResultError(
                    "conflicting_order_state",
                    "Stock result conflicts with current order state",
                )

            version_before = order.version
            business_status_before = order.business_status
            operation_state_before = order.operation_state
            reservation_state_before = order.reservation_state

            if command.event_type == SUCCESS_EVENT:
                order.business_status = BusinessStatus.RESERVED
                order.reservation_state = ReservationState.RESERVED
                order.reservation_id = command.reservation_id
                order.rejection_reason_code = None
                order.rejection_reason_text = None
            else:
                order.business_status = BusinessStatus.REJECTED
                order.reservation_state = ReservationState.NOT_RESERVED
                order.reservation_id = None
                order.rejection_reason_code = command.reason_code
                order.rejection_reason_text = None

            order.operation_state = OperationState.NONE
            order.target_terminal_status = None
            order.failure_phase = None
            order.failure_reason_code = None
            order.failure_reason_text = None
            order.version = version_before + 1
            order.updated_at = now

            history = self._history_builder(
                order=order,
                command=command,
                business_status_before=business_status_before,
                operation_state_before=operation_state_before,
                reservation_state_before=reservation_state_before,
                version_before=version_before,
                occurred_at=now,
            )
            resulting_event_id = self._event_id_factory()
            outbox = self._outbox_builder(
                order=order,
                command=command,
                event_id=resulting_event_id,
                occurred_at=now,
            )
            session.add_all([history, outbox])
            _mark_inbox_processed(inbox, now)

        return StockResultProcessingResult(
            result="RESERVED" if command.event_type == SUCCESS_EVENT else "REJECTED",
            order_id=command.order_id,
            version_before=version_before,
            version_after=version_before + 1,
            resulting_event_id=resulting_event_id,
        )

    def record_retryable_failure(
        self,
        *,
        command: StockResultCommand,
        safe_error: str,
        attempt: int,
    ) -> None:
        self._record_failed_inbox(
            command=command,
            safe_error=safe_error,
            attempt=attempt,
        )

    def mark_terminal_failure(
        self,
        *,
        command: StockResultCommand,
        safe_error: str,
        attempt: int,
    ) -> None:
        self._record_failed_inbox(
            command=command,
            safe_error=safe_error,
            attempt=attempt,
        )

    def _record_failed_inbox(
        self,
        *,
        command: StockResultCommand,
        safe_error: str,
        attempt: int,
    ) -> None:
        now = self._clock()
        statement = (
            insert(OrderInbox)
            .values(
                event_id=command.event_id,
                consumer_name=self._consumer_name,
                event_type=command.event_type,
                aggregate_id=command.order_id,
                payload_hash=command.envelope_hash,
                received_at=now,
                status=InboxStatus.FAILED,
                attempt_count=attempt,
                last_error=safe_error,
                correlation_id=command.correlation_id,
            )
            .on_conflict_do_update(
                index_elements=[
                    OrderInbox.event_id,
                    OrderInbox.consumer_name,
                ],
                set_={
                    "status": InboxStatus.FAILED,
                    "attempt_count": func.greatest(OrderInbox.attempt_count, attempt),
                    "last_error": safe_error,
                },
                where=OrderInbox.status != InboxStatus.PROCESSED,
            )
        )
        with self._session_factory.begin() as session:
            session.execute(statement)

    def _claim_inbox(
        self,
        *,
        session: Session,
        command: StockResultCommand,
        now: datetime,
    ) -> tuple[OrderInbox, bool]:
        inserted = session.scalar(
            insert(OrderInbox)
            .values(
                event_id=command.event_id,
                consumer_name=self._consumer_name,
                event_type=command.event_type,
                aggregate_id=command.order_id,
                payload_hash=command.envelope_hash,
                received_at=now,
                status=InboxStatus.PROCESSING,
                attempt_count=1,
                correlation_id=command.correlation_id,
            )
            .on_conflict_do_nothing(
                index_elements=[
                    OrderInbox.event_id,
                    OrderInbox.consumer_name,
                ]
            )
            .returning(OrderInbox.event_id)
        )
        inbox = session.scalar(
            select(OrderInbox)
            .where(
                OrderInbox.event_id == command.event_id,
                OrderInbox.consumer_name == self._consumer_name,
            )
            .with_for_update()
        )
        if inbox is None:
            raise RetryableStockResultError("Order inbox claim was not persisted")
        if inserted is not None:
            return inbox, False
        if inbox.payload_hash != command.envelope_hash:
            raise IncompatibleStockResultError(
                "event_payload_conflict",
                "Event ID was reused with another payload",
            )
        if inbox.status == InboxStatus.PROCESSED:
            return inbox, True
        if inbox.status == InboxStatus.FAILED:
            inbox.status = InboxStatus.PROCESSING
            inbox.attempt_count += 1
            inbox.last_error = None
            return inbox, False
        raise RetryableStockResultError("Order inbox event is already processing")

    @staticmethod
    def _validate_order_link(
        *,
        order: Order,
        order_items: tuple[OrderItem, ...],
        command: StockResultCommand,
    ) -> None:
        if order.supplier_id != command.supplier_id:
            raise IncompatibleStockResultError(
                "supplier_mismatch",
                "Stock result supplier does not match order",
            )
        if order.reservation_request_id != command.reservation_request_id:
            raise IncompatibleStockResultError(
                "reservation_request_mismatch",
                "Stock result reservation request does not match order",
            )
        if order.correlation_id != command.correlation_id:
            raise IncompatibleStockResultError(
                "correlation_mismatch",
                "Stock result correlation does not match order flow",
            )

        expected_items = {
            item.product_id: item.quantity
            for item in order_items
        }
        if command.event_type == SUCCESS_EVENT:
            actual_items = {
                item.product_id: item.quantity
                for item in command.reserved_items
            }
            if actual_items != expected_items:
                raise IncompatibleStockResultError(
                    "reserved_items_mismatch",
                    "Reserved items do not match order snapshot",
                )
            return

        for failed_item in command.failed_items:
            expected_quantity = expected_items.get(failed_item.product_id)
            if (
                expected_quantity is None
                or expected_quantity != failed_item.requested_quantity
            ):
                raise IncompatibleStockResultError(
                    "failed_items_mismatch",
                    "Failed items do not match order snapshot",
                )

    @staticmethod
    def _can_transition(order: Order) -> bool:
        return (
            order.business_status == BusinessStatus.PENDING_RESERVATION
            and order.operation_state == OperationState.NONE
            and order.reservation_state
            in {ReservationState.REQUESTED, ReservationState.UNKNOWN}
        )

    @staticmethod
    def _no_op_result(
        *,
        order: Order,
        command: StockResultCommand,
    ) -> str | None:
        if command.event_type == SUCCESS_EVENT:
            if (
                order.business_status
                in {BusinessStatus.RESERVED, BusinessStatus.CONFIRMED}
                and order.reservation_id == command.reservation_id
                and order.reservation_state
                in {ReservationState.RESERVED, ReservationState.RELEASE_REQUESTED}
            ):
                return "DUPLICATE_LOGICAL_SUCCESS"
            if order.operation_state in {
                OperationState.CANCELLATION_PENDING,
                OperationState.REJECTION_PENDING,
                OperationState.FAILED,
            } or order.business_status == BusinessStatus.CANCELLED:
                return "LATE_SUCCESS_DEFERRED"
            return None

        if (
            order.business_status == BusinessStatus.REJECTED
            and order.operation_state == OperationState.NONE
            and order.reservation_state == ReservationState.NOT_RESERVED
            and order.rejection_reason_code == command.reason_code
        ):
            return "DUPLICATE_LOGICAL_FAILURE"
        if order.operation_state in {
            OperationState.CANCELLATION_PENDING,
            OperationState.REJECTION_PENDING,
            OperationState.FAILED,
        } or order.business_status == BusinessStatus.CANCELLED:
            return "LATE_FAILURE_IGNORED"
        return None


def build_result_history(
    *,
    order: Order,
    command: StockResultCommand,
    business_status_before: BusinessStatus,
    operation_state_before: OperationState,
    reservation_state_before: ReservationState,
    version_before: int,
    occurred_at: datetime,
) -> OrderStatusHistory:
    return OrderStatusHistory(
        id=uuid.uuid4(),
        order_id=order.id,
        business_status_before=business_status_before,
        business_status_after=order.business_status,
        operation_state_before=operation_state_before,
        operation_state_after=order.operation_state,
        reservation_state_before=reservation_state_before,
        reservation_state_after=order.reservation_state,
        trigger=command.event_type,
        actor_type=ActorType.KAFKA_CONSUMER,
        actor_id=None,
        event_id=command.event_id,
        correlation_id=command.correlation_id,
        version_before=version_before,
        version_after=order.version,
        reason_code=command.reason_code,
        reason_text=None,
        created_at=occurred_at,
    )


def build_result_outbox(
    *,
    order: Order,
    command: StockResultCommand,
    event_id: uuid.UUID,
    occurred_at: datetime,
) -> OrderOutbox:
    if command.event_type == SUCCESS_EVENT:
        event_type = "OrderReserved"
        business_payload: dict[str, Any] = {
            "order_id": str(order.id),
            "supplier_id": order.supplier_id,
            "reservation_request_id": str(order.reservation_request_id),
            "reservation_id": str(order.reservation_id),
            "status": BusinessStatus.RESERVED.value,
            "order_version": order.version,
            "reserved_at": _timestamp_string(occurred_at),
        }
    else:
        event_type = "OrderRejected"
        business_payload = {
            "order_id": str(order.id),
            "supplier_id": order.supplier_id,
            "reservation_request_id": str(order.reservation_request_id),
            "status": BusinessStatus.REJECTED.value,
            "order_version": order.version,
            "reason_code": order.rejection_reason_code,
            "reason_text": order.rejection_reason_text,
            "rejected_at": _timestamp_string(occurred_at),
        }
    envelope = {
        "event_id": str(event_id),
        "event_type": event_type,
        "event_version": 1,
        "occurred_at": _timestamp_string(occurred_at),
        "producer": "order-service",
        "aggregate_type": "ORDER",
        "aggregate_id": str(order.id),
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
        "producer": "order-service",
        "content_type": "application/json",
    }
    return OrderOutbox(
        id=event_id,
        aggregate_type="ORDER",
        aggregate_id=order.id,
        event_type=event_type,
        event_version=1,
        payload=envelope,
        headers=headers,
        status=OutboxStatus.PENDING,
        attempt_count=0,
        next_attempt_at=occurred_at,
        created_at=occurred_at,
    )


def _mark_inbox_processed(inbox: OrderInbox, now: datetime) -> None:
    inbox.status = InboxStatus.PROCESSED
    inbox.processed_at = now
    inbox.last_error = None


def _parse_reserved_items(value: Any) -> tuple[ReservedItem, ...]:
    if not isinstance(value, list) or not value:
        raise IncompatibleStockResultError(
            "invalid_reserved_items",
            "Reserved items must be a non-empty array",
        )
    items: list[ReservedItem] = []
    seen: set[int] = set()
    for raw_item in value:
        if not isinstance(raw_item, Mapping):
            raise IncompatibleStockResultError(
                "invalid_reserved_items",
                "Reserved item must be an object",
            )
        product_id = _positive_int(raw_item.get("product_id"), "product_id")
        quantity = _positive_int(raw_item.get("quantity"), "quantity")
        if product_id in seen:
            raise IncompatibleStockResultError(
                "duplicate_reserved_item",
                "Reserved items contain duplicate products",
            )
        seen.add(product_id)
        items.append(ReservedItem(product_id=product_id, quantity=quantity))
    return tuple(items)


def _parse_failed_items(value: Any) -> tuple[FailedItem, ...]:
    if not isinstance(value, list) or not value:
        raise IncompatibleStockResultError(
            "invalid_failed_items",
            "Failed items must be a non-empty array",
        )
    items: list[FailedItem] = []
    seen: set[int] = set()
    for raw_item in value:
        if not isinstance(raw_item, Mapping):
            raise IncompatibleStockResultError(
                "invalid_failed_items",
                "Failed item must be an object",
            )
        product_id = _positive_int(raw_item.get("product_id"), "product_id")
        requested_quantity = _positive_int(
            raw_item.get("requested_quantity"),
            "requested_quantity",
        )
        available_raw = raw_item.get("available_quantity")
        available_quantity = (
            None
            if available_raw is None
            else _non_negative_int(available_raw, "available_quantity")
        )
        if product_id in seen:
            raise IncompatibleStockResultError(
                "duplicate_failed_item",
                "Failed items contain duplicate products",
            )
        seen.add(product_id)
        items.append(
            FailedItem(
                product_id=product_id,
                requested_quantity=requested_quantity,
                available_quantity=available_quantity,
            )
        )
    return tuple(items)


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
        raise IncompatibleStockResultError(
            "invalid_headers",
            "Kafka headers are incomplete",
        )
    expected = {
        "event_id": str(envelope["event_id"]),
        "event_type": str(envelope["event_type"]),
        "event_version": str(envelope["event_version"]),
        "correlation_id": str(envelope["correlation_id"]),
        "causation_id": (
            None
            if envelope["causation_id"] is None
            else str(envelope["causation_id"])
        ),
        "producer": str(envelope["producer"]),
        "content_type": "application/json",
    }
    if any(headers[name] != value for name, value in expected.items()):
        raise IncompatibleStockResultError(
            "header_mismatch",
            "Kafka headers do not match envelope",
        )


def _uuid(value: Any, field: str) -> uuid.UUID:
    try:
        return uuid.UUID(str(value))
    except (ValueError, TypeError, AttributeError) as exc:
        raise IncompatibleStockResultError(
            "invalid_uuid",
            f"{field} must be UUID",
        ) from exc


def _integer(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise IncompatibleStockResultError(
            "invalid_integer",
            f"{field} must be an integer",
        )
    return value


def _positive_int(value: Any, field: str) -> int:
    parsed = _integer(value, field)
    if parsed <= 0:
        raise IncompatibleStockResultError(
            "invalid_integer",
            f"{field} must be positive",
        )
    return parsed


def _non_negative_int(value: Any, field: str) -> int:
    parsed = _integer(value, field)
    if parsed < 0:
        raise IncompatibleStockResultError(
            "invalid_integer",
            f"{field} must be non-negative",
        )
    return parsed


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise IncompatibleStockResultError(
            "invalid_timestamp",
            f"{field} must be a timestamp",
        )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise IncompatibleStockResultError(
            "invalid_timestamp",
            f"{field} must be a timestamp",
        ) from exc
    if parsed.tzinfo is None:
        raise IncompatibleStockResultError(
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
