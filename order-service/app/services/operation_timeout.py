from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import or_, select
from sqlalchemy.orm import Session, sessionmaker

from ..enums import ActorType, FailurePhase, OperationState, ReservationState
from ..models import Order, OrderStatusHistory
from .order_actions import _finalization_command, _order_event, _release_command

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class OperationTimeoutConfig:
    batch_size: int
    retry_seconds: float
    max_attempts: int


class OperationTimeoutService:
    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        config: OperationTimeoutConfig,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._config = config
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def process_once(self) -> int:
        now = self._clock()
        retry_before = now - timedelta(seconds=self._config.retry_seconds)
        processed = 0
        with self._session_factory.begin() as session:
            orders = list(
                session.scalars(
                    select(Order)
                    .where(
                        or_(
                            (
                                (Order.operation_state == OperationState.CONFIRMATION_PENDING)
                                & (Order.finalization_last_attempt_at <= retry_before)
                            ),
                            (
                                Order.operation_state.in_(
                                    {
                                        OperationState.REJECTION_PENDING,
                                        OperationState.CANCELLATION_PENDING,
                                    }
                                )
                                & (Order.release_last_attempt_at <= retry_before)
                            ),
                        )
                    )
                    .order_by(Order.updated_at, Order.id)
                    .limit(self._config.batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            for order in orders:
                self._process_order(session=session, order=order, now=now)
                processed += 1
        return processed

    def _process_order(self, *, session: Session, order: Order, now: datetime) -> None:
        confirmation = order.operation_state == OperationState.CONFIRMATION_PENDING
        attempts = (
            order.finalization_attempt_count if confirmation else order.release_attempt_count
        )
        deadline = (
            order.finalization_deadline_at if confirmation else order.release_deadline_at
        )
        version_before = order.version
        business_before = order.business_status
        operation_before = order.operation_state
        reservation_before = order.reservation_state
        correlation_id = session.scalar(
            select(OrderStatusHistory.correlation_id)
            .where(
                OrderStatusHistory.order_id == order.id,
                OrderStatusHistory.operation_state_after == order.operation_state,
            )
            .order_by(OrderStatusHistory.version_after.desc())
            .limit(1)
        ) or order.correlation_id

        if (
            deadline is None
            or deadline <= now
            or attempts >= self._config.max_attempts
        ):
            order.operation_state = OperationState.FAILED
            order.failure_phase = (
                FailurePhase.CONFIRMATION if confirmation else FailurePhase.RELEASE
            )
            order.failure_reason_code = "OPERATION_RETRY_EXHAUSTED"
            order.failure_reason_text = "Operation did not complete before retry policy exhausted"
            if not confirmation:
                order.reservation_state = ReservationState.UNKNOWN
            trigger = "ProcessConfirmationTimeout" if confirmation else "ProcessReleaseTimeout"
            domain = _order_event(
                order=order,
                event_type="OrderProcessingFailed",
                correlation_id=correlation_id,
                causation_id=None,
                occurred_at=now,
                reason_code=order.failure_reason_code,
                reason_text=order.failure_reason_text,
            )
            result = "failed"
        else:
            if confirmation:
                order.finalization_attempt_count += 1
                order.finalization_last_attempt_at = now
                command = _finalization_command(
                    order=order,
                    correlation_id=correlation_id,
                    causation_id=order.finalization_request_id or uuid.uuid4(),
                    occurred_at=now,
                )
                trigger = "RetryFinalization"
            else:
                order.release_attempt_count += 1
                order.release_last_attempt_at = now
                command = _release_command(
                    order=order,
                    correlation_id=correlation_id,
                    causation_id=order.release_request_id or uuid.uuid4(),
                    occurred_at=now,
                    reason=(
                        "SUPPLIER_REJECTED"
                        if order.operation_state == OperationState.REJECTION_PENDING
                        else "CUSTOMER_CANCELLED"
                    ),
                )
                trigger = "RetryRelease"
            domain = command
            result = "retry_scheduled"

        order.version += 1
        order.updated_at = now
        history = OrderStatusHistory(
            id=uuid.uuid4(),
            order_id=order.id,
            business_status_before=business_before,
            business_status_after=order.business_status,
            operation_state_before=operation_before,
            operation_state_after=order.operation_state,
            reservation_state_before=reservation_before,
            reservation_state_after=order.reservation_state,
            trigger=trigger,
            actor_type=ActorType.SYSTEM,
            actor_id=None,
            event_id=None,
            correlation_id=correlation_id,
            version_before=version_before,
            version_after=order.version,
            reason_code=order.failure_reason_code,
            reason_text=order.failure_reason_text,
            created_at=now,
        )
        if domain.event_type == "OrderProcessingFailed":
            domain.payload["payload"]["order_version"] = order.version
            domain.payload["payload"]["status"] = order.status.value
            domain.payload["payload"]["operation_state"] = order.operation_state.value
        else:
            domain.payload["payload"]["order_version"] = order.version
        session.add_all([history, domain])
        logger.info(
            "pending order operation checked",
            extra={
                "worker": "order-timeout-worker",
                "order_id": str(order.id),
                "supplier_id": order.supplier_id,
                "correlation_id": str(correlation_id),
                "state_before": operation_before.value,
                "state_after": order.operation_state.value,
                "version_before": version_before,
                "version_after": order.version,
                "action": trigger,
                "result": result,
                "attempt": (
                    order.finalization_attempt_count
                    if confirmation
                    else order.release_attempt_count
                ),
            },
        )
