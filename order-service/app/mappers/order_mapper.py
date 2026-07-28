from __future__ import annotations

from decimal import Decimal

from ..enums import BusinessStatus, OperationState
from ..models import Order
from ..schemas import (
    AvailableActionsResponse,
    CustomerOrderListItem,
    OrderItemResponse,
    OrderReasonResponse,
    OrderResponse,
    SupplierOrderListItem,
)

MONEY_QUANT = Decimal("0.01")


def map_available_actions(order: Order, *, actor_role: str) -> AvailableActionsResponse:
    customer_can_cancel = (
        actor_role == "CUSTOMER"
        and order.business_status in {BusinessStatus.PENDING_RESERVATION, BusinessStatus.RESERVED}
        and order.operation_state == OperationState.NONE
    )
    supplier_can_decide = (
        actor_role == "SUPPLIER"
        and order.business_status == BusinessStatus.RESERVED
        and order.operation_state == OperationState.NONE
    )
    return AvailableActionsResponse(
        can_cancel=customer_can_cancel,
        can_confirm=supplier_can_decide,
        can_reject=supplier_can_decide,
        can_retry=False,
        can_refresh=True,
    )


def map_order_to_response(order: Order, *, actor_role: str) -> OrderResponse:
    return OrderResponse(
        order_id=order.id,
        customer_id=order.customer_id,
        supplier_id=order.supplier_id,
        status=order.status.value,
        business_status=order.business_status.value,
        operation_state=order.operation_state.value,
        reservation_state=order.reservation_state.value,
        version=order.version,
        items=[
            OrderItemResponse(
                product_id=item.product_id,
                product_name=item.product_name_snapshot,
                quantity=item.quantity,
                unit_price=item.unit_price.quantize(MONEY_QUANT),
                line_total=item.line_total.quantize(MONEY_QUANT),
                currency=item.currency,
            )
            for item in sorted(order.items, key=lambda candidate: candidate.product_id)
        ],
        total_amount=order.total_amount.quantize(MONEY_QUANT),
        currency=order.currency,
        rejection_reason=(
            OrderReasonResponse(code=order.rejection_reason_code, text=order.rejection_reason_text)
            if order.rejection_reason_code
            else None
        ),
        cancellation_reason=(
            OrderReasonResponse(code=order.cancellation_reason_code, text=order.cancellation_reason_text)
            if order.cancellation_reason_code
            else None
        ),
        created_at=order.created_at,
        updated_at=order.updated_at,
        correlation_id=order.correlation_id,
        available_actions=map_available_actions(order, actor_role=actor_role),
    )


def map_customer_order_list_item(order: Order, *, items_count: int) -> CustomerOrderListItem:
    return CustomerOrderListItem(
        order_id=order.id,
        supplier_id=order.supplier_id,
        status=order.status.value,
        version=order.version,
        items_count=items_count,
        total_amount=order.total_amount.quantize(MONEY_QUANT),
        currency=order.currency,
        created_at=order.created_at,
        updated_at=order.updated_at,
    )


def map_supplier_order_list_item(order: Order, *, items_count: int) -> SupplierOrderListItem:
    return SupplierOrderListItem(
        order_id=order.id,
        customer_id=order.customer_id,
        supplier_id=order.supplier_id,
        status=order.status.value,
        version=order.version,
        items_count=items_count,
        total_amount=order.total_amount.quantize(MONEY_QUANT),
        currency=order.currency,
        created_at=order.created_at,
        updated_at=order.updated_at,
    )
