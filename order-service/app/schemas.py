import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str
    service: str


class ReadinessDependencies(BaseModel):
    database: str
    migrations: str
    kafka: str


class ReadinessResponse(BaseModel):
    status: str
    service: str
    dependencies: ReadinessDependencies


class CreateOrderRequest(BaseModel):
    customer_id: int = Field(gt=0)
    cart_version: int = Field(gt=0)
    client_request_id: uuid.UUID | None = None


class CartSnapshotItem(BaseModel):
    product_id: int
    product_name: str
    supplier_id: int
    quantity: int
    unit_price: Decimal
    currency: str
    product_status: str
    projected_available_quantity: int
    projection_updated_at: datetime


class CartSnapshot(BaseModel):
    customer_id: int
    cart_id: uuid.UUID
    cart_version: int
    supplier_id: int | None
    items: list[CartSnapshotItem]
    generated_at: datetime


class OrderItemResponse(BaseModel):
    product_id: int
    product_name: str
    quantity: int
    unit_price: Decimal
    line_total: Decimal
    currency: str


class OrderReasonResponse(BaseModel):
    code: str
    text: str | None


class AvailableActionsResponse(BaseModel):
    can_cancel: bool
    can_confirm: bool
    can_reject: bool
    can_retry: bool
    can_refresh: bool


class OrderResponse(BaseModel):
    order_id: uuid.UUID
    customer_id: int
    supplier_id: int
    status: str
    business_status: str
    operation_state: str
    reservation_state: str
    version: int
    items: list[OrderItemResponse]
    total_amount: Decimal
    currency: str
    rejection_reason: OrderReasonResponse | None
    cancellation_reason: OrderReasonResponse | None
    created_at: datetime
    updated_at: datetime
    correlation_id: uuid.UUID
    available_actions: AvailableActionsResponse
