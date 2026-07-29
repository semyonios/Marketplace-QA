import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


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


class ConfirmOrderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")


class OrderActionReasonRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason_code: str
    reason_text: str | None = Field(default=None, max_length=500)

    @field_validator("reason_code")
    @classmethod
    def normalize_reason_code(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("reason_code must not be blank")
        return normalized

    @field_validator("reason_text")
    @classmethod
    def normalize_reason_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @model_validator(mode="after")
    def require_other_text(self) -> "OrderActionReasonRequest":
        if self.reason_code == "OTHER" and self.reason_text is None:
            raise ValueError("reason_text is required for OTHER")
        return self


class CancelOrderRequest(OrderActionReasonRequest):
    reason_code: str = "CUSTOMER_REQUEST"


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


class OrderHistoryResponse(BaseModel):
    history_id: uuid.UUID
    trigger: str
    actor_type: str
    actor_id: int | None
    event_id: uuid.UUID | None
    business_status_before: str | None
    business_status_after: str
    operation_state_before: str | None
    operation_state_after: str
    reservation_state_before: str | None
    reservation_state_after: str
    version_before: int
    version_after: int
    reason: OrderReasonResponse | None
    correlation_id: uuid.UUID
    created_at: datetime


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
    failure_reason: OrderReasonResponse | None
    created_at: datetime
    updated_at: datetime
    correlation_id: uuid.UUID
    available_actions: AvailableActionsResponse
    history: list[OrderHistoryResponse]


class CustomerOrderListItem(BaseModel):
    order_id: uuid.UUID
    supplier_id: int
    status: str
    version: int
    items_count: int
    total_amount: Decimal
    currency: str
    created_at: datetime
    updated_at: datetime


class SupplierOrderListItem(BaseModel):
    order_id: uuid.UUID
    customer_id: int
    supplier_id: int
    status: str
    version: int
    items_count: int
    total_amount: Decimal
    currency: str
    created_at: datetime
    updated_at: datetime


class CustomerOrderListResponse(BaseModel):
    items: list[CustomerOrderListItem]
    page: int
    limit: int
    count: int
    total: int


class SupplierOrderListResponse(BaseModel):
    items: list[SupplierOrderListItem]
    page: int
    limit: int
    count: int
    total: int
