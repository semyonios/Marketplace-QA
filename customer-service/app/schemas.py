import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, EmailStr, Field


class UserCreate(BaseModel):
    full_name: str = Field(min_length=3, max_length=255, examples=["Покупатель Иванов"], description="Полное имя покупателя")
    email: EmailStr = Field(examples=["buyer@example.com"], description="Email покупателя")


class UserRead(UserCreate):
    id: int
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class ProductRead(BaseModel):
    id: int
    supplier_id: int | None
    name: str
    description: str | None
    price: float
    stocks: int
    is_active: bool
    is_archived: bool
    total_price: float
    created_at: datetime | None


class ProductSummary(BaseModel):
    id: int
    supplier_id: int | None
    name: str
    price: float
    stocks: int
    is_active: bool
    is_archived: bool


class UserListRead(BaseModel):
    items: list[UserRead]
    count: int


class ProductListRead(BaseModel):
    items: list[ProductRead]
    count: int


class FavoriteCreate(BaseModel):
    user_id: int = Field(gt=0, description="Идентификатор покупателя")
    product_id: int = Field(gt=0, description="Идентификатор товара")


class FavoriteRead(BaseModel):
    id: int
    user_id: int
    product: ProductSummary
    created_at: datetime


class FavoriteListRead(BaseModel):
    items: list[FavoriteRead]
    count: int


class CartCreate(BaseModel):
    user_id: int = Field(gt=0, description="Идентификатор покупателя")
    product_id: int = Field(gt=0, description="Идентификатор товара")
    quantity: int = Field(gt=0, description="Количество товара")


class CartQuantityUpdate(BaseModel):
    user_id: int = Field(gt=0, description="Идентификатор покупателя")
    quantity: int = Field(gt=0, description="Новое количество товара")


class CartItemRead(BaseModel):
    id: int
    user_id: int
    product: ProductSummary
    quantity: int
    unit_price: float
    total_price: float
    created_at: datetime
    updated_at: datetime


class CartRead(BaseModel):
    cart_id: uuid.UUID
    cart_version: int
    supplier_id: int | None
    items: list[CartItemRead]
    count: int
    total_items_count: int
    total_price: float


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


class CartSnapshotRead(BaseModel):
    customer_id: int
    cart_id: uuid.UUID
    cart_version: int
    supplier_id: int | None
    items: list[CartSnapshotItem]
    generated_at: datetime


class OrderCreateItem(BaseModel):
    product_id: int = Field(gt=0, description="Идентификатор товара")
    quantity: int = Field(gt=0, description="Количество товара в заказе")


class OrderCreate(BaseModel):
    user_id: int = Field(gt=0, description="Идентификатор покупателя")
    items: list[OrderCreateItem] | None = Field(default=None, description="Список товаров для заказа. Если не передан, заказ собирается из корзины.")


class OrderItemRead(BaseModel):
    id: int
    product: ProductSummary
    quantity: int
    unit_price: float
    total_price: float
    created_at: datetime


class OrderRead(BaseModel):
    id: int
    user_id: int
    order_number: str
    status: str
    items: list[OrderItemRead]
    total_items_count: int
    total_price: float
    created_at: datetime


class OrderListRead(BaseModel):
    items: list[OrderRead]
    count: int
