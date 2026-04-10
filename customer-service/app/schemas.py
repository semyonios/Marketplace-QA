from datetime import datetime

from pydantic import BaseModel, ConfigDict, EmailStr, Field


class UserCreate(BaseModel):
    full_name: str = Field(min_length=3, max_length=255, examples=["Покупатель Иванов"])
    email: EmailStr = Field(examples=["buyer@example.com"])


class UserRead(UserCreate):
    id: int
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class ProductRead(BaseModel):
    id: int
    name: str
    description: str | None
    price: float
    stocks: int
    total_price: float
    created_at: datetime | None


class ProductSummary(BaseModel):
    id: int
    name: str
    price: float
    stocks: int


class FavoriteCreate(BaseModel):
    user_id: int = Field(gt=0)
    product_id: int = Field(gt=0)


class FavoriteRead(BaseModel):
    id: int
    user_id: int
    product: ProductSummary
    created_at: datetime


class FavoriteListRead(BaseModel):
    items: list[FavoriteRead]


class CartCreate(BaseModel):
    user_id: int = Field(gt=0)
    product_id: int = Field(gt=0)
    quantity: int = Field(gt=0)


class CartQuantityUpdate(BaseModel):
    user_id: int = Field(gt=0)
    quantity: int = Field(gt=0)


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
    items: list[CartItemRead]
    total_price: float


class OrderCreateItem(BaseModel):
    product_id: int = Field(gt=0)
    quantity: int = Field(gt=0)


class OrderCreate(BaseModel):
    user_id: int = Field(gt=0)
    items: list[OrderCreateItem] | None = Field(default=None)


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
    total_price: float
    created_at: datetime


class OrderListRead(BaseModel):
    items: list[OrderRead]
