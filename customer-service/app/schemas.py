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


class FavoriteRequest(BaseModel):
    user_id: int = Field(gt=0)
    product_id: int = Field(gt=0)


class CartRequest(FavoriteRequest):
    quantity: int = Field(gt=0)


class PurchaseItem(BaseModel):
    product_id: int = Field(gt=0)
    quantity: int = Field(gt=0)


class PurchaseRequest(BaseModel):
    user_id: int = Field(gt=0)
    items: list[PurchaseItem] | None = Field(default=None)


class OrderRead(BaseModel):
    id: int
    user_id: int
    product_id: int
    quantity: int
    unit_price: float
    total_price: float
    status: str
    created_at: datetime


class OrderListRead(BaseModel):
    items: list[OrderRead]
    total_price: float
