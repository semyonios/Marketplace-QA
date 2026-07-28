import { apiConfig } from "./config";
import { createUuid } from "./id";
import type {
  ApiResult,
  Cart,
  Customer,
  ErrorEnvelope,
  Favorite,
  ListResponse,
  Order,
  OrderPage,
  Product,
  Role,
  Supplier,
  Warehouse,
} from "./types";

export class ApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly correlationId: string | null;
  readonly payload: unknown;

  constructor(status: number, payload: ErrorEnvelope, correlationId: string | null) {
    const code =
      payload.error?.code ??
      (typeof payload.detail === "string" ? payload.detail : `http_${status}`);
    super(humanError(status, payload.error?.message ?? code));
    this.name = "ApiError";
    this.status = status;
    this.code = code;
    this.correlationId = correlationId ?? payload.error?.correlation_id ?? null;
    this.payload = payload;
  }
}

const errorLead: Record<number, string> = {
  400: "Запрос не прошёл проверку",
  403: "Действие запрещено для выбранного пользователя",
  404: "Объект не найден или принадлежит другому пользователю",
  409: "Текущее бизнес-состояние не позволяет выполнить действие",
  412: "Заказ уже изменился. Обновите данные и повторите действие",
  428: "Для действия требуется актуальный ETag",
  500: "Внутренняя ошибка backend",
  503: "Зависимость временно недоступна",
};

function humanError(status: number, backendMessage: string): string {
  return `HTTP ${status} — ${errorLead[status] ?? "Ошибка запроса"}: ${backendMessage}`;
}

function actorHeaders(role?: Role, subjectId?: number): Record<string, string> {
  if (!role || !subjectId) return {};
  return {
    "X-Test-Role": role,
    "X-Test-Subject-ID": String(subjectId),
    "X-Correlation-ID": createUuid(),
  };
}

async function request<T>(
  url: string,
  init: RequestInit = {},
  actor?: { role: Role; subjectId: number },
): Promise<ApiResult<T>> {
  const response = await fetch(url, {
    ...init,
    headers: {
      Accept: "application/json",
      ...(init.body ? { "Content-Type": "application/json" } : {}),
      ...actorHeaders(actor?.role, actor?.subjectId),
      ...(init.headers ?? {}),
    },
  });
  const text = await response.text();
  let payload: unknown = null;
  if (text) {
    try {
      payload = JSON.parse(text);
    } catch {
      payload = text;
    }
  }
  const headers = Object.fromEntries(response.headers.entries());
  const correlationId =
    response.headers.get("x-correlation-id") ??
    (typeof payload === "object" &&
    payload !== null &&
    "error" in payload &&
    typeof (payload as ErrorEnvelope).error?.correlation_id === "string"
      ? (payload as ErrorEnvelope).error!.correlation_id!
      : null);
  if (!response.ok) {
    throw new ApiError(response.status, (payload ?? {}) as ErrorEnvelope, correlationId);
  }
  return {
    data: payload as T,
    meta: {
      status: response.status,
      headers,
      etag: response.headers.get("etag"),
      correlationId,
      raw: payload,
      receivedAt: new Date().toISOString(),
    },
  };
}

const query = (params: Record<string, string | number | undefined>) => {
  const search = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== "") search.append(key, String(value));
  });
  const serialized = search.toString();
  return serialized ? `?${serialized}` : "";
};

export const api = {
  customers: () => request<ListResponse<Customer>>(`${apiConfig.customer}/users`),
  suppliers: () => request<ListResponse<Supplier>>(`${apiConfig.supplier}/suppliers`),
  customerProducts: () =>
    request<ListResponse<Product>>(`${apiConfig.customer}/products`),
  customerProduct: (id: number) =>
    request<Product>(`${apiConfig.customer}/products/${id}`),
  supplierProducts: () =>
    request<ListResponse<Product>>(`${apiConfig.supplier}/products`),
  supplierProduct: (id: number) =>
    request<Product>(`${apiConfig.supplier}/products/${id}`),
  createProduct: (body: {
    supplier_id: number;
    name: string;
    description: string | null;
    price: number;
    is_active: boolean;
    is_archived: boolean;
  }) =>
    request<Product>(`${apiConfig.supplier}/products`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  updateProduct: (
    id: number,
    body: {
      supplier_id?: number;
      name?: string;
      description?: string | null;
      price?: number;
      is_active?: boolean;
      is_archived?: boolean;
    },
  ) =>
    request<Product>(`${apiConfig.supplier}/products/${id}`, {
      method: "PUT",
      body: JSON.stringify(body),
    }),
  archiveProduct: (id: number) =>
    request<void>(`${apiConfig.supplier}/products/${id}`, { method: "DELETE" }),
  favorites: (userId: number) =>
    request<ListResponse<Favorite>>(
      `${apiConfig.customer}/favorites${query({ user_id: userId })}`,
    ),
  addFavorite: (userId: number, productId: number) =>
    request<Favorite>(`${apiConfig.customer}/favorites`, {
      method: "POST",
      body: JSON.stringify({ user_id: userId, product_id: productId }),
    }),
  deleteFavorite: (userId: number, productId: number) =>
    request<void>(
      `${apiConfig.customer}/favorites/${productId}${query({ user_id: userId })}`,
      { method: "DELETE" },
    ),
  cart: (userId: number) =>
    request<Cart>(`${apiConfig.customer}/cart${query({ user_id: userId })}`),
  addCart: (userId: number, productId: number, quantity: number) =>
    request(`${apiConfig.customer}/cart`, {
      method: "POST",
      body: JSON.stringify({ user_id: userId, product_id: productId, quantity }),
    }),
  updateCart: (userId: number, productId: number, quantity: number) =>
    request(`${apiConfig.customer}/cart/${productId}`, {
      method: "PATCH",
      body: JSON.stringify({ user_id: userId, quantity }),
    }),
  deleteCart: (userId: number, productId: number) =>
    request<void>(
      `${apiConfig.customer}/cart/${productId}${query({ user_id: userId })}`,
      { method: "DELETE" },
    ),
  createOrder: (customerId: number, cartVersion: number, idempotencyKey: string) =>
    request<Order>(
      `${apiConfig.order}/api/v1/orders`,
      {
        method: "POST",
        headers: { "Idempotency-Key": idempotencyKey },
        body: JSON.stringify({ customer_id: customerId, cart_version: cartVersion }),
      },
      { role: "CUSTOMER", subjectId: customerId },
    ),
  customerOrders: (
    customerId: number,
    params: Record<string, string | number | undefined>,
  ) =>
    request<OrderPage>(
      `${apiConfig.order}/api/v1/customers/${customerId}/orders${query(params)}`,
      {},
      { role: "CUSTOMER", subjectId: customerId },
    ),
  supplierOrders: (
    supplierId: number,
    params: Record<string, string | number | undefined>,
  ) =>
    request<OrderPage>(
      `${apiConfig.order}/api/v1/suppliers/${supplierId}/orders${query(params)}`,
      {},
      { role: "SUPPLIER", subjectId: supplierId },
    ),
  customerOrder: (customerId: number, orderId: string) =>
    request<Order>(
      `${apiConfig.order}/api/v1/customers/${customerId}/orders/${orderId}`,
      {},
      { role: "CUSTOMER", subjectId: customerId },
    ),
  supplierOrder: (supplierId: number, orderId: string) =>
    request<Order>(
      `${apiConfig.order}/api/v1/suppliers/${supplierId}/orders/${orderId}`,
      {},
      { role: "SUPPLIER", subjectId: supplierId },
    ),
  cancelOrder: (customerId: number, orderId: string, etag: string, reason: string) =>
    request<Order>(
      `${apiConfig.order}/api/v1/customers/${customerId}/orders/${orderId}/cancel`,
      {
        method: "POST",
        headers: { "If-Match": etag },
        body: JSON.stringify({ reason_code: reason }),
      },
      { role: "CUSTOMER", subjectId: customerId },
    ),
  confirmOrder: (supplierId: number, orderId: string, etag: string) =>
    request<Order>(
      `${apiConfig.order}/api/v1/suppliers/${supplierId}/orders/${orderId}/confirm`,
      {
        method: "POST",
        headers: { "If-Match": etag },
        body: JSON.stringify({}),
      },
      { role: "SUPPLIER", subjectId: supplierId },
    ),
  rejectOrder: (
    supplierId: number,
    orderId: string,
    etag: string,
    reasonCode: string,
    reasonText?: string,
  ) =>
    request<Order>(
      `${apiConfig.order}/api/v1/suppliers/${supplierId}/orders/${orderId}/reject`,
      {
        method: "POST",
        headers: { "If-Match": etag },
        body: JSON.stringify({
          reason_code: reasonCode,
          ...(reasonText ? { reason_text: reasonText } : {}),
        }),
      },
      { role: "SUPPLIER", subjectId: supplierId },
    ),
  warehouses: () =>
    request<ListResponse<Warehouse>>(`${apiConfig.supplier}/warehouses`),
  createWarehouse: (body: Omit<Warehouse, "id" | "created_at">) =>
    request<Warehouse>(`${apiConfig.supplier}/warehouses`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  setWarehouseStock: (warehouseId: number, productId: number, stocks: number) =>
    request<ListResponse<Product>>(
      `${apiConfig.supplier}/warehouses/${warehouseId}/stocks`,
      {
        method: "POST",
        body: JSON.stringify({ items: [{ product_id: productId, stocks }] }),
      },
    ),
  health: (service: "customer" | "supplier" | "order") =>
    request<Record<string, unknown>>(`${apiConfig[service]}/health`),
  ready: (service: "supplier" | "order") =>
    request<Record<string, unknown>>(`${apiConfig[service]}/ready`),
};
