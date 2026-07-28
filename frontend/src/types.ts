export type Role = "CUSTOMER" | "SUPPLIER";

export interface Customer {
  id: number;
  full_name: string;
  email: string;
  created_at: string;
}

export interface Supplier {
  id: number;
  full_name: string;
  phone_number: string;
  email: string;
  birth_date: string;
  city: string;
  created_at: string;
  updated_at: string;
}

export interface Product {
  id: number;
  supplier_id: number | null;
  name: string;
  description: string | null;
  price: number;
  stocks: number;
  reserved_stocks?: number;
  available_stocks?: number;
  is_active: boolean;
  is_archived: boolean;
  total_price?: number;
  created_at?: string | null;
}

export interface CartItem {
  id: number;
  user_id: number;
  product: Product;
  quantity: number;
  unit_price: number;
  total_price: number;
  created_at: string;
  updated_at: string;
}

export interface Cart {
  cart_id: string;
  cart_version: number;
  supplier_id: number | null;
  items: CartItem[];
  count: number;
  total_items_count: number;
  total_price: number;
}

export interface Favorite {
  id: number;
  user_id: number;
  product: Product;
  created_at: string;
}

export interface OrderReason {
  code: string;
  text: string | null;
}

export interface AvailableActions {
  can_cancel: boolean;
  can_confirm: boolean;
  can_reject: boolean;
  can_retry: boolean;
  can_refresh: boolean;
}

export interface OrderHistory {
  history_id: string;
  trigger: string;
  actor_type: string;
  actor_id: number | null;
  event_id: string | null;
  business_status_before: string | null;
  business_status_after: string;
  operation_state_before: string | null;
  operation_state_after: string;
  reservation_state_before: string | null;
  reservation_state_after: string;
  version_before: number;
  version_after: number;
  reason: OrderReason | null;
  correlation_id: string;
  created_at: string;
}

export interface OrderItem {
  product_id: number;
  product_name: string;
  quantity: number;
  unit_price: string;
  line_total: string;
  currency: string;
}

export interface Order {
  order_id: string;
  customer_id: number;
  supplier_id: number;
  status: string;
  business_status: string;
  operation_state: string;
  reservation_state: string;
  version: number;
  items: OrderItem[];
  total_amount: string;
  currency: string;
  rejection_reason: OrderReason | null;
  cancellation_reason: OrderReason | null;
  failure_reason: OrderReason | null;
  created_at: string;
  updated_at: string;
  correlation_id: string;
  available_actions: AvailableActions;
  history: OrderHistory[];
}

export interface OrderListItem {
  order_id: string;
  customer_id?: number;
  supplier_id: number;
  status: string;
  version: number;
  items_count: number;
  total_amount: string;
  currency: string;
  created_at: string;
  updated_at: string;
}

export interface OrderPage {
  items: OrderListItem[];
  page: number;
  limit: number;
  count: number;
  total: number;
}

export interface Warehouse {
  id: number;
  name: string;
  weekday_hours: string;
  address: string;
  created_at: string;
}

export interface ListResponse<T> {
  items: T[];
  count: number;
}

export interface ApiMeta {
  status: number;
  headers: Record<string, string>;
  etag: string | null;
  correlationId: string | null;
  raw: unknown;
  receivedAt: string;
}

export interface ApiResult<T> {
  data: T;
  meta: ApiMeta;
}

export interface ErrorEnvelope {
  error?: {
    code?: string;
    category?: string;
    message?: string;
    correlation_id?: string;
    details?: Record<string, unknown>;
  };
  detail?: unknown;
}
