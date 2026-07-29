export const apiConfig = {
  customer: import.meta.env.VITE_CUSTOMER_API_BASE ?? "/api/customer",
  supplier: import.meta.env.VITE_SUPPLIER_API_BASE ?? "/api/supplier",
  order: import.meta.env.VITE_ORDER_API_BASE ?? "/api/order",
  pollingMs: Number(import.meta.env.VITE_ORDER_POLLING_MS ?? 2000),
};
