import {
  expect,
  type APIRequestContext,
  type BrowserContext,
  type Page,
  test,
} from "@playwright/test";
import { randomUUID } from "node:crypto";

const customerHeaders = (customerId: number) => ({
  "X-Test-Role": "CUSTOMER",
  "X-Test-Subject-ID": String(customerId),
  "X-Correlation-ID": randomUUID(),
});

const supplierHeaders = (supplierId: number) => ({
  "X-Test-Role": "SUPPLIER",
  "X-Test-Subject-ID": String(supplierId),
  "X-Correlation-ID": randomUUID(),
});

async function selectActor(page: Page, role: "CUSTOMER" | "SUPPLIER", id: number) {
  await page.goto("/");
  await page
    .getByTestId(role === "CUSTOMER" ? "home-role-customer" : "home-role-supplier")
    .click();
  const actor = page.getByTestId(`test-user-${id}`);
  await expect(actor).toBeVisible();
  await actor.click();
  await page.getByTestId("select-user-submit").click();
  await expect(page.getByTestId("current-role")).toContainText(role);
  await expect(page.getByTestId("current-subject")).toContainText(`#${id}`);
}

async function clearCart(request: APIRequestContext, customerId: number) {
  const response = await request.get(`/api/customer/cart?user_id=${customerId}`);
  expect(response.ok()).toBeTruthy();
  const cart = await response.json();
  for (const item of cart.items) {
    const deleted = await request.delete(
      `/api/customer/cart/${item.product.id}?user_id=${customerId}`,
    );
    expect(deleted.status()).toBe(204);
  }
}

async function prepareReservedOrder(
  request: APIRequestContext,
  customerId = 1,
  productId = 1,
) {
  await clearCart(request, customerId);

  const added = await request.post("/api/customer/cart", {
    data: { user_id: customerId, product_id: productId, quantity: 1 },
  });
  expect(added.status()).toBe(201);

  const cartResponse = await request.get(
    `/api/customer/cart?user_id=${customerId}`,
  );
  const cart = await cartResponse.json();
  const created = await request.post("/api/order/api/v1/orders", {
    headers: {
      ...customerHeaders(customerId),
      "Idempotency-Key": `e2e-${randomUUID()}`,
    },
    data: { customer_id: customerId, cart_version: cart.cart_version },
  });
  expect(created.status()).toBe(202);
  const order = await created.json();

  let detail: Record<string, unknown> | undefined;
  await expect
    .poll(
      async () => {
        const response = await request.get(
          `/api/order/api/v1/customers/${customerId}/orders/${order.order_id}`,
          { headers: customerHeaders(customerId) },
        );
        expect(response.ok()).toBeTruthy();
        detail = await response.json();
        return detail?.status;
      },
      { message: `order ${order.order_id} should become RESERVED` },
    )
    .toBe("RESERVED");
  return detail as {
    order_id: string;
    supplier_id: number;
    version: number;
    status: string;
  };
}

async function orderStatus(page: Page) {
  return (await page.getByTestId("order-status").textContent())?.trim();
}

test.describe.configure({ mode: "serial" });

test("Scenario 1 — Customer creates an order and sees ETag/timeline", async ({
  page,
  request,
}) => {
  await clearCart(request, 1);
  await selectActor(page, "CUSTOMER", 1);
  await expect(page.getByTestId("customer-catalog-page")).toBeVisible();
  await page.getByTestId("add-to-cart-1").click();
  await expect(page.getByTestId("operation-success")).toBeVisible();
  await page.getByRole("link", { name: "Корзина", exact: true }).click();
  await page.getByTestId("cart-checkout").click();
  await page.getByTestId("checkout-submit").click();
  await expect(page).toHaveURL(/\/customer\/orders\/[0-9a-f-]+$/);
  await expect.poll(() => orderStatus(page)).toBe("RESERVED");
  await expect(page.getByTestId("order-timeline")).toBeVisible();
  await page.getByRole("button", { name: "DEV PANEL +", exact: true }).click();
  await expect(page.getByTestId("dev-etag")).toContainText('"');
  await expect(page.getByTestId("dev-order-json")).toContainText('"version"');
});

test("Scenario 2 — Supplier confirms a RESERVED order", async ({
  page,
  request,
}) => {
  const order = await prepareReservedOrder(request);
  const stockBeforeResponse = await request.get("/api/supplier/products/1");
  const stockBefore = await stockBeforeResponse.json();
  await selectActor(page, "SUPPLIER", order.supplier_id);
  await page.goto(`/supplier/orders/${order.order_id}`);
  await expect.poll(() => orderStatus(page)).toBe("RESERVED");
  await page.getByTestId("supplier-order-confirm").click();
  await expect.poll(() => orderStatus(page)).toBe("CONFIRMED");
  await expect(page.getByTestId("order-timeline")).toContainText(
    "Резерв финализирован",
  );
  await expect(page.getByTestId("supplier-order-confirm")).toHaveCount(0);
  await expect
    .poll(async () => {
      const response = await request.get("/api/supplier/products/1");
      return (await response.json()).stocks;
    })
    .toBeLessThan(stockBefore.stocks);
});

test("Scenario 3 — Supplier rejects and releases a reservation", async ({
  page,
  request,
}) => {
  const order = await prepareReservedOrder(request);
  await selectActor(page, "SUPPLIER", order.supplier_id);
  await page.goto(`/supplier/orders/${order.order_id}`);
  await expect.poll(() => orderStatus(page)).toBe("RESERVED");
  await page.getByTestId("supplier-order-reject").click();
  await expect.poll(() => orderStatus(page)).toBe("REJECTED");
  await expect(page.getByTestId("order-timeline")).toContainText(
    "Резерв освобождён",
  );
  await expect(page.getByTestId("supplier-order-reject")).toHaveCount(0);
});

test("Scenario 4 — Customer cancels and releases a reservation", async ({
  page,
  request,
}) => {
  const order = await prepareReservedOrder(request);
  await selectActor(page, "CUSTOMER", 1);
  await page.goto(`/customer/orders/${order.order_id}`);
  await expect.poll(() => orderStatus(page)).toBe("RESERVED");
  await page.getByTestId("order-cancel").click();
  await expect.poll(() => orderStatus(page)).toBe("CANCELLED");
  await expect(page.getByTestId("order-timeline")).toContainText(
    "Резерв освобождён",
  );
  await expect(page.getByTestId("order-cancel")).toHaveCount(0);
});

test("Scenario 5 — stale ETag produces 412 and refreshes data", async ({
  context,
  request,
}) => {
  const order = await prepareReservedOrder(request);
  const first = await context.newPage();
  await selectActor(first, "SUPPLIER", order.supplier_id);
  const second = await context.newPage();
  await second.goto(`/supplier/orders/${order.order_id}`);
  await first.goto(`/supplier/orders/${order.order_id}`);
  await expect.poll(() => orderStatus(first)).toBe("RESERVED");
  await expect.poll(() => orderStatus(second)).toBe("RESERVED");

  await first.getByTestId("supplier-order-confirm").click();
  await second.getByTestId("supplier-order-reject").click();
  await expect(second.getByTestId("order-mutation-error")).toContainText("412");
  await expect
    .poll(() => orderStatus(second))
    .toMatch(/CONFIRMATION_PENDING|CONFIRMED/);
  await first.close();
  await second.close();
});

test("Scenario 6 — role and ownership boundaries return not found", async ({
  page,
  request,
}) => {
  const foreignOrder = await prepareReservedOrder(request, 2, 1);
  await selectActor(page, "CUSTOMER", 1);
  await page.goto(`/customer/orders/${foreignOrder.order_id}`);
  await expect(page.getByTestId("order-error")).toContainText("не найден");

  await selectActor(page, "SUPPLIER", 2);
  await page.goto(`/supplier/orders/${foreignOrder.order_id}`);
  await expect(page.getByTestId("order-error")).toContainText("не найден");
  await expect(page.getByTestId("supplier-order-confirm")).toHaveCount(0);
});

test("Scenario 7 — temporary backend failure is recoverable with Refresh", async ({
  page,
}) => {
  await selectActor(page, "CUSTOMER", 1);
  const pattern = "**/api/order/api/v1/customers/1/orders**";
  await page.route(pattern, (route) => route.abort("failed"));
  await page.goto("/customer/orders");
  const error = page.getByTestId("customer-orders-error");
  await expect(error).toBeVisible();
  await page.unroute(pattern);
  await error.getByRole("button", { name: "Повторить", exact: true }).click();
  await expect(error).toHaveCount(0);
  await expect(page.getByRole("heading", { name: "Заказы покупателя" })).toBeVisible();
});
