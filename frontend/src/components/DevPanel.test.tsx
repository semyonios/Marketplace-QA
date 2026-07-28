import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ApiMeta, Order } from "../types";
import { DevPanel } from "./DevPanel";

const order = {
  order_id: "order-1",
  correlation_id: "correlation-1",
  version: 4,
  history: [],
} as unknown as Order;
const meta: ApiMeta = {
  status: 200,
  headers: { etag: '"4"' },
  etag: '"4"',
  correlationId: "correlation-1",
  raw: order,
  receivedAt: "2026-07-29T10:00:00Z",
};

describe("DevPanel", () => {
  it("reveals ETag, correlation ID and raw order data", async () => {
    render(<DevPanel order={order} meta={meta} />);
    await userEvent.click(screen.getByRole("button", { name: /DEV PANEL/i }));
    expect(screen.getByTestId("dev-etag")).toHaveTextContent('"4"');
    expect(screen.getByTestId("correlation-id")).toHaveTextContent("correlation-1");
    expect(screen.getByTestId("dev-order-json")).toHaveTextContent("order-1");
  });
});
