import { render, screen } from "@testing-library/react";
import type { Order } from "../types";
import { OrderTimeline } from "./OrderTimeline";

const order: Order = {
  order_id: "00000000-0000-0000-0000-000000000001",
  customer_id: 101,
  supplier_id: 201,
  status: "RESERVED",
  business_status: "RESERVED",
  operation_state: "NONE",
  reservation_state: "RESERVED",
  version: 2,
  items: [],
  total_amount: "100.00",
  currency: "RUB",
  rejection_reason: null,
  cancellation_reason: null,
  failure_reason: null,
  created_at: "2026-07-29T10:00:00Z",
  updated_at: "2026-07-29T10:00:02Z",
  correlation_id: "00000000-0000-0000-0000-000000000010",
  available_actions: {
    can_cancel: true,
    can_confirm: false,
    can_reject: false,
    can_retry: false,
    can_refresh: true,
  },
  history: [
    {
      history_id: "00000000-0000-0000-0000-000000000011",
      trigger: "CREATE_ORDER",
      actor_type: "CUSTOMER",
      actor_id: 101,
      event_id: null,
      business_status_before: null,
      business_status_after: "PENDING_RESERVATION",
      operation_state_before: null,
      operation_state_after: "NONE",
      reservation_state_before: null,
      reservation_state_after: "REQUESTED",
      version_before: 0,
      version_after: 1,
      reason: null,
      correlation_id: "00000000-0000-0000-0000-000000000010",
      created_at: "2026-07-29T10:00:00Z",
    },
    {
      history_id: "00000000-0000-0000-0000-000000000012",
      trigger: "StockReserved",
      actor_type: "KAFKA_CONSUMER",
      actor_id: null,
      event_id: "00000000-0000-0000-0000-000000000013",
      business_status_before: "PENDING_RESERVATION",
      business_status_after: "RESERVED",
      operation_state_before: "NONE",
      operation_state_after: "NONE",
      reservation_state_before: "REQUESTED",
      reservation_state_after: "RESERVED",
      version_before: 1,
      version_after: 2,
      reason: null,
      correlation_id: "00000000-0000-0000-0000-000000000010",
      created_at: "2026-07-29T10:00:02Z",
    },
  ],
};

describe("OrderTimeline", () => {
  it("shows ordered business transitions and versions", () => {
    render(<OrderTimeline order={order} />);
    expect(screen.getByText("Заказ создан")).toBeInTheDocument();
    expect(screen.getByText("Остатки зарезервированы")).toBeInTheDocument();
    expect(screen.getByText("v1 → v2")).toBeInTheDocument();
  });
});
