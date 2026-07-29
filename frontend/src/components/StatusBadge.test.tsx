import { render, screen } from "@testing-library/react";
import { StatusBadge } from "./StatusBadge";

describe("StatusBadge", () => {
  it("shows text as well as color semantics", () => {
    render(<StatusBadge status="CONFIRMATION_PENDING" testId="status" />);
    expect(screen.getByTestId("status")).toHaveTextContent("CONFIRMATION_PENDING");
    expect(screen.getByTestId("status")).toHaveClass("status-pending");
  });

  it("renders unknown backend states safely", () => {
    render(<StatusBadge status="FUTURE_STATE" testId="status" />);
    expect(screen.getByTestId("status")).toHaveClass("status-unknown");
  });
});
