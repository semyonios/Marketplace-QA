interface StatusBadgeProps {
  status: string;
  testId?: string;
}

const tones: Record<string, string> = {
  PENDING_RESERVATION: "pending",
  CONFIRMATION_PENDING: "pending",
  CANCELLATION_PENDING: "pending",
  REJECTION_PENDING: "pending",
  RESERVED: "reserved",
  CONFIRMED: "success",
  REJECTED: "danger",
  CANCELLED: "neutral",
  FAILED: "danger",
  NONE: "neutral",
  REQUESTED: "pending",
  RELEASE_REQUESTED: "pending",
  RELEASED: "neutral",
};

export function StatusBadge({ status, testId }: StatusBadgeProps) {
  return (
    <span
      className={`status-badge status-${tones[status] ?? "unknown"}`}
      data-testid={testId}
    >
      {status}
    </span>
  );
}
