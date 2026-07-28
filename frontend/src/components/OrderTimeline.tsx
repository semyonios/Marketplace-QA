import type { Order, OrderHistory } from "../types";
import { EmptyState } from "./AsyncState";
import { StatusBadge } from "./StatusBadge";

const triggerLabels: Record<string, string> = {
  CREATE_ORDER: "Заказ создан",
  StockReserved: "Остатки зарезервированы",
  StockReservationRejected: "Резервирование отклонено",
  ConfirmOrder: "Поставщик запросил подтверждение",
  StockFinalized: "Резерв финализирован",
  RejectOrder: "Поставщик отклонил заказ",
  CancelOrder: "Покупатель запросил отмену",
  StockReleased: "Резерв освобождён",
  StockReleaseFailed: "Освобождение резерва завершилось ошибкой",
  StockFinalizationFailed: "Финализация завершилась ошибкой",
  OperationTimeout: "Операция превысила лимит попыток",
};

function label(entry: OrderHistory): string {
  return triggerLabels[entry.trigger] ?? entry.trigger.replaceAll("_", " ");
}

export function OrderTimeline({ order }: { order: Order }) {
  if (!order.history.length) {
    return (
      <EmptyState screen="order-history">
        История пока пуста. Текущее состояние: {order.status}.
      </EmptyState>
    );
  }
  return (
    <ol className="timeline" data-testid="order-timeline">
      {order.history.map((entry) => (
        <li key={entry.history_id} className="timeline-item">
          <div className="timeline-marker" aria-hidden="true" />
          <div className="timeline-content">
            <div className="timeline-heading">
              <strong>{label(entry)}</strong>
              <time dateTime={entry.created_at}>
                {new Date(entry.created_at).toLocaleString("ru-RU")}
              </time>
            </div>
            <div className="badge-row">
              <StatusBadge
                status={
                  entry.operation_state_after !== "NONE"
                    ? entry.operation_state_after
                    : entry.business_status_after
                }
              />
              <span>v{entry.version_before} → v{entry.version_after}</span>
              <span>{entry.actor_type}{entry.actor_id ? ` #${entry.actor_id}` : ""}</span>
            </div>
            {entry.reason && (
              <p className="muted">
                Причина: {entry.reason.code}
                {entry.reason.text ? ` — ${entry.reason.text}` : ""}
              </p>
            )}
          </div>
        </li>
      ))}
    </ol>
  );
}
