import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { ApiError, api } from "../api";
import { apiConfig } from "../config";
import { ErrorState, LoadingState, SuccessNotice } from "../components/AsyncState";
import { DevPanel } from "../components/DevPanel";
import { OrderTimeline } from "../components/OrderTimeline";
import { StatusBadge } from "../components/StatusBadge";
import { useMarketplace } from "../context";
import { useApiResource } from "../hooks";
import type { Role } from "../types";

const terminal = new Set(["CONFIRMED", "REJECTED", "CANCELLED"]);

export function OrderDetailPage({ role }: { role: Role }) {
  const { orderId = "" } = useParams();
  const context = useMarketplace();
  const ownerId = role === "CUSTOMER" ? context.customer!.id : context.supplier!.id;
  const loader = () =>
    role === "CUSTOMER"
      ? api.customerOrder(ownerId, orderId)
      : api.supplierOrder(ownerId, orderId);
  const order = useApiResource(loader, [role, ownerId, orderId]);
  const [mutationError, setMutationError] = useState<unknown>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [pollingFailed, setPollingFailed] = useState(false);
  const [rejectCode, setRejectCode] = useState("SUPPLIER_REJECTED");
  const [rejectText, setRejectText] = useState("");
  const [cancelCode, setCancelCode] = useState("CUSTOMER_REQUEST");
  const polling =
    !!order.data &&
    !terminal.has(order.data.business_status) &&
    (order.data.operation_state !== "NONE" ||
      order.data.business_status === "PENDING_RESERVATION") &&
    !pollingFailed;

  useEffect(() => {
    if (!polling) return;
    const interval = window.setInterval(() => {
      void order.refresh().catch(() => setPollingFailed(true));
    }, apiConfig.pollingMs);
    return () => window.clearInterval(interval);
  }, [polling, order.refresh]);

  const manualRefresh = async () => {
    setPollingFailed(false);
    setMutationError(null);
    await order.refresh().catch(() => setPollingFailed(true));
  };

  const mutate = async (action: "cancel" | "confirm" | "reject") => {
    if (!order.data || !order.meta?.etag || busy) return;
    setBusy(true); setMutationError(null); setNotice(null);
    try {
      const result =
        action === "cancel"
          ? await api.cancelOrder(ownerId, orderId, order.meta.etag, cancelCode)
          : action === "confirm"
            ? await api.confirmOrder(ownerId, orderId, order.meta.etag)
            : await api.rejectOrder(ownerId, orderId, order.meta.etag, rejectCode, rejectText || undefined);
      order.setData(result.data);
      order.setMeta(result.meta);
      setNotice(`Команда ${action.toUpperCase()} принята. Начат polling.`);
      setPollingFailed(false);
    } catch (caught) {
      setMutationError(caught);
      if (caught instanceof ApiError && caught.status === 412) {
        setNotice("Получен 412: ETag устарел. Выполняется чтение актуальной версии.");
        await order.refresh().catch(() => undefined);
      }
    } finally {
      setBusy(false);
    }
  };

  const prefix = role === "CUSTOMER" ? "/customer/orders" : "/supplier/orders";
  return (
    <section>
      <div className="breadcrumbs"><Link to={prefix}>Заказы</Link><span>/</span><span className="mono">{orderId.slice(0, 8)}…</span></div>
      {order.loading && !order.data && <LoadingState screen="order" />}
      {order.error !== null && !order.data && <ErrorState screen="order" error={order.error} onRetry={() => void manualRefresh()} />}
      {order.data && (
        <>
          <div className="page-heading">
            <div>
              <div className="eyebrow">{role} ORDER DETAIL</div>
              <h1 className="mono">Order {order.data.order_id}</h1>
              <div className="badge-row">
                <StatusBadge status={order.data.status} testId="order-status" />
                <StatusBadge status={order.data.operation_state} testId="order-async-status" />
                {polling && <span className="polling-indicator" data-testid="order-polling"><span className="spinner" /> polling {apiConfig.pollingMs / 1000}s</span>}
                {pollingFailed && <span className="error-text">Polling остановлен после ошибки</span>}
              </div>
            </div>
            <button className="button secondary" data-testid="order-refresh" onClick={() => void manualRefresh()}>Refresh</button>
          </div>
          {notice && <SuccessNotice>{notice}</SuccessNotice>}
          {mutationError && <ErrorState screen="order-mutation" error={mutationError} />}
          <div className="detail-grid order-overview">
            <article className="panel">
              <h2>Общая информация</h2>
              <dl className="definition-grid">
                <dt>Customer</dt><dd>#{order.data.customer_id}</dd>
                <dt>Supplier</dt><dd>#{order.data.supplier_id}</dd>
                <dt>Order status</dt><dd>{order.data.status}</dd>
                <dt>Business status</dt><dd>{order.data.business_status}</dd>
                <dt>Operation state</dt><dd>{order.data.operation_state}</dd>
                <dt>Reservation</dt><dd>{order.data.reservation_state}</dd>
                <dt>Version</dt><dd>v{order.data.version}</dd>
                <dt>ETag</dt><dd className="mono">{order.meta?.etag ?? "—"}</dd>
                <dt>Correlation ID</dt><dd className="mono break" data-testid="correlation-id">{order.data.correlation_id}</dd>
                <dt>Created</dt><dd>{new Date(order.data.created_at).toLocaleString("ru-RU")}</dd>
              </dl>
              {order.data.failure_reason && <div className="reason danger"><b>Failure reason</b><span>{order.data.failure_reason.code}: {order.data.failure_reason.text ?? "без описания"}</span></div>}
              {order.data.rejection_reason && <div className="reason"><b>Rejection reason</b><span>{order.data.rejection_reason.code}: {order.data.rejection_reason.text ?? "без описания"}</span></div>}
              {order.data.cancellation_reason && <div className="reason"><b>Cancellation reason</b><span>{order.data.cancellation_reason.code}: {order.data.cancellation_reason.text ?? "без описания"}</span></div>}
            </article>
            <aside className="panel action-panel">
              <h2>Доступные действия</h2>
              {role === "CUSTOMER" && order.data.available_actions.can_cancel && (
                <>
                  <label><span>Причина отмены</span><select value={cancelCode} onChange={(event) => setCancelCode(event.target.value)}><option value="CUSTOMER_REQUEST">CUSTOMER_REQUEST</option><option value="DUPLICATE_ORDER">DUPLICATE_ORDER</option><option value="OTHER">OTHER</option></select></label>
                  <button className="button danger" data-testid="order-cancel" disabled={busy} onClick={() => void mutate("cancel")}>Cancel order</button>
                </>
              )}
              {role === "SUPPLIER" && order.data.available_actions.can_confirm && (
                <button className="button primary" data-testid="supplier-order-confirm" disabled={busy} onClick={() => void mutate("confirm")}>Confirm</button>
              )}
              {role === "SUPPLIER" && order.data.available_actions.can_reject && (
                <>
                  <label><span>Причина</span><select data-testid="supplier-reject-reason" value={rejectCode} onChange={(event) => setRejectCode(event.target.value)}><option value="SUPPLIER_REJECTED">SUPPLIER_REJECTED</option><option value="QUALITY_ISSUE">QUALITY_ISSUE</option><option value="OTHER">OTHER</option></select></label>
                  {rejectCode === "OTHER" && <label><span>Описание</span><textarea maxLength={500} value={rejectText} onChange={(event) => setRejectText(event.target.value)} /></label>}
                  <button className="button danger" data-testid="supplier-order-reject" disabled={busy || (rejectCode === "OTHER" && !rejectText.trim())} onClick={() => void mutate("reject")}>Reject</button>
                </>
              )}
              {!order.data.available_actions.can_cancel && !order.data.available_actions.can_confirm && !order.data.available_actions.can_reject && (
                <p className="muted">Для текущего состояния действий нет. Доступно только чтение.</p>
              )}
            </aside>
          </div>
          <article className="panel">
            <h2>Товары</h2>
            <div className="table-scroll"><table><thead><tr><th>Product</th><th>Quantity</th><th>Unit price</th><th>Line total</th></tr></thead><tbody>{order.data.items.map((item) => <tr key={item.product_id}><td>{item.product_name}<small>#{item.product_id}</small></td><td>{item.quantity}</td><td>{item.unit_price} {item.currency}</td><td>{item.line_total} {item.currency}</td></tr>)}</tbody></table></div>
            <div className="order-total">Итого: <strong>{order.data.total_amount} {order.data.currency}</strong></div>
          </article>
          <article className="panel timeline-panel"><div className="panel-heading"><div><div className="eyebrow">AUDIT TRAIL</div><h2>History timeline</h2></div><span>{order.data.history.length} событий</span></div><OrderTimeline order={order.data} /></article>
          <DevPanel order={order.data} meta={order.meta} />
        </>
      )}
    </section>
  );
}
