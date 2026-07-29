import { useMemo, useState } from "react";
import { api } from "../api";
import { EmptyState, ErrorState, LoadingState } from "../components/AsyncState";
import { OrderTable } from "../components/OrderTable";
import { useMarketplace } from "../context";
import { useApiResource } from "../hooks";
import type { Role } from "../types";

const statuses = [
  "",
  "PENDING_RESERVATION",
  "RESERVED",
  "CONFIRMED",
  "REJECTED",
  "CANCELLED",
];

export function OrdersPage({ role }: { role: Role }) {
  const context = useMarketplace();
  const ownerId = role === "CUSTOMER" ? context.customer!.id : context.supplier!.id;
  const [draftStatus, setDraftStatus] = useState("");
  const [draftFrom, setDraftFrom] = useState("");
  const [draftTo, setDraftTo] = useState("");
  const [filters, setFilters] = useState({ status: "", created_from: "", created_to: "" });
  const [page, setPage] = useState(1);
  const [sort, setSort] = useState<"desc" | "asc">("desc");
  const params = useMemo(
    () => ({
      page,
      limit: 20,
      status: filters.status || undefined,
      created_from: filters.created_from || undefined,
      created_to: filters.created_to || undefined,
    }),
    [filters, page],
  );
  const orders = useApiResource(
    () =>
      role === "CUSTOMER"
        ? api.customerOrders(ownerId, params)
        : api.supplierOrders(ownerId, params),
    [role, ownerId, params],
  );
  const screen = role === "CUSTOMER" ? "customer-orders" : "supplier-orders";

  const apply = () => {
    setPage(1);
    setFilters({
      status: draftStatus,
      created_from: draftFrom ? new Date(draftFrom).toISOString() : "",
      created_to: draftTo ? new Date(draftTo).toISOString() : "",
    });
  };

  return (
    <section>
      <div className="page-heading">
        <div>
          <div className="eyebrow">{role} · ORDER-SERVICE</div>
          <h1>Заказы {role === "CUSTOMER" ? "покупателя" : "поставщика"}</h1>
          <p>Backend сортирует стабильно: created_at DESC, id DESC. UI позволяет изменить визуальную сортировку текущей страницы.</p>
        </div>
        <button className="button secondary" data-testid={`${screen}-refresh`} onClick={() => void orders.refresh()}>Refresh</button>
      </div>
      <div className="toolbar order-filters">
        <label><span>Status</span><select data-testid={role === "SUPPLIER" ? "supplier-order-status-filter" : "customer-order-status-filter"} value={draftStatus} onChange={(event) => setDraftStatus(event.target.value)}>{statuses.map((status) => <option key={status || "all"} value={status}>{status || "Все"}</option>)}</select></label>
        <label><span>Created from</span><input type="datetime-local" value={draftFrom} onChange={(event) => setDraftFrom(event.target.value)} /></label>
        <label><span>Created to</span><input type="datetime-local" value={draftTo} onChange={(event) => setDraftTo(event.target.value)} /></label>
        <button className="button compact" type="button" onClick={apply}>Применить</button>
      </div>
      {orders.loading && <LoadingState screen={screen} />}
      {orders.error !== null && <ErrorState screen={screen} error={orders.error} onRetry={() => void orders.refresh()} />}
      {orders.data?.items.length === 0 && <EmptyState screen={screen}>Заказы не найдены.</EmptyState>}
      {orders.data && orders.data.items.length > 0 && (
        <OrderTable
          items={orders.data.items}
          role={role}
          sortDirection={sort}
          onToggleSort={() => setSort((value) => value === "desc" ? "asc" : "desc")}
        />
      )}
      {orders.data && (
        <div className="pagination">
          <button className="button secondary compact" disabled={page <= 1} onClick={() => setPage((value) => value - 1)}>←</button>
          <span>Страница {orders.data.page} · {orders.data.count} из {orders.data.total}</span>
          <button className="button secondary compact" disabled={page * orders.data.limit >= orders.data.total} onClick={() => setPage((value) => value + 1)}>→</button>
        </div>
      )}
      <p className="last-updated">Последнее чтение: {orders.meta ? new Date(orders.meta.receivedAt).toLocaleString("ru-RU") : "—"}</p>
    </section>
  );
}
