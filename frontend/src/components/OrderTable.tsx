import { Link } from "react-router-dom";
import type { OrderListItem, Role } from "../types";
import { StatusBadge } from "./StatusBadge";

export function OrderTable({
  items,
  role,
  sortDirection,
  onToggleSort,
}: {
  items: OrderListItem[];
  role: Role;
  sortDirection: "desc" | "asc";
  onToggleSort: () => void;
}) {
  const sorted = [...items].sort((left, right) => {
    const delta = Date.parse(left.created_at) - Date.parse(right.created_at);
    return sortDirection === "asc" ? delta : -delta;
  });
  const prefix = role === "CUSTOMER" ? "/customer" : "/supplier";
  return (
    <div className="table-scroll">
      <table>
        <thead>
          <tr>
            <th>Order ID</th>
            <th>Status</th>
            <th>Operation</th>
            <th>Version</th>
            <th>
              <button className="sort-button" type="button" onClick={onToggleSort}>
                Created {sortDirection === "desc" ? "↓" : "↑"}
              </button>
            </th>
            <th>{role === "CUSTOMER" ? "Supplier" : "Customer"}</th>
            <th>Items</th>
            <th>Amount</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((order) => (
            <tr key={order.order_id} data-testid={`order-row-${order.order_id}`}>
              <td className="mono">{order.order_id.slice(0, 8)}…</td>
              <td><StatusBadge status={order.status} /></td>
              <td>{order.status.endsWith("_PENDING") ? order.status : "NONE"}</td>
              <td>v{order.version}</td>
              <td>{new Date(order.created_at).toLocaleString("ru-RU")}</td>
              <td>#{role === "CUSTOMER" ? order.supplier_id : order.customer_id}</td>
              <td>{order.items_count}</td>
              <td>{order.total_amount} {order.currency}</td>
              <td>
                <Link className="text-link" to={`${prefix}/orders/${order.order_id}`}>
                  Открыть
                </Link>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
