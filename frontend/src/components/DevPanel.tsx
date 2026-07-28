import { useState } from "react";
import type { ApiMeta, Order } from "../types";

export function DevPanel({ order, meta }: { order: Order; meta: ApiMeta | null }) {
  const [open, setOpen] = useState(false);
  return (
    <section className="dev-panel">
      <button
        type="button"
        className="dev-panel-toggle"
        onClick={() => setOpen((value) => !value)}
        aria-expanded={open}
      >
        DEV PANEL <span>{open ? "−" : "+"}</span>
      </button>
      {open && (
        <div className="dev-panel-body">
          <dl className="definition-grid">
            <dt>ETag</dt>
            <dd data-testid="dev-etag">{meta?.etag ?? "—"}</dd>
            <dt>Correlation ID</dt>
            <dd data-testid="correlation-id">{order.correlation_id}</dd>
            <dt>Version</dt>
            <dd>{order.version}</dd>
            <dt>HTTP status</dt>
            <dd>{meta?.status ?? "—"}</dd>
            <dt>Последний ответ</dt>
            <dd>{meta ? new Date(meta.receivedAt).toLocaleString("ru-RU") : "—"}</dd>
          </dl>
          <details>
            <summary>Headers</summary>
            <pre>{JSON.stringify(meta?.headers ?? {}, null, 2)}</pre>
          </details>
          <details>
            <summary>Order JSON</summary>
            <pre data-testid="dev-order-json">{JSON.stringify(order, null, 2)}</pre>
          </details>
          <details>
            <summary>History JSON</summary>
            <pre data-testid="dev-history-json">{JSON.stringify(order.history, null, 2)}</pre>
          </details>
          <details>
            <summary>Raw response</summary>
            <pre>{JSON.stringify(meta?.raw ?? null, null, 2)}</pre>
          </details>
        </div>
      )}
    </section>
  );
}
