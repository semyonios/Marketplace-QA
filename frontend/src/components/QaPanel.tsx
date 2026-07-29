import { useCallback, useEffect, useState } from "react";
import { api } from "../api";

interface ServiceState {
  service: string;
  health: "UP" | "DOWN";
  readiness: "READY" | "NOT_READY" | "N/A";
  database: string;
  kafka: string;
}

export function QaPanel() {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [updatedAt, setUpdatedAt] = useState<string | null>(null);
  const [services, setServices] = useState<ServiceState[]>([]);

  const refresh = useCallback(async () => {
    setLoading(true);
    const [customerHealth, supplierHealth, supplierReady, orderHealth, orderReady] =
      await Promise.allSettled([
        api.health("customer"),
        api.health("supplier"),
        api.ready("supplier"),
        api.health("order"),
        api.ready("order"),
      ]);
    const readiness = (result: typeof supplierReady) =>
      result.status === "fulfilled"
        ? (result.value.data as { dependencies?: Record<string, string> })
        : null;
    const supplierDependencies = readiness(supplierReady)?.dependencies;
    const orderDependencies = readiness(orderReady)?.dependencies;
    setServices([
      {
        service: "customer-service",
        health: customerHealth.status === "fulfilled" ? "UP" : "DOWN",
        readiness: "N/A",
        database: "health only",
        kafka: "health only",
      },
      {
        service: "supplier-service",
        health: supplierHealth.status === "fulfilled" ? "UP" : "DOWN",
        readiness: supplierReady.status === "fulfilled" ? "READY" : "NOT_READY",
        database: supplierDependencies?.database ?? "unknown",
        kafka: supplierDependencies?.kafka ?? "unknown",
      },
      {
        service: "order-service",
        health: orderHealth.status === "fulfilled" ? "UP" : "DOWN",
        readiness: orderReady.status === "fulfilled" ? "READY" : "NOT_READY",
        database: orderDependencies?.database ?? "unknown",
        kafka: orderDependencies?.kafka ?? "unknown",
      },
    ]);
    setUpdatedAt(new Date().toISOString());
    setLoading(false);
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return (
    <aside className="qa-panel">
      <button
        type="button"
        className="qa-panel-toggle"
        onClick={() => setOpen((value) => !value)}
        aria-expanded={open}
      >
        <span className="qa-dot" aria-hidden="true" />
        QA PANEL
        <span>{open ? "−" : "+"}</span>
      </button>
      {open && (
        <div className="qa-panel-body" data-testid="qa-panel">
          {services.map((service) => (
            <article key={service.service} className="qa-service">
              <strong>{service.service}</strong>
              <div><span>Health</span><b>{service.health}</b></div>
              <div><span>Readiness</span><b>{service.readiness}</b></div>
              <div><span>Database</span><b>{service.database}</b></div>
              <div><span>Kafka</span><b>{service.kafka}</b></div>
            </article>
          ))}
          <p className="muted">
            Обновлено: {updatedAt ? new Date(updatedAt).toLocaleString("ru-RU") : "—"}
          </p>
          <button type="button" className="button secondary" onClick={() => void refresh()} disabled={loading}>
            {loading ? "Проверяем…" : "Refresh"}
          </button>
        </div>
      )}
    </aside>
  );
}
