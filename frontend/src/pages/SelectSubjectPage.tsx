import { useEffect, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { api } from "../api";
import { EmptyState, ErrorState, LoadingState } from "../components/AsyncState";
import { useMarketplace } from "../context";
import type { Customer, Role, Supplier } from "../types";

export function SelectSubjectPage() {
  const context = useMarketplace();
  const navigate = useNavigate();
  const [params] = useSearchParams();
  const requestedRole = params.get("role");
  const role: Role =
    requestedRole === "SUPPLIER" || requestedRole === "CUSTOMER"
      ? requestedRole
      : context.role ?? "CUSTOMER";
  const [items, setItems] = useState<(Customer | Supplier)[]>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<unknown>(null);

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      const result = role === "CUSTOMER" ? await api.customers() : await api.suppliers();
      setItems(result.data.items);
      setSelectedId(result.data.items[0]?.id ?? null);
    } catch (caught) {
      setError(caught);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    context.selectRole(role);
    void load();
    // Loading is intentionally tied to the role encoded in the URL.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [role]);

  const submit = () => {
    const selected = items.find((item) => item.id === selectedId);
    if (!selected) return;
    if (role === "CUSTOMER") {
      context.selectCustomer(selected as Customer);
      navigate("/customer/catalog");
    } else {
      context.selectSupplier(selected as Supplier);
      navigate("/supplier/products");
    }
  };

  return (
    <div className="centered-page">
      <section className="selection-card">
        <div className="eyebrow">TEST IDENTITY · NO AUTH</div>
        <h1>Выберите {role === "CUSTOMER" ? "покупателя" : "поставщика"}</h1>
        <p className="muted">
          Это тестовый UI-контекст, а не аутентификация или проверка полномочий.
        </p>
        <div className="segmented" role="group" aria-label="Роль">
          {(["CUSTOMER", "SUPPLIER"] as Role[]).map((candidate) => (
            <button
              key={candidate}
              type="button"
              className={role === candidate ? "active" : ""}
              data-testid={`select-role-${candidate.toLowerCase()}`}
              onClick={() => navigate(`/select-user?role=${candidate}`)}
            >
              {candidate}
            </button>
          ))}
        </div>
        {loading && <LoadingState screen="select-user" />}
        {error !== null && <ErrorState screen="select-user" error={error} onRetry={() => void load()} />}
        {!loading && !error && items.length === 0 && (
          <EmptyState screen="test-user-list">Seed-пользователи не найдены.</EmptyState>
        )}
        {!loading && !error && items.length > 0 && (
          <div className="subject-list" data-testid="test-user-list">
            {items.map((item) => (
              <label
                key={item.id}
                className={selectedId === item.id ? "subject-option selected" : "subject-option"}
                data-testid={`test-user-${item.id}`}
              >
                <input
                  type="radio"
                  name="subject"
                  value={item.id}
                  checked={selectedId === item.id}
                  onChange={() => setSelectedId(item.id)}
                />
                <span className="subject-id">#{item.id}</span>
                <span>
                  <strong>{item.full_name}</strong>
                  <small>{item.email}</small>
                </span>
              </label>
            ))}
          </div>
        )}
        <button
          type="button"
          className="button primary wide"
          data-testid="select-user-submit"
          disabled={selectedId === null}
          onClick={submit}
        >
          Открыть {role === "CUSTOMER" ? "Customer" : "Supplier"} zone
        </button>
      </section>
    </div>
  );
}
