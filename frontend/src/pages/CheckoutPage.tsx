import { useMemo, useRef, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { api } from "../api";
import { EmptyState, ErrorState, LoadingState } from "../components/AsyncState";
import { useMarketplace } from "../context";
import { useApiResource } from "../hooks";

export function CheckoutPage() {
  const { customer } = useMarketplace();
  const navigate = useNavigate();
  const cart = useApiResource(() => api.cart(customer!.id), [customer!.id]);
  const [submitting, setSubmitting] = useState(false);
  const submittingRef = useRef(false);
  const [error, setError] = useState<unknown>(null);
  const [uncertain, setUncertain] = useState(false);
  const key = useMemo(() => {
    if (!cart.data) return null;
    const storageKey = `marketplace-checkout-${customer!.id}-${cart.data.cart_version}`;
    const existing = sessionStorage.getItem(storageKey);
    if (existing) return existing;
    const created = `checkout-${customer!.id}-v${cart.data.cart_version}-${crypto.randomUUID()}`;
    sessionStorage.setItem(storageKey, created);
    return created;
  }, [cart.data, customer]);

  const submit = async () => {
    if (!cart.data || !key || submittingRef.current) return;
    submittingRef.current = true;
    setSubmitting(true);
    setError(null);
    try {
      const result = await api.createOrder(customer!.id, cart.data.cart_version, key);
      setUncertain(false);
      navigate(`/customer/orders/${result.data.order_id}`, {
        state: { createdMeta: result.meta },
      });
    } catch (caught) {
      setError(caught);
      setUncertain(caught instanceof TypeError);
    } finally {
      submittingRef.current = false;
      setSubmitting(false);
    }
  };

  return (
    <section>
      <div className="breadcrumbs"><Link to="/customer/cart">Корзина</Link><span>/</span><span>Checkout</span></div>
      <div className="page-heading"><div><div className="eyebrow">IDEMPOTENT CHECKOUT</div><h1>Создание заказа</h1><p>Повтор после сетевой ошибки использует тот же ключ и то же тело запроса.</p></div></div>
      {cart.loading && <LoadingState screen="checkout" />}
      {cart.error !== null && <ErrorState screen="checkout" error={cart.error} onRetry={() => void cart.refresh()} />}
      {cart.data?.items.length === 0 && <EmptyState screen="checkout">Нельзя оформить пустую корзину.</EmptyState>}
      {cart.data && cart.data.items.length > 0 && (
        <div className="detail-grid">
          <article className="panel">
            <h2>Состав заказа</h2>
            {cart.data.items.map((item) => (
              <div className="checkout-line" key={item.id}>
                <span>{item.product.name} × {item.quantity}</span>
                <strong>{Number(item.total_price).toFixed(2)} RUB</strong>
              </div>
            ))}
            <div className="checkout-total"><span>Итого</span><strong>{Number(cart.data.total_price).toFixed(2)} RUB</strong></div>
          </article>
          <aside className="panel action-panel">
            <dl className="definition-grid">
              <dt>Customer</dt><dd>#{customer!.id}</dd>
              <dt>Supplier</dt><dd>#{cart.data.supplier_id ?? "conflict"}</dd>
              <dt>Cart version</dt><dd>{cart.data.cart_version}</dd>
              <dt>Idempotency-Key</dt><dd className="mono break">{key}</dd>
            </dl>
            <button
              type="button"
              className="button primary"
              data-testid={uncertain ? "checkout-retry" : "checkout-submit"}
              disabled={submitting}
              onClick={() => void submit()}
            >
              {submitting ? "Отправляем…" : uncertain ? "Повторить с тем же ключом" : "Создать заказ"}
            </button>
          </aside>
        </div>
      )}
      {error !== null && (
        <div data-testid="checkout-error">
          <ErrorState screen="checkout-submit" error={error} />
          <p className="muted">Ключ сохранён: <span className="mono">{key}</span></p>
        </div>
      )}
    </section>
  );
}
