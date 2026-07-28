import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "../api";
import { EmptyState, ErrorState, LoadingState, SuccessNotice } from "../components/AsyncState";
import { useMarketplace } from "../context";
import { useApiResource } from "../hooks";

export function CartPage() {
  const { customer } = useMarketplace();
  const navigate = useNavigate();
  const cart = useApiResource(() => api.cart(customer!.id), [customer!.id]);
  const [busy, setBusy] = useState<number | "all" | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const update = async (productId: number, quantity: number) => {
    setBusy(productId); setError(null); setNotice(null);
    try {
      await api.updateCart(customer!.id, productId, quantity);
      await cart.refresh();
      setNotice("Количество обновлено");
    } catch (caught) { setError(caught); } finally { setBusy(null); }
  };
  const remove = async (productId: number) => {
    setBusy(productId); setError(null);
    try { await api.deleteCart(customer!.id, productId); await cart.refresh(); }
    catch (caught) { setError(caught); } finally { setBusy(null); }
  };
  const clear = async () => {
    if (!cart.data) return;
    setBusy("all"); setError(null);
    try {
      for (const item of cart.data.items) await api.deleteCart(customer!.id, item.product.id);
      await cart.refresh();
    } catch (caught) { setError(caught); } finally { setBusy(null); }
  };

  return (
    <section>
      <div className="page-heading">
        <div><div className="eyebrow">SINGLE-SUPPLIER CART</div><h1>Корзина</h1><p>Cart version: <b>{cart.data?.cart_version ?? "—"}</b> · Supplier: <b>#{cart.data?.supplier_id ?? "—"}</b></p></div>
        <button className="button secondary" onClick={() => void cart.refresh()}>Refresh</button>
      </div>
      {cart.loading && <LoadingState screen="cart" />}
      {cart.error !== null && <ErrorState screen="cart" error={cart.error} onRetry={() => void cart.refresh()} />}
      {cart.data?.items.length === 0 && <EmptyState screen="cart">Корзина пуста. Добавьте товар из каталога.</EmptyState>}
      {notice && <SuccessNotice>{notice}</SuccessNotice>}
      {error !== null && <ErrorState screen="cart-mutation" error={error} />}
      {cart.data && cart.data.items.length > 0 && (
        <>
          <div className="table-scroll">
            <table>
              <thead><tr><th>Товар</th><th>Supplier</th><th>Цена</th><th>Количество</th><th>Сумма</th><th /></tr></thead>
              <tbody>
                {cart.data.items.map((item) => (
                  <tr key={item.id} data-testid={`cart-item-${item.product.id}`}>
                    <td>{item.product.name}<small>#{item.product.id}</small></td>
                    <td>#{item.product.supplier_id ?? "?"}</td>
                    <td>{Number(item.unit_price).toFixed(2)} RUB</td>
                    <td>
                      <input
                        className="quantity-input"
                        type="number"
                        min={1}
                        data-testid={`cart-quantity-${item.product.id}`}
                        defaultValue={item.quantity}
                        disabled={busy !== null}
                        onBlur={(event) => {
                          const next = Math.max(1, Number(event.target.value));
                          if (next !== item.quantity) void update(item.product.id, next);
                        }}
                      />
                    </td>
                    <td>{Number(item.total_price).toFixed(2)} RUB</td>
                    <td><button className="button danger compact" data-testid={`cart-remove-${item.product.id}`} disabled={busy !== null} onClick={() => void remove(item.product.id)}>Удалить</button></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="cart-summary">
            <div><span>Позиций</span><strong>{cart.data.count}</strong></div>
            <div><span>Товаров</span><strong>{cart.data.total_items_count}</strong></div>
            <div><span>Итого</span><strong>{Number(cart.data.total_price).toFixed(2)} RUB</strong></div>
            <button className="button danger" data-testid="cart-clear" disabled={busy !== null} onClick={() => void clear()}>Очистить</button>
            <button className="button primary" data-testid="cart-checkout" disabled={busy !== null} onClick={() => navigate("/customer/checkout")}>Создать заказ →</button>
          </div>
        </>
      )}
    </section>
  );
}
