import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import { api } from "../api";
import { ErrorState, LoadingState, SuccessNotice } from "../components/AsyncState";
import { useMarketplace } from "../context";
import { useApiResource } from "../hooks";

export function CustomerProductPage() {
  const productId = Number(useParams().productId);
  const { customer } = useMarketplace();
  const product = useApiResource(() => api.customerProduct(productId), [productId]);
  const [quantity, setQuantity] = useState(1);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [notice, setNotice] = useState<string | null>(null);

  if (!Number.isInteger(productId) || productId <= 0) {
    return <ErrorState screen="product" error={new Error("Некорректный product ID")} />;
  }
  const mutate = async (favorite: boolean) => {
    if (!customer || !product.data) return;
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      if (favorite) {
        await api.addFavorite(customer.id, productId);
        setNotice("Товар добавлен в избранное");
      } else {
        await api.addCart(customer.id, productId, quantity);
        setNotice("Товар добавлен в корзину");
      }
    } catch (caught) {
      setError(caught);
    } finally {
      setBusy(false);
    }
  };

  return (
    <section>
      <div className="breadcrumbs"><Link to="/customer/catalog">Каталог</Link><span>/</span><span>Product #{productId}</span></div>
      {product.loading && <LoadingState screen="product" />}
      {product.error !== null && <ErrorState screen="product" error={product.error} onRetry={() => void product.refresh()} />}
      {product.data && (
        <div className="detail-grid">
          <article className="panel">
            <div className="eyebrow">PRODUCT #{product.data.id} · SUPPLIER #{product.data.supplier_id ?? "?"}</div>
            <h1>{product.data.name}</h1>
            <p>{product.data.description ?? "Описание не задано"}</p>
            <dl className="definition-grid">
              <dt>Цена</dt><dd>{Number(product.data.price).toFixed(2)} RUB</dd>
              <dt>Projected stock</dt><dd>{product.data.stocks}</dd>
              <dt>Статус</dt><dd>{product.data.is_archived ? "ARCHIVED" : product.data.is_active ? "ACTIVE" : "INACTIVE"}</dd>
            </dl>
          </article>
          <aside className="panel action-panel">
            <label>
              <span>Количество</span>
              <input
                type="number"
                min={1}
                data-testid="product-quantity"
                value={quantity}
                onChange={(event) => setQuantity(Math.max(1, Number(event.target.value)))}
              />
            </label>
            <button className="button primary" data-testid={`add-to-cart-${productId}`} disabled={busy} onClick={() => void mutate(false)}>
              Add to cart
            </button>
            <button className="button secondary" data-testid={`favorite-toggle-${productId}`} disabled={busy} onClick={() => void mutate(true)}>
              Add to favorites
            </button>
          </aside>
        </div>
      )}
      {notice && <SuccessNotice>{notice}</SuccessNotice>}
      {error !== null && <ErrorState screen="product-mutation" error={error} />}
    </section>
  );
}
