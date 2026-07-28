import { useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api";
import { EmptyState, ErrorState, LoadingState, SuccessNotice } from "../components/AsyncState";
import { useMarketplace } from "../context";
import { useApiResource } from "../hooks";

export function FavoritesPage() {
  const { customer } = useMarketplace();
  const favorites = useApiResource(() => api.favorites(customer!.id), [customer!.id]);
  const [error, setError] = useState<unknown>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const mutate = async (productId: number, action: "cart" | "delete") => {
    setError(null);
    setNotice(null);
    try {
      if (action === "cart") {
        await api.addCart(customer!.id, productId, 1);
        setNotice("Товар добавлен в корзину");
      } else {
        await api.deleteFavorite(customer!.id, productId);
        await favorites.refresh();
      }
    } catch (caught) {
      setError(caught);
    }
  };
  return (
    <section>
      <div className="page-heading"><div><div className="eyebrow">CUSTOMER</div><h1>Избранное</h1></div><button className="button secondary" onClick={() => void favorites.refresh()}>Refresh</button></div>
      {favorites.loading && <LoadingState screen="favorites" />}
      {favorites.error !== null && <ErrorState screen="favorites" error={favorites.error} onRetry={() => void favorites.refresh()} />}
      {favorites.data?.items.length === 0 && <EmptyState screen="favorites">Избранных товаров пока нет.</EmptyState>}
      {notice && <SuccessNotice>{notice}</SuccessNotice>}
      {error !== null && <ErrorState screen="favorites-mutation" error={error} />}
      <div className="card-grid">
        {favorites.data?.items.map((favorite) => (
          <article className="entity-card" key={favorite.id}>
            <span className="entity-id">PRODUCT #{favorite.product.id}</span>
            <h2><Link to={`/customer/products/${favorite.product.id}`}>{favorite.product.name}</Link></h2>
            <p>{Number(favorite.product.price).toFixed(2)} RUB · stock {favorite.product.stocks}</p>
            <div className="button-row">
              <button className="button compact" onClick={() => void mutate(favorite.product.id, "cart")}>Add to cart</button>
              <button className="button danger compact" onClick={() => void mutate(favorite.product.id, "delete")}>Удалить</button>
            </div>
          </article>
        ))}
      </div>
    </section>
  );
}
