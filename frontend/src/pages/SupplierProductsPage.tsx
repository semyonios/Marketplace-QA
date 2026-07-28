import { useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api";
import { EmptyState, ErrorState, LoadingState, SuccessNotice } from "../components/AsyncState";
import { useMarketplace } from "../context";
import { useApiResource } from "../hooks";

export function SupplierProductsPage() {
  const { supplier } = useMarketplace();
  const products = useApiResource(() => api.supplierProducts(), [supplier!.id]);
  const [search, setSearch] = useState("");
  const [showArchived, setShowArchived] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const ownProducts = (products.data?.items ?? [])
    .filter((product) => product.supplier_id === supplier!.id)
    .filter((product) => showArchived || !product.is_archived)
    .filter((product) => product.name.toLowerCase().includes(search.toLowerCase()));

  const archive = async (id: number) => {
    if (!window.confirm(`Архивировать товар #${id}?`)) return;
    setError(null);
    try {
      await api.archiveProduct(id);
      setNotice(`Товар #${id} архивирован. Customer projection обновится асинхронно.`);
      await products.refresh();
    } catch (caught) {
      setError(caught);
    }
  };

  return (
    <section>
      <div className="page-heading">
        <div><div className="eyebrow">SUPPLIER SOURCE</div><h1>Товары поставщика</h1><p>Показаны только товары Supplier #{supplier!.id} из общего source API.</p></div>
        <div className="button-row"><button className="button secondary" onClick={() => void products.refresh()}>Refresh</button><Link className="button primary" to="/supplier/products/new">Создать товар</Link></div>
      </div>
      <div className="toolbar">
        <label><span>Поиск</span><input value={search} onChange={(event) => setSearch(event.target.value)} /></label>
        <label className="checkbox-label"><input type="checkbox" checked={showArchived} onChange={(event) => setShowArchived(event.target.checked)} />Показывать archived</label>
        <div className="toolbar-stat"><span>Товаров</span><strong>{ownProducts.length}</strong></div>
      </div>
      {products.loading && <LoadingState screen="supplier-products" />}
      {products.error !== null && <ErrorState screen="supplier-products" error={products.error} onRetry={() => void products.refresh()} />}
      {ownProducts.length === 0 && !products.loading && <EmptyState screen="supplier-products">У поставщика нет товаров по фильтру.</EmptyState>}
      {notice && <SuccessNotice>{notice}</SuccessNotice>}
      {error !== null && <ErrorState screen="supplier-product-mutation" error={error} />}
      {ownProducts.length > 0 && <div className="table-scroll"><table><thead><tr><th>Товар</th><th>Цена</th><th>Остаток</th><th>Reserved</th><th>Available</th><th>Состояние</th><th>Действия</th></tr></thead><tbody>{ownProducts.map((product) => (
        <tr key={product.id} data-testid={`supplier-product-${product.id}`}>
          <td>{product.name}<small>#{product.id}</small></td><td>{Number(product.price).toFixed(2)} RUB</td><td>{product.stocks}</td><td>{product.reserved_stocks ?? 0}</td><td><strong>{product.available_stocks ?? product.stocks}</strong></td><td>{product.is_archived ? "ARCHIVED" : product.is_active ? "ACTIVE" : "INACTIVE"}</td>
          <td><div className="button-row"><Link className="button secondary compact" to={`/supplier/products/${product.id}/edit`}>Редактировать</Link>{!product.is_archived && <button className="button danger compact" data-testid="product-archive" onClick={() => void archive(product.id)}>Архивировать</button>}</div></td>
        </tr>
      ))}</tbody></table></div>}
      <p className="last-updated">Source обновлён: {products.meta ? new Date(products.meta.receivedAt).toLocaleString("ru-RU") : "—"}</p>
    </section>
  );
}
