import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import { api } from "../api";
import { ErrorState, LoadingState, SuccessNotice } from "../components/AsyncState";
import { useMarketplace } from "../context";
import { useApiResource } from "../hooks";

export function WarehouseStocksPage() {
  const warehouseId = Number(useParams().warehouseId);
  const { supplier } = useMarketplace();
  const products = useApiResource(() => api.supplierProducts(), [supplier!.id]);
  const [values, setValues] = useState<Record<number, string>>({});
  const [busy, setBusy] = useState<number | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const ownProducts = (products.data?.items ?? []).filter((product) => product.supplier_id === supplier!.id && !product.is_archived);
  const save = async (productId: number) => {
    const stocks = Number(values[productId]);
    if (!Number.isInteger(stocks) || stocks < 0) { setError(new Error("Остаток должен быть целым числом >= 0")); return; }
    setBusy(productId); setError(null); setNotice(null);
    try {
      await api.setWarehouseStock(warehouseId, productId, stocks);
      setNotice(`Warehouse #${warehouseId}: абсолютный остаток Product #${productId} установлен в ${stocks}. Projection обновится асинхронно.`);
      await products.refresh();
    } catch (caught) { setError(caught); } finally { setBusy(null); }
  };
  if (!Number.isInteger(warehouseId) || warehouseId <= 0) return <ErrorState screen="stocks" error={new Error("Некорректный warehouse ID")} />;
  return (
    <section>
      <div className="breadcrumbs"><Link to="/supplier/warehouses">Склады</Link><span>/</span><span>Warehouse #{warehouseId}</span></div>
      <div className="page-heading"><div><div className="eyebrow">ABSOLUTE STOCK WRITE</div><h1>Остатки склада #{warehouseId}</h1><p>Публичного API чтения Warehouse–Product rows нет. Current source ниже — агрегат по всем складам.</p></div><button className="button secondary" onClick={() => void products.refresh()}>Refresh</button></div>
      {products.loading && <LoadingState screen="stocks" />}{products.error !== null && <ErrorState screen="stocks" error={products.error} onRetry={() => void products.refresh()} />}
      {notice && <SuccessNotice>{notice}</SuccessNotice>}{error !== null && <ErrorState screen="stock-mutation" error={error} />}
      <div className="table-scroll"><table><thead><tr><th>Product</th><th>Aggregate total</th><th>Reserved</th><th>Available</th><th>Новый абсолютный stock этого склада</th><th /></tr></thead><tbody>{ownProducts.map((product) => <tr key={product.id} data-testid={`stock-row-${product.id}`}><td>{product.name}<small>#{product.id}</small></td><td>{product.stocks}</td><td>{product.reserved_stocks ?? 0}</td><td>{product.available_stocks ?? product.stocks}</td><td><input className="quantity-input" data-testid={`stock-input-${product.id}`} type="number" min={0} value={values[product.id] ?? ""} onChange={(event) => setValues((current) => ({ ...current, [product.id]: event.target.value }))} /></td><td><button className="button compact" data-testid={`stock-submit-${product.id}`} disabled={busy !== null || values[product.id] === undefined} onClick={() => void save(product.id)}>{busy === product.id ? "Сохраняем…" : "Установить"}</button></td></tr>)}</tbody></table></div>
    </section>
  );
}
