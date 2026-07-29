import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api";
import { EmptyState, ErrorState, LoadingState, SuccessNotice } from "../components/AsyncState";
import { useMarketplace } from "../context";
import { useApiResource } from "../hooks";
import type { Product } from "../types";

export function CustomerCatalogPage() {
  const { customer } = useMarketplace();
  const catalog = useApiResource(() => api.customerProducts(), []);
  const stockSource = useApiResource(() => api.supplierProducts(), []);
  const [search, setSearch] = useState("");
  const [availability, setAvailability] = useState("available");
  const [supplier, setSupplier] = useState("all");
  const [mutationError, setMutationError] = useState<unknown>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [adding, setAdding] = useState<number | null>(null);

  const sourceById = useMemo(
    () => new Map(stockSource.data?.items.map((product) => [product.id, product]) ?? []),
    [stockSource.data],
  );
  const products = (catalog.data?.items ?? [])
    .map<Product>((projected) => {
      const source = sourceById.get(projected.id);
      return {
        ...projected,
        reserved_stocks: source?.reserved_stocks,
        available_stocks: source?.available_stocks ?? projected.stocks,
      };
    })
    .filter((product) => product.name.toLowerCase().includes(search.trim().toLowerCase()))
    .filter((product) => supplier === "all" || String(product.supplier_id) === supplier)
    .filter((product) =>
      availability === "all"
        ? true
        : availability === "available"
          ? (product.available_stocks ?? product.stocks) > 0 && product.is_active && !product.is_archived
          : product.is_archived || !product.is_active,
    );
  const supplierIds = Array.from(
    new Set((catalog.data?.items ?? []).map((item) => item.supplier_id).filter(Boolean)),
  );

  const add = async (product: Product) => {
    if (!customer) return;
    setAdding(product.id);
    setMutationError(null);
    setSuccess(null);
    try {
      await api.addCart(customer.id, product.id, 1);
      setSuccess(`${product.name}: добавлено в корзину`);
    } catch (caught) {
      setMutationError(caught);
    } finally {
      setAdding(null);
    }
  };

  const refresh = () => {
    void Promise.all([catalog.refresh(), stockSource.refresh()]).catch(() => undefined);
  };

  return (
    <section data-testid="customer-catalog-page">
      <div className="page-heading">
        <div>
          <div className="eyebrow">CUSTOMER PROJECTION</div>
          <h1>Каталог товаров</h1>
          <p>Цена и каталог читаются из customer-service; резерв и available показаны из source для QA-сверки.</p>
        </div>
        <button className="button secondary" data-testid="catalog-refresh" type="button" onClick={refresh}>
          Refresh
        </button>
      </div>
      <div className="toolbar">
        <label>
          <span>Поиск</span>
          <input
            data-testid="catalog-search"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Название товара"
          />
        </label>
        <label>
          <span>Доступность</span>
          <select
            data-testid="catalog-available-filter"
            value={availability}
            onChange={(event) => setAvailability(event.target.value)}
          >
            <option value="available">Доступные</option>
            <option value="unavailable">Недоступные</option>
            <option value="all">Все</option>
          </select>
        </label>
        <label>
          <span>Поставщик</span>
          <select value={supplier} onChange={(event) => setSupplier(event.target.value)}>
            <option value="all">Все</option>
            {supplierIds.map((id) => <option key={id} value={String(id)}>Supplier #{id}</option>)}
          </select>
        </label>
        <div className="toolbar-stat"><span>Найдено</span><strong>{products.length}</strong></div>
      </div>
      {success && <SuccessNotice>{success}</SuccessNotice>}
      {mutationError !== null && <ErrorState screen="catalog-mutation" error={mutationError} />}
      {(catalog.loading || stockSource.loading) && <LoadingState screen="catalog" label="Читаем projection и source stock…" />}
      {(catalog.error !== null || stockSource.error !== null) && (
        <ErrorState screen="catalog" error={catalog.error ?? stockSource.error} onRetry={refresh} />
      )}
      {!catalog.loading && !catalog.error && products.length === 0 && (
        <EmptyState screen="catalog">Товары по выбранным условиям не найдены.</EmptyState>
      )}
      {products.length > 0 && (
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Товар</th><th>Supplier</th><th>Цена</th><th>Остаток</th>
                <th>Зарезервировано</th><th>Доступно</th><th>Состояние</th><th>Действие</th>
              </tr>
            </thead>
            <tbody>
              {products.map((product) => (
                <tr key={product.id} data-testid={`product-card-${product.id}`}>
                  <td><Link className="product-link" to={`/customer/products/${product.id}`}>{product.name}</Link><small>#{product.id}</small></td>
                  <td>#{product.supplier_id ?? "?"}</td>
                  <td>{Number(product.price).toFixed(2)} RUB</td>
                  <td>{product.stocks}</td>
                  <td>{product.reserved_stocks ?? "—"}</td>
                  <td><strong>{product.available_stocks ?? product.stocks}</strong></td>
                  <td>{product.is_archived ? "ARCHIVED" : product.is_active ? "ACTIVE" : "INACTIVE"}</td>
                  <td>
                    <button
                      type="button"
                      className="button compact"
                      data-testid={`add-to-cart-${product.id}`}
                      disabled={adding !== null || !product.is_active || product.is_archived}
                      onClick={() => void add(product)}
                    >
                      {adding === product.id ? "Добавляем…" : "Add to cart"}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <p className="last-updated">
        Последнее чтение: {catalog.meta ? new Date(catalog.meta.receivedAt).toLocaleString("ru-RU") : "—"}
      </p>
    </section>
  );
}
