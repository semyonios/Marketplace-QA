import { type FormEvent, useEffect, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { api } from "../api";
import { ErrorState, LoadingState, SuccessNotice } from "../components/AsyncState";
import { useMarketplace } from "../context";

export function ProductFormPage() {
  const { supplier } = useMarketplace();
  const params = useParams();
  const productId = params.productId ? Number(params.productId) : null;
  const editing = productId !== null;
  const navigate = useNavigate();
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [price, setPrice] = useState("");
  const [active, setActive] = useState(true);
  const [loading, setLoading] = useState(editing);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [notice, setNotice] = useState<string | null>(null);

  useEffect(() => {
    if (!editing || !productId) return;
    setLoading(true);
    api.supplierProduct(productId)
      .then(({ data }) => {
        if (data.supplier_id !== supplier!.id) throw new Error("Товар принадлежит другому поставщику");
        setName(data.name); setDescription(data.description ?? ""); setPrice(String(data.price)); setActive(data.is_active);
      })
      .catch(setError)
      .finally(() => setLoading(false));
  }, [editing, productId, supplier]);

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    const normalizedName = name.trim();
    const numericPrice = Number(price);
    if (!normalizedName || !Number.isFinite(numericPrice) || numericPrice <= 0) {
      setError(new Error("Название обязательно, цена должна быть больше нуля"));
      return;
    }
    setBusy(true); setError(null); setNotice(null);
    try {
      if (editing && productId) {
        await api.updateProduct(productId, { name: normalizedName, description: description.trim() || null, price: numericPrice, is_active: active });
        setNotice("Source product обновлён. Customer projection может измениться с задержкой.");
      } else {
        await api.createProduct({ supplier_id: supplier!.id, name: normalizedName, description: description.trim() || null, price: numericPrice, is_active: active, is_archived: false });
        setNotice("Source product создан. Customer projection появится после Kafka-синхронизации.");
      }
      window.setTimeout(() => navigate("/supplier/products"), 700);
    } catch (caught) { setError(caught); } finally { setBusy(false); }
  };

  if (editing && (!Number.isInteger(productId) || productId! <= 0)) return <ErrorState screen="product-form" error={new Error("Некорректный product ID")} />;
  return (
    <section className="form-page">
      <div className="breadcrumbs"><Link to="/supplier/products">Товары</Link><span>/</span><span>{editing ? `Edit #${productId}` : "New"}</span></div>
      <div className="page-heading"><div><div className="eyebrow">SUPPLIER SOURCE MUTATION</div><h1>{editing ? "Редактировать товар" : "Создать товар"}</h1></div></div>
      {loading && <LoadingState screen="product-form" />}
      {!loading && <form className="panel form-stack" onSubmit={(event) => void submit(event)}>
        <label><span>Название *</span><input data-testid="product-name" required maxLength={255} value={name} onChange={(event) => setName(event.target.value)} /></label>
        <label><span>Описание</span><textarea maxLength={1000} value={description} onChange={(event) => setDescription(event.target.value)} /></label>
        <label><span>Цена, RUB *</span><input data-testid="product-price" type="number" min="0.01" step="0.01" required value={price} onChange={(event) => setPrice(event.target.value)} /></label>
        <label className="checkbox-label"><input type="checkbox" checked={active} onChange={(event) => setActive(event.target.checked)} />Товар активен</label>
        <div className="button-row"><button className="button primary" data-testid="product-submit" disabled={busy}>{busy ? "Сохраняем…" : "Сохранить"}</button><Link className="button secondary" to="/supplier/products">Отмена</Link></div>
      </form>}
      {notice && <SuccessNotice>{notice}</SuccessNotice>}
      {error !== null && <ErrorState screen="product-form" error={error} />}
    </section>
  );
}
