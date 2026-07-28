import { type FormEvent, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api";
import { EmptyState, ErrorState, LoadingState, SuccessNotice } from "../components/AsyncState";
import { useApiResource } from "../hooks";

export function WarehousesPage() {
  const warehouses = useApiResource(() => api.warehouses(), []);
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [hours, setHours] = useState("");
  const [address, setAddress] = useState("");
  const [error, setError] = useState<unknown>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const submit = async (event: FormEvent) => {
    event.preventDefault(); setBusy(true); setError(null);
    try {
      await api.createWarehouse({ name: name.trim(), weekday_hours: hours.trim(), address: address.trim() });
      setNotice("Склад создан"); setOpen(false); setName(""); setHours(""); setAddress(""); await warehouses.refresh();
    } catch (caught) { setError(caught); } finally { setBusy(false); }
  };
  return (
    <section>
      <div className="page-heading"><div><div className="eyebrow">SHARED MVP WAREHOUSES</div><h1>Склады</h1><p>AS IS Warehouse не содержит supplier_id; список общий для стенда.</p></div><div className="button-row"><button className="button secondary" onClick={() => void warehouses.refresh()}>Refresh</button><button className="button primary" data-testid="warehouse-create" onClick={() => setOpen((value) => !value)}>Создать склад</button></div></div>
      {open && <form className="panel inline-form" onSubmit={(event) => void submit(event)}><label><span>Название</span><input required minLength={2} maxLength={255} value={name} onChange={(event) => setName(event.target.value)} /></label><label><span>Часы работы</span><input required minLength={2} maxLength={255} value={hours} onChange={(event) => setHours(event.target.value)} /></label><label><span>Адрес</span><input required minLength={5} maxLength={500} value={address} onChange={(event) => setAddress(event.target.value)} /></label><button className="button primary" disabled={busy}>Сохранить</button></form>}
      {warehouses.loading && <LoadingState screen="warehouses" />}
      {warehouses.error !== null && <ErrorState screen="warehouses" error={warehouses.error} onRetry={() => void warehouses.refresh()} />}
      {warehouses.data?.items.length === 0 && <EmptyState screen="warehouses">Склады отсутствуют.</EmptyState>}
      {notice && <SuccessNotice>{notice}</SuccessNotice>}{error !== null && <ErrorState screen="warehouse-mutation" error={error} />}
      <div className="card-grid">{warehouses.data?.items.map((warehouse) => <article className="entity-card" key={warehouse.id} data-testid={`warehouse-row-${warehouse.id}`}><span className="entity-id">WAREHOUSE #{warehouse.id}</span><h2>{warehouse.name}</h2><p>{warehouse.address}</p><p className="muted">{warehouse.weekday_hours}</p><Link className="button compact" to={`/supplier/warehouses/${warehouse.id}/stocks`}>Управлять остатками →</Link></article>)}</div>
    </section>
  );
}
