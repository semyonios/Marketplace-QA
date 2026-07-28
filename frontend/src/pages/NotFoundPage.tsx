import { Link } from "react-router-dom";

export function NotFoundPage() {
  return (
    <div className="centered-page">
      <section className="selection-card">
        <div className="eyebrow">404 · FRONTEND ROUTE</div>
        <h1>Страница не найдена</h1>
        <p className="muted">Маршрут отсутствует в Marketplace-QA frontend.</p>
        <Link className="button primary wide" to="/">Вернуться на главную</Link>
      </section>
    </div>
  );
}
