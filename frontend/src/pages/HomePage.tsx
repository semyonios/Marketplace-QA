import { useNavigate } from "react-router-dom";
import { useMarketplace } from "../context";
import type { Role } from "../types";

export function HomePage() {
  const navigate = useNavigate();
  const context = useMarketplace();

  const choose = (role: Role) => {
    context.selectRole(role);
    navigate(`/select-user?role=${role}`);
  };

  return (
    <div className="landing">
      <header className="landing-header">
        <div className="eyebrow">DISTRIBUTED E-COMMERCE QA LAB</div>
        <h1>Marketplace-QA <span>2.0</span></h1>
        <p>
          Демонстрационный frontend для проверки REST, PostgreSQL, Kafka,
          eventual consistency, идемпотентности и жизненного цикла заказов.
        </p>
      </header>
      <section className="role-grid">
        <button type="button" className="role-card customer" data-testid="home-role-customer" onClick={() => choose("CUSTOMER")}>
          <span className="role-index">01</span>
          <strong>Customer</strong>
          <p>Каталог, single-supplier корзина, checkout и отмена заказа.</p>
          <span className="role-action">Выбрать покупателя →</span>
        </button>
        <button type="button" className="role-card supplier" data-testid="home-role-supplier" onClick={() => choose("SUPPLIER")}>
          <span className="role-index">02</span>
          <strong>Supplier</strong>
          <p>Товары, склады, остатки, подтверждение и отклонение заказов.</p>
          <span className="role-action">Выбрать поставщика →</span>
        </button>
      </section>
      <section className="context-strip">
        <div><span>role</span><b>{context.role ?? "not selected"}</b></div>
        <div><span>user id</span><b>{context.customer?.id ?? "—"}</b></div>
        <div><span>customer id</span><b>{context.customer?.id ?? "—"}</b></div>
        <div><span>supplier id</span><b>{context.supplier?.id ?? "—"}</b></div>
      </section>
    </div>
  );
}
