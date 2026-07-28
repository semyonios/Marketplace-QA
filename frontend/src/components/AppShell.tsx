import { NavLink, Outlet, useNavigate } from "react-router-dom";
import { useMarketplace } from "../context";
import type { Role } from "../types";
import { QaPanel } from "./QaPanel";

const nav: Record<Role, { to: string; label: string }[]> = {
  CUSTOMER: [
    { to: "/customer/catalog", label: "Каталог" },
    { to: "/customer/favorites", label: "Избранное" },
    { to: "/customer/cart", label: "Корзина" },
    { to: "/customer/orders", label: "Заказы" },
  ],
  SUPPLIER: [
    { to: "/supplier/products", label: "Товары" },
    { to: "/supplier/warehouses", label: "Склады" },
    { to: "/supplier/orders", label: "Заказы" },
  ],
};

export function AppShell() {
  const context = useMarketplace();
  const navigate = useNavigate();
  const role = context.role!;
  const switchRole = () => {
    const next: Role = role === "CUSTOMER" ? "SUPPLIER" : "CUSTOMER";
    context.selectRole(next);
    navigate(`/select-user?role=${next}`);
  };

  return (
    <div className="app-shell">
      <header className="topbar">
        <button className="brand" type="button" onClick={() => navigate("/")}>
          <span>MQ</span>
          <span>Marketplace-QA <small>2.0</small></span>
        </button>
        <nav aria-label="Основная навигация">
          {nav[role].map((item) => (
            <NavLink key={item.to} to={item.to}>
              {item.label}
            </NavLink>
          ))}
        </nav>
        <div className="actor-context">
          <div data-testid="current-role">
            <span>ROLE</span>
            <strong>{role}</strong>
          </div>
          <div data-testid="current-subject">
            <span>TEST USER</span>
            <strong>#{context.subjectId} · {context.subjectName}</strong>
          </div>
          <button
            type="button"
            className="button ghost"
            data-testid="role-switcher"
            onClick={switchRole}
          >
            Switch role
          </button>
        </div>
      </header>
      <main className="page-container">
        <Outlet />
      </main>
      <QaPanel />
    </div>
  );
}
