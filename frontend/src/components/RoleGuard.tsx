import { Navigate, Outlet } from "react-router-dom";
import { useMarketplace } from "../context";
import type { Role } from "../types";

export function RoleGuard({ role }: { role: Role }) {
  const context = useMarketplace();
  const selected =
    role === "CUSTOMER" ? context.customer !== null : context.supplier !== null;
  if (context.role !== role || !selected) {
    return <Navigate to={`/select-user?role=${role}`} replace />;
  }
  return <Outlet />;
}
