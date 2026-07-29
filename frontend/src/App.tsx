import { Navigate, Route, Routes } from "react-router-dom";
import { AppShell } from "./components/AppShell";
import { RoleGuard } from "./components/RoleGuard";
import { CartPage } from "./pages/CartPage";
import { CheckoutPage } from "./pages/CheckoutPage";
import { CustomerCatalogPage } from "./pages/CustomerCatalogPage";
import { CustomerProductPage } from "./pages/CustomerProductPage";
import { FavoritesPage } from "./pages/FavoritesPage";
import { HomePage } from "./pages/HomePage";
import { NotFoundPage } from "./pages/NotFoundPage";
import { OrderDetailPage } from "./pages/OrderDetailPage";
import { OrdersPage } from "./pages/OrdersPage";
import { ProductFormPage } from "./pages/ProductFormPage";
import { SelectSubjectPage } from "./pages/SelectSubjectPage";
import { SupplierProductsPage } from "./pages/SupplierProductsPage";
import { WarehousesPage } from "./pages/WarehousesPage";
import { WarehouseStocksPage } from "./pages/WarehouseStocksPage";

export function App() {
  return (
    <Routes>
      <Route path="/" element={<HomePage />} />
      <Route path="/select-user" element={<SelectSubjectPage />} />

      <Route element={<RoleGuard role="CUSTOMER" />}>
        <Route element={<AppShell />}>
          <Route path="/customer" element={<Navigate to="/customer/catalog" replace />} />
          <Route path="/customer/catalog" element={<CustomerCatalogPage />} />
          <Route path="/customer/products/:productId" element={<CustomerProductPage />} />
          <Route path="/customer/favorites" element={<FavoritesPage />} />
          <Route path="/customer/cart" element={<CartPage />} />
          <Route path="/customer/checkout" element={<CheckoutPage />} />
          <Route path="/customer/orders" element={<OrdersPage role="CUSTOMER" />} />
          <Route path="/customer/orders/:orderId" element={<OrderDetailPage role="CUSTOMER" />} />
        </Route>
      </Route>

      <Route element={<RoleGuard role="SUPPLIER" />}>
        <Route element={<AppShell />}>
          <Route path="/supplier" element={<Navigate to="/supplier/products" replace />} />
          <Route path="/supplier/products" element={<SupplierProductsPage />} />
          <Route path="/supplier/products/new" element={<ProductFormPage />} />
          <Route path="/supplier/products/:productId/edit" element={<ProductFormPage />} />
          <Route path="/supplier/warehouses" element={<WarehousesPage />} />
          <Route path="/supplier/warehouses/:warehouseId/stocks" element={<WarehouseStocksPage />} />
          <Route path="/supplier/orders" element={<OrdersPage role="SUPPLIER" />} />
          <Route path="/supplier/orders/:orderId" element={<OrderDetailPage role="SUPPLIER" />} />
        </Route>
      </Route>

      <Route path="/not-found" element={<NotFoundPage />} />
      <Route path="*" element={<Navigate to="/not-found" replace />} />
    </Routes>
  );
}
