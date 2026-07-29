import {
  createContext,
  type PropsWithChildren,
  useContext,
  useMemo,
  useState,
} from "react";
import type { Customer, Role, Supplier } from "./types";

interface StoredContext {
  role: Role | null;
  customer: Customer | null;
  supplier: Supplier | null;
}

interface MarketplaceContextValue extends StoredContext {
  selectRole: (role: Role) => void;
  selectCustomer: (customer: Customer) => void;
  selectSupplier: (supplier: Supplier) => void;
  clearSubject: () => void;
  subjectId: number | null;
  subjectName: string | null;
}

const STORAGE_KEY = "marketplace-qa-context";

const initialContext = (): StoredContext => {
  try {
    const parsed = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "{}") as Partial<StoredContext>;
    return {
      role: parsed.role === "CUSTOMER" || parsed.role === "SUPPLIER" ? parsed.role : null,
      customer: parsed.customer ?? null,
      supplier: parsed.supplier ?? null,
    };
  } catch {
    return { role: null, customer: null, supplier: null };
  }
};

const MarketplaceContext = createContext<MarketplaceContextValue | null>(null);

export function MarketplaceProvider({ children }: PropsWithChildren) {
  const [state, setState] = useState<StoredContext>(initialContext);

  const persist = (next: StoredContext) => {
    setState(next);
    localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
  };

  const value = useMemo<MarketplaceContextValue>(
    () => ({
      ...state,
      selectRole: (role) => persist({ ...state, role }),
      selectCustomer: (customer) => persist({ ...state, role: "CUSTOMER", customer }),
      selectSupplier: (supplier) => persist({ ...state, role: "SUPPLIER", supplier }),
      clearSubject: () =>
        persist({
          ...state,
          customer: state.role === "CUSTOMER" ? null : state.customer,
          supplier: state.role === "SUPPLIER" ? null : state.supplier,
        }),
      subjectId:
        state.role === "CUSTOMER"
          ? state.customer?.id ?? null
          : state.supplier?.id ?? null,
      subjectName:
        state.role === "CUSTOMER"
          ? state.customer?.full_name ?? null
          : state.supplier?.full_name ?? null,
    }),
    [state],
  );

  return (
    <MarketplaceContext.Provider value={value}>
      {children}
    </MarketplaceContext.Provider>
  );
}

export function useMarketplace() {
  const context = useContext(MarketplaceContext);
  if (!context) throw new Error("useMarketplace must be used inside MarketplaceProvider");
  return context;
}
