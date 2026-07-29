import type { ReactNode } from "react";

export function LoadingState({ screen, label = "Загрузка…" }: { screen: string; label?: string }) {
  return (
    <div className="state-panel loading" data-testid={`${screen}-loading`} role="status">
      <span className="spinner" aria-hidden="true" />
      {label}
    </div>
  );
}

export function EmptyState({
  screen,
  children,
}: {
  screen: string;
  children: ReactNode;
}) {
  return (
    <div className="state-panel empty" data-testid={`${screen}-empty`}>
      {children}
    </div>
  );
}

export function ErrorState({
  screen,
  error,
  onRetry,
}: {
  screen: string;
  error: unknown;
  onRetry?: () => void;
}) {
  const message = error instanceof Error ? error.message : "Неизвестная ошибка";
  return (
    <div className="state-panel error" data-testid={`${screen}-error`} role="alert">
      <strong>Запрос завершился ошибкой</strong>
      <span>{message}</span>
      {onRetry && (
        <button type="button" className="button secondary" onClick={onRetry}>
          Повторить
        </button>
      )}
    </div>
  );
}

export function SuccessNotice({ children }: { children: ReactNode }) {
  return (
    <div className="notice success" role="status" data-testid="operation-success">
      {children}
    </div>
  );
}
