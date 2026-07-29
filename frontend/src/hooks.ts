import { type DependencyList, useCallback, useEffect, useState } from "react";
import type { ApiMeta, ApiResult } from "./types";

export function useApiResource<T>(
  loader: () => Promise<ApiResult<T>>,
  dependencies: DependencyList,
) {
  const [data, setData] = useState<T | null>(null);
  const [meta, setMeta] = useState<ApiMeta | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await loader();
      setData(result.data);
      setMeta(result.meta);
      return result;
    } catch (caught) {
      setError(caught);
      throw caught;
    } finally {
      setLoading(false);
    }
    // The caller supplies the identity dependencies for its loader.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, dependencies);

  useEffect(() => {
    void refresh().catch(() => undefined);
  }, [refresh]);

  return { data, setData, meta, setMeta, error, setError, loading, refresh };
}
