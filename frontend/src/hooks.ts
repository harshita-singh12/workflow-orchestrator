import { useEffect, useRef, useState, useCallback } from "react";

/** Polls `fn` every `intervalMs` while `active` is true, exposing loading/error/data state
 * and a manual `refresh` trigger. Used throughout the dashboard for the "live tail" feel
 * (run status, step list, history) without any bespoke websocket/streaming layer — the
 * server's state is cheap to poll at this scale and it keeps the frontend dependency-free. */
export function usePolling<T>(fn: () => Promise<T>, intervalMs: number, deps: unknown[], active = true) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const fnRef = useRef(fn);
  fnRef.current = fn;

  const refresh = useCallback(async () => {
    try {
      const result = await fnRef.current();
      setData(result);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    setLoading(true);
    refresh();
    if (!active) return;
    const id = setInterval(refresh, intervalMs);
    return () => clearInterval(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [refresh, intervalMs, active, ...deps]);

  return { data, error, loading, refresh };
}
