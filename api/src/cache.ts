/**
 * Process-level TTL cache. Warm Function instances serve repeat reads without
 * touching Fabric SQL — which matters here because the capacity is shared with
 * the Safety Dashboard, the intranet and the nightly Procore ingest.
 *
 * `?fresh=1` on any endpoint bypasses this (see cacheKey usage in the functions).
 */
type Entry = { value: unknown; expires: number };

const store = new Map<string, Entry>();

export function cacheGet<T>(key: string): T | undefined {
  const hit = store.get(key);
  if (!hit) return undefined;
  if (Date.now() > hit.expires) {
    store.delete(key);
    return undefined;
  }
  return hit.value as T;
}

export function cacheSet(key: string, ttlSec: number, value: unknown): void {
  if (ttlSec <= 0) {
    store.delete(key);
    return;
  }
  store.set(key, { value, expires: Date.now() + ttlSec * 1000 });
}

/** Drop every entry whose key starts with `prefix`. Called after admin writes so
 *  an override takes effect on the next read rather than after the TTL. */
export function cacheBust(prefix: string): void {
  for (const key of store.keys()) {
    if (key.startsWith(prefix)) store.delete(key);
  }
}
