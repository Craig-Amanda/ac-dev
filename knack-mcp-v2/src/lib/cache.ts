import { type CacheEntry, type CacheSource } from '../types.js';
import { CACHE_TTL_MS } from '../config.js';

export function makeCacheEntry<T>(
    value: T,
    source: CacheSource,
): CacheEntry<T> {
    const loadedAt = Date.now();
    return {
        value,
        source,
        loadedAt,
        expiresAt: loadedAt + CACHE_TTL_MS,
    };
}

export function getCacheEntry<T>(
    cache: Map<string, CacheEntry<T>>,
    key: string,
): CacheEntry<T> | null {
    const entry = cache.get(key);
    if (!entry) return null;
    if (Date.now() >= entry.expiresAt) {
        cache.delete(key);
        return null;
    }
    return entry;
}
