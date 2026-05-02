export interface TTLCacheOptions<K, V> {
  max?: number;
  ttl?: number;
  onEvict?: (key: K, value: V) => void;
  sweepIntervalMs?: number;
}

interface Entry<V> {
  value: V;
  expireAt: number | null;
}

export class TTLCache<K, V> {
  private max: number;
  private ttl: number;
  private onEvict: ((key: K, value: V) => void) | null;
  private map = new Map<K, Entry<V>>();
  private sweepTimer: ReturnType<typeof setInterval> | null = null;

  constructor(opts: TTLCacheOptions<K, V> = {}) {
    this.max = opts.max ?? Infinity;
    this.ttl = opts.ttl ?? 0;
    this.onEvict = opts.onEvict ?? null;
    const sweepInterval = opts.sweepIntervalMs ?? 0;
    if (sweepInterval > 0) {
      this.sweepTimer = setInterval(() => this.sweep(), sweepInterval);
      if (typeof this.sweepTimer.unref === "function") this.sweepTimer.unref();
    }
  }

  get size(): number {
    return this.map.size;
  }

  get(key: K): V | undefined {
    const entry = this.map.get(key);
    if (!entry) return undefined;
    if (entry.expireAt != null && entry.expireAt <= Date.now()) {
      this.map.delete(key);
      return undefined;
    }
    this.map.delete(key);
    this.map.set(key, entry);
    return entry.value;
  }

  set(key: K, value: V, ttlMs: number = this.ttl): this {
    const expireAt = ttlMs > 0 ? Date.now() + ttlMs : null;
    if (this.map.has(key)) this.map.delete(key);
    this.map.set(key, { value, expireAt });
    this.enforceMax();
    return this;
  }

  has(key: K): boolean {
    const entry = this.map.get(key);
    if (!entry) return false;
    if (entry.expireAt != null && entry.expireAt <= Date.now()) {
      this.map.delete(key);
      return false;
    }
    return true;
  }

  delete(key: K): boolean {
    return this.map.delete(key);
  }

  clear(): void {
    this.map.clear();
  }

  sweep(): void {
    const now = Date.now();
    for (const [k, entry] of this.map) {
      if (entry.expireAt != null && entry.expireAt <= now) {
        this.map.delete(k);
        if (this.onEvict) {
          try {
            this.onEvict(k, entry.value);
          } catch {
            /* ignore */
          }
        }
      }
    }
  }

  dispose(): void {
    if (this.sweepTimer) clearInterval(this.sweepTimer);
    this.sweepTimer = null;
  }

  private enforceMax(): void {
    if (!Number.isFinite(this.max)) return;
    while (this.map.size > this.max) {
      const oldestKey = this.map.keys().next().value;
      if (oldestKey === undefined) break;
      const entry = this.map.get(oldestKey);
      this.map.delete(oldestKey);
      if (this.onEvict && entry) {
        try {
          this.onEvict(oldestKey, entry.value);
        } catch {
          /* ignore */
        }
      }
    }
  }
}

export class TTLSet<T> {
  private cache: TTLCache<T, true>;

  constructor(opts: Omit<TTLCacheOptions<T, true>, "onEvict"> = {}) {
    this.cache = new TTLCache<T, true>(opts);
  }

  get size(): number {
    return this.cache.size;
  }
  add(value: T): this {
    this.cache.set(value, true);
    return this;
  }
  has(value: T): boolean {
    return this.cache.has(value);
  }
  delete(value: T): boolean {
    return this.cache.delete(value);
  }
  clear(): void {
    this.cache.clear();
  }
  dispose(): void {
    this.cache.dispose();
  }
}
