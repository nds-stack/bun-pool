export class PoolError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "PoolError";
  }
}

interface PoolOptions<T> {
  min?: number;
  max?: number;
  acquireTimeoutMs?: number;
  idleTimeoutMs?: number;
  factory: {
    create(): T | Promise<T>;
    destroy(conn: T): void | Promise<void>;
    validate?(conn: T): boolean | Promise<boolean>;
  };
}

interface PoolEntry<T> {
  conn: T;
  lastUsed: number;
  idle: boolean;
}

export class Pool<T> {
  #items: PoolEntry<T>[] = [];
  #idleSet: Set<PoolEntry<T>> = new Set();
  #waiters: Array<{ resolve: (conn: T) => void; reject: (err: unknown) => void; timer?: Timer }> = [];
  #opts: Required<PoolOptions<T>>;
  #created = 0;
  #idleTimer: Timer | null = null;
  #drained = false;

  constructor(options: PoolOptions<T>) {
    this.#opts = {
      min: options.min ?? 0,
      max: options.max ?? 10,
      acquireTimeoutMs: options.acquireTimeoutMs ?? 5000,
      idleTimeoutMs: options.idleTimeoutMs ?? 30000,
      factory: options.factory,
    };

    if (!Number.isFinite(this.#opts.min)) throw new PoolError("min must be a finite number");
    if (!Number.isFinite(this.#opts.max)) throw new PoolError("max must be a finite number");
    if (!Number.isFinite(this.#opts.acquireTimeoutMs)) throw new PoolError("acquireTimeoutMs must be a finite number");
    if (!Number.isFinite(this.#opts.idleTimeoutMs)) throw new PoolError("idleTimeoutMs must be a finite number");

    if (this.#opts.min < 0) throw new PoolError("min must be >= 0");
    if (this.#opts.max < 1) throw new PoolError("max must be >= 1");
    if (this.#opts.min > this.#opts.max) throw new PoolError("min cannot exceed max");

    (async () => { try { await this.#initMin(); } catch { /* ignored */ } })();
    this.#startIdleCheck();
  }

  get size(): number { return this.#items.length; }
  get available(): number { return this.#idleSet.size; }
  get pending(): number { return this.#waiters.length; }
  get created(): number { return this.#created; }

  async acquire(timeoutMs?: number): Promise<T> {
    return this.#acquireInternal(timeoutMs, 0);
  }

  async #acquireInternal(timeoutMs?: number, retries = 0): Promise<T> {
    if (retries > 3) throw new PoolError("Pool acquire failed after 3 retries — all idle connections failed validation");
    if (this.#drained) throw new PoolError("Pool has been drained");

    const idleEntry = this.#idleSet.values().next().value;
    if (idleEntry) {
      this.#idleSet.delete(idleEntry);
      idleEntry.idle = false;
      idleEntry.lastUsed = performance.now();
      if (this.#opts.factory.validate) {
        const ok = await Promise.resolve(this.#opts.factory.validate(idleEntry.conn));
        if (!ok) {
          const idx = this.#items.indexOf(idleEntry);
          if (idx >= 0) this.#items.splice(idx, 1);
          return this.#acquireInternal(timeoutMs, retries + 1);
        }
      }
      return idleEntry.conn;
    }

    if (this.#items.length < this.#opts.max) {
      const conn = await Promise.resolve(this.#opts.factory.create());
      this.#created++;
      const entry: PoolEntry<T> = { conn, lastUsed: performance.now(), idle: false };
      this.#items.push(entry);
      return conn;
    }

    return new Promise<T>((resolve, reject) => {
      const effectiveTimeout = timeoutMs ?? this.#opts.acquireTimeoutMs;
      const timer = effectiveTimeout > 0
        ? setTimeout(() => {
            const idx = this.#waiters.findIndex((w) => w.resolve === resolve);
            if (idx >= 0) this.#waiters.splice(idx, 1);
            reject(new PoolError("Pool acquire timed out"));
          }, effectiveTimeout)
        : undefined;

      this.#waiters.push({ resolve, reject, timer });
    });
  }

  release(conn: T): void {
    if (this.#drained) {
      Promise.resolve(this.#opts.factory.destroy(conn)).catch(() => {});
      return;
    }

    const entry = this.#items.find((e) => e.conn === conn);
    if (!entry) throw new PoolError("Released unknown connection that does not belong to this pool");
    if (entry.idle) throw new PoolError("Connection already released");

    if (this.#waiters.length > 0) {
      const waiter = this.#waiters.shift()!;
      if (waiter.timer) clearTimeout(waiter.timer);
      entry.lastUsed = performance.now();
      waiter.resolve(conn);
      return;
    }

    entry.idle = true;
    this.#idleSet.add(entry);
    entry.lastUsed = performance.now();
  }

  async withPool<R>(fn: (conn: T) => Promise<R>, timeoutMs?: number): Promise<R> {
    const conn = await this.acquire(timeoutMs);
    try {
      return await fn(conn);
    } finally {
      this.release(conn);
    }
  }

  async drain(): Promise<void> {
    this.#drained = true;
    if (this.#idleTimer) {
      clearInterval(this.#idleTimer);
      this.#idleTimer = null;
    }

    for (const waiter of this.#waiters) {
      if (waiter.timer) clearTimeout(waiter.timer);
      waiter.reject(new PoolError("Pool has been drained"));
    }
    this.#waiters.length = 0;

    this.#idleSet.clear();

    const toDestroy = this.#items.splice(0);
    const errors: Error[] = [];
    for (const e of toDestroy) {
      try {
        await Promise.resolve(this.#opts.factory.destroy(e.conn));
      } catch (err) {
        errors.push(err instanceof Error ? err : new Error(String(err)));
      }
    }
    if (errors.length > 0) throw new PoolError("Drain completed with errors", { cause: errors[0] });
  }

  [Symbol.dispose](): void {
    if (this.#drained) return;
    this.drain().catch(() => {});
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.drain();
  }

  async #initMin(): Promise<void> {
    const toCreate = Math.max(0, this.#opts.min - this.#items.length);
    for (let i = 0; i < toCreate; i++) {
      try {
        const conn = await Promise.resolve(this.#opts.factory.create());
        this.#created++;
        const entry: PoolEntry<T> = { conn, lastUsed: performance.now(), idle: true };
        this.#items.push(entry);
        this.#idleSet.add(entry);
      } catch {
        process.stderr.write("[Pool] Failed to create initial connection, will create on demand\n");
      }
    }
  }

  #startIdleCheck(): void {
    const interval = Math.min(30_000, this.#opts.idleTimeoutMs / 2);
    this.#idleTimer = setInterval(() => {
      const now = performance.now();
      let removedCount = 0;
      const minCount = this.#opts.min;

      this.#items = this.#items.filter((entry) => {
        if (!entry.idle) return true;
        if (now - entry.lastUsed > this.#opts.idleTimeoutMs) {
          if (this.#items.length - removedCount > minCount) {
            removedCount++;
            this.#idleSet.delete(entry);
            (async () => { try { await Promise.resolve(this.#opts.factory.destroy(entry.conn)); } catch { void 0; } })();
            return false;
          }
        }
        return true;
      });
    }, interval);
  }
}
