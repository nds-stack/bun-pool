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
  #waiters: Array<{ resolve: (conn: T) => void; timer?: Timer }> = [];
  #opts: Required<PoolOptions<T>>;
  #created = 0;
  #idleTimer: Timer | null = null;

  constructor(options: PoolOptions<T>) {
    this.#opts = {
      min: options.min ?? 0,
      max: options.max ?? 10,
      acquireTimeoutMs: options.acquireTimeoutMs ?? 5000,
      idleTimeoutMs: options.idleTimeoutMs ?? 30000,
      factory: options.factory,
    };

    if (this.#opts.min < 0) throw new PoolError("min must be >= 0");
    if (this.#opts.max < 1) throw new PoolError("max must be >= 1");
    if (this.#opts.min > this.#opts.max) throw new PoolError("min cannot exceed max");

    this.#initMin().catch(() => {});
    this.#startIdleCheck();
  }

  get size(): number { return this.#items.length; }
  get available(): number { return this.#items.filter((e) => e.idle).length; }
  get pending(): number { return this.#waiters.length; }
  get created(): number { return this.#created; }

  async acquire(timeoutMs?: number): Promise<T> {
    const idle = this.#items.find((e) => e.idle);
    if (idle) {
      idle.idle = false;
      idle.lastUsed = performance.now();
      if (this.#opts.factory.validate) {
        const ok = await Promise.resolve(this.#opts.factory.validate(idle.conn));
        if (!ok) {
          this.#items.splice(this.#items.indexOf(idle), 1);
          return this.acquire(timeoutMs);
        }
      }
      return idle.conn;
    }

    if (this.#items.length < this.#opts.max) {
      const conn = await Promise.resolve(this.#opts.factory.create());
      this.#created++;
      const entry: PoolEntry<T> = { conn, lastUsed: performance.now(), idle: false };
      this.#items.push(entry);
      return conn;
    }

    return new Promise<T>((resolve, reject) => {
      const timer = timeoutMs !== undefined && timeoutMs > 0
        ? setTimeout(() => {
            const idx = this.#waiters.findIndex((w) => w.resolve === resolve);
            if (idx >= 0) this.#waiters.splice(idx, 1);
            reject(new PoolError("Pool acquire timed out"));
          }, timeoutMs)
        : undefined;

      this.#waiters.push({ resolve, timer });
    });
  }

  release(conn: T): void {
    if (this.#waiters.length > 0) {
      const waiter = this.#waiters.shift()!;
      if (waiter.timer) clearTimeout(waiter.timer);
      waiter.resolve(conn);
      return;
    }

    const entry = this.#items.find((e) => e.conn === conn);
    if (entry) {
      entry.idle = true;
      entry.lastUsed = performance.now();
    }
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
    if (this.#idleTimer) clearInterval(this.#idleTimer);
    const toDestroy = this.#items.splice(0);
    await Promise.all(toDestroy.map((e) => Promise.resolve(this.#opts.factory.destroy(e.conn))));
  }

  async #initMin(): Promise<void> {
    const toCreate = Math.max(0, this.#opts.min - this.#items.length);
    for (let i = 0; i < toCreate; i++) {
      const conn = await Promise.resolve(this.#opts.factory.create());
      this.#created++;
      this.#items.push({ conn, lastUsed: performance.now(), idle: true });
    }
  }

  #startIdleCheck(): void {
    this.#idleTimer = setInterval(() => {
      const now = performance.now();
      const toClose: PoolEntry<T>[] = [];

      for (const entry of this.#items) {
        if (!entry.idle) continue;
        if (now - entry.lastUsed > this.#opts.idleTimeoutMs) {
          if (this.#items.length - toClose.length > this.#opts.min) {
            toClose.push(entry);
          }
        }
      }

      for (const entry of toClose) {
        const idx = this.#items.indexOf(entry);
        if (idx >= 0) this.#items.splice(idx, 1);
        Promise.resolve(this.#opts.factory.destroy(entry.conn)).catch(() => {});
      }
    }, 30_000);
  }
}
