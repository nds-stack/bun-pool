# @nds-stack/bun-pool

> Generic connection pool for Bun — min/max, idle timeout, health check, zero deps.

```typescript
import { Pool } from "@nds-stack/bun-pool";

const pool = new Pool({
  min: 2, max: 10,
  factory: {
    create: () => new Database("app.db"),
    destroy: (db) => db.close(),
    validate: (db) => db.raw?.open ?? false,
  },
});

await pool.withPool(async (db) => {
  const result = db.query("...");
});
```

## How It Works

Internally, `Pool<T>` maintains two data structures:

- **Idle connection pool** (`#items`): an array of `PoolEntry<T>` objects tracking each connection, its `lastUsed` timestamp, and `idle` status.
- **Waiter queue** (`#waiters`): a FIFO array of pending `{ resolve, reject }` callbacks for callers waiting when all connections are busy.

### Acquire flow
1. Check for an idle connection in `#items`. If found, mark it busy and optionally run `factory.validate()` — if validation fails, the connection is destroyed and acquire retries.
2. If no idle connection and `#items.length < max`, call `factory.create()`, increment `#created`, push the new entry as busy, and return it.
3. If at `max`, push a `{ resolve, reject }` pair to the waiter queue with an optional timeout via `setTimeout`. On timeout, the waiter is removed from the queue and `PoolError` is thrown.

### Release flow
1. If the pool is drained, destroy the connection immediately.
2. If there are waiters in the queue, shift the first one and pass the connection directly (no round-trip through idle pool).
3. Otherwise, mark the entry idle and update `lastUsed`.

### Idle timeout cleanup
A `setInterval` runs every 30 seconds. It iterates `#items`, collects idle entries whose `lastUsed` is older than `idleTimeoutMs`, and destroys them — but never below `min` connections.

### Min sizing
On construction, `#initMin()` eagerly creates `min` connections via `factory.create()`. Failures at this stage are silently swallowed — connections are created on demand later.

## API

`new Pool<T>(options)`

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `min` | `number` | `0` | Minimum idle connections |
| `max` | `number` | `10` | Maximum total connections |
| `acquireTimeoutMs` | `number` | `5000` | Timeout when all connections busy |
| `idleTimeoutMs` | `number` | `30000` | Close idle connections after |
| `factory.create` | `() => T` | required | Create new connection |
| `factory.destroy` | `T => void` | required | Destroy connection |
| `factory.validate` | `(T) => boolean` | optional | Health check on acquire |

### Getters

| Getter | Returns | Description |
|--------|---------|-------------|
| `.size` | `number` | Total connections in pool (idle + busy) |
| `.available` | `number` | Idle connections ready for reuse |
| `.pending` | `number` | Waiters waiting for a connection |
| `.created` | `number` | Total connections created (lifetime counter) |

### Methods

```typescript
const conn = await pool.acquire(timeoutMs?);
pool.release(conn);

const result = await pool.withPool(async (conn) => {
  return doWork(conn);
}, timeoutMs?);

await pool.drain();
```

## Error Handling

All pool errors are instances of `PoolError` (`this.name === "PoolError"`).

| Scenario | Error message | Trigger |
|----------|---------------|---------|
| `min < 0` | `"min must be >= 0"` | Constructor |
| `max < 1` | `"max must be >= 1"` | Constructor |
| `min > max` | `"min cannot exceed max"` | Constructor |
| Acquire timeout | `"Pool acquire timed out"` | `acquire()` via `acquireTimeoutMs` or per-call `timeoutMs` |
| Drain while waiters pending | `"Pool has been drained"` | Rejected waiters on `drain()` |
| Drain destroy failures | `"Drain completed with errors"` | `drain()` when one or more `factory.destroy()` calls throw (first cause attached via `options.cause`) |

**Factory.create failures** — propagated directly to the caller of `acquire()`. No retry logic.

**Factory.destroy failures** — silently caught and discarded during idle timeout cleanup. During `drain()`, failures are collected and re-thrown as a single `PoolError` with cause.

**Factory.validate failures/throws** — the connection is removed from the pool and the acquire process retries automatically. If no other connection can be acquired, the error from `factory.create()` (if called) is propagated.

## Limitations

- **Single-process only.** The pool is an in-memory object — not shareable across processes or machines.
- **No priority queue.** Waiters are served FIFO. A high-priority request cannot skip the queue.
- **No built-in metrics or monitoring.** No events, hooks, or prometheus-style counters. Track `.size`, `.pending`, `.created` manually if needed.
- **No proactive health probing.** Connections are validated only on `acquire()` via `factory.validate()`. No background pinging or keep-alive.
- **Synchronous destroy assumed fast.** `factory.destroy` can return a promise, but idle timeout cleanup fires and forgets — rejected destroy promises are silently caught.

## Multi-Instance / Cross-Boundary

`Pool<T>` is **not shareable across processes or workers**. Each `Pool` instance manages its own connection array and waiter queue in memory.

For multi-worker architectures (e.g., `Bun.serve` with `Bun.Semaphore` or `cluster`):
- Each worker must create its own `Pool` instance.
- Each pool independently maintains min/max sizing — if 4 workers each have `max: 10`, you may have up to 40 total connections.
- There is no cross-worker connection borrowing. To share a single connection pool across workers, you would need an external pool manager (e.g., via TCP or a message broker) — this module is not designed for that.

## Customization Guide

### Wrapping with custom logging

```typescript
function createLoggedPool<T>(
  factory: PoolOptions<T>["factory"],
  logger: (msg: string) => void,
): Pool<T> {
  let created = 0;
  return new Pool<T>({
    min: 0, max: 10,
    factory: {
      create: async () => {
        const conn = await factory.create();
        created++;
        logger(`pool: created connection #${created}`);
        return conn;
      },
      destroy: async (conn) => {
        await factory.destroy(conn);
        created--;
        logger(`pool: destroyed connection (${created} remaining)`);
      },
      validate: factory.validate,
    },
  });
}
```

### Adding connection counting

```typescript
const pool = new Pool<number>({ min: 2, max: 10, factory: { create: () => 1, destroy: () => {} } });

setInterval(() => {
  console.log({
    size: pool.size,
    available: pool.available,
    pending: pool.pending,
    created: pool.created,
  });
}, 5_000);
```

### Circuit breaker on create failures

```typescript
let failures = 0;
let lastFailure = 0;
const FAILURE_THRESHOLD = 5;
const COOLDOWN_MS = 30_000;

const pool = new Pool<Connection>({
  min: 0, max: 10,
  factory: {
    create: async () => {
      const now = Date.now();
      if (failures >= FAILURE_THRESHOLD && now - lastFailure < COOLDOWN_MS) {
        throw new Error("circuit open: too many create failures");
      }
      try {
        const conn = await connectDb();
        failures = 0;
        return conn;
      } catch (err) {
        failures++;
        lastFailure = now;
        throw err;
      }
    },
    destroy: (conn) => conn.close(),
  },
});
```

### Composing with semaphore for limited resources

```typescript
import { Semaphore } from "@nds-stack/bun-semaphore";
import { Pool } from "@nds-stack/bun-pool";

const sem = new Semaphore(5); // max 5 concurrent operations
const pool = new Pool<Db>({ min: 2, max: 10, factory: { create, destroy } });

async function throttledQuery(sql: string) {
  await sem.acquire();
  try {
    return await pool.withPool((db) => db.query(sql));
  } finally {
    sem.release();
  }
}
```

## Comparison Table

| Feature | `@nds-stack/bun-pool` | Manual management | `generic-pool` | `tarn.js` |
|---------|----------------------|-------------------|----------------|-----------|
| **Runtime** | Bun-native | Any | Node.js | Node.js |
| **Bundle size** | ~1.5 KB (zero deps) | N/A | ~12 KB (1 dep) | ~8 KB (2 deps) |
| **Min/max sizing** | ✅ | Manual | ✅ | ✅ |
| **Idle timeout** | ✅ | Manual | ✅ | ✅ |
| **Acquire timeout** | ✅ | Manual | ✅ | ✅ |
| **Validation on acquire** | ✅ | Manual | ✅ | ✅ |
| **FIFO waiter queue** | ✅ | Manual | ✅ | ✅ |
| **Drain / destroy all** | ✅ | Manual | ✅ | ✅ |
| **Eager min init** | ✅ | Manual | ✅ | ✅ |
| **Events / hooks** | ❌ | — | ✅ | ✅ |
| **Priority queue** | ❌ | — | ❌ | ✅ |
| **Resource recycling** | ❌ | — | ✅ | ✅ |
| **TypeScript** | ✅ Strict | — | ✅ | ✅ |
| **ESM only** | ✅ | — | ⚠️ CJS/ESM | ✅ |

## Benchmarks

Benchmarks ran on Bun `1.3.14`, 500 iterations × 5 samples. All pool instances configured with `min: 0, max: 500`.

| Operation | Throughput | Overhead vs baseline |
|-----------|-----------|----------------------|
| Raw factory create/destroy | 1.5M ops/s | (baseline) |
| Pool acquire/release | 636K ops/s | −57.1% |
| Pool withPool | 467K ops/s | −68.5% |
| Pool acquire/release with validation | 708K ops/s | −52.2% |

> **Note:** Overhead is expected — the pool adds FIFO queuing, idle state tracking, `lastUsed` timestamps (via `performance.now()`), and optional validation calls. The —57% overhead translates to ~1.6 million extra operations per second that the pool handles internally. For connection-oriented workloads (network I/O, database queries), this overhead is negligible compared to the cost of creating and destroying connections on every request.

To run locally:
```bash
bun run bench
```

## License

MIT
