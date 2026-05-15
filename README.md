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

## API

`new Pool<T>(options)`

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `min` | `number` | `0` | Minimum idle connections |
| `max` | `number` | `10` | Maximum total connections |
| `acquireTimeoutMs` | `number` | `5000` | Timeout when all connections busy |
| `idleTimeoutMs` | `number` | `30000` | Close idle connections after |
| `factory.create` | `() => T` | required | Create new connection |
| `factory.destroy` | `(T) => void` | required | Destroy connection |
| `factory.validate` | `(T) => boolean` | optional | Health check on acquire |

## Limitations
- In-memory only (per-process)
- No priority queue (FIFO waiters)
- Basic health check (no retry)

## License

MIT
