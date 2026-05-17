import { describe, test, expect, afterEach } from "bun:test";
import { Pool, PoolError } from "../src/index.ts";

describe("Pool", () => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let pool: Pool<any>;

  afterEach(async () => {
    if (pool) await pool.drain();
  });

  test("acquire creates new connection", async () => {
    pool = new Pool({
      min: 0, max: 3,
      factory: { create: () => "conn", destroy: () => {} },
    });
    const conn = await pool.acquire();
    expect(conn).toBe("conn");
  });

  test("release returns to pool", async () => {
    pool = new Pool({
      min: 0, max: 3,
      factory: { create: () => "conn", destroy: () => {} },
    });
    const conn = await pool.acquire();
    pool.release(conn);
    const c2 = await pool.acquire();
    expect(c2).toBe("conn");
  });

  test("withPool auto releases", async () => {
    pool = new Pool({
      min: 0, max: 3,
      factory: { create: () => "conn", destroy: () => {} },
    });
    await pool.withPool(async (c) => `got:${c}`);
    expect(pool.available).toBe(1);
  });

  test("limits concurrent connections", async () => {
    pool = new Pool({
      min: 0, max: 2,
      factory: { create: () => "conn", destroy: () => {} },
    });
    await pool.acquire();
    await pool.acquire();
    expect(pool.size).toBe(2);
    let flag = false;
    pool.acquire().then(() => { flag = true; }).catch(() => {});
    expect(flag).toBe(false);
  });

  test("pending count with waiters", async () => {
    pool = new Pool({
      min: 0, max: 1,
      factory: { create: () => "conn", destroy: () => {} },
    });
    await pool.acquire();
    pool.acquire().catch(() => {});
    expect(pool.pending).toBe(1);
  });

  test("drain destroys all connections", async () => {
    let destroyed = 0;
    pool = new Pool({
      min: 2, max: 5,
      factory: { create: () => "conn", destroy: () => { destroyed++; } },
    });
    await Bun.sleep(10);
    await pool.drain();
    expect(destroyed).toBe(2);
  });

  test("acquire timeout rejects", async () => {
    pool = new Pool({
      min: 0, max: 1, acquireTimeoutMs: 50,
      factory: { create: () => "conn", destroy: () => {} },
    });
    await pool.acquire();
    await expect(pool.acquire(10)).rejects.toThrow();
  });

  test("throws on invalid options", () => {
    expect(() => new Pool({ min: -1, max: 5, factory: { create: () => "x", destroy: () => {} } })).toThrow();
    expect(() => new Pool({ min: 10, max: 5, factory: { create: () => "x", destroy: () => {} } })).toThrow();
  });

  test("acquire creates new connection when below max", async () => {
    let counter = 0;
    pool = new Pool({
      min: 0, max: 5,
      factory: { create: () => ++counter, destroy: () => {} },
    });
    const c1 = await pool.acquire();
    expect(c1).toBe(1);
    expect(pool.size).toBe(1);
    expect(pool.created).toBe(1);
    const c2 = await pool.acquire();
    expect(c2).toBe(2);
    expect(pool.size).toBe(2);
    expect(pool.created).toBe(2);
  });

  test("acquire returns idle connection first", async () => {
    let counter = 0;
    pool = new Pool({
      min: 0, max: 5,
      factory: { create: () => ++counter, destroy: () => {} },
    });
    const c1 = await pool.acquire();
    expect(c1).toBe(1);
    pool.release(c1);
    const c2 = await pool.acquire();
    expect(c2).toBe(1);
    expect(counter).toBe(1);
  });

  test("release after drain destroys the connection", async () => {
    let destroyCalled: boolean;
    pool = new Pool({
      min: 0, max: 3,
      factory: { create: () => "conn", destroy: () => { destroyCalled = true; } },
    });
    const conn = await pool.acquire();
    await pool.drain();
    destroyCalled = false;
    pool.release(conn);
    expect(destroyCalled).toBe(true);
    expect(pool.available).toBe(0);
  });

  test("release returns to idle connection queue", async () => {
    let counter = 0;
    pool = new Pool({
      min: 0, max: 3,
      factory: { create: () => ++counter, destroy: () => {} },
    });
    const conn = await pool.acquire();
    expect(pool.available).toBe(0);
    pool.release(conn);
    expect(pool.available).toBe(1);
  });

  test("withPool releases on success", async () => {
    let counter = 0;
    pool = new Pool({
      min: 0, max: 3,
      factory: { create: () => ++counter, destroy: () => {} },
    });
    const result = await pool.withPool(async (c) => `result:${c}`);
    expect(result).toBe("result:1");
    expect(pool.available).toBe(1);
  });

  test("withPool releases on error", async () => {
    let counter = 0;
    pool = new Pool({
      min: 0, max: 3,
      factory: { create: () => ++counter, destroy: () => {} },
    });
    await expect(
      pool.withPool(async () => { throw new Error("boom"); })
    ).rejects.toThrow("boom");
    expect(pool.available).toBe(1);
  });

  test("pool drain rejects pending acquires", async () => {
    let counter = 0;
    pool = new Pool({
      min: 0, max: 1,
      factory: { create: () => ++counter, destroy: () => {} },
    });
    await pool.acquire();
    const pending = pool.acquire();
    await pool.drain();
    await expect(pending).rejects.toThrow(PoolError);
  });

  test("pool validation removes invalid connections and re-acquires", async () => {
    let counter = 0;
    let valid = true;
    pool = new Pool({
      min: 0, max: 5,
      factory: {
        create: () => ++counter,
        destroy: () => {},
        validate: () => valid,
      },
    });
    const c1 = await pool.acquire();
    expect(c1).toBe(1);
    pool.release(c1);
    valid = false;
    const c2 = await pool.acquire();
    expect(c2).toBe(2);
    expect(pool.created).toBe(2);
  });

  test("max pool size is respected", async () => {
    let counter = 0;
    pool = new Pool({
      min: 0, max: 2,
      factory: { create: () => ++counter, destroy: () => {} },
    });
    const c1 = await pool.acquire();
    await pool.acquire();
    expect(pool.size).toBe(2);
    expect(pool.available).toBe(0);
    const pending = pool.acquire();
    await Bun.sleep(10);
    expect(pool.size).toBe(2);
    expect(pool.pending).toBe(1);
    expect(counter).toBe(2);
    pool.release(c1);
    const c3 = await pending;
    expect(c3).toBe(1);
    expect(pool.pending).toBe(0);
  });

  test("throws on NaN options", () => {
    expect(() => new Pool({ min: NaN, max: 5, factory: { create: () => "x", destroy: () => {} } })).toThrow();
    expect(() => new Pool({ min: 0, max: NaN, factory: { create: () => "x", destroy: () => {} } })).toThrow();
    expect(() => new Pool({ min: 0, max: 5, acquireTimeoutMs: NaN, factory: { create: () => "x", destroy: () => {} } })).toThrow();
    expect(() => new Pool({ min: 0, max: 5, idleTimeoutMs: NaN, factory: { create: () => "x", destroy: () => {} } })).toThrow();
  });

  test("throws on Infinity options", () => {
    expect(() => new Pool({ min: Infinity, max: 5, factory: { create: () => "x", destroy: () => {} } })).toThrow();
    expect(() => new Pool({ min: 0, max: Infinity, factory: { create: () => "x", destroy: () => {} } })).toThrow();
  });

  test("acquire with timeoutMs=0 waits indefinitely", async () => {
    pool = new Pool({
      min: 0, max: 1, acquireTimeoutMs: 50,
      factory: { create: () => "conn", destroy: () => {} },
    });
    await pool.acquire();
    const result = await Promise.race([
      pool.acquire(0).then(() => "acquired"),
      Bun.sleep(100).then(() => "timeout"),
    ]);
    expect(result).toBe("timeout");
  });

  test("acquire with undefined timeoutMs uses default acquireTimeoutMs", async () => {
    pool = new Pool({
      min: 0, max: 1, acquireTimeoutMs: 50,
      factory: { create: () => "conn", destroy: () => {} },
    });
    await pool.acquire();
    await expect(pool.acquire(undefined)).rejects.toThrow(PoolError);
  });

  test("release directly passes to waiter and updates lastUsed", async () => {
    pool = new Pool({
      min: 0, max: 1,
      factory: { create: () => "conn", destroy: () => {} },
    });
    const c1 = await pool.acquire();
    const acquirePromise = pool.acquire();
    pool.release(c1);
    const result = await acquirePromise;
    expect(result).toBe("conn");
    expect(pool.available).toBe(0);
    pool.release(result);
    expect(pool.available).toBe(1);
  });

  test("double release throws", async () => {
    pool = new Pool({
      min: 0, max: 3,
      factory: { create: () => "conn", destroy: () => {} },
    });
    const conn = await pool.acquire();
    pool.release(conn);
    expect(() => pool.release(conn)).toThrow(PoolError);
  });

  test("acquire after drain throws", async () => {
    pool = new Pool({
      min: 0, max: 3,
      factory: { create: () => "conn", destroy: () => {} },
    });
    await pool.drain();
    await expect(pool.acquire()).rejects.toThrow(PoolError);
  });

  test("concurrent multiple waiters resolved in FIFO order", async () => {
    pool = new Pool({
      min: 0, max: 1,
      factory: { create: () => "conn", destroy: () => {} },
    });
    const c1 = await pool.acquire();
    const order: number[] = [];
    const p1 = pool.acquire().then((c) => { order.push(1); return c; });
    const p2 = pool.acquire().then((c) => { order.push(2); return c; });
    pool.release(c1);
    const r1 = await p1;
    pool.release(r1);
    const r2 = await p2;
    pool.release(r2);
    expect(order).toEqual([1, 2]);
  });

  test("release after drain for unknown connection", async () => {
    pool = new Pool({
      min: 0, max: 1,
      factory: { create: () => "conn", destroy: () => {} },
    });
    await pool.drain();
    pool.release("unknown-conn");
    expect(pool.available).toBe(0);
  });

  test("using dispose drains the pool", async () => {
    let destroyed = 0;
    {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      using _ = new Pool({
        min: 1, max: 3,
        factory: { create: () => "conn", destroy: () => { destroyed++; } },
      });
      await Bun.sleep(10);
    }
    expect(destroyed).toBe(1);
  });

  test("async using dispose drains the pool", async () => {
    let destroyed = 0;
    {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      await using _ = new Pool({
        min: 1, max: 3,
        factory: { create: () => "conn", destroy: () => { destroyed++; } },
      });
      await Bun.sleep(10);
    }
    expect(destroyed).toBe(1);
  });
});
